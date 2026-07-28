#!/usr/bin/env python3
"""Run one data-only single-VM pilot for every final root-cause class."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

import yaml

from proberca.dataplane.archive import CollectionArchive
from proberca.dataplane.burst_archive import BurstArchive
from proberca.dataplane.burst_collection import BURST_CHANNEL_MODES
from proberca.controlplane import (
    CalibrationNotReadyError,
    FinalControlConfig,
    load_ready_calibration_report,
)
from proberca.controlplane.service_model import allowed_service_graph


REPOSITORY = Path(os.environ.get(
    "PROBERCA_REPOSITORY",
    str(Path(__file__).resolve().parents[1]),
)).resolve()
ACTOR = Path(os.environ.get(
    "PROBERCA_FAULT_ACTOR",
    str(REPOSITORY / "scripts/final_fault_actor.py"),
)).resolve()
KUBECONFIG = Path(os.environ.get(
    "KUBECONFIG", "/home/jyz/.kube/config",
)).resolve()
USER_SITE_PACKAGES = Path(
    "/home/jyz/.local/lib/python3.10/site-packages"
)
KUBE_CONTEXT = "kind-proberca-ob"
NAMESPACE = "online-boutique"
NORMAL_CONFIG = REPOSITORY / "configs/final_live_collector.example.yaml"
BURST_CONFIG = REPOSITORY / "configs/final_live_burst.example.yaml"
CONTRACT = REPOSITORY / "configs/final_collection_contract.yaml"
CONTROL_CONFIG = REPOSITORY / "configs/final_control.yaml"
STATE_SERVICES = (
    "proberca-final-ebpf.service",
    "proberca-final-burst.service",
    "proberca-final-primitive-exporter.service",
    "prometheus.service",
)
WINDOW_WALL_BUDGET_SEC = 10


class ExperimentError(RuntimeError):
    pass


def run(
    arguments: list[str],
    *,
    check: bool = True,
    timeout: float | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        arguments,
        check=check,
        timeout=timeout,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            payload, ensure_ascii=False, indent=2, sort_keys=True
        ) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def log_event(root: Path, event: str, **values: Any) -> None:
    record = {
        "event": event,
        "timestamp_ns": time.time_ns(),
        **values,
    }
    with (root / "run-events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(
            record, ensure_ascii=False, sort_keys=True
        ) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(json.dumps(record, ensure_ascii=False, sort_keys=True), flush=True)


def kube_json(arguments: list[str]) -> Any:
    result = run([
        "kubectl", "--context", KUBE_CONTEXT,
        *arguments, "-o", "json",
    ], timeout=30)
    return json.loads(result.stdout)


def service_info(service: str) -> dict[str, Any]:
    payload = kube_json([
        "-n", NAMESPACE, "get", "pods", "-l", f"app={service}",
    ])
    ready = [
        item for item in payload["items"]
        if (item.get("status") or {}).get("phase") == "Running"
    ]
    if len(ready) != 1:
        raise ExperimentError(f"{service} does not resolve to one Pod")
    pod = ready[0]
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    statuses = [
        item for item in statuses
        if item.get("ready") and item.get("containerID")
    ]
    if len(statuses) != 1:
        raise ExperimentError(f"{service} has no exact ready container")
    container_id = statuses[0]["containerID"].rsplit("://", 1)[-1]
    matches = list(Path("/sys/fs/cgroup").rglob(
        f"cri-containerd-{container_id}.scope"
    ))
    if len(matches) != 1:
        raise ExperimentError(f"{service} cgroup is missing or ambiguous")
    pids = [
        int(item)
        for item in (matches[0] / "cgroup.procs").read_text().split()
    ]
    if not pids:
        raise ExperimentError(f"{service} cgroup has no process")
    return {
        "cgroup": matches[0],
        "container_id": container_id,
        "pid": pids[0],
        "pod": pod["metadata"]["name"],
        "pod_ip": pod["status"]["podIP"],
    }


def service_address(service: str) -> tuple[str, int]:
    payload = kube_json([
        "-n", NAMESPACE, "get", "service", service,
    ])
    ports = (payload.get("spec") or {}).get("ports") or []
    if len(ports) != 1:
        raise ExperimentError(f"{service} does not have one service port")
    return payload["spec"]["clusterIP"], int(ports[0]["port"])


def dns_address() -> tuple[str, list[str]]:
    service = kube_json([
        "-n", "kube-system", "get", "service", "kube-dns",
    ])
    pods = kube_json([
        "-n", "kube-system", "get", "pods", "-l", "k8s-app=kube-dns",
    ])
    addresses = [
        item["status"]["podIP"] for item in pods["items"]
        if (item.get("status") or {}).get("phase") == "Running"
    ]
    if not addresses:
        raise ExperimentError("no running DNS Pod")
    return service["spec"]["clusterIP"], sorted(addresses)


def node_pid() -> int:
    result = run([
        "docker", "inspect", "-f", "{{.State.Pid}}",
        "proberca-ob-control-plane",
    ])
    return int(result.stdout.strip())


def node_command(arguments: list[str], **kwargs) -> subprocess.CompletedProcess:
    return run([
        "nsenter", "-t", str(node_pid()), "-n", *arguments
    ], **kwargs)


def wait_data_plane(root: Path, *, restart_on_failure: bool = True) -> None:
    deadline = time.monotonic() + 90
    restarted = False
    exporter_failures = 0
    while time.monotonic() < deadline:
        active = all(
            run(
                ["systemctl", "is-active", "--quiet", service],
                check=False,
            ).returncode == 0
            for service in STATE_SERVICES
        )
        pods = run([
            "kubectl", "--context", KUBE_CONTEXT, "-n", NAMESPACE,
            "wait", "--for=condition=Ready", "pods", "--all",
            "--timeout=5s",
        ], check=False)
        exporter = run([
            "curl", "-fsS", "--max-time", "8",
            "http://127.0.0.1:9477/metrics",
        ], check=False)
        exporter_failures = (
            0 if exporter.returncode == 0 else exporter_failures + 1
        )
        prometheus = run([
            "curl", "-fsSG", "--max-time", "5",
            "--data-urlencode",
            "query=count(proberca_service_request_total)",
            "http://127.0.0.1:9090/api/v1/query",
        ], check=False)
        has_series = False
        if prometheus.returncode == 0:
            try:
                has_series = bool(
                    json.loads(prometheus.stdout)["data"]["result"]
                )
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
        if active and pods.returncode == 0 \
                and exporter.returncode == 0 and has_series:
            return
        # The exporter can miss an HTTP deadline briefly while the injected
        # host fault is active or immediately after recovery.  Restarting it
        # on the first miss resets process-local cumulative state and creates
        # artificial counter boundaries.  Restart only after a sustained
        # failure, or immediately when the unit itself is no longer active.
        exporter_unit_active = run(
            [
                "systemctl", "is-active", "--quiet",
                "proberca-final-primitive-exporter.service",
            ],
            check=False,
        ).returncode == 0
        if restart_on_failure and not restarted and (
            not exporter_unit_active or exporter_failures >= 10
        ):
            run([
                "systemctl", "restart",
                "proberca-final-primitive-exporter.service",
            ])
            restarted = True
            exporter_failures = 0
            log_event(root, "primitive_exporter_restarted")
        time.sleep(3)
    raise ExperimentError("data plane did not become ready")


def assert_current_readiness_handshake(
    readiness: dict[str, Any], root: Path,
) -> dict[str, Any]:
    payload = yaml.safe_load(CONTROL_CONFIG.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ExperimentError("final control config is not a mapping")
    config = FinalControlConfig.from_dict(payload)
    expected = {
        "control_config_fingerprint": config.config_fingerprint,
        "collection_contract_fingerprint": (
            config.collection_contract_fingerprint
        ),
        "required_scope_fingerprint": (
            config.required_scope_fingerprint
        ),
        "scale_config_fingerprint": (
            config.scale_config_fingerprint
        ),
    }
    mismatched = [
        name for name, value in expected.items()
        if readiness.get(name) != value
    ]
    if mismatched:
        raise ExperimentError(
            "fault injection refused: readiness/config fingerprint "
            "mismatch: " + ",".join(mismatched)
        )

    with tempfile.TemporaryDirectory(
        prefix=".readiness-preflight-", dir=root,
    ) as temporary:
        preflight = Path(temporary)
        normal_root = preflight / "normal"
        burst_root = preflight / "burst"
        command = [
            sys.executable, "-u", "-m", "proberca.cli.collect_final",
            "--source-config", str(NORMAL_CONFIG),
            "--collection-contract", str(CONTRACT),
            "--burst-config", str(BURST_CONFIG),
            "--output", str(normal_root),
            "--burst-output", str(burst_root),
            "--windows", "1",
        ]
        try:
            run(
                command,
                timeout=WINDOW_WALL_BUDGET_SEC + 60,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) \
                as exc:
            raise ExperimentError(
                "fault injection refused: readiness preflight "
                "collection failed"
            ) from exc
        archive = CollectionArchive.load(normal_root)
        windows = tuple(archive.iter_windows())
        if len(windows) != 1 \
                or len(windows[0].topology_events) != 1:
            raise ExperimentError(
                "fault injection refused: readiness preflight has "
                "ambiguous topology"
            )
        graph = allowed_service_graph(
            windows[0].topology_events[0]
        )
        live = {
            "collection_contract_fingerprint": (
                archive.collection_contract_fingerprint
            ),
            "topology_fingerprint": graph.topology_fingerprint,
            "runtime_identity_fingerprint": (
                graph.runtime_identity_fingerprint
            ),
        }
        live_mismatch = [
            name for name, value in live.items()
            if readiness.get(name) != value
        ]
        if live_mismatch:
            raise ExperimentError(
                "fault injection refused: readiness/live fingerprint "
                "mismatch: " + ",".join(live_mismatch)
            )
        return {
            **expected,
            **live,
            "snapshot_id": graph.snapshot_id,
            "preflight_dataset_id": archive.dataset_id,
            "preflight_manifest_fingerprint": (
                archive.manifest_fingerprint
            ),
        }


def validate_archives(
    normal_root: Path, burst_root: Path, expected_windows: int,
) -> dict[str, Any]:
    normal = CollectionArchive.load(normal_root)
    burst = BurstArchive.load(burst_root)
    normal_windows = tuple(normal.iter_windows())
    burst_windows = tuple(burst.iter_windows())
    if normal.dataset_id != burst.dataset_id:
        raise ExperimentError("normal/Burst dataset identity mismatch")
    if len(normal_windows) != expected_windows \
            or len(burst_windows) != expected_windows:
        raise ExperimentError("sealed window count mismatch")
    minimum_mapping = 1.0
    maximum_loss = 0.0
    previous_end_ns: int | None = None
    for left, right in zip(normal_windows, burst_windows):
        if (
            left.sequence != right.sequence
            or left.window_start_ns != right.window_start_ns
            or left.window_end_ns != right.window_end_ns
        ):
            raise ExperimentError("normal/Burst window alignment mismatch")
        if (
            previous_end_ns is not None
            and left.window_start_ns != previous_end_ns
        ):
            raise ExperimentError("sealed windows are not contiguous")
        previous_end_ns = left.window_end_ns
        if left.burst_evidence:
            raise ExperimentError(
                "data plane embedded normalized Burst evidence"
            )
        channels = {item.channel_id for item in right.samples}
        if channels != set(BURST_CHANNEL_MODES):
            raise ExperimentError("Burst channel coverage mismatch")
        normal_services = {
            item.service_name for item in left.node_metrics
            if item.scope == "service"
        }
        burst_services = {
            item.entity_id.rsplit("::", 1)[-1]
            for item in right.samples if item.entity_type == "service"
        }
        if normal_services != burst_services:
            raise ExperimentError("normal/Burst service identity mismatch")
        minimum_mapping = min(
            minimum_mapping,
            min(item.mapping_quality for item in right.samples),
        )
        maximum_loss = max(maximum_loss, right.event_loss_rate)
    if minimum_mapping < 1.0 or maximum_loss > 0.01:
        raise ExperimentError("Burst quality gate failed")
    return {
        "burst_manifest_fingerprint": burst.manifest_fingerprint,
        "burst_manifest_sha256": sha256_file(
            burst_root / "burst-manifest.json"
        ),
        "dataset_id": normal.dataset_id,
        "maximum_event_loss_rate": maximum_loss,
        "minimum_mapping_quality": minimum_mapping,
        "normal_manifest_fingerprint": normal.manifest_fingerprint,
        "normal_manifest_sha256": sha256_file(
            normal_root / "collection-manifest.json"
        ),
        "window_count": expected_windows,
        "window_end_ns": normal_windows[-1].window_end_ns,
        "window_start_ns": normal_windows[0].window_start_ns,
    }


def collect_phase(
    root: Path,
    experiment_root: Path,
    phase: str,
    windows: int,
) -> dict[str, Any]:
    phase_root = experiment_root / f"phase-{phase}"
    phase_root.mkdir(parents=True, exist_ok=False)
    for attempt in (1, 2):
        normal_root = phase_root / "normal"
        burst_root = phase_root / "burst"
        command = [
            sys.executable, "-u", "-m", "proberca.cli.collect_final",
            "--source-config", str(NORMAL_CONFIG),
            "--collection-contract", str(CONTRACT),
            "--burst-config", str(BURST_CONFIG),
            "--output", str(normal_root),
            "--burst-output", str(burst_root),
            "--windows", str(windows),
        ]
        log_path = phase_root / f"collection-attempt-{attempt}.log"
        log_event(
            root, "phase_collection_started",
            attempt=attempt, phase=phase,
            experiment=experiment_root.name,
        )
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.run(
                command,
                cwd=REPOSITORY,
                env={
                    **os.environ,
                    "PYTHONPATH": os.pathsep.join(
                        (str(REPOSITORY), str(USER_SITE_PACKAGES))
                    ),
                },
                text=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=(
                    windows * WINDOW_WALL_BUDGET_SEC + 180
                ),
            )
        if process.returncode == 0:
            result = validate_archives(
                normal_root, burst_root, windows
            )
            result.update({
                "phase_variable": "experiment_phase",
                "phase_value": phase,
                "normal_archive": str(normal_root),
                "burst_archive": str(burst_root),
            })
            atomic_json(phase_root / "phase-manifest.json", result)
            log_event(
                root, "phase_collection_completed",
                phase=phase, experiment=experiment_root.name,
                dataset_id=result["dataset_id"],
            )
            return result
        log_event(
            root, "phase_collection_failed",
            attempt=attempt, phase=phase,
            experiment=experiment_root.name,
            returncode=process.returncode,
        )
        for archive_root in (normal_root, burst_root):
            if archive_root.exists():
                resolved = archive_root.resolve()
                if not resolved.is_relative_to(phase_root.resolve()):
                    raise ExperimentError("unsafe retry cleanup target")
                shutil.rmtree(resolved)
        wait_data_plane(root)
    raise ExperimentError(
        f"{experiment_root.name}/{phase} collection failed twice"
    )


class FaultContext:
    def __init__(self, root: Path, experiment_root: Path):
        self.root = root
        self.experiment_root = experiment_root
        self.processes: list[subprocess.Popen] = []
        self.logs: list[Any] = []
        self.cleanups: list[Callable[[], None]] = []
        self.metadata: dict[str, Any] = {}

    def add_cleanup(self, callback: Callable[[], None]) -> None:
        self.cleanups.append(callback)

    def start_actor(
        self,
        mode: str,
        *,
        service: str | None = None,
        network_namespace: bool = False,
        duration: float,
        arguments: list[str] | None = None,
        name: str | None = None,
    ) -> subprocess.Popen:
        command = [
            sys.executable, "-u", str(ACTOR),
            "--mode", mode, "--duration", str(duration),
        ]
        actor_metadata: dict[str, Any] = {"mode": mode}
        if service:
            identity = service_info(service)
            command.extend(["--cgroup", str(identity["cgroup"])])
            actor_metadata.update({
                "service": service,
                "pod": identity["pod"],
            })
            if network_namespace:
                command = [
                    "nsenter", "-t", str(identity["pid"]), "-n",
                    *command,
                ]
        command.extend(arguments or [])
        actor_name = name or mode
        log = (self.experiment_root / f"{actor_name}.log").open(
            "w", encoding="utf-8"
        )
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, text=True
        )
        self.logs.append(log)
        self.processes.append(process)
        time.sleep(2)
        if process.poll() is not None:
            log.flush()
            raise ExperimentError(f"{actor_name} actor exited early")
        self.metadata.setdefault("actors", []).append(actor_metadata)
        return process

    def start_cpu(
        self, *, count: int, service: str | None = None,
    ) -> None:
        cgroup = service_info(service)["cgroup"] if service else None
        for _index in range(count):
            process = subprocess.Popen(
                ["/usr/bin/yes"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if cgroup:
                (cgroup / "cgroup.procs").write_text(
                    f"{process.pid}\n", encoding="ascii"
                )
            self.processes.append(process)
        self.metadata["cpu_workers"] = count
        self.metadata["service"] = service
        time.sleep(2)
        if any(process.poll() is not None for process in self.processes):
            raise ExperimentError("CPU actor exited early")

    def cleanup(self) -> None:
        errors = []
        for callback in reversed(self.cleanups):
            try:
                callback()
            except Exception as error:
                errors.append(str(error))
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + 8
        for process in self.processes:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for log in self.logs:
            log.close()
        if errors:
            raise ExperimentError("; ".join(errors))


def actor_fault(
    mode: str,
    *,
    service: str | None,
    network_namespace: bool = False,
    arguments: list[str] | None = None,
    name: str | None = None,
) -> Callable[[FaultContext, int], None]:
    def activate(context: FaultContext, windows: int) -> None:
        context.start_actor(
            mode,
            service=service,
            network_namespace=network_namespace,
            duration=(
                windows * WINDOW_WALL_BUDGET_SEC + 300
            ),
            arguments=arguments,
            name=name,
        )
    return activate


def service_memory(context: FaultContext, windows: int) -> None:
    # recommendationservice has enough headroom for a sustained, observable
    # working-set increase without crossing the Pod's hard memory limit.  Do
    # not alter memory.high here: throttling the whole application cgroup can
    # make the Kubernetes health probe kill an otherwise non-OOM workload,
    # which changes topology instead of producing a stable memory fault.
    target_service = "recommendationservice"
    byte_count = 192 * 1024 * 1024
    context.metadata["target_service"] = target_service
    context.metadata["bytes_touched"] = byte_count
    context.start_actor(
        "memory",
        service=target_service,
        duration=windows * WINDOW_WALL_BUDGET_SEC + 300,
        arguments=["--bytes", str(byte_count)],
    )


def host_nic(context: FaultContext, _windows: int) -> None:
    peer_index = int(run([
        "docker", "exec", "proberca-ob-control-plane",
        "cat", "/sys/class/net/eth0/iflink",
    ]).stdout.strip())
    devices = [
        path.name
        for path in Path("/sys/class/net").iterdir()
        if int((path / "ifindex").read_text().strip()) == peer_index
    ]
    if len(devices) != 1:
        raise ExperimentError(
            "kind node host-veth is missing or ambiguous"
        )
    for device in devices:
        run([
            "tc", "qdisc", "replace", "dev", device, "root",
            "netem", "delay", "2ms", "loss", "1%",
        ])

    def cleanup() -> None:
        for device in devices:
            run(
                ["tc", "qdisc", "del", "dev", device, "root"],
                check=False,
            )

    context.add_cleanup(cleanup)
    context.metadata["devices"] = devices
    context.metadata["netem"] = {"delay_ms": 2, "loss_percent": 1.0}
    time.sleep(2)


def add_iptables_rule(arguments: list[str]) -> None:
    node_command(["iptables", *arguments])


def tcp_edge(context: FaultContext, _windows: int) -> None:
    source = service_info("frontend")
    destination = service_info("productcatalogservice")
    _cluster_ip, port = service_address("productcatalogservice")
    rule = [
        "-I", "FORWARD", "1",
        "-s", source["pod_ip"], "-d", destination["pod_ip"],
        "-p", "tcp", "--dport", str(port),
        "-m", "statistic", "--mode", "random",
        "--probability", "0.30",
        "-m", "comment", "--comment", "proberca-final-tcp-edge",
        "-j", "REJECT", "--reject-with", "tcp-reset",
    ]
    add_iptables_rule(rule)
    delete_rule = ["-D", "FORWARD", *rule[3:]]
    context.add_cleanup(
        lambda: node_command(
            ["iptables", *delete_rule], check=False
        )
    )
    context.metadata.update({
        "source_service": "frontend",
        "destination_service": "productcatalogservice",
        "probability": 0.30,
        "protocol": "tcp",
    })
    # Beyla creates status-specific cumulative series lazily.  Keep the
    # abnormal phase boundary strict by letting the active probe materialize
    # those series before the first archived one-second window starts.
    time.sleep(15)


def dns_edge(context: FaultContext, _windows: int) -> None:
    source = service_info("frontend")
    _cluster_ip, dns_pods = dns_address()
    rules = []
    for destination in dns_pods:
        rule = [
            "-I", "FORWARD", "1",
            "-s", source["pod_ip"], "-d", destination,
            "-p", "udp", "--dport", "53",
            "-m", "statistic", "--mode", "random",
            "--probability", "0.80",
            "-m", "comment", "--comment", "proberca-final-dns-edge",
            "-j", "DROP",
        ]
        add_iptables_rule(rule)
        rules.append(["-D", "FORWARD", *rule[3:]])

    def cleanup() -> None:
        for rule in rules:
            node_command(["iptables", *rule], check=False)

    context.add_cleanup(cleanup)
    context.metadata.update({
        "source_service": "frontend",
        "destination_service": "kube-dns",
        "probability": 0.80,
        "protocol": "dns",
    })
    # CoreDNS/eBPF series may be first observed only after the fault begins.
    # Prewarm them instead of relaxing the two-boundary counter contract.
    time.sleep(15)


def start_probe(
    context: FaultContext, fault_type: str, total_duration: int,
) -> None:
    if fault_type == "tcp_edge":
        host, port = service_address("productcatalogservice")
        context.start_actor(
            "tcp",
            service="frontend",
            network_namespace=True,
            duration=total_duration,
            arguments=[
                "--host", host, "--port", str(port),
                "--interval", "0.02",
            ],
            name="normal-tcp-probe",
        )
    elif fault_type == "dns_edge":
        host, _pods = dns_address()
        context.start_actor(
            "dns",
            service="frontend",
            network_namespace=True,
            duration=total_duration,
            arguments=[
                "--host", host, "--interval", "0.05",
            ],
            name="normal-dns-probe",
        )


def record_injector_stats(
    context: FaultContext, fault_type: str,
) -> None:
    if fault_type in {"tcp_edge", "dns_edge"}:
        listing = node_command([
            "iptables", "-nvxL", "FORWARD", "--line-numbers",
        ]).stdout
        (context.experiment_root / "iptables-before-cleanup.txt").write_text(
            listing, encoding="utf-8"
        )
    if fault_type == "host_nic":
        output = node_command(["tc", "-s", "qdisc", "show"]).stdout
        (context.experiment_root / "tc-before-cleanup.txt").write_text(
            output, encoding="utf-8"
        )


def experiment_specs() -> list[dict[str, Any]]:
    return [
        {
            "fault_type": "service_cpu",
            "root_scope": "service",
            "root_category": "CPU",
            "activate": lambda context, _windows: context.start_cpu(
                count=2, service="paymentservice"
            ),
        },
        {
            "fault_type": "service_memory",
            "root_scope": "service",
            "root_category": "Memory",
            "activate": service_memory,
        },
        {
            "fault_type": "service_io",
            "root_scope": "service",
            "root_category": "IO",
            "activate": actor_fault(
                "io", service="redis-cart",
                arguments=[
                    "--file", "/var/tmp/proberca-final-service-io.bin",
                    "--bytes", str(64 * 1024 * 1024),
                ],
            ),
            "temporary_files": ["/var/tmp/proberca-final-service-io.bin"],
        },
        {
            "fault_type": "service_lock",
            "root_scope": "service",
            "root_category": "Lock",
            "activate": actor_fault(
                "futex", service="cartservice",
                arguments=["--threads", "12", "--hold-ms", "75"],
            ),
        },
        {
            "fault_type": "service_localnet",
            "root_scope": "service",
            "root_category": "LocalNet",
            "activate": actor_fault(
                "localnet", service="frontend",
                network_namespace=True,
                arguments=["--threads", "20"],
            ),
        },
        {
            "fault_type": "host_cpu",
            "root_scope": "host",
            "root_category": "CPU",
            "activate": lambda context, _windows: context.start_cpu(
                count=min(8, (os.cpu_count() or 4) + 2)
            ),
        },
        {
            "fault_type": "host_memory",
            "root_scope": "host",
            "root_category": "Memory",
            "activate": actor_fault(
                "memory", service=None,
                # Keep enough headroom for kubelet and the instrumented
                # workload.  Eight GiB caused health-probe restarts on this
                # 16-GiB validation VM, changing topology rather than
                # producing a stable host-memory-pressure interval.
                arguments=["--bytes", str(6 * 1024 * 1024 * 1024)],
            ),
        },
        {
            "fault_type": "host_io",
            "root_scope": "host",
            "root_category": "IO",
            "activate": actor_fault(
                "io", service=None,
                arguments=[
                    "--file", "/var/tmp/proberca-final-host-io.bin",
                    "--bytes", str(128 * 1024 * 1024),
                ],
            ),
            "temporary_files": ["/var/tmp/proberca-final-host-io.bin"],
        },
        {
            "fault_type": "host_nic",
            "root_scope": "host",
            "root_category": "NIC",
            "activate": host_nic,
        },
        {
            "fault_type": "tcp_edge",
            "root_scope": "edge",
            "root_category": "TCP",
            "activate": tcp_edge,
            "probe": True,
        },
    ]


def assert_no_stale_network_faults() -> None:
    iptables = node_command(["iptables-save", "-t", "filter"]).stdout
    if "proberca-final-" in iptables:
        raise ExperimentError("stale ProbeRCA iptables rule exists")
    qdiscs = node_command(["tc", "qdisc", "show"]).stdout
    if "netem" in qdiscs:
        raise ExperimentError("stale netem qdisc exists")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--calibration-readiness",
        type=Path,
        required=True,
        help=(
            "validated calibration-readiness.json from an independent "
            "Healthy Pilot"
        ),
    )
    parser.add_argument("--normal-windows", type=int, default=60)
    parser.add_argument("--abnormal-windows", type=int, default=60)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume an interrupted matrix and preserve failed attempts",
    )
    arguments = parser.parse_args()
    try:
        readiness = load_ready_calibration_report(
            arguments.calibration_readiness
        )
    except CalibrationNotReadyError as exc:
        raise SystemExit(
            f"fault injection refused: {exc}"
        ) from exc
    if os.geteuid() != 0:
        raise SystemExit("runner must execute as root")
    if not KUBECONFIG.is_file():
        raise SystemExit(f"kubeconfig is missing: {KUBECONFIG}")
    if not USER_SITE_PACKAGES.is_dir():
        raise SystemExit(
            f"collector Python dependencies are missing: {USER_SITE_PACKAGES}"
        )
    os.environ["KUBECONFIG"] = str(KUBECONFIG)
    if arguments.normal_windows < 5 or arguments.abnormal_windows < 5:
        raise SystemExit("each phase requires at least five windows")
    root = arguments.output.resolve()
    specs = experiment_specs()
    readiness_reference = {
        "report_fingerprint": readiness["report_fingerprint"],
        **{
            name: readiness[name]
            for name in (
                "control_config_fingerprint",
                "collection_contract_fingerprint",
                "required_scope_fingerprint",
                "scale_config_fingerprint",
                "As_fingerprint",
                "Av_fingerprint",
                "calibration_fingerprint",
                "topology_fingerprint",
                "runtime_identity_fingerprint",
            )
        },
        "topology_snapshot_id": readiness["topology_snapshot_id"],
    }
    manifest_path = root / "dataset-manifest.json"
    if root.exists():
        if not arguments.resume:
            raise SystemExit(
                "output root already exists; use --resume only for this run"
            )
        if not manifest_path.is_file():
            raise SystemExit("resume root has no dataset-manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("schema_version")
            != "proberca-final-single-vm-fault-pilot-v1"
            or manifest.get("platform") != "Google Online Boutique"
            or manifest.get("control_plane_executed") is not False
        ):
            raise SystemExit("resume manifest is not this data-only pilot")
        if (
            manifest.get("normal_windows_per_experiment")
            != arguments.normal_windows
            or manifest.get("abnormal_windows_per_experiment")
            != arguments.abnormal_windows
        ):
            raise SystemExit("resume window counts differ from original run")
        if manifest.get("calibration_readiness") != readiness_reference:
            raise SystemExit(
                "resume calibration readiness differs from original run"
            )
        if manifest.get("status") == "complete":
            raise SystemExit("dataset is already complete")
        manifest["status"] = "running"
        manifest.setdefault("failed_attempts", [])
        log_event(root, "matrix_resumed")
    else:
        root.mkdir(parents=True)
        manifest = {
            "schema_version": "proberca-final-single-vm-fault-pilot-v1",
            "platform": "Google Online Boutique",
            "cluster": KUBE_CONTEXT,
            "control_plane_executed": False,
            "calibration_readiness": readiness_reference,
            "phase_variable": "experiment_phase",
            "phase_values": {
                "normal": "fault is not active",
                "abnormal": "declared fault is active",
            },
            "normal_windows_per_experiment": arguments.normal_windows,
            "abnormal_windows_per_experiment": arguments.abnormal_windows,
            "experiments": [],
            "failed_attempts": [],
            "started_at_ns": time.time_ns(),
            "status": "running",
        }
        log_event(root, "matrix_started", experiment_count=len(specs))
    atomic_json(root / "dataset-manifest.json", manifest)
    assert_no_stale_network_faults()
    wait_data_plane(root)
    handshake = assert_current_readiness_handshake(readiness, root)
    manifest["readiness_handshake"] = handshake
    atomic_json(root / "dataset-manifest.json", manifest)
    log_event(
        root,
        "readiness_handshake_passed",
        topology_fingerprint=handshake["topology_fingerprint"],
        runtime_identity_fingerprint=(
            handshake["runtime_identity_fingerprint"]
        ),
    )

    for index, spec in enumerate(specs, 1):
        fault_type = spec["fault_type"]
        existing = next(
            (
                item for item in manifest["experiments"]
                if item.get("fault_type") == fault_type
            ),
            None,
        )
        if existing and existing.get("status") == "complete":
            log_event(root, "experiment_skipped_complete",
                      fault_type=fault_type)
            continue
        experiment_root = root / f"{index:02d}-{fault_type}"
        if existing:
            failed_root = root / "failed-attempts"
            failed_root.mkdir(exist_ok=True)
            archived_name = (
                f"{experiment_root.name}-{time.time_ns()}"
            )
            archived_path = failed_root / archived_name
            if experiment_root.exists():
                shutil.move(str(experiment_root), str(archived_path))
            existing["archived_directory"] = str(
                archived_path.relative_to(root)
            )
            manifest["failed_attempts"].append(existing)
            manifest["experiments"] = [
                item for item in manifest["experiments"]
                if item is not existing
            ]
        experiment_root.mkdir()
        record: dict[str, Any] = {
            "experiment_index": index,
            "fault_type": fault_type,
            "root_scope": spec["root_scope"],
            "root_category": spec["root_category"],
            "status": "running",
            "started_at_ns": time.time_ns(),
        }
        manifest["experiments"].append(record)
        atomic_json(root / "dataset-manifest.json", manifest)
        log_event(root, "experiment_started", fault_type=fault_type)
        context = FaultContext(root, experiment_root)
        try:
            if spec.get("probe"):
                start_probe(
                    context, fault_type,
                    (
                        arguments.normal_windows
                        + arguments.abnormal_windows
                    ) * WINDOW_WALL_BUDGET_SEC + 600,
                )
                time.sleep(3)
            wait_data_plane(root)
            record["normal"] = collect_phase(
                root, experiment_root, "normal",
                arguments.normal_windows,
            )
            spec["activate"](context, arguments.abnormal_windows)
            record["injector"] = context.metadata
            log_event(root, "fault_activated", fault_type=fault_type)
            record["abnormal"] = collect_phase(
                root, experiment_root, "abnormal",
                arguments.abnormal_windows,
            )
            record_injector_stats(context, fault_type)
            record["status"] = "collected"
        except Exception as error:
            record["status"] = "failed"
            record["error"] = f"{type(error).__name__}: {error}"
            log_event(
                root, "experiment_failed",
                fault_type=fault_type, error=record["error"],
            )
            raise
        finally:
            active_exception = sys.exc_info()[0] is not None
            cleanup_errors: list[str] = []
            try:
                context.cleanup()
            except Exception as error:
                cleanup_errors.append(
                    f"{type(error).__name__}: {error}"
                )
            try:
                for name in spec.get("temporary_files", []):
                    path = Path(name)
                    if path.is_file() and str(path).startswith("/var/tmp/"):
                        path.unlink()
            except Exception as error:
                cleanup_errors.append(
                    f"{type(error).__name__}: {error}"
                )
            log_event(root, "fault_deactivated", fault_type=fault_type)
            time.sleep(10)
            try:
                wait_data_plane(root)
                record["recovery_verified_at_ns"] = time.time_ns()
            except Exception as error:
                cleanup_errors.append(
                    f"{type(error).__name__}: {error}"
                )
            if cleanup_errors:
                record["cleanup_errors"] = cleanup_errors
                record["status"] = "failed"
            record["finished_at_ns"] = time.time_ns()
            if record["status"] == "collected":
                record["status"] = "complete"
            atomic_json(root / "dataset-manifest.json", manifest)
            log_event(
                root, "experiment_finished",
                fault_type=fault_type, status=record["status"],
            )
            if cleanup_errors and not active_exception:
                raise ExperimentError(
                    "fault cleanup/recovery failed: "
                    + "; ".join(cleanup_errors)
                )

    manifest["finished_at_ns"] = time.time_ns()
    manifest["status"] = "complete"
    atomic_json(root / "dataset-manifest.json", manifest)
    (root / "dataset-manifest.sha256").write_text(
        f"{sha256_file(root / 'dataset-manifest.json')}"
        "  dataset-manifest.json\n",
        encoding="ascii",
    )
    run(["chown", "-R", "jyz:jyz", str(root)])
    log_event(root, "matrix_completed", experiment_count=len(specs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

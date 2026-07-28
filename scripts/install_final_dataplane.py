#!/usr/bin/env python3
"""Install the final ProbeRCA primitive producer on the frozen single VM.

This installer changes deployment state only.  It does not collect experiment
windows, run control-plane code, or inject faults.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml


REQUIRED_REPOSITORY = Path("/home/jyz/probeRCA")
PROMETHEUS_CONFIG = Path("/etc/prometheus/prometheus.yml")
PROMETHEUS_JOB_NAME = "proberca-final-primitives"


def _run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    return subprocess.run(arguments, check=True, **kwargs)


def _require_root() -> None:
    if os.geteuid() != 0:
        raise SystemExit("install_final_dataplane.py must run as root")


def _find_bpftool() -> str:
    candidates = [
        shutil.which("bpftool"),
        "/usr/lib/linux-tools-5.15.0-185/bpftool",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            result = subprocess.run(
                [candidate, "version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode == 0:
                return candidate
    raise SystemExit("a working bpftool executable is required")


def _load_mapping(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} must contain a YAML mapping")
    return payload


def _atomic_copy(source: Path, destination: Path, mode: int) -> None:
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}-",
        dir=str(destination.parent),
    )
    os.close(descriptor)
    temporary = Path(name)
    try:
        shutil.copyfile(source, temporary)
        os.chmod(temporary, mode)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _install_prometheus_job(repository: Path) -> None:
    configuration = _load_mapping(PROMETHEUS_CONFIG)
    scrape_configs = configuration.get("scrape_configs")
    if not isinstance(scrape_configs, list):
        raise SystemExit("Prometheus scrape_configs must be a list")
    desired = _load_mapping(
        repository
        / "deploy/final-dataplane/prometheus-scrape-job.yaml"
    )
    matches = [
        item for item in scrape_configs
        if isinstance(item, dict)
        and item.get("job_name") == PROMETHEUS_JOB_NAME
    ]
    if matches and matches != [desired]:
        raise SystemExit(
            "an incompatible proberca-final-primitives job already exists"
        )
    if not matches:
        scrape_configs.append(desired)
    rendered = yaml.safe_dump(
        configuration, sort_keys=False, allow_unicode=True
    )
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(PROMETHEUS_CONFIG.parent),
        prefix=".proberca-prometheus-",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        _run(["promtool", "check", "config", str(temporary)])
        os.chmod(temporary, PROMETHEUS_CONFIG.stat().st_mode & 0o777)
        os.replace(temporary, PROMETHEUS_CONFIG)
    finally:
        temporary.unlink(missing_ok=True)


def _service_matches_contract(
    service: dict[str, Any],
    contract: dict[str, Any],
) -> bool:
    return (
        service.get("metadata", {}).get("name") == contract["name"]
        and any(
            isinstance(port, dict)
            and port.get("port") == contract["port"]
            and port.get("targetPort") == contract["targetPort"]
            and port.get("protocol", "TCP") == "TCP"
            for port in service.get("spec", {}).get("ports", [])
        )
    )


def _configure_healthy_probe_cadence(repository: Path) -> None:
    path = (
        repository
        / "deploy/final-dataplane/healthy-probe-cadence.yaml"
    )
    configuration = _load_mapping(path)
    if set(configuration) != {
        "schema_version", "namespace", "readiness_period_seconds",
        "probe_profiles", "deployments",
    }:
        raise SystemExit("healthy probe cadence fields are not frozen")
    if configuration["schema_version"] \
            != "proberca-healthy-probe-cadence-v2":
        raise SystemExit("healthy probe cadence schema is unsupported")
    namespace = configuration["namespace"]
    readiness_period_seconds = configuration[
        "readiness_period_seconds"
    ]
    probe_profiles = configuration["probe_profiles"]
    deployments = configuration["deployments"]
    probe_fields = {
        "liveness_initial_delay_seconds",
        "liveness_failure_threshold",
        "liveness_timeout_seconds",
        "readiness_initial_delay_seconds",
        "readiness_failure_threshold",
        "readiness_timeout_seconds",
    }
    if (
        namespace != "online-boutique"
        or readiness_period_seconds != 1
        or not isinstance(probe_profiles, dict)
        or not probe_profiles
        or any(
            not isinstance(name, str)
            or not name
            or not isinstance(settings, dict)
            or set(settings) != probe_fields
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in settings.values()
            )
            or settings["liveness_failure_threshold"] <= 0
            or settings["liveness_timeout_seconds"] <= 0
            or settings["readiness_failure_threshold"] <= 0
            or settings["readiness_timeout_seconds"] <= 0
            for name, settings in probe_profiles.items()
        )
        or not isinstance(deployments, dict)
        or not deployments
        or any(
            name != "emailservice"
            and (
                not isinstance(name, str)
                or not name
                or not isinstance(profile, dict)
                or set(profile) != {
                    "container", "liveness_period_seconds",
                    "probe_profile",
                }
                or not isinstance(profile["container"], str)
                or not profile["container"]
                or isinstance(profile["liveness_period_seconds"], bool)
                or not isinstance(
                    profile["liveness_period_seconds"], int
                )
                or profile["liveness_period_seconds"] <= 1
                or not isinstance(profile["probe_profile"], str)
                or profile["probe_profile"] not in probe_profiles
            )
            for name, profile in deployments.items()
        )
        or not isinstance(deployments.get("emailservice"), dict)
        or set(deployments["emailservice"]) != {
            "service_contract", "strategic_merge_patch",
        }
        or not isinstance(
            deployments["emailservice"]["service_contract"], dict
        )
        or set(
            deployments["emailservice"]["service_contract"]
        ) != {"name", "port", "targetPort"}
        or deployments["emailservice"][
            "service_contract"
        ]["name"] != "emailservice"
        or not isinstance(
            deployments["emailservice"]["strategic_merge_patch"],
            dict,
        )
    ):
        raise SystemExit("healthy probe cadence configuration is invalid")
    base = [
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", namespace,
    ]
    for deployment, profile in sorted(deployments.items()):
        if deployment == "emailservice":
            contract = profile["service_contract"]
            service_result = _run(
                [
                    *base,
                    "get", f"service/{contract['name']}",
                    "-o", "json",
                ],
                capture_output=True,
                text=True,
            )
            service = json.loads(service_result.stdout)
            if not _service_matches_contract(service, contract):
                raise SystemExit(
                    "emailservice Service does not match "
                    "the frozen port contract"
                )
            _run([
                *base,
                "patch", "deployment/emailservice",
                "--type", "strategic",
                "--patch", json.dumps(
                    profile["strategic_merge_patch"],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ])
            continue
        container_name = profile["container"]
        liveness_period_seconds = profile[
            "liveness_period_seconds"
        ]
        probe_profile = profile["probe_profile"]
        settings = probe_profiles[probe_profile]
        result = _run(
            [
                *base,
                "get", f"deployment/{deployment}",
                "-o", "json",
            ],
            capture_output=True,
            text=True,
        )
        current = json.loads(result.stdout)
        containers = {
            item["name"]: item
            for item in current["spec"]["template"]["spec"]["containers"]
        }
        container = containers.get(container_name)
        if container is None \
                or "livenessProbe" not in container \
                or "readinessProbe" not in container:
            raise SystemExit(
                f"{deployment}/{container_name} lacks frozen health probes"
            )
        patch = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "proberca.io/healthy-probe-cadence": (
                                f"liveness-{liveness_period_seconds}s_"
                                f"readiness-{readiness_period_seconds}s_"
                                f"profile-{probe_profile}"
                            ),
                        },
                    },
                    "spec": {
                        "containers": [{
                            "name": container_name,
                            "livenessProbe": {
                                "periodSeconds": (
                                    liveness_period_seconds
                                ),
                                "initialDelaySeconds": settings[
                                    "liveness_initial_delay_seconds"
                                ],
                                "failureThreshold": settings[
                                    "liveness_failure_threshold"
                                ],
                                "timeoutSeconds": settings[
                                    "liveness_timeout_seconds"
                                ],
                            },
                            "readinessProbe": {
                                "periodSeconds": (
                                    readiness_period_seconds
                                ),
                                "initialDelaySeconds": settings[
                                    "readiness_initial_delay_seconds"
                                ],
                                "failureThreshold": settings[
                                    "readiness_failure_threshold"
                                ],
                                "timeoutSeconds": settings[
                                    "readiness_timeout_seconds"
                                ],
                            },
                        }],
                    },
                },
            },
        }
        _run([
            *base,
            "patch", f"deployment/{deployment}",
            "--type", "strategic",
            "--patch", json.dumps(
                patch, sort_keys=True, separators=(",", ":"),
            ),
        ])
    for deployment in sorted(deployments):
        _run([
            *base,
            "rollout", "status", f"deployment/{deployment}",
            "--timeout=120s",
        ])


def install(repository: Path) -> None:
    _require_root()
    repository = repository.resolve()
    if repository != REQUIRED_REPOSITORY:
        raise SystemExit(
            f"this frozen VM deployment requires {REQUIRED_REPOSITORY}"
        )
    bpftool = _find_bpftool()
    build_directory = Path("/tmp/proberca-final-bpf-build")
    _run([
        "make", "-f", "Makefile.final",
        f"BPFTOOL={bpftool}",
        f"BUILD_DIR={build_directory}",
    ], cwd=repository)

    install_directory = Path("/usr/local/lib/proberca-final")
    install_directory.mkdir(parents=True, exist_ok=True)
    _atomic_copy(
        build_directory / "proberca-final-ebpf-loader",
        install_directory / "proberca-final-ebpf-loader",
        0o755,
    )
    _atomic_copy(
        build_directory / "final_normal.bpf.o",
        install_directory / "final_normal.bpf.o",
        0o644,
    )
    _atomic_copy(
        build_directory / "proberca-final-burst-loader",
        install_directory / "proberca-final-burst-loader",
        0o755,
    )
    _atomic_copy(
        build_directory / "final_burst.bpf.o",
        install_directory / "final_burst.bpf.o",
        0o644,
    )

    unit_directory = Path("/etc/systemd/system")
    for name in (
        "proberca-final-ebpf.service",
        "proberca-final-burst.service",
        "proberca-final-primitive-exporter.service",
    ):
        _atomic_copy(
            repository / "deploy/final-dataplane" / name,
            unit_directory / name,
            0o644,
        )
    _install_prometheus_job(repository)
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", "kube-system",
        "patch", "deployment/coredns",
        "--type", "strategic",
        "--patch-file",
        str(
            repository
            / "deploy/final-dataplane/coredns-cpu-accounting-patch.yaml"
        ),
    ])
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", "kube-system",
        "rollout", "status", "deployment/coredns",
        "--timeout=120s",
    ])
    _configure_healthy_probe_cadence(repository)
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "apply", "-f",
        str(
            repository
            / "deploy/final-dataplane/healthy-calibration-load.yaml"
        ),
    ])
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", "online-boutique",
        "rollout", "status",
        "deployment/proberca-healthy-checkout-load",
        "--timeout=120s",
    ])
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "apply", "-f",
        str(repository / "deploy/final-dataplane/beyla.yaml"),
    ])
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", "proberca-observe",
        "rollout", "status", "daemonset/proberca-beyla",
        "--timeout=120s",
    ])
    _run(["systemctl", "daemon-reload"])
    _run([
        "systemctl", "enable",
        "proberca-final-ebpf.service",
        "proberca-final-burst.service",
        "proberca-final-primitive-exporter.service",
    ])
    _run(["systemctl", "restart", "proberca-final-ebpf.service"])
    _run(["systemctl", "restart", "proberca-final-burst.service"])
    _run([
        "systemctl", "restart",
        "proberca-final-primitive-exporter.service",
    ])
    _run(["systemctl", "reload", "prometheus.service"])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-ebpf.service",
    ])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-burst.service",
    ])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-primitive-exporter.service",
    ])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repository",
        type=Path,
        default=REQUIRED_REPOSITORY,
    )
    arguments = parser.parse_args()
    install(arguments.repository)
    print("final data-plane primitive producer installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

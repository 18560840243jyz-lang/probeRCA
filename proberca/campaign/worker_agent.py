"""Root-side, journaled Linux mutations for the multi-node worker agent."""

from __future__ import annotations

import ipaddress
import json
import os
import re
import signal
import socket
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from .model import fingerprint


class WorkerAgentError(RuntimeError):
    pass


_SESSION = re.compile(r"^[0-9a-f]{64}$")
_INTERFACE = re.compile(r"^[A-Za-z0-9_.:-]{1,32}$")
_CONTAINER_ID = re.compile(r"^[0-9a-f]{12,64}$")
_MECHANISMS = frozenset({
    "service_cpu", "service_cpu_throttle", "service_memory", "service_io",
    "service_futex", "service_local_socket", "host_cpu", "host_memory",
    "host_io", "host_nic", "tcp_latency", "tcp_failure",
})


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


class LinuxWorkerBackend:
    """Apply only the twelve frozen campaign mechanism families.

    Target resolution happens on S0.  The binding carries cgroup/netns inode
    evidence, which this agent rechecks immediately before every mutation.
    """

    def __init__(
        self,
        *,
        node_id: str,
        state_root: Path = Path("/var/lib/proberca-campaign/agent-sessions"),
        work_root: Path = Path("/var/lib/proberca-campaign/fault-work"),
        actor_path: Path = Path(
            "/opt/proberca/current/scripts/multinode_fault_actor.py"
        ),
        runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
        popen: Callable[..., subprocess.Popen] = subprocess.Popen,
    ) -> None:
        self.node_id = node_id
        self.state_root = Path(state_root).resolve()
        self.work_root = Path(work_root).resolve()
        self.actor_path = Path(actor_path).resolve()
        self.runner = runner
        self.popen = popen

    def _journal_path(self, session_id: str) -> Path:
        if not _SESSION.fullmatch(session_id):
            raise WorkerAgentError("session ID is invalid")
        return self.state_root / f"{session_id}.json"

    @staticmethod
    def _cgroup_path(target: dict[str, Any]) -> Path:
        value = target.get("attributes", {}).get("cgroup_path")
        if not isinstance(value, str):
            raise WorkerAgentError("target has no cgroup path")
        path = Path(value).resolve()
        root = Path("/sys/fs/cgroup").resolve()
        if root not in path.parents or not (path / "cgroup.procs").is_file():
            raise WorkerAgentError("target cgroup is missing or outside cgroup v2")
        return path

    @staticmethod
    def _network_prefix(target: dict[str, Any]) -> list[str]:
        value = target.get("attributes", {}).get("network_pid")
        if value is None:
            return []
        pid = int(value)
        if pid <= 1 or not Path(f"/proc/{pid}/ns/net").exists():
            raise WorkerAgentError("target network namespace is unavailable")
        return ["nsenter", "--target", str(pid), "--net"]

    def _identity(self, target: dict[str, Any]) -> str:
        attributes = target.get("attributes", {})
        declared = attributes.get("runtime_identity")
        if not isinstance(declared, dict):
            raise WorkerAgentError("target has no immutable runtime identity")
        observed = dict(declared)
        cgroup = self._cgroup_path(target)
        observed_inode = cgroup.stat().st_ino
        if int(declared.get("cgroup_inode", -1)) != observed_inode:
            raise WorkerAgentError("target cgroup identity changed")
        pid = attributes.get("network_pid")
        if pid is not None:
            netns_inode = Path(f"/proc/{int(pid)}/ns/net").stat().st_ino
            if int(declared.get("network_namespace_inode", -1)) != netns_inode:
                raise WorkerAgentError("target network namespace identity changed")
        result = fingerprint(observed)
        if result != target.get("runtime_identity_fingerprint"):
            raise WorkerAgentError("runtime identity fingerprint mismatch")
        return result

    def _run(self, arguments: list[str], *, tolerate_missing: bool = False) -> str:
        completed = self.runner(
            arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False, timeout=20,
        )
        if completed.returncode != 0 and not tolerate_missing:
            raise WorkerAgentError(
                f"allow-listed host operation failed: {completed.stderr.strip()}"
            )
        return completed.stdout

    def _tc_snapshot(self, target: dict[str, Any]) -> dict[str, Any]:
        interface = target.get("attributes", {}).get("interface")
        if not isinstance(interface, str) or not _INTERFACE.fullmatch(interface):
            raise WorkerAgentError("target network interface is invalid")
        prefix = self._network_prefix(target)
        qdisc = self._run(prefix + ["tc", "-j", "qdisc", "show", "dev", interface])
        filters = {}
        for direction in ("ingress", "egress"):
            raw = self._run(
                prefix + ["tc", "-j", "filter", "show", "dev", interface, direction],
                tolerate_missing=True,
            )
            filters[direction] = json.loads(raw or "[]")
        return {"qdisc": json.loads(qdisc or "[]"), "filters": filters}

    def _packet_filter_snapshot(self, target: dict[str, Any]) -> dict[str, Any]:
        """Return the normalized filter rules without mutable packet counters."""

        prefix = self._network_prefix(target)
        raw = self._run(prefix + [
            "iptables", "-w", "5", "-t", "filter", "-S",
        ])
        return {
            "filter_rules": [
                line.strip() for line in raw.splitlines() if line.strip()
            ],
        }

    def _packet_filter_evidence(self, journal: dict[str, Any]) -> dict[str, Any]:
        """Read the dedicated RST rule counters before exact cleanup."""

        prefix = list(journal["packet_filter_prefix"])
        raw = self._run(prefix + [
            "iptables-save", "-c", "-t", "filter",
        ], tolerate_missing=True)
        chain = str(journal["packet_filter_chain"])
        comment = str(journal["packet_filter_comment"])
        rule_pattern = re.compile(
            rf"^\[(\d+):(\d+)\]\s+-A\s+{re.escape(chain)}\s+.*"
            rf"--comment\s+\"?{re.escape(comment)}\"?.*"
            r"-j\s+REJECT\s+--reject-with\s+tcp-reset\s*$"
        )
        packets = bytes_count = 0
        matched = False
        for line in raw.splitlines():
            match = rule_pattern.match(line.strip())
            if match is not None:
                if matched:
                    raise WorkerAgentError("RST mutation rule is not unique")
                matched = True
                packets = int(match.group(1))
                bytes_count = int(match.group(2))
        jump_present = any(
            line.startswith("[")
            and f"-A OUTPUT " in line
            and f"--comment \"{comment}\"" in line
            and f"-j {chain}" in line
            for line in raw.splitlines()
        )
        return {
            "matched_filter_present": matched and jump_present,
            "packets": packets,
            "bytes": bytes_count,
            "action": "reject_with_tcp_reset",
            "raw_fingerprint": fingerprint({
                "chain": chain,
                "comment": comment,
                "rules": [
                    line.strip() for line in raw.splitlines()
                    if chain in line or comment in line
                ],
            }),
        }

    def _tc_filter_evidence(self, journal: dict[str, Any]) -> dict[str, Any]:
        prefix = list(journal["tc_prefix"])
        if journal["tc_mode"] == "root_prio_netem":
            filter_raw = self._run(prefix + [
                "tc", "-s", "-j", "filter", "show",
                "dev", journal["tc_interface"], "parent", "1:",
                "pref", str(journal["tc_preference"]),
            ], tolerate_missing=True)
            qdisc_raw = self._run(prefix + [
                "tc", "-s", "-j", "qdisc", "show",
                "dev", journal["tc_interface"],
            ], tolerate_missing=True)
            filter_payload = json.loads(filter_raw or "[]")
            qdisc_payload = json.loads(qdisc_raw or "[]")
            # A flower classifier that selects a class with ``flowid`` does
            # not expose packet counters consistently across iproute2/kernel
            # combinations.  The dedicated 30: netem child receives only the
            # selected flow, so its counters are the authoritative hit
            # evidence while the classifier must still be present.
            payload = [
                item for item in qdisc_payload
                if item.get("handle") == "30:"
            ]
        else:
            raw = self._run(prefix + [
                "tc", "-s", "-j", "qdisc", "show",
                "dev", journal["tc_interface"],
            ], tolerate_missing=True)
            filter_payload = None
            qdisc_payload = json.loads(raw or "[]")
            payload = qdisc_payload

        def total(value: Any, key: str) -> int:
            if isinstance(value, dict):
                return sum(
                    int(child) if name == key and isinstance(child, (int, float))
                    else total(child, key)
                    for name, child in value.items()
                )
            if isinstance(value, list):
                return sum(total(item, key) for item in value)
            return 0

        return {
            "matched_filter_present": (
                bool(filter_payload)
                if filter_payload is not None else bool(payload)
            ),
            "packets": total(payload, "packets"),
            "drops": total(payload, "drops"),
            "overlimits": total(payload, "overlimits"),
            "raw_fingerprint": fingerprint({
                "filter": filter_payload,
                "qdisc": qdisc_payload,
            }),
        }

    @staticmethod
    def _fq_codel_restore_arguments(item: dict[str, Any]) -> list[str]:
        """Render the exact, allow-listed fq_codel leaf state from tc JSON."""

        if item.get("kind") != "fq_codel" \
                or not re.fullmatch(r":[1-9][0-9]*", str(item.get("parent", ""))):
            raise WorkerAgentError("multi-queue leaf qdisc is not supported")
        options = item.get("options")
        required = {
            "limit", "flows", "quantum", "target", "interval",
            "memory_limit", "ecn", "drop_batch",
        }
        if not isinstance(options, dict) or set(options) != required:
            raise WorkerAgentError("fq_codel options are not exactly restorable")
        numeric = {
            name: int(options[name])
            for name in required if name != "ecn"
        }
        if any(value <= 0 for value in numeric.values()) \
                or not isinstance(options["ecn"], bool):
            raise WorkerAgentError("fq_codel options are invalid")
        return [
            "parent", str(item["parent"]), "fq_codel",
            "limit", str(numeric["limit"]),
            "flows", str(numeric["flows"]),
            "quantum", str(numeric["quantum"]),
            "target", f"{numeric['target']}us",
            "interval", f"{numeric['interval']}us",
            "memory_limit", str(numeric["memory_limit"]),
            "ecn" if options["ecn"] else "noecn",
            "drop_batch", str(numeric["drop_batch"]),
        ]

    def _state(self, mechanism: str, target: dict[str, Any]) -> dict[str, Any]:
        state: dict[str, Any] = {"runtime_identity": self._identity(target)}
        cgroup = self._cgroup_path(target)
        if mechanism in {"service_cpu", "service_cpu_throttle"}:
            state["cpu.max"] = (cgroup / "cpu.max").read_text(encoding="ascii").strip()
        if mechanism in {"service_memory", "host_memory"}:
            state["memory.high"] = (cgroup / "memory.high").read_text(
                encoding="ascii"
            ).strip()
        if mechanism in {"host_nic", "tcp_latency"}:
            state["traffic_control"] = self._tc_snapshot(target)
        if mechanism == "tcp_failure":
            state["packet_filter"] = self._packet_filter_snapshot(target)
        return state

    def preflight(self, payload: dict[str, Any]) -> dict[str, Any]:
        tools = {}
        for name in (
            "tc", "nsenter", "iptables", "iptables-save", "python3",
            "bpftool", "clang", "make",
        ):
            tools[name] = shutil.which(name) is not None
        stat = os.statvfs(self.work_root.parent if self.work_root.parent.exists() else "/")
        synchronized = False
        clock_offset_seconds: float | None = None
        if shutil.which("chronyc"):
            tracking = self._run(["chronyc", "tracking"], tolerate_missing=True)
            for line in tracking.splitlines():
                if line.strip().startswith("Leap status"):
                    synchronized = "Normal" in line
                if line.strip().startswith("Last offset"):
                    try:
                        clock_offset_seconds = float(line.split(":", 1)[1].split()[0])
                    except (IndexError, ValueError):
                        pass
        elif shutil.which("timedatectl"):
            value = self._run([
                "timedatectl", "show", "--property=NTPSynchronized", "--value",
            ], tolerate_missing=True).strip().lower()
            synchronized = value == "yes"
        interface = payload.get("fault_interface")
        interface_ready = (
            payload.get("role") != "worker"
            or (
                isinstance(interface, str)
                and _INTERFACE.fullmatch(interface) is not None
                and not interface.startswith("CHANGE_ME")
                and Path("/sys/class/net", interface).is_dir()
            )
        )
        host_fault_cgroup_ready = (
            payload.get("role") != "worker"
            or Path(
                "/sys/fs/cgroup/proberca-campaign-host-fault/cgroup.procs"
            ).is_file()
        )
        endpoints = {}
        for name, port in (("node_exporter", 9100), ("beyla", 9400)):
            if payload.get("role") != "worker":
                endpoints[name] = True
                continue
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                    endpoints[name] = True
            except OSError:
                endpoints[name] = False
        return {
            "node_id": self.node_id,
            "cgroup_v2": Path("/sys/fs/cgroup/cgroup.controllers").is_file(),
            "btf": Path("/sys/kernel/btf/vmlinux").is_file(),
            "clock_synchronized": synchronized,
            "clock_offset_seconds": clock_offset_seconds,
            "clock_offset_within_10ms": (
                synchronized
                and clock_offset_seconds is not None
                and abs(clock_offset_seconds) <= 0.010
            ),
            "swap_disabled": (
                Path("/proc/swaps").read_text(encoding="utf-8").count("\n") <= 1
            ),
            "required_tools": all(tools.values()),
            "tools": tools,
            "fault_interface_ready": interface_ready,
            "host_fault_cgroup_ready": host_fault_cgroup_ready,
            "node_exporter_ready": endpoints["node_exporter"],
            "beyla_ready": endpoints["beyla"],
            "free_bytes": stat.f_bavail * stat.f_frsize,
            "logical_cpu_count": os.cpu_count() or 0,
            "memory_bytes": (
                os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
            ),
        }

    @staticmethod
    def _container_cgroup(container_id: str) -> Path:
        value = container_id.removeprefix("containerd://")
        if not _CONTAINER_ID.fullmatch(value):
            raise WorkerAgentError("container ID is invalid")
        root = Path("/sys/fs/cgroup")
        matches: list[Path] = []
        for directory, names, _files in os.walk(root):
            names.sort()
            current = Path(directory)
            if value in current.name and (current / "cgroup.procs").is_file():
                matches.append(current.resolve())
                if len(matches) > 1:
                    break
        if len(matches) != 1:
            raise WorkerAgentError(
                "container ID does not resolve to exactly one cgroup"
            )
        return matches[0]

    @staticmethod
    def _network_pid(cgroup: Path) -> int:
        values = sorted({
            int(line) for line in (cgroup / "cgroup.procs").read_text(
                encoding="ascii"
            ).splitlines()
            if line.isdigit() and int(line) > 1
        })
        for pid in values:
            if Path(f"/proc/{pid}/ns/net").exists():
                return pid
        raise WorkerAgentError("container cgroup has no live network namespace")

    def resolve(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Resolve one label-side target to an immutable local runtime binding."""

        kind = payload.get("entity_kind")
        entity_id = payload.get("entity_id")
        if kind not in {"service", "host", "tcp_edge"} \
                or not isinstance(entity_id, str) or not entity_id:
            raise WorkerAgentError("target resolution identity is invalid")
        attributes: dict[str, Any] = {}
        identity: dict[str, Any] = {
            "node_id": self.node_id,
            "entity_kind": kind,
            "entity_id": entity_id,
        }
        if kind == "host":
            path = Path(
                payload.get(
                    "host_fault_cgroup",
                    "/sys/fs/cgroup/proberca-campaign-host-fault",
                )
            ).resolve()
            if not (path / "cgroup.procs").is_file():
                raise WorkerAgentError("isolated host fault cgroup is missing")
            interface = payload.get("interface")
            if interface is not None:
                if not isinstance(interface, str) or not _INTERFACE.fullmatch(interface):
                    raise WorkerAgentError("host interface is invalid")
                attributes["interface"] = interface
            identity["cgroup_inode"] = path.stat().st_ino
        else:
            container_id = str(payload.get("container_id", ""))
            path = self._container_cgroup(container_id)
            pid = self._network_pid(path)
            identity.update({
                "container_id": container_id.removeprefix("containerd://"),
                "cgroup_inode": path.stat().st_ino,
                "network_namespace_inode": Path(
                    f"/proc/{pid}/ns/net"
                ).stat().st_ino,
            })
            attributes["network_pid"] = pid
            if kind == "tcp_edge":
                destination = str(payload.get("destination_ip", ""))
                ipaddress.ip_address(destination)
                port = int(payload.get("destination_port", 0))
                if not 1 <= port <= 65535:
                    raise WorkerAgentError("TCP destination port is invalid")
                interface = str(payload.get("interface", "eth0"))
                if not _INTERFACE.fullmatch(interface):
                    raise WorkerAgentError("TCP source interface is invalid")
                attributes.update({
                    "interface": interface,
                    "destination_ip": destination,
                    "destination_port": port,
                })
        attributes["cgroup_path"] = str(path)
        attributes["runtime_identity"] = identity
        return {
            "node_id": self.node_id,
            "entity_kind": kind,
            "entity_id": entity_id,
            "runtime_identity_fingerprint": fingerprint(identity),
            "attributes": attributes,
        }

    def snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        mechanism = str(payload.get("mechanism"))
        if mechanism not in _MECHANISMS:
            raise WorkerAgentError("mechanism is not allow-listed")
        state = self._state(mechanism, payload["target"])
        return {
            "runtime_identity_fingerprint": state["runtime_identity"],
            "state_fingerprint": fingerprint(state),
            "state": state,
        }

    def _spawn_actor(
        self, mechanism: str, payload: dict[str, Any], journal: dict[str, Any],
    ) -> None:
        if not self.actor_path.is_file():
            raise WorkerAgentError("allow-listed fault actor is not installed")
        intensity = payload["intensity"]
        cgroup = self._cgroup_path(payload["target"])
        mode = {
            "service_cpu": "cpu", "host_cpu": "cpu",
            "service_memory": "memory", "host_memory": "memory",
            "service_io": "io", "host_io": "io",
            "service_futex": "futex", "service_local_socket": "local_socket",
        }[mechanism]
        workers = int(intensity.get("actor_workers", intensity.get("threads", 1)))
        arguments = [
            "python3", str(self.actor_path), "--mode", mode,
            "--duration", "3600", "--cgroup", str(cgroup),
            "--workers", str(workers),
        ]
        if mechanism == "service_cpu":
            duty_cycle = float(intensity["duty_cycle"])
            if not 0.0 < duty_cycle <= 1.0:
                raise WorkerAgentError("service CPU duty cycle is invalid")
            arguments.extend(["--duty-cycle", f"{duty_cycle:g}"])
        if mode == "memory":
            if "working_set_bytes" in intensity:
                byte_count = int(intensity["working_set_bytes"])
            else:
                raw_limit = (cgroup / "memory.max").read_text(encoding="ascii").strip()
                if raw_limit == "max":
                    raise WorkerAgentError("fractional memory Pilot requires a finite cgroup limit")
                byte_count = int(int(raw_limit) * float(
                    intensity["working_set_fraction_of_limit"]
                ))
            arguments.extend(["--bytes", str(byte_count)])
        if mode == "io":
            file_bytes = int(intensity["file_bytes"])
            work = self.work_root / payload["session_id"]
            work.mkdir(parents=True, exist_ok=False)
            fault_file = work / "fault.bin"
            arguments.extend(["--bytes", str(file_bytes), "--file", str(fault_file)])
            if "write_bytes_per_sec" in intensity:
                arguments.extend([
                    "--bytes-per-second", str(int(intensity["write_bytes_per_sec"])),
                ])
            if intensity.get("fsync_each_block") is not True:
                raise WorkerAgentError("I/O profile must require per-block fsync")
            journal["fault_file"] = str(fault_file)
            journal["work_directory"] = str(work)
        prefix = []
        if mechanism == "service_local_socket":
            prefix = self._network_prefix(payload["target"])
        log_path = self.state_root / f"{payload['session_id']}.actor.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("xb")
        try:
            process = self.popen(
                prefix + arguments, stdin=subprocess.DEVNULL, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True,
            )
        finally:
            log.close()
        journal["actor_pid"] = int(process.pid)
        journal["actor_log"] = str(log_path)

    def _apply_tc(
        self, mechanism: str, payload: dict[str, Any], journal: dict[str, Any],
    ) -> None:
        target = payload["target"]
        attributes = target["attributes"]
        interface = attributes.get("interface")
        if not isinstance(interface, str) or not _INTERFACE.fullmatch(interface):
            raise WorkerAgentError("target network interface is invalid")
        prefix = self._network_prefix(target)
        before = journal["baseline_state"]["traffic_control"]
        qdiscs = before["qdisc"]
        noqueue_mode = bool(qdiscs) and all(
            item.get("kind") == "noqueue" for item in qdiscs
        )
        roots = [item for item in qdiscs if item.get("root") is True]
        mq_leaves = [item for item in qdiscs if item.get("kind") == "fq_codel"]
        mq_mode = (
            mechanism == "host_nic"
            and len(roots) == 1 and roots[0].get("kind") == "mq"
            and roots[0].get("handle") == "0:"
            and len(mq_leaves) >= 1
            and len(qdiscs) == len(mq_leaves) + 1
            and len({item.get("parent") for item in mq_leaves}) == len(mq_leaves)
            and all(item.get("handle") == "0:" for item in mq_leaves)
        )
        if any(before["filters"].values()) or not (noqueue_mode or mq_mode):
            raise WorkerAgentError(
                "target interface qdisc is not safely replaceable for netem Pilot"
            )
        if mq_mode:
            for item in mq_leaves:
                self._fq_codel_restore_arguments(item)
        preference = 40000 + int(payload["session_id"][:4], 16) % 20000
        if mq_mode:
            tc_mode = "mq_root_netem"
        else:
            tc_mode = "root_prio_netem" if mechanism == "tcp_latency" \
                else "root_netem"
        # Persist enough cleanup intent in the in-memory journal before the
        # first mutation. ``apply`` writes this journal on any partial failure.
        journal.update({
            "tc_interface": interface, "tc_mode": tc_mode,
            "tc_preference": preference, "tc_prefix": prefix,
        })
        if mq_mode:
            percent = float(payload["intensity"]["drop_percent"])
            # Kernel-created mq leaves use anonymous handles and cannot be
            # addressed safely by ``tc qdisc replace parent ...``. Replace
            # the mq root for the bounded fault interval instead. Cleanup
            # deletes this explicit root, which makes the kernel recreate its
            # default ``mq handle 0:`` plus fq_codel leaves; the exact-state
            # comparison below still fails closed if that reconstruction ever
            # differs from the captured baseline.
            self._run(prefix + [
                "tc", "qdisc", "replace", "dev", interface, "root",
                "handle", "30:", "netem", "loss", f"{percent:g}%",
            ])
            return
        if mechanism == "tcp_latency":
            self._run(prefix + [
                "tc", "qdisc", "add", "dev", interface, "root",
                "handle", "1:", "prio", "bands", "3", "priomap",
                *("0" for _ in range(16)),
            ])
            netem = prefix + [
                "tc", "qdisc", "add", "dev", interface,
                "parent", "1:3", "handle", "30:", "netem",
            ]
            netem.extend(["delay", f"{int(payload['intensity']['delay_ms'])}ms"])
            self._run(netem)
            destination = str(attributes.get("destination_ip", ""))
            ipaddress.ip_address(destination)
            command = prefix + [
                "tc", "filter", "add", "dev", interface, "parent", "1:",
                "protocol", "ip", "pref", str(preference),
                "flower", "dst_ip", destination, "ip_proto", "tcp",
            ]
            port = attributes.get("destination_port")
            if port is not None:
                command.extend(["dst_port", str(int(port))])
            command.extend(["flowid", "1:3"])
            self._run(command)
        else:
            percent = float(payload["intensity"]["drop_percent"])
            self._run(prefix + [
                "tc", "qdisc", "add", "dev", interface, "root",
                "handle", "30:", "netem", "loss", f"{percent:g}%",
            ])

    def _apply_tcp_reset(
        self, payload: dict[str, Any], journal: dict[str, Any],
    ) -> None:
        """Install one direction/port-scoped RST rule in the caller Pod netns."""

        target = payload["target"]
        attributes = target["attributes"]
        intensity = payload["intensity"]
        if intensity.get("action") != "reject_with_tcp_reset" \
                or intensity.get("direction") != "caller_to_callee":
            raise WorkerAgentError("TCP failure profile is not directional RST")
        destination = str(attributes.get("destination_ip", ""))
        ipaddress.ip_address(destination)
        port = int(attributes.get("destination_port", 0))
        if not 1 <= port <= 65535:
            raise WorkerAgentError("TCP reset destination port is invalid")
        prefix = self._network_prefix(target)
        token = payload["session_id"][:16]
        chain = f"PRCA_{token.upper()}"
        comment = f"proberca:{token}"
        jump_spec = [
            "OUTPUT", "-d", destination, "-p", "tcp", "--dport", str(port),
            "-m", "comment", "--comment", comment, "-j", chain,
        ]
        reject_spec = [
            chain, "-d", destination, "-p", "tcp", "--dport", str(port),
            "-m", "comment", "--comment", comment,
            "-j", "REJECT", "--reject-with", "tcp-reset",
        ]
        journal.update({
            "packet_filter_prefix": prefix,
            "packet_filter_chain": chain,
            "packet_filter_comment": comment,
            "packet_filter_jump_spec": jump_spec,
            "packet_filter_reject_spec": reject_spec,
        })
        # Persist cleanup intent before the first mutation so a partial apply
        # remains recoverable without flushing unrelated rules.
        _atomic_json(self._journal_path(payload["session_id"]), journal)
        self._run(prefix + [
            "iptables", "-w", "5", "-t", "filter", "-N", chain,
        ])
        self._run(prefix + [
            "iptables", "-w", "5", "-t", "filter", "-A", *reject_spec,
        ])
        self._run(prefix + [
            "iptables", "-w", "5", "-t", "filter", "-I", *jump_spec,
        ])

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        mechanism = str(payload.get("mechanism"))
        if mechanism not in _MECHANISMS:
            raise WorkerAgentError("mechanism is not allow-listed")
        path = self._journal_path(payload["session_id"])
        if path.exists():
            raise WorkerAgentError("session mutation journal already exists")
        baseline = self._state(mechanism, payload["target"])
        journal: dict[str, Any] = {
            "schema_version": "probeRCA-worker-mutation-journal-v1",
            "session_id": payload["session_id"], "mechanism": mechanism,
            "target": payload["target"], "baseline_state": baseline,
            "status": "applying",
        }
        _atomic_json(path, journal)
        try:
            cgroup = self._cgroup_path(payload["target"])
            if mechanism == "service_cpu_throttle":
                intensity = payload["intensity"]
                value = f"{int(intensity['cpu_max_quota_us'])} " \
                    f"{int(intensity['cpu_max_period_us'])}\n"
                (cgroup / "cpu.max").write_text(value, encoding="ascii")
            elif mechanism in {"service_memory", "host_memory"}:
                intensity = payload["intensity"]
                if "memory_high_bytes" in intensity:
                    value = int(intensity["memory_high_bytes"])
                else:
                    raw_limit = (cgroup / "memory.max").read_text(
                        encoding="ascii"
                    ).strip()
                    if raw_limit == "max":
                        raise WorkerAgentError("fractional memory.high requires finite limit")
                    value = int(int(raw_limit) * float(
                        intensity["memory_high_fraction_of_limit"]
                    ))
                (cgroup / "memory.high").write_text(f"{value}\n", encoding="ascii")
                self._spawn_actor(mechanism, payload, journal)
            elif mechanism in {"host_nic", "tcp_latency"}:
                self._apply_tc(mechanism, payload, journal)
            elif mechanism == "tcp_failure":
                self._apply_tcp_reset(payload, journal)
            else:
                self._spawn_actor(mechanism, payload, journal)
            journal["status"] = "active"
            _atomic_json(path, journal)
            direct_controls = {
                "service_cpu_throttle": ["cpu.max"],
                "service_memory": ["memory.high"],
                "host_memory": ["memory.high"],
                "host_nic": ["traffic_control"],
                "tcp_latency": ["traffic_control"],
                "tcp_failure": ["packet_filter"],
            }.get(mechanism, [])
            return {
                "applied": True,
                "journal_fingerprint": fingerprint(journal),
                "direct_mutation_controls": direct_controls,
            }
        except Exception:
            journal["status"] = "partial"
            _atomic_json(path, journal)
            raise

    def cleanup(self, payload: dict[str, Any]) -> dict[str, Any]:
        path = self._journal_path(payload["session_id"])
        if not path.is_file():
            # No journal means apply failed before any mutation was persisted.
            return {"cleaned": True, "mutation_was_absent": True}
        journal = json.loads(path.read_text(encoding="utf-8"))
        if journal.get("target") != payload.get("target"):
            raise WorkerAgentError("cleanup target differs from mutation journal")
        mechanism = journal["mechanism"]
        pid = journal.get("actor_pid")
        if pid:
            try:
                os.killpg(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 5.0
            while Path(f"/proc/{int(pid)}").exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            if Path(f"/proc/{int(pid)}").exists():
                try:
                    os.killpg(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if Path(f"/proc/{int(pid)}").exists():
                raise WorkerAgentError("fault actor did not terminate during cleanup")
        cgroup = self._cgroup_path(payload["target"])
        baseline = journal["baseline_state"]
        if mechanism == "service_cpu_throttle":
            (cgroup / "cpu.max").write_text(baseline["cpu.max"] + "\n", encoding="ascii")
        if mechanism in {"service_memory", "host_memory"}:
            (cgroup / "memory.high").write_text(
                baseline["memory.high"] + "\n", encoding="ascii"
            )
        if mechanism in {"host_nic", "tcp_latency"} \
                and "tc_preference" in journal:
            traffic_control_evidence = self._tc_filter_evidence(journal)
            prefix = list(journal["tc_prefix"])
            self._run(prefix + [
                "tc", "qdisc", "del", "dev",
                journal["tc_interface"], "root",
            ], tolerate_missing=True)
        if mechanism == "tcp_failure" \
                and "packet_filter_chain" in journal:
            packet_filter_evidence = self._packet_filter_evidence(journal)
            prefix = list(journal["packet_filter_prefix"])
            jump_spec = list(journal["packet_filter_jump_spec"])
            chain = str(journal["packet_filter_chain"])
            self._run(prefix + [
                "iptables", "-w", "5", "-t", "filter", "-D", *jump_spec,
            ], tolerate_missing=True)
            self._run(prefix + [
                "iptables", "-w", "5", "-t", "filter", "-F", chain,
            ], tolerate_missing=True)
            self._run(prefix + [
                "iptables", "-w", "5", "-t", "filter", "-X", chain,
            ], tolerate_missing=True)
        fault_file = journal.get("fault_file")
        if fault_file:
            Path(fault_file).unlink(missing_ok=True)
        work = journal.get("work_directory")
        if work:
            try:
                Path(work).rmdir()
            except OSError:
                pass
        restored = self._state(mechanism, payload["target"])
        if restored != baseline:
            journal["status"] = "cleanup_mismatch"
            journal["restored_state"] = restored
            _atomic_json(path, journal)
            raise WorkerAgentError("cleanup did not restore exact pre-injection state")
        journal["status"] = "cleaned"
        if "traffic_control_evidence" in locals():
            journal["traffic_control_evidence"] = traffic_control_evidence
        if "packet_filter_evidence" in locals():
            journal["packet_filter_evidence"] = packet_filter_evidence
        _atomic_json(path, journal)
        result = {
            "cleaned": True,
            "restored_state_fingerprint": fingerprint(restored),
        }
        if "traffic_control_evidence" in locals():
            result["traffic_control_evidence"] = traffic_control_evidence
        if "packet_filter_evidence" in locals():
            result["packet_filter_evidence"] = packet_filter_evidence
        return result


class WorkerAgent:
    allowed_actions = frozenset({
        "preflight", "resolve", "snapshot", "apply", "cleanup",
    })

    def __init__(self, backend: LinuxWorkerBackend) -> None:
        self.backend = backend

    def dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        core = {
            key: value for key, value in request.items()
            if key != "request_fingerprint"
        }
        if request.get("schema_version") != "probeRCA-worker-agent-request-v1":
            raise WorkerAgentError("worker request schema is unsupported")
        if request.get("request_fingerprint") != fingerprint(core):
            raise WorkerAgentError("worker request fingerprint mismatch")
        if request.get("node_id") != self.backend.node_id:
            raise WorkerAgentError("worker request node mismatch")
        action = request.get("action")
        if action not in self.allowed_actions:
            raise WorkerAgentError("worker action is not allow-listed")
        payload = request.get("payload")
        if not isinstance(payload, dict):
            raise WorkerAgentError("worker request payload must be a mapping")
        method = getattr(self.backend, action)
        return method(payload)


def response_for_request(
    request: dict[str, Any], backend: LinuxWorkerBackend,
) -> dict[str, Any]:
    core: dict[str, Any] = {"request_id": request.get("request_id")}
    try:
        core.update({"ok": True, "result": WorkerAgent(backend).dispatch(request)})
    except Exception as error:
        core.update({"ok": False, "error": str(error)})
    return {**core, "response_fingerprint": fingerprint(core)}

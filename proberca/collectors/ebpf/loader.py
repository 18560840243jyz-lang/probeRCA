"""libbpf loader subprocess backend; bpftool is never used for event reads."""
from __future__ import annotations

import json
import os
import signal
import select
import subprocess
import time
from pathlib import Path

from .contracts import KernelEvent


class LibbpfBackend:
    def __init__(
        self, *, loader_binary, object_path, probe_name, capability_report,
        cgroup_path="/sys/fs/cgroup", sudo=True, audit_path=None,
    ):
        self.loader_binary = Path(loader_binary)
        self.object_path = Path(object_path)
        self.probe_name = str(probe_name)
        self.capability_report = Path(capability_report)
        self.cgroup_path = Path(cgroup_path)
        self.sudo = bool(sudo)
        self.audit_path = Path(audit_path) if audit_path else None
        self.process = None
        self._lines: list[dict] = []
        self._snapshot = None

    def preflight(self) -> dict:
        if not self.loader_binary.is_file() or not os.access(self.loader_binary, os.X_OK):
            return {"status": "dependency_missing", "reason_code": "probe_unavailable"}
        if not self.object_path.is_file() or not Path("/sys/kernel/btf/vmlinux").is_file():
            return {"status": "kernel_feature_missing", "reason_code": "probe_unavailable"}
        if not self.cgroup_path.is_dir():
            return {"status": "runtime_failed", "reason_code": "probe_unavailable"}
        report = json.loads(self.capability_report.read_text())
        if report.get("status") != "supported":
            return {
                "status": report.get("status", "runtime_failed"),
                "reason_code": "probe_unavailable",
            }
        if self.sudo:
            check = subprocess.run(
                ["sudo", "-n", "true"], capture_output=True, timeout=10,
            )
            if check.returncode:
                return {"status": "permission_missing", "reason_code": "probe_unavailable"}
        return {"status": "supported"}

    def cleanup_orphans(self):
        # P12 does not pin maps/programs. A dead loader closes every owning FD.
        return 0

    def _command(self, snapshot, attach_epoch):
        command = []
        if self.sudo:
            command.extend(("sudo", "-n"))
        command.extend((
            str(self.loader_binary), "--object", str(self.object_path),
            "--probe", self.probe_name, "--ttl", str(snapshot.ttl_sec),
            "--attach-epoch", str(attach_epoch),
            "--candidate-version", str(snapshot.version),
            "--cgroup-path", str(self.cgroup_path),
        ))
        for cgroup_id in snapshot.cgroup_ids:
            command.extend(("--candidate-cgroup", str(cgroup_id)))
        for source, target in (snapshot.kernel_edge_keys or snapshot.service_pairs):
            command.extend(("--candidate-edge", f"{source}:{target}"))
        return command

    def _record_line(self, line: str) -> dict:
        payload = json.loads(line)
        self._lines.append(payload)
        if self.audit_path is not None:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(
                self.audit_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600,
            )
            try:
                os.write(descriptor, line.encode("utf-8") + b"\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return payload

    def attach(self, snapshot, attach_epoch):
        if self.process is not None:
            raise RuntimeError("loader already attached")
        self._snapshot = snapshot
        self.process = subprocess.Popen(
            self._command(snapshot, attach_epoch), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            bufsize=1,
        )
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            readable, _, _ = select.select([self.process.stdout], [], [], remaining)
            if not readable:
                break
            line = self.process.stdout.readline()
            if line:
                payload = self._record_line(line.rstrip("\n"))
                if payload.get("record_type") == "control" and payload.get("state") == "ACTIVE":
                    return
            elif self.process.poll() is not None:
                error = self.process.stderr.read().strip()
                raise RuntimeError(f"loader exited before ACTIVE: {error}")
        raise TimeoutError("loader did not enter ACTIVE")

    def update_candidates(self, snapshot):
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("loader is not active")
        cgroups = ",".join(str(item) for item in snapshot.cgroup_ids) or "-"
        edges = ",".join(
            f"{left}:{right}" for left, right in
            (snapshot.kernel_edge_keys or snapshot.service_pairs)
        ) or "-"
        self.process.stdin.write(f"replace {snapshot.version} {cgroups} {edges}\n")
        self.process.stdin.flush()
        self._snapshot = snapshot

    def read_until(self, deadline_monotonic):
        if self.process is None:
            raise RuntimeError("loader is not active")
        timeout = max(1.0, deadline_monotonic - time.monotonic() + 10.0)
        try:
            stdout, stderr = self.process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.process.send_signal(signal.SIGINT)
            raise TimeoutError("loader exceeded bounded TTL") from error
        for line in stdout.splitlines():
            if line.strip():
                self._record_line(line)
        if self.process.returncode:
            raise RuntimeError(f"loader failed rc={self.process.returncode}: {stderr.strip()}")
        events = tuple(
            KernelEvent.from_loader_dict(item) for item in self._lines
            if item.get("record_type") == "event"
        )
        summary = next((
            item for item in reversed(self._lines)
            if item.get("record_type") == "summary"
        ), {})
        return events, {
            "received_events": len(events),
            "ring_buffer_drops": int(summary.get("ring_buffer_drops", 0)),
            "filtered_events": int(summary.get("filtered_events", 0)),
            "mapping_failures": 0,
        }

    def detach(self, timeout_sec):
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=timeout_sec)

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=5)
            self.process = None


class CompositeLibbpfBackend:
    """One controller backend that owns the complete probe set."""

    def __init__(self, backends):
        self.backends = tuple(backends)
        if not self.backends:
            raise ValueError("at least one libbpf backend is required")
        names = [backend.probe_name for backend in self.backends]
        if len(names) != len(set(names)):
            raise ValueError("probe backend names must be unique")
        self._attached = []

    def preflight(self):
        results = [backend.preflight() for backend in self.backends]
        failure = next((item for item in results if item.get("status") != "supported"), None)
        return failure or {"status": "supported"}

    def cleanup_orphans(self):
        return sum(backend.cleanup_orphans() for backend in self.backends)

    def attach(self, snapshot, attach_epoch):
        try:
            for backend in self.backends:
                backend.attach(snapshot, attach_epoch)
                self._attached.append(backend)
        except Exception:
            for backend in reversed(self._attached):
                try:
                    backend.detach(5.0)
                finally:
                    backend.close()
            self._attached.clear()
            raise

    def update_candidates(self, snapshot):
        for backend in self._attached:
            backend.update_candidates(snapshot)

    def read_until(self, deadline_monotonic):
        events = []
        combined = {
            "received_events": 0, "ring_buffer_drops": 0,
            "filtered_events": 0, "mapping_failures": 0,
        }
        for backend in self._attached:
            backend_events, statistics = backend.read_until(deadline_monotonic)
            events.extend(backend_events)
            for name in combined:
                combined[name] += int(statistics.get(name, 0))
        return tuple(sorted(events, key=lambda item: (
            item.timestamp_ns, item.cpu, item.event_sequence,
        ))), combined

    def detach(self, timeout_sec):
        errors = []
        for backend in reversed(self._attached):
            try:
                backend.detach(timeout_sec)
            except Exception as error:
                errors.append(error)
        self._attached.clear()
        if errors:
            raise RuntimeError("one or more probes failed to detach") from errors[0]

    def close(self):
        for backend in self.backends:
            backend.close()
        self._attached.clear()

__all__ = ["CompositeLibbpfBackend", "LibbpfBackend"]

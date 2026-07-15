"""Credential-free live health, readiness, and Prometheus metrics."""
from __future__ import annotations

from contextlib import suppress
import json
import os
import threading
import uuid
from pathlib import Path


RUNTIME_FIELDS = {
    "committed_sequence", "coordinator_state", "eligible_window_count",
    "last_now_ns", "next_sequence", "next_start_ns",
}


COUNTER_HELP = {
    "processed_windows_total": "Successfully committed engine windows.",
    "topology_builds_total": "Successfully built live topology snapshots.",
    "metric_queries_total": "Prometheus queries attempted.",
    "metric_query_failures_total": "Prometheus queries that failed.",
    "watch_reconnects_total": "Kubernetes watch reconnect attempts.",
    "watch_relists_total": "Kubernetes cache relists.",
    "engine_failures_total": "Canonical engine window failures.",
    "alerts_total": "Alert events emitted by the engine.",
    "reports_total": "RCA reports emitted by the engine.",
    "failures_total": "Structured incident failures emitted by the engine.",
    "leader_transitions_total": "Transitions into or out of active leadership.",
    "checkpoint_saves_total": "Committed checkpoint generations.",
    "checkpoint_failures_total": "Checkpoint probe or save failures.",
    "output_conflicts_total": "Output probe, write, or identity conflicts.",
    "live_stage_timeout_total": "Live stages exceeding configured deadlines.",
    "live_watchdog_stall_total": "Distinct live progress stalls detected.",
    "live_fail_stop_total": "Live processes terminated by fail-stop.",
    "live_collection_retry_total": "Whole-window collection retries.",
    "live_collection_exhausted_total": "Exhausted collection retry sequences.",
    "live_attempt_audit_write_failures_total": (
        "Durable live-attempt audit write failures."
    ),
}

GAUGE_HELP = {
    "live_backlog_current": "Current eligible live window backlog.",
    "live_stage_age_seconds": "Age of the current live stage.",
    "live_last_commit_age_seconds": "Age of the last durable live commit.",
    "live_working_engines": "Current working Engine worker count.",
}


class LiveHealthState:
    POD_REQUIRED = ("kubernetes_connected", "watchers_synchronized")

    REQUIRED = (
        "kubernetes_connected", "watchers_synchronized", "prometheus_healthy",
        "leader", "checkpoint_writable", "output_writable", "engine_available",
    )

    def __init__(self, *, code_revision="unknown", source_fingerprint="unknown",
                 schema_version="1.0", image_digest=None):
        self.values = {name: False for name in (*self.REQUIRED, *self.POD_REQUIRED)}
        self.values.update({
            "inventory_stale": True, "watcher_relisting": False,
            "watcher_fatal": False, "catchup_exceeded": False,
            "progress_stalled": False, "fatal_error": None,
            "stage_timed_out": False, "backlog_exceeded": False,
            "collection_retrying": False,
            "audit_write_failed": False,
        })
        self.counters = {name: 0 for name in COUNTER_HELP}
        self.runtime = {}
        self.progress = {}
        self.build = {
            "code_revision": str(code_revision),
            "source_fingerprint": str(source_fingerprint),
            "schema_version": str(schema_version),
            "image_digest": str(image_digest) if image_digest else None,
        }
        self._lock = threading.RLock()

    def update(self, **values) -> None:
        unknown = set(values) - set(self.values)
        if unknown:
            raise ValueError(f"unknown health fields {sorted(unknown)}")
        with self._lock:
            self.values.update(values)

    def update_runtime(self, **values) -> None:
        unknown = set(values) - RUNTIME_FIELDS
        if unknown:
            raise ValueError(f"unknown runtime fields {sorted(unknown)}")
        with self._lock:
            self.runtime.update(values)

    def update_progress(self, snapshot: dict) -> None:
        if not isinstance(snapshot, dict):
            raise TypeError("progress snapshot must be a dictionary")
        with self._lock:
            self.progress = dict(snapshot)

    def update_progress_health(
        self, *, backlog_count, last_commit_age_sec, current_stage,
        current_stage_age_sec, current_stage_timeout_sec,
        progress_timeout_sec, backlog_not_ready_threshold,
        working_engine_count, stalled, committed_sequence=None,
        next_sequence=None, attempt=None, active_transaction_state=None,
    ) -> None:
        stage_name = current_stage.value if hasattr(current_stage, "value") else str(current_stage)
        progress_stalled = bool(stalled or (backlog_count > 0 and last_commit_age_sec > progress_timeout_sec))
        stage_timed_out = bool(current_stage_age_sec > current_stage_timeout_sec)
        backlog_exceeded = bool(backlog_count >= backlog_not_ready_threshold)
        with self._lock:
            self.values.update({
                "progress_stalled": progress_stalled,
                "stage_timed_out": stage_timed_out,
                "backlog_exceeded": backlog_exceeded,
            })
            self.progress.update({
                "backlog_count": int(backlog_count),
                "last_commit_age_sec": float(last_commit_age_sec),
                "stage": stage_name,
                "stage_age_sec": float(current_stage_age_sec),
                "stage_timeout_sec": float(current_stage_timeout_sec),
                "working_engine_count": int(working_engine_count),
                "stalled": bool(stalled),
            })
            for key, value in (
                ("committed_sequence", committed_sequence),
                ("next_sequence", next_sequence),
                ("attempt", attempt),
                ("active_transaction_state", active_transaction_state),
            ):
                if value is not None:
                    self.progress[key] = value

    def increment(self, name: str, amount: int = 1) -> None:
        if name not in self.counters:
            raise ValueError(f"unknown live counter {name}")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValueError("counter increments must be non-negative integers")
        with self._lock:
            self.counters[name] += amount

    def counter(self, name: str) -> int:
        with self._lock:
            return self.counters[name]

    def record_processed_window(self) -> None:
        if not self.values["leader"]:
            raise RuntimeError("standby cannot record processed windows")
        self.increment("processed_windows_total")

    @property
    def ready(self) -> bool:
        with self._lock:
            return not self.reason_codes()

    @property
    def pod_ready(self) -> bool:
        with self._lock:
            return not self.pod_reason_codes()

    def pod_reason_codes(self) -> list[str]:
        reasons = []
        if not self.values["kubernetes_connected"]:
            reasons.append("kubernetes_unavailable")
        if not self.values["watchers_synchronized"]:
            reasons.append("watchers_unsynchronized")
        for field, reason in (
            ("inventory_stale", "inventory_stale"),
            ("watcher_relisting", "watcher_relisting"),
            ("watcher_fatal", "watcher_fatal"),
            ("catchup_exceeded", "scheduler_catchup_exceeded"),
        ):
            if self.values[field]:
                reasons.append(reason)
        if self.values["fatal_error"] is not None:
            reasons.append("fatal_error")
        return sorted(reasons)

    def reason_codes(self) -> list[str]:
        reasons = []
        mapping = {
            "kubernetes_connected": "kubernetes_unavailable",
            "watchers_synchronized": "watchers_unsynchronized",
            "prometheus_healthy": "prometheus_unavailable",
            "leader": "standby",
            "checkpoint_writable": "checkpoint_not_writable",
            "output_writable": "output_not_writable",
            "engine_available": "engine_unavailable",
        }
        for field, reason in mapping.items():
            if not self.values[field]:
                reasons.append(reason)
        if self.values["inventory_stale"]:
            reasons.append("inventory_stale")
        if self.values["watcher_relisting"]:
            reasons.append("watcher_relisting")
        if self.values["watcher_fatal"]:
            reasons.append("watcher_fatal")
        if self.values["catchup_exceeded"]:
            reasons.append("scheduler_catchup_exceeded")
        if self.values["progress_stalled"]:
            reasons.append("live_progress_stalled")
        if self.values["stage_timed_out"]:
            reasons.append("live_stage_timeout")
        if self.values["backlog_exceeded"]:
            reasons.append("live_backlog_threshold")
        if self.values["collection_retrying"]:
            reasons.append("live_collection_retry")
        if self.values["audit_write_failed"]:
            reasons.append("audit_write_failed")
        if self.values["fatal_error"] is not None:
            reasons.append("fatal_error")
        return sorted(reasons)

    def status(self) -> dict:
        with self._lock:
            return {
                "ready": not self.reason_codes(),
                "reason_codes": self.reason_codes(),
                "state": dict(self.values),
                "metrics": dict(sorted(self.counters.items())),
                "runtime": dict(sorted(self.runtime.items())),
                "progress": dict(sorted(self.progress.items())),
                "build": dict(self.build),
            }

    def prometheus_metrics(self) -> str:
        with self._lock:
            lines = []
            for name, help_text in COUNTER_HELP.items():
                metric = f"proberca_{name}"
                lines.extend((f"# HELP {metric} {help_text}",
                              f"# TYPE {metric} counter",
                              f"{metric} {self.counters[name]}"))
            gauges = {
                "live_backlog_current": self.progress.get("backlog_count", 0),
                "live_stage_age_seconds": self.progress.get("stage_age_sec", 0.0),
                "live_last_commit_age_seconds": self.progress.get("last_commit_age_sec", 0.0),
                "live_working_engines": self.progress.get("working_engine_count", 0),
            }
            for name, help_text in GAUGE_HELP.items():
                metric = f"proberca_{name}"
                lines.extend((f"# HELP {metric} {help_text}",
                              f"# TYPE {metric} gauge",
                              f"{metric} {gauges[name]}"))
            return "\n".join(lines) + "\n"


def probe_writable_directory(directory) -> bool:
    path = Path(directory)
    probe = path / f".proberca-write-probe-{uuid.uuid4().hex}.tmp"
    descriptor = None
    try:
        if not path.is_dir():
            return False
        descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.write(descriptor, b"probe")
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        probe.unlink()
        directory_fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except OSError:
        return False
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(OSError):
            probe.unlink()


def serve_health(state: LiveHealthState, bind: str):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    host, port = bind.rsplit(":", 1)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/livez":
                payload, code = {"live": True}, 200
            elif self.path == "/podreadyz":
                payload, code = {"ready": state.pod_ready,
                                 "reason_codes": state.pod_reason_codes()}, 200 if state.pod_ready else 503
            elif self.path == "/readyz":
                payload, code = {"ready": state.ready,
                                 "reason_codes": state.reason_codes()}, 200 if state.ready else 503
            elif self.path == "/status":
                payload, code = state.status(), 200
            elif self.path == "/metrics":
                body = state.prometheus_metrics().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                self.wfile.write(body)
                return
            else:
                payload, code = {"error": "not found"}, 404
            body = json.dumps(payload, sort_keys=True).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            return

    return ThreadingHTTPServer((host, int(port)), Handler)

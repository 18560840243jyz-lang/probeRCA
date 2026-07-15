"""Bounded durable audit sink for sanitized live-attempt events."""
from __future__ import annotations

import json
import os
from pathlib import Path
import threading

from .progress import StageEvent


LIVE_ATTEMPT_AUDIT_SCHEMA = "p11-live-attempt-audit-v1"


def canonical_audit_line(event: StageEvent) -> bytes:
    if not isinstance(event, StageEvent):
        raise TypeError("attempt audit requires StageEvent")
    payload = event.to_dict()
    payload["schema_version"] = LIVE_ATTEMPT_AUDIT_SCHEMA
    payload["attempt_index"] = payload.pop("attempt")
    payload["monotonic_event_index"] = payload.pop("event_index")
    payload["transaction_id_fingerprint"] = payload.pop("transaction_id")
    payload["staging_path_fingerprint"] = payload.pop(
        "generation_staging_fingerprint"
    )
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


class BoundedAttemptAuditWriter:
    """Write identical canonical events to stdout and a bounded durable log."""

    def __init__(
        self, stream, path, *, max_bytes=1_048_576, backup_count=2,
        on_failure=None,
    ):
        if not all(hasattr(stream, name) for name in ("write", "flush")):
            raise TypeError("audit stream must provide write and flush")
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
            raise TypeError("audit max_bytes must be an integer")
        if max_bytes < 4096:
            raise ValueError("audit max_bytes must be at least 4096")
        if isinstance(backup_count, bool) or not isinstance(backup_count, int):
            raise TypeError("audit backup_count must be an integer")
        if backup_count < 1:
            raise ValueError("audit backup_count must be positive")
        if on_failure is not None and not callable(on_failure):
            raise TypeError("audit failure callback must be callable")
        self.stream = stream
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.on_failure = on_failure
        self.failed = False
        self.failure_reason = None
        self._lock = threading.Lock()

    def _report_failure(self, error: BaseException) -> None:
        self.failed = True
        self.failure_reason = "audit_write_failed"
        if self.on_failure is not None:
            try:
                self.on_failure("audit_write_failed", type(error).__name__)
            except Exception:
                pass

    def _fsync_directory(self) -> None:
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _rotate(self) -> None:
        oldest = self.path.with_name(
            f"{self.path.name}.{self.backup_count}"
        )
        if oldest.exists():
            oldest.unlink()
        for index in range(self.backup_count - 1, 0, -1):
            source = self.path.with_name(f"{self.path.name}.{index}")
            if source.exists():
                os.replace(
                    source,
                    self.path.with_name(f"{self.path.name}.{index + 1}"),
                )
        if self.path.exists():
            os.replace(self.path, self.path.with_name(f"{self.path.name}.1"))
        self._fsync_directory()

    def _write_file(self, line: bytes) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        current_size = self.path.stat().st_size if self.path.exists() else 0
        if current_size and current_size + len(line) > self.max_bytes:
            self._rotate()
        descriptor = os.open(
            self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        try:
            view = memoryview(line)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("short attempt-audit write")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._fsync_directory()

    def __call__(self, event: StageEvent) -> None:
        line = canonical_audit_line(event)
        with self._lock:
            try:
                self.stream.write(line.decode("utf-8"))
                self.stream.flush()
            except Exception as error:
                self._report_failure(error)
            try:
                self._write_file(line)
            except Exception as error:
                self._report_failure(error)

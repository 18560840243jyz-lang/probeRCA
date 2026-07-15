"""Fail-closed interpretation of persisted P12 capability reports."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CapabilityStatus:
    status: str
    reason_code: str | None
    report: dict

    @property
    def supported(self) -> bool:
        return self.status == "supported"


def load_capability_report(path) -> CapabilityStatus:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "p12-capability-report-v1":
        raise ValueError("unsupported capability report schema")
    required = {
        "kernel", "privilege", "mounts", "toolchain", "kernel_features",
        "status", "blocking_issues",
    }
    if required - set(payload):
        raise ValueError("capability report is incomplete")
    status = str(payload["status"])
    allowed = {
        "supported", "permission_missing", "kernel_feature_missing",
        "dependency_missing", "attach_failed", "verifier_rejected",
        "runtime_failed",
    }
    if status not in allowed:
        raise ValueError("invalid capability status")
    reason = None if status == "supported" else (
        str(payload["blocking_issues"][0]) if payload["blocking_issues"]
        else "probe_unavailable"
    )
    return CapabilityStatus(status, reason, payload)


__all__ = ["CapabilityStatus", "load_capability_report"]

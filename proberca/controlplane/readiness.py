"""Validated calibration-readiness reports used to gate fault experiments."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from proberca.dataplane.contracts import fingerprint


class CalibrationNotReadyError(RuntimeError):
    """A fault experiment was requested without a valid READY report."""


def load_ready_calibration_report(path: str | Path) -> dict[str, Any]:
    report_path = Path(path)
    if not report_path.is_file():
        raise CalibrationNotReadyError(
            f"calibration readiness report is missing: {report_path}"
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CalibrationNotReadyError(
            f"calibration readiness report is unreadable: {report_path}"
        ) from exc
    if not isinstance(report, dict):
        raise CalibrationNotReadyError(
            "calibration readiness report must contain an object"
        )
    if report.get("schema_version") != "probeRCA-calibration-readiness-v1":
        raise CalibrationNotReadyError(
            "unsupported calibration readiness schema"
        )
    claimed_fingerprint = report.get("report_fingerprint")
    unsigned = dict(report)
    unsigned["report_fingerprint"] = ""
    if claimed_fingerprint != fingerprint(unsigned):
        raise CalibrationNotReadyError(
            "calibration readiness report fingerprint mismatch"
        )
    required_flags = (
        "ready",
        "core_ready",
        "baseline_ready",
        "service_model_ready",
        "metric_model_ready",
        "planned_scope_ready",
    )
    failed = [name for name in required_flags if report.get(name) is not True]
    if failed:
        raise CalibrationNotReadyError(
            "calibration is not READY: " + ",".join(failed)
        )
    if report.get("state") != "ready":
        raise CalibrationNotReadyError(
            "calibration readiness state is not ready"
        )
    completed = report.get("healthy_validation_windows")
    required = report.get("required_healthy_validation_windows")
    if (
        isinstance(completed, bool)
        or not isinstance(completed, int)
        or isinstance(required, bool)
        or not isinstance(required, int)
        or completed < required
    ):
        raise CalibrationNotReadyError(
            "healthy validation window requirement is not satisfied"
        )
    if not report.get("topology_snapshot_id") \
            or not report.get("required_scope_fingerprint") \
            or not report.get("control_config_fingerprint"):
        raise CalibrationNotReadyError(
            "calibration readiness provenance is incomplete"
        )
    return report

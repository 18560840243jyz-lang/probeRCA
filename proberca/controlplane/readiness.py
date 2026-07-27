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
    learned = report.get("calibration_learning_windows")
    required_learning = report.get(
        "required_calibration_learning_windows"
    )
    if (
        isinstance(learned, bool)
        or not isinstance(learned, int)
        or isinstance(required_learning, bool)
        or not isinstance(required_learning, int)
        or learned < required_learning
        or report.get("calibration_learning_complete") is not True
    ):
        raise CalibrationNotReadyError(
            "calibration learning window requirement is not satisfied"
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
    if report.get("healthy_validation_result") != "passed" \
            or report.get("healthy_validation_alerts") != []:
        raise CalibrationNotReadyError(
            "independent healthy validation did not pass"
        )
    topology_epoch = report.get("topology_epoch")
    if not report.get("snapshot_id") \
            or not report.get("topology_snapshot_id") \
            or not report.get("topology_fingerprint") \
            or not report.get("runtime_identity_fingerprint") \
            or isinstance(topology_epoch, bool) \
            or not isinstance(topology_epoch, int) \
            or topology_epoch <= 0 \
            or not report.get("required_scope_fingerprint") \
            or not report.get("control_config_fingerprint") \
            or not report.get("collection_contract_fingerprint") \
            or not report.get("scale_config_fingerprint") \
            or not report.get("As_fingerprint") \
            or not report.get("Av_fingerprint") \
            or not report.get("calibration_fingerprint"):
        raise CalibrationNotReadyError(
            "calibration readiness provenance is incomplete"
        )
    for ready_name, required_name in (
        ("baseline_ready_count", "baseline_required_count"),
        ("As_ready_count", "As_required_count"),
        ("Av_ready_count", "Av_required_count"),
    ):
        ready_count = report.get(ready_name)
        required_count = report.get(required_name)
        if (
            isinstance(ready_count, bool)
            or not isinstance(ready_count, int)
            or isinstance(required_count, bool)
            or not isinstance(required_count, int)
            or required_count <= 0
            or ready_count != required_count
        ):
            raise CalibrationNotReadyError(
                f"calibration readiness count mismatch: {ready_name}"
            )
    return report

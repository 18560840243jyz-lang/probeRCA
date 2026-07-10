"""Online Boutique adapter for A7 evidence channel preview."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.evidence.evidence_channel import (
    EvidenceChannelConfig,
    build_evidence_channel,
    evaluate_evidence_channel_debug,
)

FAULT_SOURCES = {
    "cpu": ("CPU", "cpu_paymentservice_repeated_controlled"),
    "network": ("Network", "network_shippingservice_repeated"),
    "io": ("I/O", "io_rediscart_repeated"),
    "lock": ("Lock", "lock_cartservice_repeated_phaseaware"),
}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _max(values: list[float]) -> float:
    return float(np.max(np.asarray(values, dtype=float))) if values else 0.0


def resolve_repeat_dirs(fault_type: str, repeat_index: int) -> dict[str, str]:
    key = fault_type.lower()
    if key not in FAULT_SOURCES:
        raise ValueError(f"unknown fault_type: {fault_type}")
    _label, raw_root = FAULT_SOURCES[key]
    repeat = f"repeat_{repeat_index:02d}"
    raw_input_dir = Path("data/p2_online_boutique") / raw_root / repeat / "raw"
    blind_evidence_dir = Path("data/p2_online_boutique/blind_rerun") / key / repeat
    probe_policy_dir = Path("data/p2_online_boutique/a5_probe_policy_preview") / key / repeat
    ipw_rls_dir = Path("data/p2_online_boutique/a6_ipw_rls_preview") / key / repeat
    required = [
        raw_input_dir / "incidents.jsonl",
        blind_evidence_dir / "input" / "blind_evidence.jsonl",
        probe_policy_dir / "sampling_log.jsonl",
        probe_policy_dir / "observation_mask.jsonl",
        ipw_rls_dir / "ipw_rls_residuals.jsonl",
        ipw_rls_dir / "ipw_rls_metadata.json",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing A7 input files: {missing}")
    return {
        "raw_input_dir": str(raw_input_dir),
        "blind_evidence_dir": str(blind_evidence_dir),
        "probe_policy_dir": str(probe_policy_dir),
        "ipw_rls_dir": str(ipw_rls_dir),
    }


def run_single_evidence_channel_repeat(
    fault_type: str,
    repeat_index: int,
    output_root: str,
    config: EvidenceChannelConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    dirs = resolve_repeat_dirs(fault_type, repeat_index)
    key = fault_type.lower()
    out = Path(output_root) / key / f"repeat_{repeat_index:02d}"
    result = build_evidence_channel(dirs["blind_evidence_dir"], dirs["probe_policy_dir"], dirs["ipw_rls_dir"], str(out), config)
    debug = None
    incidents_path = Path(dirs["raw_input_dir"]) / "incidents.jsonl"
    if debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_evidence_channel_debug(str(out), str(incidents_path))
        _write_json(out / "evidence_channel_debug_evaluation.json", debug)
    metadata = result["metadata"]
    row: dict[str, Any] = {
        "fault_type": FAULT_SOURCES[key][0],
        "fault_type_key": key,
        "repeat_index": repeat_index,
        "raw_input_dir": dirs["raw_input_dir"],
        "blind_evidence_dir": dirs["blind_evidence_dir"],
        "probe_policy_dir": dirs["probe_policy_dir"],
        "ipw_rls_dir": dirs["ipw_rls_dir"],
        "output_dir": str(out),
        "residual_count": metadata["residual_count"],
        "calibrated_residual_count": metadata["calibrated_residual_count"],
        "average_abs_raw_residual": metadata["average_abs_raw_residual"],
        "average_abs_calibrated_residual": metadata["average_abs_calibrated_residual"],
        "max_abs_raw_residual": metadata["max_abs_raw_residual"],
        "max_abs_calibrated_residual": metadata["max_abs_calibrated_residual"],
        "completed": True,
    }
    if debug:
        row["debug_root_metric_calibrated_residual_rank_mean"] = debug.get("root_metric_calibrated_residual_rank_mean")
        row["debug_root_service_calibrated_residual_rank_mean"] = debug.get("root_service_calibrated_residual_rank_mean")
    return row


def run_p2_evidence_channel_preview(
    output_dir: str = "data/p2_online_boutique/a7_evidence_channel_preview",
    config: EvidenceChannelConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}
    for key, (label, _raw_root) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            row = run_single_evidence_channel_repeat(key, index, str(output), config, debug_evaluate_incidents)
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": label,
            "repeats": len(rows),
            "repeats_completed": sum(1 for row in rows if row.get("completed")),
            "average_abs_raw_residual": _mean([float(row["average_abs_raw_residual"]) for row in rows]),
            "average_abs_calibrated_residual": _mean([float(row["average_abs_calibrated_residual"]) for row in rows]),
            "max_abs_raw_residual": _max([float(row["max_abs_raw_residual"]) for row in rows]),
            "max_abs_calibrated_residual": _max([float(row["max_abs_calibrated_residual"]) for row in rows]),
            "debug_root_metric_calibrated_residual_rank_mean": _mean([float(row.get("debug_root_metric_calibrated_residual_rank_mean") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_root_service_calibrated_residual_rank_mean": _mean([float(row.get("debug_root_service_calibrated_residual_rank_mean") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }
    summary: dict[str, Any] = {
        "total_repeats": len(per_repeat),
        "repeats_completed": sum(1 for row in per_repeat if row.get("completed")),
        "average_abs_raw_residual": _mean([float(row["average_abs_raw_residual"]) for row in per_repeat]),
        "average_abs_calibrated_residual": _mean([float(row["average_abs_calibrated_residual"]) for row in per_repeat]),
        "max_abs_raw_residual": _max([float(row["max_abs_raw_residual"]) for row in per_repeat]),
        "max_abs_calibrated_residual": _max([float(row["max_abs_calibrated_residual"]) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_channel": False,
        "uses_target_config_for_channel": False,
        "uses_injected_path_for_channel": False,
        "uses_incident_start_end_for_channel": False,
        "consumes_blind_evidence": True,
        "consumes_probe_policy": True,
        "consumes_ipw_rls_residuals": True,
        "produces_calibrated_residuals": True,
        "raw_residual_directly_used_for_sparse_inversion": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_root_metric_calibrated_residual_rank_mean"] = _mean([float(row.get("debug_root_metric_calibrated_residual_rank_mean") or 0.0) for row in per_repeat])
        summary["debug_root_service_calibrated_residual_rank_mean"] = _mean([float(row.get("debug_root_service_calibrated_residual_rank_mean") or 0.0) for row in per_repeat])
    _write_json(output / "p2_evidence_channel_preview_summary.json", summary)
    return summary

"""Online Boutique adapter for A6 IPW-masked RLS preview."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.propagation.ipw_rls_online import RLSConfig, evaluate_ipw_rls_debug, run_ipw_rls_preview

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


def resolve_repeat_dirs(fault_type: str, repeat_index: int) -> dict[str, str]:
    key = fault_type.lower()
    if key not in FAULT_SOURCES:
        raise ValueError(f"unknown fault_type: {fault_type}")
    _label, raw_root = FAULT_SOURCES[key]
    repeat = f"repeat_{repeat_index:02d}"
    raw_input_dir = Path("data/p2_online_boutique") / raw_root / repeat / "raw"
    candidate_dir = Path("data/p2_online_boutique/a4_candidate_preview") / key / repeat
    probe_policy_dir = Path("data/p2_online_boutique/a5_probe_policy_preview") / key / repeat
    required = [raw_input_dir / "metrics.jsonl", candidate_dir / "repeat_candidate_summary.json", probe_policy_dir / "sampling_log.jsonl", probe_policy_dir / "observation_mask.jsonl"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing A6 input files: {missing}")
    return {"raw_input_dir": str(raw_input_dir), "candidate_dir": str(candidate_dir), "probe_policy_dir": str(probe_policy_dir)}


def run_single_ipw_rls_repeat(fault_type: str, repeat_index: int, output_root: str, config: RLSConfig | None = None, debug_evaluate_incidents: bool = False) -> dict[str, Any]:
    dirs = resolve_repeat_dirs(fault_type, repeat_index)
    out = Path(output_root) / fault_type.lower() / f"repeat_{repeat_index:02d}"
    result = run_ipw_rls_preview(dirs["raw_input_dir"], dirs["candidate_dir"], dirs["probe_policy_dir"], str(out), config)
    debug = None
    incidents_path = Path(dirs["raw_input_dir"]) / "incidents.jsonl"
    if debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_ipw_rls_debug(str(out), str(incidents_path))
        _write_json(out / "ipw_rls_debug_evaluation.json", debug)
    metadata = result["metadata"]
    row = {
        "fault_type": FAULT_SOURCES[fault_type.lower()][0],
        "fault_type_key": fault_type.lower(),
        "repeat_index": repeat_index,
        "raw_input_dir": dirs["raw_input_dir"],
        "candidate_dir": dirs["candidate_dir"],
        "probe_policy_dir": dirs["probe_policy_dir"],
        "output_dir": str(out),
        "node_count": metadata["node_count"],
        "total_updates": metadata["total_updates"],
        "skipped_updates": metadata["skipped_updates"],
        "average_abs_residual": metadata["average_abs_residual"],
        "completed": True,
    }
    if debug:
        row["debug_root_metric_residual_rank_mean"] = debug.get("root_metric_residual_rank_mean")
        row["debug_root_service_residual_rank_mean"] = debug.get("root_service_residual_rank_mean")
    return row


def run_p2_ipw_rls_preview(output_dir: str = "data/p2_online_boutique/a6_ipw_rls_preview", config: RLSConfig | None = None, debug_evaluate_incidents: bool = False) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}
    for key, (label, _raw_root) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            row = run_single_ipw_rls_repeat(key, index, str(output), config, debug_evaluate_incidents)
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": label,
            "repeats": len(rows),
            "repeats_completed": sum(1 for row in rows if row.get("completed")),
            "average_node_count": _mean([float(row["node_count"]) for row in rows]),
            "average_total_updates": _mean([float(row["total_updates"]) for row in rows]),
            "average_skipped_updates": _mean([float(row["skipped_updates"]) for row in rows]),
            "average_abs_residual": _mean([float(row["average_abs_residual"]) for row in rows]),
            "debug_root_metric_residual_rank_mean": _mean([float(row.get("debug_root_metric_residual_rank_mean") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_root_service_residual_rank_mean": _mean([float(row.get("debug_root_service_residual_rank_mean") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }
    summary = {
        "total_repeats": len(per_repeat),
        "repeats_completed": sum(1 for row in per_repeat if row.get("completed")),
        "average_node_count": _mean([float(row["node_count"]) for row in per_repeat]),
        "average_total_updates": _mean([float(row["total_updates"]) for row in per_repeat]),
        "average_skipped_updates": _mean([float(row["skipped_updates"]) for row in per_repeat]),
        "average_abs_residual": _mean([float(row["average_abs_residual"]) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_learning": False,
        "uses_target_config_for_learning": False,
        "uses_injected_path_for_learning": False,
        "uses_incident_start_end_for_learning": False,
        "consumes_sampling_probability": True,
        "consumes_observation_mask": True,
        "update_mode": "online_rls",
        "batch_ridge_used": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_root_metric_residual_rank_mean"] = _mean([float(row.get("debug_root_metric_residual_rank_mean") or 0.0) for row in per_repeat])
        summary["debug_root_service_residual_rank_mean"] = _mean([float(row.get("debug_root_service_residual_rank_mean") or 0.0) for row in per_repeat])
    _write_json(output / "p2_ipw_rls_preview_summary.json", summary)
    return summary

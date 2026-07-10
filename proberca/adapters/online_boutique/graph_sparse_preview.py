"""Online Boutique adapter for A8 graph sparse inversion preview."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.inference.graph_sparse_inversion import GraphSparseConfig, evaluate_graph_sparse_debug, run_graph_sparse_inversion

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
    candidate_dir = Path("data/p2_online_boutique/a4_candidate_preview") / key / repeat
    evidence_channel_dir = Path("data/p2_online_boutique/a7_evidence_channel_preview") / key / repeat
    raw_input_dir = Path("data/p2_online_boutique") / raw_root / repeat / "raw"
    required = [
        candidate_dir / "repeat_candidate_summary.json",
        evidence_channel_dir / "calibrated_residuals.jsonl",
        evidence_channel_dir / "evidence_channel_metadata.json",
        raw_input_dir / "incidents.jsonl",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing A8 input files: {missing}")
    return {"candidate_dir": str(candidate_dir), "evidence_channel_dir": str(evidence_channel_dir), "raw_input_dir": str(raw_input_dir)}


def run_single_graph_sparse_repeat(
    fault_type: str,
    repeat_index: int,
    output_root: str,
    config: GraphSparseConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    dirs = resolve_repeat_dirs(fault_type, repeat_index)
    key = fault_type.lower()
    out = Path(output_root) / key / f"repeat_{repeat_index:02d}"
    result = run_graph_sparse_inversion(dirs["candidate_dir"], dirs["evidence_channel_dir"], str(out), config)
    debug = None
    incidents_path = Path(dirs["raw_input_dir"]) / "incidents.jsonl"
    if debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_graph_sparse_debug(str(out), str(incidents_path))
        _write_json(out / "graph_sparse_debug_evaluation.json", debug)
    metadata = result["metadata"]
    row: dict[str, Any] = {
        "fault_type": FAULT_SOURCES[key][0],
        "fault_type_key": key,
        "repeat_index": repeat_index,
        "candidate_dir": dirs["candidate_dir"],
        "evidence_channel_dir": dirs["evidence_channel_dir"],
        "raw_input_dir": dirs["raw_input_dir"],
        "output_dir": str(out),
        "node_count": metadata["node_count"],
        "edge_count": metadata["edge_count"],
        "nonzero_intervention_count": metadata["nonzero_intervention_count"],
        "pre_sparsify_nonzero_count": metadata.get("pre_sparsify_nonzero_count"),
        "post_sparsify_nonzero_count": metadata.get("post_sparsify_nonzero_count"),
        "post_sparsify_applied": metadata.get("post_sparsify_applied"),
        "raw_metric_edge_count": metadata.get("raw_metric_edge_count"),
        "capped_metric_edge_count": metadata.get("capped_metric_edge_count"),
        "effective_lambda_l1": metadata.get("effective_lambda_l1"),
        "effective_lambda_group": metadata.get("effective_lambda_group"),
        "solver_status": metadata["solver_status"],
        "iterations": metadata["iterations"],
        "final_objective": metadata["final_objective"],
        "completed": metadata["solver_status"] != "failed_numeric",
    }
    if debug:
        row["debug_service_hit_at_1"] = debug.get("debug_service_hit_at_1")
        row["debug_metric_hit_at_3"] = debug.get("debug_metric_hit_at_3")
        row["debug_root_type_accuracy"] = debug.get("debug_root_type_match_by_metric_family")
    return row


def run_p2_graph_sparse_preview(
    output_dir: str = "data/p2_online_boutique/a8_graph_sparse_preview",
    config: GraphSparseConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}
    for key, (label, _raw_root) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            row = run_single_graph_sparse_repeat(key, index, str(output), config, debug_evaluate_incidents)
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": label,
            "repeats": len(rows),
            "repeats_completed": sum(1 for row in rows if row.get("completed")),
            "average_node_count": _mean([float(row["node_count"]) for row in rows]),
            "average_edge_count": _mean([float(row["edge_count"]) for row in rows]),
            "average_nonzero_intervention_count": _mean([float(row["nonzero_intervention_count"]) for row in rows]),
            "nonzero_ratio": (_mean([float(row["nonzero_intervention_count"]) for row in rows]) / _mean([float(row["node_count"]) for row in rows])) if _mean([float(row["node_count"]) for row in rows]) > 0 else 0.0,
            "average_pre_sparsify_nonzero_count": _mean([float(row.get("pre_sparsify_nonzero_count") or 0.0) for row in rows]),
            "average_raw_metric_edge_count": _mean([float(row.get("raw_metric_edge_count") or 0.0) for row in rows]),
            "solver_status_counts": {status: sum(1 for row in rows if row.get("solver_status") == status) for status in sorted({str(row.get("solver_status")) for row in rows})},
            "debug_service_hit_at_1": _mean([float(row.get("debug_service_hit_at_1") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_metric_hit_at_3": _mean([float(row.get("debug_metric_hit_at_3") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_root_type_accuracy": _mean([float(row.get("debug_root_type_accuracy") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }
    summary: dict[str, Any] = {
        "total_repeats": len(per_repeat),
        "repeats_completed": sum(1 for row in per_repeat if row.get("completed")),
        "average_node_count": _mean([float(row["node_count"]) for row in per_repeat]),
        "average_edge_count": _mean([float(row["edge_count"]) for row in per_repeat]),
        "average_nonzero_intervention_count": _mean([float(row["nonzero_intervention_count"]) for row in per_repeat]),
        "nonzero_ratio": (_mean([float(row["nonzero_intervention_count"]) for row in per_repeat]) / _mean([float(row["node_count"]) for row in per_repeat])) if _mean([float(row["node_count"]) for row in per_repeat]) > 0 else 0.0,
        "average_pre_sparsify_nonzero_count": _mean([float(row.get("pre_sparsify_nonzero_count") or 0.0) for row in per_repeat]),
        "average_raw_metric_edge_count": _mean([float(row.get("raw_metric_edge_count") or 0.0) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_inversion": False,
        "uses_target_config_for_inversion": False,
        "uses_injected_path_for_inversion": False,
        "uses_incident_start_end_for_inversion": False,
        "consumes_calibrated_residuals": True,
        "consumes_raw_residuals": False,
        "residual_lift_fallback_used": False,
        "optimization": "admm_graph_sparse_inversion",
        "runs_old_p1_rca_pipeline": False,
        "reinjects_faults": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_service_hit_at_1_overall"] = _mean([float(row.get("debug_service_hit_at_1") or 0.0) for row in per_repeat])
        summary["debug_metric_hit_at_3_overall"] = _mean([float(row.get("debug_metric_hit_at_3") or 0.0) for row in per_repeat])
        summary["debug_root_type_accuracy_overall"] = _mean([float(row.get("debug_root_type_accuracy") or 0.0) for row in per_repeat])
        summary["debug_per_fault_type"] = {key: {"debug_service_hit_at_1": row.get("debug_service_hit_at_1"), "debug_metric_hit_at_3": row.get("debug_metric_hit_at_3"), "debug_root_type_accuracy": row.get("debug_root_type_accuracy")} for key, row in per_fault_type.items()}
    _write_json(output / "p2_graph_sparse_preview_summary.json", summary)
    return summary

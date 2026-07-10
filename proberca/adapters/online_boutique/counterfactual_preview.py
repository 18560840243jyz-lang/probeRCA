"""Online Boutique adapter for A9 counterfactual explanation preview."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.explain.counterfactual_explanation import (
    CounterfactualConfig,
    evaluate_counterfactual_debug,
    run_counterfactual_explanation,
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


def resolve_repeat_dirs(fault_type: str, repeat_index: int) -> dict[str, str]:
    key = fault_type.lower()
    if key not in FAULT_SOURCES:
        raise ValueError(f"unknown fault_type: {fault_type}")
    _label, raw_root = FAULT_SOURCES[key]
    repeat = f"repeat_{repeat_index:02d}"
    graph_sparse_dir = Path("data/p2_online_boutique/a8r_graph_sparse_preview") / key / repeat
    candidate_dir = Path("data/p2_online_boutique/a4_candidate_preview") / key / repeat
    evidence_channel_dir = Path("data/p2_online_boutique/a7_evidence_channel_preview") / key / repeat
    raw_input_dir = Path("data/p2_online_boutique") / raw_root / repeat / "raw"
    required = [
        graph_sparse_dir / "sparse_interventions.jsonl",
        graph_sparse_dir / "metric_scores.jsonl",
        graph_sparse_dir / "service_scores.jsonl",
        graph_sparse_dir / "graph_sparse_metadata.json",
        evidence_channel_dir / "calibrated_residuals.jsonl",
        raw_input_dir / "incidents.jsonl",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing A9 input files: {missing}")
    return {
        "graph_sparse_dir": str(graph_sparse_dir),
        "candidate_dir": str(candidate_dir),
        "evidence_channel_dir": str(evidence_channel_dir),
        "raw_input_dir": str(raw_input_dir),
    }


def run_single_counterfactual_repeat(
    fault_type: str,
    repeat_index: int,
    output_root: str,
    config: CounterfactualConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    dirs = resolve_repeat_dirs(fault_type, repeat_index)
    key = fault_type.lower()
    out = Path(output_root) / key / f"repeat_{repeat_index:02d}"
    result = run_counterfactual_explanation(
        dirs["graph_sparse_dir"],
        dirs["candidate_dir"],
        dirs["evidence_channel_dir"],
        str(out),
        config,
    )
    metadata = result["metadata"]
    debug = None
    incidents_path = Path(dirs["raw_input_dir"]) / "incidents.jsonl"
    if debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_counterfactual_debug(str(out), str(incidents_path))
        _write_json(out / "counterfactual_debug_evaluation.json", debug)
    row: dict[str, Any] = {
        "fault_type": FAULT_SOURCES[key][0],
        "fault_type_key": key,
        "repeat_index": repeat_index,
        "graph_sparse_dir": dirs["graph_sparse_dir"],
        "candidate_dir": dirs["candidate_dir"],
        "evidence_channel_dir": dirs["evidence_channel_dir"],
        "raw_input_dir": dirs["raw_input_dir"],
        "output_dir": str(out),
        "metric_counterfactual_count": metadata["metric_counterfactual_count"],
        "service_counterfactual_count": metadata["service_counterfactual_count"],
        "average_metric_delta_loss": metadata["average_metric_delta_loss"],
        "average_service_delta_loss": metadata["average_service_delta_loss"],
        "max_metric_delta_loss": metadata["max_metric_delta_loss"],
        "max_service_delta_loss": metadata["max_service_delta_loss"],
        "completed": True,
        "uses_root_labels_for_counterfactual": False,
        "uses_target_config_for_counterfactual": False,
        "uses_injected_path_for_counterfactual": False,
        "uses_incident_start_end_for_counterfactual": False,
        "consumes_a8r_sparse_interventions": True,
        "reoptimizes_with_candidate_removed": True,
        "fast_approximation_only": False,
    }
    if debug:
        row["debug_counterfactual_service_hit_at_1"] = debug.get("debug_counterfactual_service_hit_at_1")
        row["debug_counterfactual_metric_hit_at_3"] = debug.get("debug_counterfactual_metric_hit_at_3")
        row["debug_counterfactual_root_type_accuracy"] = debug.get("debug_root_type_by_top_metric_family")
    return row


def run_p2_counterfactual_preview(
    output_dir: str = "data/p2_online_boutique/a9_counterfactual_preview",
    config: CounterfactualConfig | None = None,
    debug_evaluate_incidents: bool = False,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    per_repeat: list[dict[str, Any]] = []
    per_fault_type: dict[str, dict[str, Any]] = {}
    for key, (label, _raw_root) in FAULT_SOURCES.items():
        rows: list[dict[str, Any]] = []
        for index in range(1, 6):
            row = run_single_counterfactual_repeat(key, index, str(output), config, debug_evaluate_incidents)
            rows.append(row)
            per_repeat.append(row)
        per_fault_type[key] = {
            "fault_type": label,
            "repeats": len(rows),
            "repeats_completed": sum(1 for row in rows if row.get("completed")),
            "average_metric_counterfactual_count": _mean([float(row["metric_counterfactual_count"]) for row in rows]),
            "average_service_counterfactual_count": _mean([float(row["service_counterfactual_count"]) for row in rows]),
            "average_metric_delta_loss": _mean([float(row["average_metric_delta_loss"]) for row in rows]),
            "average_service_delta_loss": _mean([float(row["average_service_delta_loss"]) for row in rows]),
            "debug_counterfactual_service_hit_at_1": _mean([float(row.get("debug_counterfactual_service_hit_at_1") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_counterfactual_metric_hit_at_3": _mean([float(row.get("debug_counterfactual_metric_hit_at_3") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
            "debug_counterfactual_root_type_accuracy": _mean([float(row.get("debug_counterfactual_root_type_accuracy") or 0.0) for row in rows]) if debug_evaluate_incidents else None,
        }
    summary: dict[str, Any] = {
        "total_repeats": len(per_repeat),
        "repeats_completed": sum(1 for row in per_repeat if row.get("completed")),
        "average_metric_counterfactual_count": _mean([float(row["metric_counterfactual_count"]) for row in per_repeat]),
        "average_service_counterfactual_count": _mean([float(row["service_counterfactual_count"]) for row in per_repeat]),
        "average_metric_delta_loss": _mean([float(row["average_metric_delta_loss"]) for row in per_repeat]),
        "average_service_delta_loss": _mean([float(row["average_service_delta_loss"]) for row in per_repeat]),
        "per_fault_type": per_fault_type,
        "per_repeat": per_repeat,
        "uses_root_labels_for_counterfactual": False,
        "uses_target_config_for_counterfactual": False,
        "uses_injected_path_for_counterfactual": False,
        "uses_incident_start_end_for_counterfactual": False,
        "consumes_a8r_sparse_interventions": True,
        "reoptimizes_with_candidate_removed": True,
        "fast_approximation_only": False,
        "runs_old_p1_rca_pipeline": False,
        "reinjects_faults": False,
    }
    if debug_evaluate_incidents:
        summary["debug_only"] = True
        summary["debug_counterfactual_service_hit_at_1_overall"] = _mean([float(row.get("debug_counterfactual_service_hit_at_1") or 0.0) for row in per_repeat])
        summary["debug_counterfactual_metric_hit_at_3_overall"] = _mean([float(row.get("debug_counterfactual_metric_hit_at_3") or 0.0) for row in per_repeat])
        summary["debug_counterfactual_root_type_accuracy_overall"] = _mean([float(row.get("debug_counterfactual_root_type_accuracy") or 0.0) for row in per_repeat])
        summary["debug_per_fault_type"] = {key: {"debug_counterfactual_service_hit_at_1": row.get("debug_counterfactual_service_hit_at_1"), "debug_counterfactual_metric_hit_at_3": row.get("debug_counterfactual_metric_hit_at_3"), "debug_counterfactual_root_type_accuracy": row.get("debug_counterfactual_root_type_accuracy")} for key, row in per_fault_type.items()}
    _write_json(output / "p2_counterfactual_preview_summary.json", summary)
    return summary

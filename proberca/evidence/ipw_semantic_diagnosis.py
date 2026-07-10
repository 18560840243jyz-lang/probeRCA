"""Diagnosis helpers for P1D semantic sibling metric errors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from proberca.data.io import read_jsonl
from proberca.evidence.semantic import metric_to_evidence_type


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _required(input_path: Path, name: str) -> Path:
    path = input_path / name
    if not path.exists():
        raise FileNotFoundError(f"missing required P1D diagnosis input: {path}")
    return path


def _top5(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda row: int(row.get("semantic_rank", 10**9)))[:5]


def _node(service: str, metric: str) -> str:
    return f"{service}.{metric}"


def diagnose_ipw_semantic_sibling_errors(input_dir: str | Path) -> dict:
    """Diagnose sibling metric errors from existing P1D outputs.

    True root labels are read only for synthetic debug diagnosis and are never
    fed back into scoring.
    """

    input_path = Path(input_dir)
    semantic_rows = read_jsonl(_required(input_path, "ipw_semantic_interventions.jsonl"))
    summary = _load_json(_required(input_path, "ipw_semantic_evidence_summary.json"))
    sparse_rows = read_jsonl(_required(input_path, "ipw_sparse_interventions.jsonl"))
    incidents = read_jsonl(_required(input_path, "incidents.jsonl"))

    sparse_by_node = {
        (str(row.get("incident_id")), str(row.get("node"))): row for row in sparse_rows
    }
    semantic_by_incident: dict[str, list[dict]] = {}
    for row in semantic_rows:
        semantic_by_incident.setdefault(str(row.get("incident_id")), []).append(row)

    failed_top1_incidents: list[str] = []
    same_service_sibling_errors: list[dict] = []
    same_type_sibling_errors: list[dict] = []
    per_incident_top5: list[dict] = []
    per_incident_failures: list[dict] = []

    for incident in incidents:
        incident_id = str(incident["incident_id"])
        root_service = str(incident["root_service"])
        root_metric = str(incident["root_metric"])
        root_node = _node(root_service, root_metric)
        top_rows = _top5(semantic_by_incident.get(incident_id, []))
        true_row = next((row for row in semantic_by_incident.get(incident_id, []) if row.get("node") == root_node), None)
        predicted_top1 = top_rows[0] if top_rows else None
        predicted_node = str(predicted_top1.get("node", "")) if predicted_top1 else ""
        predicted_metric = str(predicted_top1.get("metric", "")) if predicted_top1 else ""
        predicted_service = str(predicted_top1.get("service", "")) if predicted_top1 else ""
        true_rank = int(true_row["semantic_rank"]) if true_row else None

        item = {
            "incident_id": incident_id,
            "true_root_metric_debug": root_node,
            "predicted_top1_metric": predicted_node,
            "true_root_semantic_rank_debug": true_rank,
            "top5_metrics": [str(row.get("node", "")) for row in top_rows],
            "top5_semantic_scores": [float(row.get("semantic_score", 0.0)) for row in top_rows],
            "top5_sparse_scores": [float(row.get("sparse_score", 0.0)) for row in top_rows],
            "top5_evidence_types": [str(row.get("evidence_type", "")) for row in top_rows],
            "top5_specificity_weights": [float(row.get("specificity_weight", 0.0)) for row in top_rows],
            "top5_anchor_bonus": [float(row.get("semantic_anchor_bonus", 0.0)) for row in top_rows],
            "top5_diagnostic_priority_bonus": [float(row.get("diagnostic_priority_bonus", 0.0)) for row in top_rows],
        }
        per_incident_top5.append(item)

        if predicted_node != root_node:
            failed_top1_incidents.append(incident_id)
            failure_patterns: list[str] = []
            if predicted_service == root_service and predicted_metric != root_metric:
                failure_patterns.append("same_service_sibling_error")
                same_service_sibling_errors.append(item)
            if metric_to_evidence_type(predicted_metric) == metric_to_evidence_type(root_metric) and predicted_metric != root_metric:
                failure_patterns.append("same_type_sibling_error")
                same_type_sibling_errors.append(item)
            if not failure_patterns:
                failure_patterns.append("other_top1_error")
            sparse_true = sparse_by_node.get((incident_id, root_node), {})
            item["failure_patterns"] = failure_patterns
            item["true_root_sparse_rank_debug"] = sparse_true.get("rank")
            per_incident_failures.append(item)

    parent = input_path.parent
    full_vs_ablation_available = {
        "no_specificity": (parent / f"{input_path.name}_semantic_no_specificity" / "ipw_semantic_evidence_summary.json").exists(),
        "no_anchor": (parent / f"{input_path.name}_semantic_no_anchor" / "ipw_semantic_evidence_summary.json").exists(),
    }
    result = {
        "failed_top1_incidents": failed_top1_incidents,
        "same_service_sibling_errors": same_service_sibling_errors,
        "same_type_sibling_errors": same_type_sibling_errors,
        "per_incident_top5": per_incident_top5,
        "per_incident_failures": per_incident_failures,
        "full_vs_ablation_available": full_vs_ablation_available,
        "summary_debug": {
            "mean_true_root_semantic_rank_debug": summary.get("mean_true_root_semantic_rank_debug"),
            "metric_hit_at_1_debug": summary.get("metric_hit_at_1_debug"),
            "metric_hit_at_3_debug": summary.get("metric_hit_at_3_debug"),
        },
    }
    output_path = input_path / "ipw_semantic_sibling_diagnosis.json"
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    result["output_path"] = str(output_path)
    return result

"""Build final P1 RCAResult records from P1A-P1E outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON file is not an object: {path}")
    return data


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict], dict, dict, dict, dict, dict, list[dict]]:
    """Load all P1F inputs from a P1 demo directory."""

    input_path = Path(input_dir)
    required = {
        "ipw_semantic_interventions": input_path / "ipw_semantic_interventions.jsonl",
        "ipw_semantic_type_scores": input_path / "ipw_semantic_type_scores.jsonl",
        "ipw_path_explanations": input_path / "ipw_path_explanations.jsonl",
        "ipw_semantic_evidence_summary": input_path / "ipw_semantic_evidence_summary.json",
        "ipw_path_explanation_summary": input_path / "ipw_path_explanation_summary.json",
        "adaptive_observation_metadata": input_path / "adaptive_observation_metadata.json",
        "ipw_propagation_metadata": input_path / "ipw_propagation_metadata.json",
        "ipw_sparse_inversion_summary": input_path / "ipw_sparse_inversion_summary.json",
        "incidents": input_path / "incidents.jsonl",
    }
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required P1F input for {name}: {path}")
    return (
        read_jsonl(required["ipw_semantic_interventions"]),
        read_jsonl(required["ipw_semantic_type_scores"]),
        read_jsonl(required["ipw_path_explanations"]),
        _load_json(required["ipw_semantic_evidence_summary"]),
        _load_json(required["ipw_path_explanation_summary"]),
        _load_json(required["adaptive_observation_metadata"]),
        _load_json(required["ipw_propagation_metadata"]),
        _load_json(required["ipw_sparse_inversion_summary"]),
        read_jsonl(required["incidents"]),
    )


def build_top_metrics(semantic_records: list[dict], top_k: int = 5) -> list[dict]:
    """Build Top-K metric candidates from one incident's semantic records."""

    rows = sorted(semantic_records, key=lambda row: (int(row["semantic_rank"]), str(row["node"])))[:top_k]
    return [
        {
            "node": str(row["node"]),
            "service": str(row["service"]),
            "metric": str(row["metric"]),
            "score": float(row["semantic_score"]),
            "rank": int(row["semantic_rank"]),
            "evidence_type": str(row.get("evidence_type", "Unknown")),
            "evidence_score": float(row.get("evidence_score", 0.0)),
            "sparse_score": float(row.get("sparse_score", 0.0)),
            "semantic_score": float(row.get("semantic_score", 0.0)),
            "confidence": float(row.get("confidence", 0.0)),
            "low_confidence": bool(row.get("low_confidence", False)),
        }
        for row in rows
    ]


def build_top_services(top_metrics: list[dict]) -> list[dict]:
    """Aggregate metric candidates to service candidates using max semantic score."""

    best_by_service: dict[str, dict] = {}
    for metric in top_metrics:
        service = str(metric["service"])
        score = float(metric["semantic_score"])
        current = best_by_service.get(service)
        if current is None or score > float(current["score"]) or (score == float(current["score"]) and str(metric["metric"]) < str(current["best_metric"])):
            best_by_service[service] = {"service": service, "score": score, "best_metric": str(metric["metric"])}
    return sorted(best_by_service.values(), key=lambda item: (-float(item["score"]), item["service"]))


def select_root_type(type_records: list[dict]) -> str:
    """Select the rank-1 P1 semantic type candidate."""

    rows = sorted(type_records, key=lambda row: (int(row.get("rank", 10**9)), str(row.get("root_type_candidate", "unknown"))))
    return str(rows[0].get("root_type_candidate", "unknown")) if rows else "unknown"


def select_path(path_records: list[dict]) -> dict:
    """Select the semantic-rank-1 path record, or the highest scoring path."""

    if not path_records:
        return {
            "candidate_node": "",
            "path_services": [],
            "path_edges": [],
            "path_score": 0.0,
            "path_missing": True,
            "propagation_support": 0.0,
            "reason": "path_missing",
        }
    rank_one = [row for row in path_records if int(row.get("semantic_rank", 10**9)) == 1]
    source_rows = rank_one if rank_one else path_records
    selected = sorted(source_rows, key=lambda row: (-float(row.get("path_score", 0.0)), int(row.get("path_length", 0)), str(row.get("candidate_node", ""))))[0]
    return {
        "candidate_node": str(selected.get("candidate_node", "")),
        "path_services": [str(item) for item in selected.get("path_services", [])],
        "path_edges": list(selected.get("path_edges", [])),
        "path_score": float(selected.get("path_score", 0.0)),
        "path_missing": bool(selected.get("path_missing", False)),
        "propagation_support": float(selected.get("propagation_support", 0.0)),
        "reason": str(selected.get("reason", "unknown")),
    }


def _observation_summary(observation_metadata: dict, propagation_metadata: dict) -> dict:
    return {
        "observed_ratio": float(observation_metadata.get("observed_ratio", 0.0)),
        "observed_records": int(observation_metadata.get("observed_records", 0)),
        "total_records": int(observation_metadata.get("total_records", 0)),
        "mean_sampling_probability": float(propagation_metadata.get("mean_sampling_probability", 0.0)),
        "mean_ipw_weight": float(propagation_metadata.get("mean_ipw_weight", 0.0)),
    }


def build_p1_result_for_incident(
    semantic_records: list[dict],
    type_records: list[dict],
    path_records: list[dict],
    incident: dict,
    observation_metadata: dict,
    propagation_metadata: dict,
    top_k: int = 5,
) -> dict:
    """Build one final P1 RCAResult without adding synthetic true labels."""

    incident_id = str(incident["incident_id"])
    current_semantic = [row for row in semantic_records if str(row.get("incident_id")) == incident_id]
    current_types = [row for row in type_records if str(row.get("incident_id")) == incident_id]
    current_paths = [row for row in path_records if str(row.get("incident_id")) == incident_id]
    if not current_semantic:
        raise ValueError(f"no P1 semantic records found for incident_id={incident_id}")

    top_metrics = build_top_metrics(current_semantic, top_k)
    top_services = build_top_services(top_metrics)
    root_type = select_root_type(current_types)
    selected_path = select_path(current_paths)
    top_metric = top_metrics[0]
    observation = _observation_summary(observation_metadata, propagation_metadata)
    low_confidence = bool(top_metric.get("low_confidence", False)) or bool(selected_path["path_missing"])
    evidence = {
        "top_metric_node": top_metric["node"],
        "top_metric_evidence_type": top_metric["evidence_type"],
        "top_metric_semantic_score": float(top_metric["semantic_score"]),
        "top_metric_sparse_score": float(top_metric["sparse_score"]),
        "top_metric_confidence": float(top_metric["confidence"]),
        "explanation": f"P1 IPW RCA selected {top_metric['node']} with semantic evidence type {top_metric['evidence_type']} and path reason {selected_path['reason']}.",
    }
    confidence = {
        "top_metric_confidence": float(top_metric["confidence"]),
        "path_missing": bool(selected_path["path_missing"]),
        "low_confidence": low_confidence,
    }
    return {
        "incident_id": incident_id,
        "symptom_service": str(incident["symptom_service"]),
        "top_services": top_services,
        "top_metrics": top_metrics,
        "root_type": root_type,
        "evidence": evidence,
        "path": selected_path,
        "observation": observation,
        "confidence": confidence,
        "latency_ms": None,
        "source": "p1_ipw_end_to_end_rca",
    }


def build_p1_results(input_dir: str | Path, output_dir: str | Path | None = None, top_k: int = 5) -> dict:
    """Build final P1 RCAResult records for all incidents."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    output_path.mkdir(parents=True, exist_ok=True)
    (
        semantic_records,
        type_records,
        path_records,
        _semantic_summary,
        _path_summary,
        observation_metadata,
        propagation_metadata,
        _sparse_summary,
        incidents,
    ) = load_required_dataset(input_path)

    results = [
        build_p1_result_for_incident(
            semantic_records,
            type_records,
            path_records,
            incident,
            observation_metadata,
            propagation_metadata,
            top_k=top_k,
        )
        for incident in incidents
    ]
    observation = _observation_summary(observation_metadata, propagation_metadata)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "results_count": len(results),
        "top_k": top_k,
        "source": "p1_ipw_end_to_end_rca",
        "observed_ratio": observation["observed_ratio"],
        "mean_sampling_probability": observation["mean_sampling_probability"],
        "mean_ipw_weight": observation["mean_ipw_weight"],
    }
    results_path = output_path / "p1_results.jsonl"
    metadata_path = output_path / "p1_results_metadata.json"
    write_jsonl(results_path, results)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "p1_results_path": str(results_path),
        "p1_results_metadata_path": str(metadata_path),
        "results": results,
        "metadata": metadata,
    }

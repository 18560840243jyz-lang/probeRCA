"""Run P2A-2 real Online Boutique CPU injection data through the frozen P1 pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.p1_bridge import (
    build_real_observation_files,
    load_real_ob_dataset,
    refresh_real_observation_from_normalized,
)
from proberca.data.io import read_jsonl
from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence
from proberca.eval.p1_metrics import evaluate_p1_results
from proberca.eval.p1_result import build_p1_results
from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


def validate_real_cpu_input(input_dir: str | Path) -> dict[str, Any]:
    """Validate P2A-1R data quality before running RCA."""

    dataset = load_real_ob_dataset(input_dir)
    quality = dataset["data_quality_report"]
    required_true = [
        "root_service_metric_coverage_passed",
        "paymentservice_cpu_metric_present",
        "paymentservice_throttled_metric_present",
        "frontend_latency_metric_present",
        "cadvisor_metrics_available",
        "fault_injection_succeeded",
        "restore_succeeded",
    ]
    failed = [key for key in required_true if quality.get(key) is not True]
    if failed:
        raise ValueError(f"real CPU input failed quality gates: {failed}")
    if not dataset["metrics"]:
        raise ValueError("real CPU input metrics.jsonl is empty")
    if len(dataset["incidents"]) != 1:
        raise ValueError(f"P2A-2 expects exactly one real incident, got {len(dataset['incidents'])}")
    return dataset


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON file is not an object: {path}")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_result_summary(results: list[dict], evaluation: dict[str, Any]) -> dict[str, Any]:
    result = results[0] if results else {}
    per = evaluation.get("per_incident", [{}])[0] if evaluation.get("per_incident") else {}
    top_metrics = result.get("top_metrics", [])
    top_services = result.get("top_services", [])
    return {
        "predicted_top1_service": str(top_services[0].get("service", "")) if top_services else "",
        "predicted_top1_metric": str(top_metrics[0].get("node", "")) if top_metrics else "",
        "predicted_root_type": str(result.get("root_type", "unknown")),
        "true_root_service_debug": per.get("true_root_service_debug"),
        "true_root_metric_debug": per.get("true_root_metric_debug"),
        "metric_rank_debug": per.get("metric_rank_debug"),
        "path_services": result.get("path", {}).get("path_services", []),
    }


def run_p2a2_real_cpu_rca(
    input_dir: str = "data/p2_online_boutique/cpu_paymentservice_001_cadvisor",
    output_dir: str = "data/p2_online_boutique/cpu_paymentservice_001_p1rca",
    top_k: int = 5,
) -> dict[str, Any]:
    """Run one real CPU injection dataset through the P1B-P1F pipeline."""

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    dataset = validate_real_cpu_input(input_path)
    quality = dataset["data_quality_report"]
    output_path.mkdir(parents=True, exist_ok=True)

    bridge_result = build_real_observation_files(input_path, output_path, sampling_probability=1.0)
    normalize_result = normalize_dataset(output_path, output_path)
    refreshed_observation = refresh_real_observation_from_normalized(output_path, sampling_probability=1.0)
    propagation_result = train_ipw_masked_propagation(output_path, output_path, IPWPropagationConfig())
    sparse_result = solve_ipw_sparse_inversion(output_path, output_path, IPWSparseInversionConfig())
    semantic_result = score_ipw_semantic_evidence(output_path, output_path, IPWSemanticEvidenceConfig())
    path_result = explain_ipw_paths(output_path, output_path, IPWPathExplanationConfig(top_k_candidates=top_k))
    p1_result = build_p1_results(output_path, output_path, top_k=top_k)

    incidents = read_jsonl(output_path / "incidents.jsonl")
    results = read_jsonl(output_path / "p1_results.jsonl")
    path_summary = _load_json(output_path / "ipw_path_explanation_summary.json")
    evaluation = evaluate_p1_results(results, incidents, path_summary=path_summary)
    evaluation_path = output_path / "p1_evaluation_summary.json"
    _write_json(evaluation_path, evaluation)

    first = _first_result_summary(results, evaluation)
    summary = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "real_collection": True,
        "incident_count": int(evaluation["incidents_count"]),
        "service_hit_at_1": float(evaluation["service_hit_at_1"]),
        "service_hit_at_3": float(evaluation["service_hit_at_3"]),
        "metric_hit_at_1": float(evaluation["metric_hit_at_1"]),
        "metric_hit_at_3": float(evaluation["metric_hit_at_3"]),
        "metric_mrr": float(evaluation["metric_mrr"]),
        "root_type_accuracy": float(evaluation["root_type_accuracy"]),
        "path_fidelity": float(evaluation["path_fidelity"]),
        "observed_ratio": float(evaluation.get("observed_ratio", 0.0)),
        "root_service_metric_coverage_passed": bool(quality.get("root_service_metric_coverage_passed")),
        "paymentservice_throttled_metric_present": bool(quality.get("paymentservice_throttled_metric_present")),
        "frontend_latency_lift_debug": float(quality.get("frontend_faulty_p99_mean", 0.0)) - float(quality.get("frontend_baseline_p99_mean", 0.0)),
        "paymentservice_throttling_lift_debug": float(quality.get("paymentservice_throttling_lift", 0.0)),
        **first,
    }
    summary_path = output_path / "real_p1_rca_summary.json"
    _write_json(summary_path, summary)

    generated_files = sorted(str(path.relative_to(output_path)) for path in output_path.iterdir() if path.is_file())
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "top_k": int(top_k),
        "real_collection": True,
        "pipeline_steps": [
            "build_real_observation_files",
            "normalize_dataset",
            "refresh_real_observation_from_normalized",
            "train_ipw_masked_propagation",
            "solve_ipw_sparse_inversion",
            "score_ipw_semantic_evidence",
            "explain_ipw_paths",
            "build_p1_results",
            "evaluate_p1_results",
        ],
        "generated_files": generated_files,
        "note": "P2A-2 single real CPU injection case; not multi-fault accuracy.",
    }
    metadata_path = output_path / "real_p1_rca_metadata.json"
    _write_json(metadata_path, metadata)
    return {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "summary": summary,
        "metadata": metadata,
        "evaluation": evaluation,
        "results": results,
        "step_outputs": {
            "build_real_observation_files": bridge_result,
            "normalize_dataset": normalize_result.get("metadata", {}),
            "refresh_real_observation_from_normalized": refreshed_observation,
            "train_ipw_masked_propagation": propagation_result.get("metadata", {}),
            "solve_ipw_sparse_inversion": sparse_result.get("metadata", {}),
            "score_ipw_semantic_evidence": semantic_result.get("metadata", {}),
            "explain_ipw_paths": path_result.get("metadata", {}),
            "build_p1_results": p1_result.get("metadata", {}),
        },
        "real_p1_rca_summary_path": str(summary_path),
        "real_p1_rca_metadata_path": str(metadata_path),
        "p1_evaluation_summary_path": str(evaluation_path),
    }

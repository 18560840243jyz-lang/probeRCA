"""P1 full audit and failure analysis utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.eval.p1_experiment import run_p1f_experiment

_DEFAULT_SEEDS = [1, 2, 3, 4, 5, 7, 11, 13, 17, 19]
_QUICK_SEEDS = [1, 2]

_SCAN_FILES = [
    "proberca/observation/adaptive.py",
    "proberca/propagation/ipw.py",
    "proberca/inference/ipw_sparse.py",
    "proberca/evidence/ipw_semantic.py",
    "proberca/explain/ipw_path.py",
    "proberca/eval/p1_result.py",
]

_LARGE_INTERMEDIATES = {
    "metrics.jsonl",
    "normalized_metrics.jsonl",
    "observed_metrics.jsonl",
    "sampling_log.jsonl",
    "observation_mask.jsonl",
    "ipw_stable_residuals.jsonl",
    "ipw_stable_propagation_model.json",
    "ipw_sparse_interventions.jsonl",
    "ipw_semantic_interventions.jsonl",
    "ipw_path_explanations.jsonl",
    "robust_stats.jsonl",
}

_KEEP_FILES = {
    "p1_results.jsonl",
    "p1_results_metadata.json",
    "p1_evaluation_summary.json",
    "p1_experiment_metadata.json",
    "adaptive_observation_metadata.json",
    "ipw_propagation_metadata.json",
    "ipw_sparse_inversion_summary.json",
    "ipw_semantic_evidence_summary.json",
    "ipw_path_explanation_summary.json",
    "incidents.jsonl",
    "metadata.json",
    "service_graph.jsonl",
}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def scan_p1_label_leakage() -> dict:
    """Conservatively scan P1 scoring/result files for root-label usage."""

    suspicious_lines: list[dict] = []
    scoring_terms = ("score", "rank", "semantic_score", "path_score", "intervention_score", "RCAResult", "top_services", "top_metrics")
    root_terms = ("root_service", "root_metric", "root_type")
    allowed_context = ("true_root", "true_node", "debug", "summary", "synthetic", "label", "root_type_candidate", "select_root_type")
    for file_name in _SCAN_FILES:
        path = Path(file_name)
        if not path.exists():
            suspicious_lines.append({"file": file_name, "line": 0, "text": "missing scanned file"})
            continue
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            text = line.strip()
            lower = text.lower()
            if not any(term in lower for term in root_terms):
                continue
            if any(token in lower for token in allowed_context):
                continue
            if any(term.lower() in lower for term in scoring_terms) or "incident[" in lower or "incident.get" in lower:
                suspicious_lines.append({"file": file_name, "line": line_no, "text": text})
    suspicious_files = sorted({item["file"] for item in suspicious_lines})
    return {
        "label_leakage_passed": len(suspicious_lines) == 0,
        "suspicious_files": suspicious_files,
        "suspicious_lines": suspicious_lines,
    }


def cleanup_large_p1_intermediates(run_dir: str | Path) -> dict:
    """Remove large P1 intermediates from one seed directory while preserving summaries."""

    path = Path(run_dir)
    deleted: list[str] = []
    bytes_deleted = 0
    if not path.exists():
        return {"run_dir": str(path), "deleted_files": deleted, "bytes_deleted": bytes_deleted}
    for file_path in path.iterdir():
        if not file_path.is_file():
            continue
        name = file_path.name
        if name in _KEEP_FILES:
            continue
        if name in _LARGE_INTERMEDIATES:
            size = file_path.stat().st_size
            file_path.unlink()
            deleted.append(str(file_path))
            bytes_deleted += size
    return {"run_dir": str(path), "deleted_files": deleted, "bytes_deleted": bytes_deleted}


def _mean_min_max(values: list[float]) -> dict[str, float]:
    arr = np.asarray(values, dtype=float)
    return {"mean": float(np.mean(arr)), "min": float(np.min(arr)), "max": float(np.max(arr))}


def run_p1_multi_seed_audit(output_base: str | Path, seeds: list[int] | None = None, cleanup: bool = True) -> dict:
    """Run P1F for multiple seeds and aggregate single-seed metrics."""

    base = Path(output_base)
    seeds = seeds or _DEFAULT_SEEDS
    multi_dir = base / "multi_seed"
    multi_dir.mkdir(parents=True, exist_ok=True)
    per_seed: list[dict] = []
    cleanup_reports: list[dict] = []

    for seed in seeds:
        run_dir = multi_dir / f"seed_{seed}"
        result = run_p1f_experiment(output_dir=str(run_dir), seed=seed, top_k=5)
        evaluation = result["evaluation"]
        row = {
            "seed": seed,
            "run_dir": str(run_dir),
            "service_hit_at_1": float(evaluation["service_hit_at_1"]),
            "service_hit_at_3": float(evaluation["service_hit_at_3"]),
            "metric_hit_at_1": float(evaluation["metric_hit_at_1"]),
            "metric_hit_at_3": float(evaluation["metric_hit_at_3"]),
            "metric_mrr": float(evaluation["metric_mrr"]),
            "root_type_accuracy": float(evaluation["root_type_accuracy"]),
            "path_fidelity": float(evaluation["path_fidelity"]),
            "observed_ratio": float(evaluation["observed_ratio"]),
        }
        per_seed.append(row)
        if cleanup:
            cleanup_reports.append(cleanup_large_p1_intermediates(run_dir))

    metrics = [
        "service_hit_at_1",
        "service_hit_at_3",
        "metric_hit_at_1",
        "metric_hit_at_3",
        "metric_mrr",
        "root_type_accuracy",
        "path_fidelity",
        "observed_ratio",
    ]
    aggregate = {metric: _mean_min_max([row[metric] for row in per_seed]) for metric in metrics}
    return {"seeds": seeds, "per_seed": per_seed, "aggregate": aggregate, "cleanup_reports": cleanup_reports}


def run_p1_observation_audit(multi_seed_results: dict) -> dict:
    """Check that P1 remains partial-observation rather than full or too sparse."""

    observed = multi_seed_results["aggregate"]["observed_ratio"]
    observed_ratio_mean = float(observed["mean"])
    observed_ratio_min = float(observed["min"])
    observed_ratio_max = float(observed["max"])
    passed = observed_ratio_min >= 0.45 and observed_ratio_max <= 0.80 and 0.50 <= observed_ratio_mean <= 0.70
    return {
        "observed_ratio_mean": observed_ratio_mean,
        "observed_ratio_min": observed_ratio_min,
        "observed_ratio_max": observed_ratio_max,
        "observation_audit_passed": passed,
    }


def analyze_p1_failures(output_base: str | Path) -> dict:
    """Analyze per-seed and per-incident P1 failures from audit outputs."""

    base = Path(output_base)
    multi_dir = base / "multi_seed"
    failed_seeds_metric_hit_at_1: list[int] = []
    failed_incidents_metric_top1: list[dict] = []
    per_seed_metrics: list[dict] = []
    per_incident_failures: list[dict] = []

    for summary_path in sorted(multi_dir.glob("seed_*/p1_evaluation_summary.json")):
        seed_text = summary_path.parent.name.replace("seed_", "")
        try:
            seed = int(seed_text)
        except ValueError:
            seed = -1
        summary = _load_json(summary_path)
        seed_metrics = {
            "seed": seed,
            "service_hit_at_1": summary.get("service_hit_at_1"),
            "metric_hit_at_1": summary.get("metric_hit_at_1"),
            "metric_hit_at_3": summary.get("metric_hit_at_3"),
            "metric_mrr": summary.get("metric_mrr"),
            "root_type_accuracy": summary.get("root_type_accuracy"),
            "path_fidelity": summary.get("path_fidelity"),
            "observed_ratio": summary.get("observed_ratio"),
        }
        per_seed_metrics.append(seed_metrics)
        if float(summary.get("metric_hit_at_1", 0.0)) < 1.0:
            failed_seeds_metric_hit_at_1.append(seed)
        for item in summary.get("per_incident", []):
            patterns: list[str] = []
            if item.get("metric_rank_debug") is None or int(item.get("metric_rank_debug", 10**9)) > 1:
                patterns.append("metric_top1_failure")
            if item.get("service_rank_debug") is None or int(item.get("service_rank_debug", 10**9)) > 1:
                patterns.append("service_top1_failure")
            if item.get("path_intersects_injected_path_debug") is False:
                patterns.append("path_failure")
            if patterns:
                failure = {"seed": seed, "failure_patterns": patterns, **item}
                per_incident_failures.append(failure)
                if "metric_top1_failure" in patterns:
                    failed_incidents_metric_top1.append(failure)
    return {
        "failed_seeds_metric_hit_at_1": sorted(set(failed_seeds_metric_hit_at_1)),
        "failed_incidents_metric_top1": failed_incidents_metric_top1,
        "per_seed_metrics": sorted(per_seed_metrics, key=lambda row: row["seed"]),
        "per_incident_failures": per_incident_failures,
    }


def run_p1_audit(output_dir: str | Path, quick: bool = False) -> dict:
    """Run P1 label scan, multi-seed audit, observation audit, and failure analysis."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    seeds = _QUICK_SEEDS if quick else _DEFAULT_SEEDS
    label_scan = scan_p1_label_leakage()
    multi = run_p1_multi_seed_audit(output_path, seeds=seeds, cleanup=True)
    observation = run_p1_observation_audit(multi)
    failures = analyze_p1_failures(output_path)
    aggregate = multi["aggregate"]

    summary = {
        "label_leakage_passed": bool(label_scan["label_leakage_passed"]),
        "suspicious_files": label_scan["suspicious_files"],
        "multi_seed_mean_service_hit_at_1": aggregate["service_hit_at_1"]["mean"],
        "multi_seed_min_service_hit_at_1": aggregate["service_hit_at_1"]["min"],
        "multi_seed_mean_metric_hit_at_1": aggregate["metric_hit_at_1"]["mean"],
        "multi_seed_min_metric_hit_at_1": aggregate["metric_hit_at_1"]["min"],
        "multi_seed_mean_metric_hit_at_3": aggregate["metric_hit_at_3"]["mean"],
        "multi_seed_min_metric_hit_at_3": aggregate["metric_hit_at_3"]["min"],
        "multi_seed_mean_metric_mrr": aggregate["metric_mrr"]["mean"],
        "multi_seed_min_metric_mrr": aggregate["metric_mrr"]["min"],
        "multi_seed_mean_root_type_accuracy": aggregate["root_type_accuracy"]["mean"],
        "multi_seed_min_root_type_accuracy": aggregate["root_type_accuracy"]["min"],
        "multi_seed_mean_path_fidelity": aggregate["path_fidelity"]["mean"],
        "multi_seed_min_path_fidelity": aggregate["path_fidelity"]["min"],
        "observed_ratio_mean": observation["observed_ratio_mean"],
        "observed_ratio_min": observation["observed_ratio_min"],
        "observed_ratio_max": observation["observed_ratio_max"],
        "observation_audit_passed": observation["observation_audit_passed"],
    }
    summary["audit_passed"] = bool(
        summary["label_leakage_passed"]
        and summary["multi_seed_min_service_hit_at_1"] >= 0.75
        and summary["multi_seed_mean_metric_hit_at_1"] >= 0.75
        and summary["multi_seed_min_metric_hit_at_3"] >= 0.75
        and summary["multi_seed_mean_metric_mrr"] >= 0.80
        and summary["multi_seed_min_root_type_accuracy"] >= 0.75
        and summary["multi_seed_min_path_fidelity"] >= 0.75
        and summary["observation_audit_passed"]
    )

    metadata = {
        "output_dir": str(output_path),
        "quick": quick,
        "seeds": seeds,
        "multi_seed_dir": str(output_path / "multi_seed"),
        "cleanup_enabled": True,
        "label_scan": label_scan,
        "multi_seed_results": multi,
        "observation_audit": observation,
    }
    _write_json(output_path / "p1_audit_summary.json", summary)
    _write_json(output_path / "p1_audit_metadata.json", metadata)
    _write_json(output_path / "p1_failure_analysis.json", failures)
    return {"summary": summary, "metadata": metadata, "failure_analysis": failures}

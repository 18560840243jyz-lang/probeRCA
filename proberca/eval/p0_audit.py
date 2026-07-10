"""P0 sanity audit for label leakage, robustness, ablation, and noise sensitivity."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.semantic import canonical_root_type, metric_to_evidence_type, score_semantic_evidence
from proberca.eval.metrics import evaluate_results
from proberca.eval.p0_experiment import run_p0_experiment
from proberca.eval.p0_result import build_p0_results
from proberca.explain.path import explain_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.sparse import solve_sparse_inversion
from proberca.propagation.stable import train_stable_propagation

DEFAULT_SCAN_FILES = [
    "proberca/features/robust.py",
    "proberca/propagation/stable.py",
    "proberca/inference/sparse.py",
    "proberca/evidence/semantic.py",
    "proberca/explain/path.py",
    "proberca/eval/p0_result.py",
]
DEFAULT_SEEDS = [1, 2, 3, 4, 5, 7, 11, 13, 17, 19]
DEFAULT_NOISE_STDS = [0.02, 0.05, 0.1, 0.2]


def scan_for_label_leakage(paths: list[str] | None = None) -> dict:
    """Conservatively scan core P0 files for root-label use in scoring code."""

    scan_paths = paths or DEFAULT_SCAN_FILES
    root_markers = ["root_service", "root_metric", "root_type"]
    scoring_markers = ["score", "rank", "semantic_score", "intervention_score", "path_score", "sort", "lambda"]
    allowed_markers = [
        "true_root",
        "debug",
        "summary",
        "evaluation",
        "canonical_type",
        "root_type = str(type_sorted",
        "root_type\": root_type",
        "root_type_candidate",
        "supporting",
        "docstring",
    ]
    suspicious_lines: list[dict] = []
    suspicious_files: set[str] = set()

    for file_name in scan_paths:
        path = Path(file_name)
        if not path.exists():
            suspicious_files.add(file_name)
            suspicious_lines.append({"file": file_name, "line_number": 0, "line": "missing file"})
            continue
        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            lowered = line.lower()
            if not any(marker in lowered for marker in root_markers):
                continue
            if any(marker in lowered for marker in allowed_markers):
                continue
            has_incident_root_ref = "incident[" in lowered or "incident.get" in lowered
            has_scoring_context = any(marker in lowered for marker in scoring_markers)
            if has_incident_root_ref and has_scoring_context:
                suspicious_files.add(file_name)
                suspicious_lines.append({"file": file_name, "line_number": line_number, "line": line})

    return {
        "suspicious_files": sorted(suspicious_files),
        "suspicious_lines": suspicious_lines,
        "passed": not suspicious_lines,
    }


def _summarize_metric(values: list[float]) -> dict:
    arr = np.asarray(values, dtype=float)
    return {"mean": float(np.mean(arr)), "min": float(np.min(arr)), "max": float(np.max(arr))}


def _metric_subset(summary: dict) -> dict:
    keys = [
        "service_hit_at_1",
        "service_hit_at_3",
        "metric_hit_at_1",
        "metric_hit_at_3",
        "root_type_accuracy",
        "path_fidelity",
    ]
    return {key: float(summary[key]) for key in keys}


def _prune_heavy_dataset_files(dataset_dir: Path) -> None:
    """Remove large intermediate files after audit metrics are written."""

    heavy_files = [
        "metrics.jsonl",
        "normalized_metrics.jsonl",
        "stable_residuals.jsonl",
        "robust_stats.jsonl",
        "stable_propagation_model.json",
    ]
    for file_name in heavy_files:
        file_path = dataset_dir / file_name
        if file_path.exists():
            file_path.unlink()




def run_multi_seed_audit(output_base: str, seeds: list[int] | None = None) -> dict:
    """Run the full P0 pipeline over multiple random seeds."""

    selected_seeds = seeds or DEFAULT_SEEDS
    base = Path(output_base)
    base.mkdir(parents=True, exist_ok=True)
    runs: list[dict] = []
    for seed in selected_seeds:
        output_dir = base / f"seed_{seed}"
        result = run_p0_experiment(str(output_dir), seed=seed)
        metrics = _metric_subset(result["evaluation"])
        _prune_heavy_dataset_files(output_dir)
        runs.append({"seed": seed, "output_dir": str(output_dir), **metrics})

    metric_names = [key for key in runs[0] if key not in {"seed", "output_dir"}] if runs else []
    aggregate = {name: _summarize_metric([float(row[name]) for row in runs]) for name in metric_names}
    return {"seeds": selected_seeds, "runs": runs, "aggregate": aggregate}


def _run_pipeline_until_sparse(output_dir: Path, seed: int = 7, noise_std: float = 0.05) -> None:
    generate_dataset(SyntheticConfig(seed=seed, noise_std=noise_std, output_dir=str(output_dir)))
    normalize_dataset(output_dir)
    train_stable_propagation(output_dir)
    solve_sparse_inversion(output_dir)


def _write_no_semantic_files(dataset_dir: Path) -> None:
    sparse_records = read_jsonl(dataset_dir / "sparse_interventions.jsonl")
    semantic_records: list[dict] = []
    type_records: list[dict] = []
    incident_ids = sorted({str(row["incident_id"]) for row in sparse_records})
    for incident_id in incident_ids:
        current = [row for row in sparse_records if row["incident_id"] == incident_id]
        ordered = sorted(current, key=lambda row: (-float(row["intervention_score"]), str(row["node"])))
        for rank, row in enumerate(ordered, start=1):
            metric = str(row["metric"])
            evidence_type = metric_to_evidence_type(metric)
            semantic_records.append(
                {
                    "incident_id": incident_id,
                    "service": str(row["service"]),
                    "metric": metric,
                    "node": str(row["node"]),
                    "sparse_rank": int(row["rank"]),
                    "sparse_score": float(row["intervention_score"]),
                    "evidence_type": evidence_type,
                    "evidence_score": 0.0,
                    "evidence_metrics": [],
                    "semantic_score": float(row["intervention_score"]),
                    "semantic_rank": rank,
                    "source": "semantic_ablation_no_evidence",
                }
            )
        type_scores: dict[str, dict[str, Any]] = {}
        for row in semantic_records:
            if row["incident_id"] != incident_id:
                continue
            root_type_candidate = canonical_root_type(str(row["evidence_type"]))
            bucket = type_scores.setdefault(root_type_candidate, {"score": 0.0, "services": set(), "metrics": set()})
            bucket["score"] += float(row["semantic_score"])
            bucket["services"].add(str(row["service"]))
            bucket["metrics"].add(str(row["metric"]))
        ranked_types = sorted(type_scores.items(), key=lambda item: (-float(item[1]["score"]), item[0]))
        for rank, (root_type_candidate, bucket) in enumerate(ranked_types, start=1):
            type_records.append(
                {
                    "incident_id": incident_id,
                    "root_type_candidate": root_type_candidate,
                    "type_score": float(bucket["score"]),
                    "rank": rank,
                    "supporting_services": sorted(bucket["services"])[:10],
                    "supporting_metrics": sorted(bucket["metrics"])[:10],
                    "source": "semantic_ablation_no_evidence",
                }
            )
    write_jsonl(dataset_dir / "semantic_interventions.jsonl", semantic_records)
    write_jsonl(dataset_dir / "semantic_type_scores.jsonl", type_records)


def _finish_from_semantic(dataset_dir: Path, top_k: int = 5) -> dict:
    explain_paths(dataset_dir)
    build_p0_results(dataset_dir, top_k=top_k)
    results = read_jsonl(dataset_dir / "p0_results.jsonl")
    incidents = read_jsonl(dataset_dir / "incidents.jsonl")
    evaluation = evaluate_results(results, incidents)
    with (dataset_dir / "p0_evaluation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(evaluation, handle, ensure_ascii=False, indent=2)
    return evaluation


def run_semantic_ablation(input_dir: str, output_dir: str) -> dict:
    """Compare full P0 results with a no-semantic-evidence ablation."""

    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    full_dir = output_path / "full"
    no_semantic_dir = output_path / "no_semantic_evidence"

    full_result = run_p0_experiment(str(full_dir), seed=7)
    _run_pipeline_until_sparse(no_semantic_dir, seed=7)
    _write_no_semantic_files(no_semantic_dir)
    no_semantic_eval = _finish_from_semantic(no_semantic_dir)
    _prune_heavy_dataset_files(full_dir)
    _prune_heavy_dataset_files(no_semantic_dir)

    return {
        "input_dir": input_dir,
        "output_dir": str(output_path),
        "full": _metric_subset(full_result["evaluation"]),
        "no_semantic_evidence": _metric_subset(no_semantic_eval),
        "delta_metric_hit_at_1": float(full_result["evaluation"]["metric_hit_at_1"] - no_semantic_eval["metric_hit_at_1"]),
        "delta_service_hit_at_1": float(full_result["evaluation"]["service_hit_at_1"] - no_semantic_eval["service_hit_at_1"]),
    }


def run_noise_sensitivity_audit(output_base: str, noise_stds: list[float] | None = None) -> dict:
    """Run the full P0 pipeline across noise levels."""

    selected_noise = noise_stds or DEFAULT_NOISE_STDS
    base = Path(output_base)
    base.mkdir(parents=True, exist_ok=True)
    runs: list[dict] = []
    for noise_std in selected_noise:
        output_dir = base / f"noise_{noise_std:g}"
        _run_pipeline_until_sparse(output_dir, seed=7, noise_std=noise_std)
        score_semantic_evidence(output_dir)
        evaluation = _finish_from_semantic(output_dir)
        _prune_heavy_dataset_files(output_dir)
        runs.append({"noise_std": noise_std, "output_dir": str(output_dir), **_metric_subset(evaluation)})

    metric_names = [key for key in runs[0] if key not in {"noise_std", "output_dir"}] if runs else []
    aggregate = {name: _summarize_metric([float(row[name]) for row in runs]) for name in metric_names}
    return {"noise_stds": selected_noise, "runs": runs, "aggregate": aggregate}


def run_p0_audit(output_dir: str = "data/p0_single_vm/audit", quick: bool = False) -> dict:
    """Run the P0 sanity audit suite."""

    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    seeds = [1, 2] if quick else DEFAULT_SEEDS
    noise_stds = [0.05, 0.1] if quick else DEFAULT_NOISE_STDS

    label_leakage = scan_for_label_leakage(DEFAULT_SCAN_FILES)
    multi_seed = run_multi_seed_audit(str(output_path / "multi_seed"), seeds)
    semantic_ablation = run_semantic_ablation("data/p0_single_vm/demo", str(output_path / "semantic_ablation"))
    noise_sensitivity = run_noise_sensitivity_audit(str(output_path / "noise_sensitivity"), noise_stds)

    noise_pass = all(
        float(row["metric_hit_at_1"]) >= 0.75
        for row in noise_sensitivity["runs"]
        if float(row["noise_std"]) <= 0.1
    )
    full_vs_no_semantic_pass = semantic_ablation["full"]["metric_hit_at_1"] >= semantic_ablation["no_semantic_evidence"]["metric_hit_at_1"]
    audit_passed = bool(
        label_leakage["passed"]
        and multi_seed["aggregate"]["service_hit_at_1"]["min"] >= 0.75
        and multi_seed["aggregate"]["metric_hit_at_1"]["min"] >= 0.75
        and full_vs_no_semantic_pass
        and noise_pass
    )

    summary = {
        "label_leakage_passed": bool(label_leakage["passed"]),
        "suspicious_files": label_leakage["suspicious_files"],
        "suspicious_lines": label_leakage["suspicious_lines"],
        "multi_seed_mean_service_hit_at_1": multi_seed["aggregate"]["service_hit_at_1"]["mean"],
        "multi_seed_min_service_hit_at_1": multi_seed["aggregate"]["service_hit_at_1"]["min"],
        "multi_seed_mean_metric_hit_at_1": multi_seed["aggregate"]["metric_hit_at_1"]["mean"],
        "multi_seed_min_metric_hit_at_1": multi_seed["aggregate"]["metric_hit_at_1"]["min"],
        "full_vs_no_semantic": semantic_ablation,
        "noise_sensitivity": noise_sensitivity,
        "audit_passed": audit_passed,
    }
    metadata = {
        "output_dir": str(output_path),
        "quick": quick,
        "seeds": seeds,
        "noise_stds": noise_stds,
        "scan_files": DEFAULT_SCAN_FILES,
    }
    with (output_path / "p0_audit_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
    with (output_path / "p0_audit_metadata.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, ensure_ascii=False, indent=2)
    return {"summary": summary, "metadata": metadata, "summary_path": str(output_path / "p0_audit_summary.json"), "metadata_path": str(output_path / "p0_audit_metadata.json")}

"""End-to-end P0 single-VM pseudo-distributed experiment."""

from __future__ import annotations

import json
from pathlib import Path

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.semantic import score_semantic_evidence
from proberca.eval.metrics import evaluate_results
from proberca.eval.p0_result import build_p0_results
from proberca.explain.path import explain_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.sparse import solve_sparse_inversion
from proberca.propagation.stable import train_stable_propagation


def run_p0_experiment(
    output_dir: str,
    seed: int = 7,
    baseline_windows: int = 30,
    faulty_windows: int = 30,
    instances_per_service: int = 2,
    top_k: int = 5,
) -> dict:
    """Run the full P0 pipeline and write final RCAResult plus metrics."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    pipeline_steps = [
        "generate_dataset",
        "normalize_dataset",
        "train_stable_propagation",
        "solve_sparse_inversion",
        "score_semantic_evidence",
        "explain_paths",
        "build_p0_results",
        "evaluate_results",
    ]

    generate_result = generate_dataset(
        SyntheticConfig(
            seed=seed,
            baseline_windows=baseline_windows,
            faulty_windows=faulty_windows,
            instances_per_service=instances_per_service,
            output_dir=str(output_path),
        )
    )
    normalize_result = normalize_dataset(output_path)
    propagation_result = train_stable_propagation(output_path)
    sparse_result = solve_sparse_inversion(output_path)
    semantic_result = score_semantic_evidence(output_path)
    path_result = explain_paths(output_path)
    p0_result = build_p0_results(output_path, top_k=top_k)

    incidents = read_jsonl(output_path / "incidents.jsonl")
    results = read_jsonl(output_path / "p0_results.jsonl")
    evaluation = evaluate_results(results, incidents)

    evaluation_path = output_path / "p0_evaluation_summary.json"
    with evaluation_path.open("w", encoding="utf-8") as handle:
        json.dump(evaluation, handle, ensure_ascii=False, indent=2)

    generated_files = sorted(str(path.relative_to(output_path)) for path in output_path.iterdir() if path.is_file())
    metadata = {
        "output_dir": str(output_path),
        "seed": seed,
        "baseline_windows": baseline_windows,
        "faulty_windows": faulty_windows,
        "instances_per_service": instances_per_service,
        "top_k": top_k,
        "pipeline_steps": pipeline_steps,
        "generated_files": generated_files,
    }
    metadata_path = output_path / "p0_experiment_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, ensure_ascii=False, indent=2)

    return {
        "output_dir": str(output_path),
        "pipeline_steps": pipeline_steps,
        "generated_files": generated_files,
        "p0_results_path": p0_result["p0_results_path"],
        "p0_results_metadata_path": p0_result["p0_results_metadata_path"],
        "p0_evaluation_summary_path": str(evaluation_path),
        "p0_experiment_metadata_path": str(metadata_path),
        "evaluation": evaluation,
        "results": results,
        "step_outputs": {
            "generate_dataset": generate_result.get("metadata", {}),
            "normalize_dataset": normalize_result.get("metadata", {}),
            "train_stable_propagation": propagation_result.get("metadata", {}),
            "solve_sparse_inversion": sparse_result.get("metadata", {}),
            "score_semantic_evidence": semantic_result.get("metadata", {}),
            "explain_paths": path_result.get("metadata", {}),
            "build_p0_results": p0_result.get("metadata", {}),
        },
        "metadata": metadata,
    }

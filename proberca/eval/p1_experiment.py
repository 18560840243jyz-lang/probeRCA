"""P1F single-seed end-to-end P1 RCA experiment."""

from __future__ import annotations

import json
from pathlib import Path

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence
from proberca.eval.p1_metrics import evaluate_p1_results
from proberca.eval.p1_result import build_p1_results
from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


def run_p1f_experiment(output_dir: str, seed: int = 7, top_k: int = 5) -> dict:
    """Run the P1F single-seed pipeline and write P1 RCAResult plus metrics."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    pipeline_steps = [
        "generate_dataset",
        "normalize_dataset",
        "simulate_adaptive_observation",
        "train_ipw_masked_propagation",
        "solve_ipw_sparse_inversion",
        "score_ipw_semantic_evidence",
        "explain_ipw_paths",
        "build_p1_results",
        "evaluate_p1_results",
    ]

    generate_result = generate_dataset(SyntheticConfig(seed=seed, output_dir=str(output_path)))
    normalize_result = normalize_dataset(output_path, output_path)
    observation_result = simulate_adaptive_observation(output_path, output_path, ObservationPolicyConfig(seed=seed))
    propagation_result = train_ipw_masked_propagation(output_path, output_path, IPWPropagationConfig())
    sparse_result = solve_ipw_sparse_inversion(output_path, output_path, IPWSparseInversionConfig())
    semantic_result = score_ipw_semantic_evidence(output_path, output_path, IPWSemanticEvidenceConfig())
    path_result = explain_ipw_paths(output_path, output_path, IPWPathExplanationConfig(top_k_candidates=top_k))
    p1_result = build_p1_results(output_path, output_path, top_k=top_k)

    incidents = read_jsonl(output_path / "incidents.jsonl")
    results = read_jsonl(output_path / "p1_results.jsonl")
    path_summary = json.loads((output_path / "ipw_path_explanation_summary.json").read_text(encoding="utf-8"))
    evaluation = evaluate_p1_results(results, incidents, path_summary=path_summary)
    evaluation_path = output_path / "p1_evaluation_summary.json"
    evaluation_path.write_text(json.dumps(evaluation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    generated_files = sorted(str(path.relative_to(output_path)) for path in output_path.iterdir() if path.is_file())
    metadata = {
        "output_dir": str(output_path),
        "seed": seed,
        "top_k": top_k,
        "pipeline_steps": pipeline_steps,
        "generated_files": generated_files,
        "note": "P1F single-seed evaluation; not P1 gate.",
    }
    metadata_path = output_path / "p1_experiment_metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "output_dir": str(output_path),
        "pipeline_steps": pipeline_steps,
        "generated_files": generated_files,
        "p1_results_path": p1_result["p1_results_path"],
        "p1_results_metadata_path": p1_result["p1_results_metadata_path"],
        "p1_evaluation_summary_path": str(evaluation_path),
        "p1_experiment_metadata_path": str(metadata_path),
        "evaluation": evaluation,
        "results": results,
        "step_outputs": {
            "generate_dataset": generate_result.get("metadata", {}),
            "normalize_dataset": normalize_result.get("metadata", {}),
            "simulate_adaptive_observation": observation_result.get("metadata", {}),
            "train_ipw_masked_propagation": propagation_result.get("metadata", {}),
            "solve_ipw_sparse_inversion": sparse_result.get("metadata", {}),
            "score_ipw_semantic_evidence": semantic_result.get("metadata", {}),
            "explain_ipw_paths": path_result.get("metadata", {}),
            "build_p1_results": p1_result.get("metadata", {}),
        },
        "metadata": metadata,
    }

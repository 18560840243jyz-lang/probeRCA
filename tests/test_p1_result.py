import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import score_ipw_semantic_evidence
from proberca.eval.p1_metrics import evaluate_p1_results
from proberca.eval.p1_result import build_p1_results
from proberca.explain.ipw_path import explain_ipw_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import train_ipw_masked_propagation


def _prepare_dataset(output_dir):
    generate_dataset(
        SyntheticConfig(
            output_dir=str(output_dir),
            seed=7,
            baseline_windows=30,
            faulty_windows=30,
            instances_per_service=2,
        )
    )
    normalize_dataset(output_dir)
    simulate_adaptive_observation(output_dir, config=ObservationPolicyConfig(seed=7))
    train_ipw_masked_propagation(output_dir)
    solve_ipw_sparse_inversion(output_dir)
    score_ipw_semantic_evidence(output_dir)
    explain_ipw_paths(output_dir)


def test_p1_results_and_evaluation(tmp_path):
    output_dir = tmp_path / "p1f"
    _prepare_dataset(output_dir)
    build_result = build_p1_results(output_dir)

    assert (output_dir / "p1_results.jsonl").exists()
    assert (output_dir / "p1_results_metadata.json").exists()

    results = read_jsonl(output_dir / "p1_results.jsonl")
    incidents = read_jsonl(output_dir / "incidents.jsonl")
    path_summary = json.loads((output_dir / "ipw_path_explanation_summary.json").read_text(encoding="utf-8"))
    evaluation = evaluate_p1_results(results, incidents, path_summary=path_summary)
    (output_dir / "p1_evaluation_summary.json").write_text(json.dumps(evaluation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    assert (output_dir / "p1_evaluation_summary.json").exists()
    assert build_result["metadata"]["results_count"] == 4
    assert len(results) == 4
    for result in results:
        assert "top_services" in result
        assert "top_metrics" in result
        assert "root_type" in result
        assert "evidence" in result
        assert "path" in result
        assert "observation" in result
        assert "root_service" not in result
        assert "root_metric" not in result
        assert "injected_path" not in result
        assert "true_root_rank" not in result

    assert evaluation["service_hit_at_1"] >= 0.75
    assert evaluation["service_hit_at_3"] == 1.0
    assert evaluation["metric_hit_at_1"] >= 0.75
    assert evaluation["metric_hit_at_3"] == 1.0
    assert evaluation["root_type_accuracy"] >= 0.75
    assert evaluation["path_fidelity"] >= 0.75


def test_p1_build_results_cli(tmp_path):
    output_dir = tmp_path / "p1f_cli"
    _prepare_dataset(output_dir)

    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.build_p1_results", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1 RCAResult 构建完成" in completed.stdout
    assert (output_dir / "p1_results.jsonl").exists()

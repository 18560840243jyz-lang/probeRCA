import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import score_ipw_semantic_evidence
from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths
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


def test_ipw_path_explanation_outputs(tmp_path):
    output_dir = tmp_path / "p1e"
    _prepare_dataset(output_dir)
    config = IPWPathExplanationConfig(top_k_candidates=5)
    result = explain_ipw_paths(output_dir, config=config)

    assert (output_dir / "ipw_path_explanations.jsonl").exists()
    assert (output_dir / "ipw_path_explanation_summary.json").exists()
    assert (output_dir / "ipw_path_explanation_metadata.json").exists()

    records = read_jsonl(output_dir / "ipw_path_explanations.jsonl")
    assert records
    for record in records:
        assert "path_services" in record
        assert "path_edges" in record
        assert "path_score" in record
        assert "propagation_support" in record
        assert "semantic_rank" in record
        assert "top_services" not in record
        assert "top_metrics" not in record
        assert "RCAResult" not in record

    summary = result["summary"]
    assert summary["incidents_count"] == 4
    assert summary["path_records_count"] == 4 * config.top_k_candidates
    assert summary["candidates_explained_count"] == 4 * config.top_k_candidates
    assert summary["paths_missing_count"] <= summary["path_records_count"]
    assert summary["path_fidelity_debug"] >= 0.75


def test_ipw_path_explanation_cli(tmp_path):
    output_dir = tmp_path / "p1e_cli"
    _prepare_dataset(output_dir)

    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.explain_ipw_paths", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1E IPW semantic path explanation 完成" in completed.stdout
    assert "不包含最终 RCAResult" in completed.stdout

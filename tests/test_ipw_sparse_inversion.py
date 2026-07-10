import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import train_ipw_masked_propagation


def _prepare_dataset(output_dir):
    generate_dataset(
        SyntheticConfig(
            output_dir=str(output_dir),
            seed=7,
            baseline_windows=8,
            faulty_windows=8,
            instances_per_service=1,
        )
    )
    normalize_dataset(output_dir)
    simulate_adaptive_observation(output_dir, config=ObservationPolicyConfig(seed=7))
    train_ipw_masked_propagation(output_dir)


def test_ipw_sparse_inversion_outputs(tmp_path):
    output_dir = tmp_path / "p1c"
    _prepare_dataset(output_dir)
    result = solve_ipw_sparse_inversion(output_dir)

    assert (output_dir / "ipw_sparse_interventions.jsonl").exists()
    assert (output_dir / "ipw_sparse_inversion_summary.json").exists()
    assert (output_dir / "ipw_sparse_inversion_metadata.json").exists()

    metadata = result["metadata"]
    assert metadata["incidents_count"] == 4
    assert metadata["candidates_count"] > 0

    candidates = read_jsonl(output_dir / "ipw_sparse_interventions.jsonl")
    assert candidates
    for record in candidates:
        assert "intervention_score" in record
        assert "residual_lift" in record
        assert "confidence" in record
        assert "mean_ipw_weight" in record
        assert "top_services" not in record
        assert "top_metrics" not in record
        assert "root_type" not in record
        assert "path" not in record
        assert "RCAResult" not in record
        assert "true_root_rank_debug" not in record

    summary = result["summary"]
    assert "true_root_rank_debug" in summary["per_incident"][0]
    for item in summary["per_incident"]:
        assert item["nonzero_candidates_count"] > 0


def test_ipw_sparse_inversion_cli(tmp_path):
    output_dir = tmp_path / "p1c_cli"
    _prepare_dataset(output_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.solve_ipw_sparse_inversion", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1C IPW residual sparse inversion 完成" in completed.stdout
    assert "不包含 semantic evidence" in completed.stdout

    summary = json.loads((output_dir / "ipw_sparse_inversion_summary.json").read_text(encoding="utf-8"))
    assert summary["incidents_count"] == 4

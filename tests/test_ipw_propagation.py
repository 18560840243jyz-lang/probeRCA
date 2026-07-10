import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import (
    IPWPropagationConfig,
    safe_ipw_weight,
    train_ipw_masked_propagation,
)


def _prepare_dataset(output_dir, *, baseline_windows=30, faulty_windows=30, instances_per_service=2):
    generate_dataset(
        SyntheticConfig(
            output_dir=str(output_dir),
            seed=7,
            baseline_windows=baseline_windows,
            faulty_windows=faulty_windows,
            instances_per_service=instances_per_service,
        )
    )
    normalize_dataset(output_dir)
    simulate_adaptive_observation(output_dir, config=ObservationPolicyConfig(seed=7))


def test_safe_ipw_weight_clips():
    assert safe_ipw_weight(0.5, 0.05, 20.0) == 2.0
    assert safe_ipw_weight(0.0, 0.05, 20.0) == 20.0
    assert safe_ipw_weight(0.001, 0.05, 10.0) == 10.0


def test_ipw_propagation_outputs_default_shape(tmp_path):
    output_dir = tmp_path / "p1b"
    _prepare_dataset(output_dir)
    result = train_ipw_masked_propagation(output_dir)

    assert (output_dir / "ipw_stable_propagation_model.json").exists()
    assert (output_dir / "ipw_stable_residuals.jsonl").exists()
    assert (output_dir / "ipw_propagation_metadata.json").exists()

    metadata = result["metadata"]
    assert metadata["incidents_count"] == 4
    assert metadata["coefficients_count"] > 0
    assert metadata["residuals_count"] > 0
    assert metadata["mean_sampling_probability"] > 0
    assert metadata["mean_ipw_weight"] >= 1

    residuals = read_jsonl(output_dir / "ipw_stable_residuals.jsonl")
    assert residuals
    for record in residuals:
        assert "observed" in record
        assert record["observed"] is True
        assert "sampling_probability" in record
        assert "ipw_weight" in record
        assert "residual" in record
        assert "top_services" not in record
        assert "top_metrics" not in record
        assert "RCAResult" not in record

    model = json.loads((output_dir / "ipw_stable_propagation_model.json").read_text(encoding="utf-8"))
    summaries = model["summaries"]
    assert len(summaries) == 4
    for summary in summaries:
        assert summary["timestamp_count"] == 60
        assert summary["node_count"] == 176
        assert summary["residual_count"] > 0


def test_ipw_and_no_ipw_both_run(tmp_path):
    output_dir = tmp_path / "p1b_compare"
    _prepare_dataset(output_dir, baseline_windows=8, faulty_windows=8, instances_per_service=1)
    ipw_result = train_ipw_masked_propagation(output_dir, output_dir / "ipw")
    no_ipw_result = train_ipw_masked_propagation(
        output_dir,
        output_dir / "no_ipw",
        IPWPropagationConfig(use_ipw=False, use_parent_ipw=False, use_target_ipw=False),
    )

    assert ipw_result["metadata"]["residuals_count"] > 0
    assert no_ipw_result["metadata"]["residuals_count"] > 0
    assert no_ipw_result["metadata"]["mean_ipw_weight"] == 1.0
    assert ipw_result["metadata"]["mean_ipw_weight"] >= no_ipw_result["metadata"]["mean_ipw_weight"]


def test_ipw_propagation_cli(tmp_path):
    output_dir = tmp_path / "p1b_cli"
    _prepare_dataset(output_dir, baseline_windows=8, faulty_windows=8, instances_per_service=1)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.train_ipw_propagation", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1B IPW-masked stable propagation 完成" in completed.stdout
    assert "不包含 sparse inversion" in completed.stdout

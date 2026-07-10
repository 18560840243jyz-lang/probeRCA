import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation


def test_adaptive_observation_outputs_and_mask(tmp_path):
    output_dir = tmp_path / "p1a"
    generate_dataset(
        SyntheticConfig(
            output_dir=str(output_dir),
            seed=7,
            baseline_windows=4,
            faulty_windows=4,
            instances_per_service=1,
        )
    )
    normalize_dataset(output_dir)
    result = simulate_adaptive_observation(output_dir, config=ObservationPolicyConfig(seed=7))

    assert (output_dir / "observed_metrics.jsonl").exists()
    assert (output_dir / "sampling_log.jsonl").exists()
    assert (output_dir / "observation_mask.jsonl").exists()
    assert (output_dir / "adaptive_observation_metadata.json").exists()

    metadata = result["metadata"]
    assert metadata["total_records"] > 0
    assert metadata["observed_records"] > 0
    assert metadata["observed_ratio"] > 0
    assert metadata["observed_ratio"] < 1

    sampling_log = read_jsonl(output_dir / "sampling_log.jsonl")
    observed_values = {record["observed"] for record in sampling_log}
    assert observed_values == {True, False}

    always_on_metrics = set(ObservationPolicyConfig().always_on_metrics)
    always_on_records = [record for record in sampling_log if record["metric"] in always_on_metrics]
    assert always_on_records
    assert all(record["observed"] is True for record in always_on_records)

    minimum = ObservationPolicyConfig().min_sampling_probability
    assert all(record["sampling_probability"] >= minimum for record in sampling_log)

    observed_metrics = read_jsonl(output_dir / "observed_metrics.jsonl")
    for record in sampling_log + observed_metrics:
        assert "root_service" not in record
        assert "root_metric" not in record
        assert "root_type" not in record


def test_adaptive_observation_cli(tmp_path):
    output_dir = tmp_path / "p1a-cli"
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_p1a_observation", "--output", str(output_dir), "--seed", "7"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1A adaptive observation pipeline 完成" in completed.stdout
    assert "不包含 stable propagation" in completed.stdout

    metadata = json.loads((output_dir / "adaptive_observation_metadata.json").read_text(encoding="utf-8"))
    assert metadata["observed_records"] > 0
    assert metadata["observed_ratio"] < 1

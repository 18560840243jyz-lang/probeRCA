import json
import subprocess
import sys
from collections import Counter

import numpy as np

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.propagation.stable import train_stable_propagation


def _prepare_dataset(output_dir, config: SyntheticConfig) -> None:
    generate_dataset(config)
    normalize_dataset(output_dir)


def test_stable_propagation_outputs_and_root_residual(tmp_path) -> None:
    output_dir = tmp_path / "demo"
    _prepare_dataset(
        output_dir,
        SyntheticConfig(
            seed=17,
            output_dir=str(output_dir),
            baseline_windows=8,
            faulty_windows=8,
            instances_per_service=1,
        ),
    )
    result = train_stable_propagation(output_dir)

    model_path = output_dir / "stable_propagation_model.json"
    residuals_path = output_dir / "stable_residuals.jsonl"
    metadata_path = output_dir / "propagation_metadata.json"

    assert model_path.exists()
    assert residuals_path.exists()
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    residuals = read_jsonl(residuals_path)
    incidents = read_jsonl(output_dir / "incidents.jsonl")

    assert metadata["incidents_count"] >= 4
    assert metadata["coefficients_count"] > 0
    assert metadata["residuals_count"] > 0
    assert metadata["residuals_count"] == metadata["expected_residuals_count"]
    assert metadata["residuals_count_matches_expected"] is True
    assert result["metadata"]["residuals_count"] == metadata["residuals_count"]
    assert "predicted_z" in residuals[0]
    assert "residual" in residuals[0]

    for incident in incidents:
        root_service = incident["root_service"]
        root_metric = incident["root_metric"]
        start_ts = float(incident["start_ts"])
        end_ts = float(incident["end_ts"])
        incident_id = incident["incident_id"]
        baseline = [
            abs(float(row["residual"]))
            for row in residuals
            if row["incident_id"] == incident_id
            and row["service"] == root_service
            and row["metric"] == root_metric
            and float(row["timestamp"]) < start_ts
        ]
        faulty = [
            abs(float(row["residual"]))
            for row in residuals
            if row["incident_id"] == incident_id
            and row["service"] == root_service
            and row["metric"] == root_metric
            and start_ts <= float(row["timestamp"]) < end_ts
        ]
        assert baseline
        assert faulty
        assert float(np.mean(faulty)) > float(np.mean(baseline))


def test_default_stable_propagation_residual_counts_are_service_metric_level(tmp_path) -> None:
    output_dir = tmp_path / "default-demo"
    _prepare_dataset(output_dir, SyntheticConfig(seed=23, output_dir=str(output_dir)))
    train_stable_propagation(output_dir)

    metadata = json.loads((output_dir / "propagation_metadata.json").read_text(encoding="utf-8"))
    model = json.loads((output_dir / "stable_propagation_model.json").read_text(encoding="utf-8"))
    residuals = read_jsonl(output_dir / "stable_residuals.jsonl")

    assert metadata["incidents_count"] == 4
    assert metadata["expected_residuals_count"] == 41536
    assert metadata["residuals_count"] == metadata["expected_residuals_count"]
    assert metadata["residuals_count"] == 41536
    assert metadata["residuals_count_matches_expected"] is True

    for incident_model in model["incidents"]:
        summary = incident_model["summary"]
        assert summary["timestamp_count"] == 60
        assert summary["node_count"] == 176
        assert summary["residual_count"] == 10384

    assert all("instance" not in row for row in residuals)
    assert all(row.get("service") for row in residuals)
    assert all(row.get("metric") for row in residuals)
    assert len({f"{row['service']}.{row['metric']}" for row in residuals}) == 176

    counts_by_incident = Counter(row["incident_id"] for row in residuals)
    assert set(counts_by_incident.values()) == {10384}


def test_stable_propagation_cli(tmp_path) -> None:
    output_dir = tmp_path / "demo-cli"
    _prepare_dataset(
        output_dir,
        SyntheticConfig(
            seed=19,
            output_dir=str(output_dir),
            baseline_windows=8,
            faulty_windows=8,
            instances_per_service=1,
        ),
    )
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.train_stable_propagation", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "稳定传播学习完成" in completed.stdout
    assert "residuals_count_matches_expected：True" in completed.stdout
    assert (output_dir / "stable_propagation_model.json").exists()

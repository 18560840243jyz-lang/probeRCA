import json
import math

import numpy as np

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset


def test_robust_normalization_outputs_and_fault_score(tmp_path) -> None:
    output_dir = tmp_path / "demo"
    generate_dataset(
        SyntheticConfig(
            seed=13,
            output_dir=str(output_dir),
            baseline_windows=8,
            faulty_windows=8,
            instances_per_service=1,
        )
    )

    result = normalize_dataset(output_dir)

    normalized_path = output_dir / "normalized_metrics.jsonl"
    stats_path = output_dir / "robust_stats.jsonl"
    metadata_path = output_dir / "normalization_metadata.json"

    assert normalized_path.exists()
    assert stats_path.exists()
    assert metadata_path.exists()

    normalized = read_jsonl(normalized_path)
    stats = read_jsonl(stats_path)
    incidents = read_jsonl(output_dir / "incidents.jsonl")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert metadata["normalized_count"] > 0
    assert metadata["stats_count"] > 0
    assert result["metadata"]["normalized_count"] == metadata["normalized_count"]
    assert len(stats) == metadata["stats_count"]
    assert "z_value" in normalized[0]
    assert all(math.isfinite(float(row["z_value"])) for row in normalized)

    for incident in incidents:
        root_service = incident["root_service"]
        root_metric = incident["root_metric"]
        start_ts = float(incident["start_ts"])
        end_ts = float(incident["end_ts"])
        baseline = [
            abs(float(row["z_value"]))
            for row in normalized
            if row["service"] == root_service
            and row["metric"] == root_metric
            and float(row["timestamp"]) < start_ts
        ]
        faulty = [
            abs(float(row["z_value"]))
            for row in normalized
            if row["service"] == root_service
            and row["metric"] == root_metric
            and start_ts <= float(row["timestamp"]) < end_ts
        ]
        assert baseline
        assert faulty
        assert float(np.mean(faulty)) > float(np.mean(baseline))

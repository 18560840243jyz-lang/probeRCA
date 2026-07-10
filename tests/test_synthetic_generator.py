import json

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset


def test_synthetic_generator_outputs(tmp_path) -> None:
    output_dir = tmp_path / "demo"
    result = generate_dataset(
        SyntheticConfig(
            seed=11,
            output_dir=str(output_dir),
            baseline_windows=2,
            faulty_windows=2,
            instances_per_service=1,
        )
    )

    metrics_path = output_dir / "metrics.jsonl"
    evidence_path = output_dir / "evidence.jsonl"
    incidents_path = output_dir / "incidents.jsonl"
    graph_path = output_dir / "service_graph.jsonl"
    metadata_path = output_dir / "metadata.json"

    assert metrics_path.exists()
    assert evidence_path.exists()
    assert incidents_path.exists()
    assert graph_path.exists()
    assert metadata_path.exists()

    metrics = read_jsonl(metrics_path)
    evidence = read_jsonl(evidence_path)
    incidents = read_jsonl(incidents_path)
    graph_edges = read_jsonl(graph_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert len(incidents) >= 4
    for incident in incidents:
        assert incident["root_service"]
        assert incident["root_metric"]
        assert incident["root_type"]
        assert incident["symptom_service"]

    assert any(row["service"] == "frontend" and row["metric"] == "request.p99_latency_ms" for row in metrics)
    assert {"CPU", "Net", "IO", "Lock"}.issubset({row["evidence_type"] for row in evidence})
    assert metadata["metrics_count"] > 0
    assert metadata["evidence_count"] > 0
    assert metadata["incidents_count"] > 0
    assert metadata["graph_edges_count"] > 0
    assert len(graph_edges) == metadata["graph_edges_count"]
    assert result["metadata"]["incidents_count"] == metadata["incidents_count"]

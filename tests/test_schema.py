import pytest

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.schema import EvidenceRecord, IncidentRecord, MetricRecord, RCAResult
from proberca.graph.schema import GraphEdge


def test_records_jsonl_roundtrip(tmp_path) -> None:
    metric = MetricRecord(
        timestamp=1.0,
        service="checkout",
        instance="checkout-1",
        node="node-a",
        metric="cpu_usage",
        value=0.9,
        source="synthetic",
        incident_id="inc-1",
    )
    evidence = EvidenceRecord(
        timestamp=1.0,
        service="checkout",
        instance="checkout-1",
        node="node-a",
        evidence_type="cpu",
        metric="run_queue",
        value=3.0,
        source="simulated",
        probe_id="probe-cpu",
        sampling_rate=1.0,
        incident_id="inc-1",
    )
    incident = IncidentRecord(
        incident_id="inc-1",
        root_service="checkout",
        root_metric="cpu_usage",
        root_type="cpu",
        symptom_service="frontend",
        start_ts=1.0,
        end_ts=2.0,
        injected_path=["checkout", "frontend"],
    )
    result = RCAResult(
        incident_id="inc-1",
        symptom_service="frontend",
        top_services=[{"service": "checkout", "score": 1.0}],
        top_metrics=[{"metric": "cpu_usage", "score": 1.0}],
        root_type="cpu",
        evidence=["run_queue"],
        path=["checkout", "frontend"],
    )

    output_path = tmp_path / "records.jsonl"
    write_jsonl(output_path, [metric, evidence, incident, result])
    rows = read_jsonl(output_path)

    assert len(rows) == 4
    assert rows[0]["service"] == "checkout"
    assert rows[0]["incident_id"] == "inc-1"
    assert rows[1]["probe_id"] == "probe-cpu"
    assert rows[2]["root_service"] == "checkout"
    assert rows[3]["path"] == ["checkout", "frontend"]


def test_graph_edge_type_validation() -> None:
    edge = GraphEdge(src="checkout", dst="frontend", edge_type="call")
    assert edge.edge_type == "call"

    with pytest.raises(ValueError):
        GraphEdge(src="checkout", dst="frontend", edge_type="invalid")

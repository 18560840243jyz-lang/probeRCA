from __future__ import annotations

import json
from pathlib import Path

from proberca.explain.counterfactual_explanation import CounterfactualConfig, run_counterfactual_explanation
from proberca.inference.graph_sparse_inversion import GraphSparseConfig, run_graph_sparse_inversion


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _fake_inputs(tmp_path: Path, values: dict[str, float]) -> tuple[Path, Path, Path, Path]:
    candidate = tmp_path / "candidate"
    evidence = tmp_path / "evidence"
    graph = tmp_path / "graph"
    out = tmp_path / "cf"
    nodes = []
    for node in values:
        service, metric = node.split(".", 1)
        family = "CPU" if metric.startswith("cpu.") else "storage I/O" if metric.startswith("io.") else "load"
        nodes.append({"node_id": node, "service": service, "metric": metric, "metric_family": family})
    _write_jsonl(candidate / "candidate_metric_nodes.jsonl", nodes)
    _write_jsonl(candidate / "candidate_edges.jsonl", [{"src": "serviceA", "dst": "frontend", "edge_type": "call"}])
    _write_json(candidate / "candidate_subgraph_metadata.json", {"uses_root_labels": False})
    _write_json(evidence / "evidence_channel_metadata.json", {"produces_calibrated_residuals": True, "raw_residual_directly_used_for_sparse_inversion": False})
    rows = []
    for node, value in values.items():
        service, metric = node.split(".", 1)
        for _ in range(4):
            rows.append({"node_id": node, "service": service, "metric": metric, "calibrated_residual": value, "raw_residual": 10**9, "evidence_effect": 0.0})
    _write_jsonl(evidence / "calibrated_residuals.jsonl", rows)
    _write_jsonl(evidence / "evidence_vectors.jsonl", [])
    _write_jsonl(evidence / "evidence_effects.jsonl", [])
    run_graph_sparse_inversion(str(candidate), str(evidence), str(graph), GraphSparseConfig(max_iter=80, max_nonzero_ratio=0.8, service_top_metric_keep=4))
    return candidate, evidence, graph, out


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_metric_counterfactual_delta_positive_for_top_node(tmp_path: Path) -> None:
    candidate, evidence, graph, out = _fake_inputs(tmp_path, {"serviceA.cpu.hot": 5.0, "serviceB.cpu.cool": 0.1, "frontend.request.p99_latency_ms": 0.0})
    result = run_counterfactual_explanation(str(graph), str(candidate), str(evidence), str(out), CounterfactualConfig(top_k_metrics=3, top_k_services=2, max_reopt_iter=60))
    metric_rows = result["rankings"]["counterfactual_metric_ranking"]
    assert metric_rows
    assert any(row["node_id"] == "serviceA.cpu.hot" and row["delta_loss"] > 0 for row in metric_rows)
    assert result["metadata"]["reoptimizes_with_candidate_removed"] is True


def test_service_counterfactual_strong_service_delta_exceeds_weak(tmp_path: Path) -> None:
    candidate, evidence, graph, out = _fake_inputs(tmp_path, {"serviceA.cpu.hot": 5.0, "serviceA.cpu.hot2": 4.5, "serviceB.cpu.cool": 0.1, "frontend.request.p99_latency_ms": 0.0})
    result = run_counterfactual_explanation(str(graph), str(candidate), str(evidence), str(out), CounterfactualConfig(top_k_metrics=4, top_k_services=3, max_reopt_iter=60))
    by_service = {row["service"]: row for row in result["rankings"]["counterfactual_service_ranking"]}
    assert by_service["serviceA"]["delta_loss"] > by_service.get("serviceB", {"delta_loss": -1})["delta_loss"]


def test_counterfactual_outputs_and_label_safety(tmp_path: Path) -> None:
    candidate, evidence, graph, out = _fake_inputs(tmp_path, {"serviceA.cpu.hot": 5.0, "serviceB.cpu.cool": 0.1, "frontend.request.p99_latency_ms": 0.0})
    incidents = tmp_path / "incidents.jsonl"
    _write_jsonl(incidents, [{"root_service": "serviceA", "root_metric": "cpu.hot", "root_type": "CPU", "start_ts": 1, "end_ts": 2}])
    result = run_counterfactual_explanation(str(graph), str(candidate), str(evidence), str(out), CounterfactualConfig(max_reopt_iter=40))
    metadata = result["metadata"]
    assert metadata["uses_root_labels"] is False
    assert metadata["fast_approximation_only"] is False
    for name in ["counterfactual_metric_explanations.jsonl", "counterfactual_service_explanations.jsonl", "counterfactual_metric_ranking.jsonl", "counterfactual_service_ranking.jsonl", "counterfactual_metadata.json"]:
        assert (out / name).exists()

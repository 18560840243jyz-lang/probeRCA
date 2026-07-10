from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from proberca.inference.graph_sparse_inversion import (
    GraphSparseConfig,
    _effective_config,
    aggregate_residual_signal,
    build_sparse_rankings,
    load_calibrated_residuals,
    load_candidate_graph,
    post_sparsify_solution,
    run_graph_sparse_inversion,
    solve_graph_sparse_admm,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_admm_solver_prefers_connected_high_signal_nodes() -> None:
    cfg = GraphSparseConfig(max_iter=80)
    r = np.asarray([5.0, 4.8, 0.1, 0.0])
    edges = [{"src_index": 0, "dst_index": 1, "weight": 1.0}]
    groups = {"strong": [0, 1], "weak": [2, 3]}
    result = solve_graph_sparse_admm(r, edges, groups, cfg)
    assert result["solver_status"] in {"converged", "max_iter_reached"}
    assert result["solver_status"] != "failed_numeric"
    assert abs(result["x"][0]) > abs(result["x"][2])
    assert abs(result["x"][1]) > abs(result["x"][3])
    assert result["trace"]
    assert result["trace"][-1]["nonzero_count"] > 0


def test_group_lasso_strong_group_scores_higher() -> None:
    cfg = GraphSparseConfig(max_iter=50)
    x = np.asarray([4.0, 3.0, 0.2, 0.1])
    node_ids = ["svc1.cpu.usage", "svc1.cpu.throttled_usec", "svc2.cpu.usage", "svc2.memory.usage"]
    node_metadata = {
        "node_ids": node_ids,
        "node_to_service": {node_ids[0]: "svc1", node_ids[1]: "svc1", node_ids[2]: "svc2", node_ids[3]: "svc2"},
        "node_to_metric": {node_ids[0]: "cpu.usage", node_ids[1]: "cpu.throttled_usec", node_ids[2]: "cpu.usage", node_ids[3]: "memory.usage"},
        "node_to_family": {node_ids[0]: "CPU", node_ids[1]: "CPU", node_ids[2]: "CPU", node_ids[3]: "memory"},
    }
    groups = {"svc1": [0, 1], "svc2": [2, 3]}
    residual_signal = {"signal_by_node": {node: float(x[i]) for i, node in enumerate(node_ids)}}
    rankings = build_sparse_rankings(x, residual_signal, {"evidence_support_by_node": {}}, node_metadata, groups, cfg)
    assert rankings["service_scores"][0]["service"] == "svc1"
    assert rankings["service_scores"][0]["service_score"] > rankings["service_scores"][1]["service_score"]


def test_aggregate_uses_calibrated_residual_not_raw() -> None:
    cfg = GraphSparseConfig()
    rows = [
        {"node_id": "svc.cpu.usage", "calibrated_residual": 1.0, "raw_residual": 10**12, "evidence_effect": 0.0},
        {"node_id": "svc.cpu.usage", "calibrated_residual": 2.0, "raw_residual": 10**12, "evidence_effect": 0.0},
        {"node_id": "svc.cpu.usage", "calibrated_residual": 3.0, "raw_residual": 10**12, "evidence_effect": 0.0},
    ]
    signal = aggregate_residual_signal(rows, ["svc.cpu.usage"], cfg)["signal_by_node"]["svc.cpu.usage"]
    assert signal <= cfg.max_signal
    assert signal < 10**6


def test_missing_calibrated_residual_is_rejected(tmp_path: Path) -> None:
    _write_json(tmp_path / "evidence_channel_metadata.json", {"produces_calibrated_residuals": True, "raw_residual_directly_used_for_sparse_inversion": False})
    _write_jsonl(tmp_path / "calibrated_residuals.jsonl", [{"node_id": "svc.cpu.usage", "raw_residual": 1.0, "evidence_effect": 0.0}])
    with pytest.raises(ValueError):
        load_calibrated_residuals(str(tmp_path))


def test_run_graph_sparse_inversion_fake_dirs(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    evidence = tmp_path / "evidence"
    out = tmp_path / "out"
    _write_jsonl(candidate / "candidate_metric_nodes.jsonl", [
        {"node_id": "svc.cpu.usage", "service": "svc", "metric": "cpu.usage", "metric_family": "CPU"},
        {"node_id": "svc.cpu.throttled_usec", "service": "svc", "metric": "cpu.throttled_usec", "metric_family": "CPU"},
        {"node_id": "other.memory.usage", "service": "other", "metric": "memory.usage", "metric_family": "memory"},
    ])
    _write_jsonl(candidate / "candidate_edges.jsonl", [{"src": "svc", "dst": "other", "edge_type": "call"}])
    _write_json(candidate / "candidate_subgraph_metadata.json", {"uses_root_labels": False})
    _write_json(evidence / "evidence_channel_metadata.json", {"produces_calibrated_residuals": True, "raw_residual_directly_used_for_sparse_inversion": False})
    rows = []
    for node, value in [("svc.cpu.usage", 5.0), ("svc.cpu.throttled_usec", 4.0), ("other.memory.usage", 0.1)]:
        service, metric = node.split(".", 1)
        rows.extend({"node_id": node, "service": service, "metric": metric, "calibrated_residual": value, "raw_residual": 10**9, "evidence_effect": 0.0} for _ in range(3))
    _write_jsonl(evidence / "calibrated_residuals.jsonl", rows)
    _write_jsonl(evidence / "evidence_vectors.jsonl", [])
    _write_jsonl(evidence / "evidence_effects.jsonl", [])
    result = run_graph_sparse_inversion(str(candidate), str(evidence), str(out), GraphSparseConfig(max_iter=40))
    assert result["metadata"]["uses_root_labels"] is False
    assert result["metadata"]["consumes_calibrated_residuals"] is True
    assert (out / "sparse_interventions.jsonl").exists()
    assert (out / "metric_scores.jsonl").exists()
    assert (out / "service_scores.jsonl").exists()
    assert (out / "graph_sparse_metadata.json").exists()



def test_metric_edge_cap_limits_degree(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    rows = []
    services = ["svc_a", "svc_b"]
    metrics = [f"cpu.metric_{idx}" for idx in range(5)]
    for svc in services:
        for metric in metrics:
            rows.append({"node_id": f"{svc}.{metric}", "service": svc, "metric": metric, "metric_family": "CPU"})
    _write_jsonl(candidate / "candidate_metric_nodes.jsonl", rows)
    _write_jsonl(candidate / "candidate_edges.jsonl", [{"src": "svc_a", "dst": "svc_b", "edge_type": "call"}])
    _write_json(candidate / "candidate_subgraph_metadata.json", {"uses_root_labels": False})
    graph = load_candidate_graph(str(candidate), GraphSparseConfig(max_graph_degree=3))
    degree = {node: 0 for node in graph["node_ids"]}
    for edge in graph["graph_edges"]:
        degree[edge["src"]] += 1
        degree[edge["dst"]] += 1
    assert max(degree.values()) <= 3
    assert graph["metadata"]["degree_cap_applied"] is True


def test_positive_topk_signal_penalty_and_evidence_boost() -> None:
    cfg = GraphSparseConfig(topk_fraction=0.5, min_topk=1, symptom_family_penalty=0.5, evidence_signal_boost=0.5)
    rows = [
        {"node_id": "frontend.request.p99_latency_ms", "calibrated_residual": 8.0, "raw_residual": 10**9, "evidence_effect": 0.0},
        {"node_id": "frontend.request.p99_latency_ms", "calibrated_residual": 6.0, "raw_residual": 10**9, "evidence_effect": 0.0},
        {"node_id": "pay.cpu.throttled_usec", "calibrated_residual": 4.0, "raw_residual": 10**9, "evidence_effect": 0.0},
        {"node_id": "pay.cpu.throttled_usec", "calibrated_residual": 2.0, "raw_residual": 10**9, "evidence_effect": 0.0},
    ]
    result = aggregate_residual_signal(
        rows,
        ["frontend.request.p99_latency_ms", "pay.cpu.throttled_usec"],
        cfg,
        evidence_support={"evidence_support_by_node": {"pay.cpu.throttled_usec": 1.0}},
        node_to_family={"frontend.request.p99_latency_ms": "load", "pay.cpu.throttled_usec": "CPU"},
    )
    request_components = result["details_by_node"]["frontend.request.p99_latency_ms"]
    cpu_components = result["details_by_node"]["pay.cpu.throttled_usec"]
    assert request_components["family_penalty"] == 0.5
    assert cpu_components["evidence_support"] == 1.0
    assert cpu_components["final_signal"] > cpu_components["raw_positive_topk_mean"]


def test_auto_lambda_within_bounds() -> None:
    cfg = GraphSparseConfig(auto_lambda=True, adaptive_group_lambda=True, min_lambda_l1=0.2, max_lambda_l1=2.0, min_lambda_group=0.1, max_lambda_group=1.0)
    effective, meta = _effective_config(np.asarray([0.0, 1.0, 4.0, 8.0]), {"svc1": [0, 1], "svc2": [2, 3]}, cfg)
    assert 0.2 <= effective.lambda_l1 <= 2.0
    assert 0.1 <= effective.lambda_group <= 1.0
    assert meta["auto_lambda"] is True


def test_post_sparsify_caps_nonzero_nodes() -> None:
    node_ids = [f"svc{idx // 5}.cpu.metric_{idx}" for idx in range(20)]
    node_to_service = {node: node.split(".", 1)[0] for node in node_ids}
    x = np.asarray([float(idx + 1) for idx in range(20)])
    sparse, meta = post_sparsify_solution(x, node_ids, node_to_service, GraphSparseConfig(max_nonzero_ratio=0.25, min_keep_nodes=3, service_top_metric_keep=5))
    assert meta["post_sparsify_applied"] is True
    assert int(np.sum(np.abs(sparse) > 0.01)) <= 5

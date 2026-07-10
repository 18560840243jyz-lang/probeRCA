from pathlib import Path
import json

from proberca.propagation.structured_multilag import (
    StructuredPropagationConfig,
    build_structured_parent_sets,
    compute_service_to_symptom_propagation_support,
    fit_structured_multilag_propagation,
    load_metrics_panel,
    parse_service_graph,
    robust_normalize_panel,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_parent_set_has_structured_cross_service_resource_to_request(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    rows=[]
    for t in range(8):
        rows += [
            {"timestamp": t, "service": "serviceA", "metric": "cpu.throttled_usec", "value": float(t)},
            {"timestamp": t, "service": "serviceB", "metric": "request.p99_latency_ms", "value": float(t)},
            {"timestamp": t, "service": "serviceC", "metric": "cpu.throttled_usec", "value": float(t)},
        ]
    _write_jsonl(raw / "metrics.jsonl", rows)
    _write_jsonl(raw / "service_graph.jsonl", [{"src": "serviceB", "dst": "serviceA"}, {"src": "frontend", "dst": "serviceB"}])
    panel = load_metrics_panel(str(raw / "metrics.jsonl"))
    graph = parse_service_graph(str(raw / "service_graph.jsonl"))
    parents = build_structured_parent_sets(panel, graph, set(panel["node_ids"]), StructuredPropagationConfig())
    wanted = [p for p in parents if p["target_node"] == "serviceB.request.p99_latency_ms"]
    assert any(p["parent_node"] == "serviceA.cpu.throttled_usec" and p["relation_type"] == "cross_service_resource_to_request" for p in wanted)
    assert not any(p["parent_node"] == "serviceC.cpu.throttled_usec" and p["relation_type"].startswith("cross_service") for p in wanted)


def test_multilag_fit_finds_lag2(tmp_path):
    raw = tmp_path / "raw"
    cand = tmp_path / "cand"
    probe = tmp_path / "probe"
    alert = tmp_path / "alert"
    for p in [raw, cand, probe, alert]:
        p.mkdir()
    rows=[]
    x=[0,0,1,0,0,2,0,0,3,0,0,4]
    for t,v in enumerate(x):
        rows.append({"timestamp": t, "service": "serviceA", "metric": "cpu.throttled_usec", "value": v})
        rows.append({"timestamp": t, "service": "serviceB", "metric": "request.p99_latency_ms", "value": x[t-2] if t>=2 else 0})
    _write_jsonl(raw / "metrics.jsonl", rows)
    _write_jsonl(raw / "service_graph.jsonl", [{"src": "serviceB", "dst": "serviceA"}])
    _write_jsonl(cand / "candidate_metric_nodes.jsonl", [
        {"node_id": "serviceA.cpu.throttled_usec", "service": "serviceA", "metric": "cpu.throttled_usec"},
        {"node_id": "serviceB.request.p99_latency_ms", "service": "serviceB", "metric": "request.p99_latency_ms"},
    ])
    _write_jsonl(probe / "sampling_log.jsonl", [])
    _write_jsonl(probe / "observation_mask.jsonl", [])
    _write_jsonl(alert / "alert_windows.jsonl", [{"start_ts": 5, "end_ts": 9}])
    out = tmp_path / "out"
    result = fit_structured_multilag_propagation(str(raw), str(cand), str(probe), str(alert), str(out), StructuredPropagationConfig(lags=[1,2,3], min_points=3))
    edges = [e for e in result["edges"] if e["parent_node"] == "serviceA.cpu.throttled_usec" and e["target_node"] == "serviceB.request.p99_latency_ms"]
    assert edges
    best = max(edges, key=lambda e: e["abs_coefficient"])
    assert best["lag"] == 2
    assert result["metadata"]["stable_only"] is True
    assert result["metadata"]["propagation_drift_used"] is False


def test_parent_set_not_full_connection(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    rows=[]
    graph=[]
    for s in range(10):
        svc=f"service{s}"
        if s>0:
            graph.append({"src": svc, "dst": f"service{s-1}"})
        for t in range(6):
            rows.append({"timestamp": t, "service": svc, "metric": "cpu.throttled_usec", "value": t})
            rows.append({"timestamp": t, "service": svc, "metric": "request.p99_latency_ms", "value": t})
    _write_jsonl(raw / "metrics.jsonl", rows)
    _write_jsonl(raw / "service_graph.jsonl", graph)
    panel=load_metrics_panel(str(raw/'metrics.jsonl'))
    parents=build_structured_parent_sets(panel, parse_service_graph(str(raw/'service_graph.jsonl')), set(panel['node_ids']), StructuredPropagationConfig(max_parents_per_target=5))
    assert parents
    assert len(parents) < len(panel['node_ids']) * len(panel['node_ids'])
    by_target={}
    for p in parents:
        by_target[p['target_node']]=by_target.get(p['target_node'],0)+1
    assert max(by_target.values()) <= 5


def test_service_propagation_support_prefers_learned_path():
    graph={"children": {"serviceA": ["serviceB"], "serviceB": ["frontend"], "serviceC": []}}
    edges=[
        {"parent_node": "serviceA.cpu.throttled_usec", "target_node": "serviceB.request.p99_latency_ms", "relation_type": "cross_service_resource_to_request", "effective_weight": 3.0, "lag": 2},
        {"parent_node": "serviceB.request.p99_latency_ms", "target_node": "frontend.request.p99_latency_ms", "relation_type": "cross_service_request_to_request", "effective_weight": 2.0, "lag": 1},
    ]
    residual={"service_request_support": {"serviceB": 0.8, "frontend": 1.0}}
    a=compute_service_to_symptom_propagation_support("serviceA", "frontend", edges, graph, residual)
    c=compute_service_to_symptom_propagation_support("serviceC", "frontend", edges, graph, residual)
    assert a["structured_propagation_support"] > c["structured_propagation_support"]
    assert a["uses_labels"] is False

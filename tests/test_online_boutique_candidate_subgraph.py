from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.candidate_subgraph import (
    build_candidate_metric_nodes,
    build_candidate_services,
    build_candidate_subgraphs_for_repeat,
    evaluate_candidate_subgraph_for_debug,
    parse_metric_services,
    parse_service_graph,
)
from proberca.cli.check_a4_candidate_subgraph import check_a4_candidate_subgraph


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _make_fake_raw_and_alert(tmp_path: Path) -> tuple[Path, Path]:
    raw = tmp_path / "raw"
    alert = tmp_path / "alert"
    _write_jsonl(raw / "service_graph.jsonl", [
        {"src": "paymentservice", "dst": "checkoutservice"},
        {"src": "shippingservice", "dst": "checkoutservice"},
        {"src": "checkoutservice", "dst": "frontend"},
        {"src": "redis-cart", "dst": "cartservice"},
        {"src": "cartservice", "dst": "frontend"},
    ])
    metrics = []
    for service, metric in [
        ("frontend", "request.p99_latency_ms"),
        ("checkoutservice", "request.p99_latency_ms"),
        ("paymentservice", "cpu.throttled_usec"),
        ("shippingservice", "net.retrans"),
        ("redis-cart", "io.write_bytes"),
        ("cartservice", "lock.futex_wait_ms"),
        ("unrelatedservice", "cpu.usage"),
    ]:
        metrics.append({"timestamp": 1.0, "service": service, "metric": metric, "value": 1.0})
    _write_jsonl(raw / "metrics.jsonl", metrics)
    _write_jsonl(raw / "incidents.jsonl", [
        {"incident_id": "fake", "root_service": "unrelatedservice", "root_metric": "cpu.usage", "start_ts": 1.0, "end_ts": 2.0}
    ])
    _write_jsonl(alert / "alert_windows.jsonl", [
        {"alert_window_id": "alert-window-0001", "start_ts": 0.0, "end_ts": 5.0, "symptom_service": "frontend", "trigger_metrics": ["frontend.request.p99_latency_ms"], "severity": "hard", "max_z_score": 8.0}
    ])
    return raw, alert


def test_build_candidate_services_reverse_hops_without_incident_labels(tmp_path: Path) -> None:
    raw, _alert = _make_fake_raw_and_alert(tmp_path)
    graph = parse_service_graph(str(raw / "service_graph.jsonl"))
    metric_services = parse_metric_services(str(raw / "metrics.jsonl"))
    result = build_candidate_services("frontend", graph, metric_services, reverse_hops=2, forward_hops=1)
    candidates = set(result["candidate_services"])
    assert "frontend" in candidates
    assert "checkoutservice" in candidates
    assert "paymentservice" in candidates
    assert "shippingservice" in candidates
    assert "cartservice" in candidates
    assert "redis-cart" in candidates
    assert "unrelatedservice" not in candidates


def test_candidate_metric_nodes_only_include_candidate_services(tmp_path: Path) -> None:
    raw, _alert = _make_fake_raw_and_alert(tmp_path)
    metric_services = parse_metric_services(str(raw / "metrics.jsonl"))
    nodes = build_candidate_metric_nodes(["frontend", "paymentservice"], metric_services["service_to_metrics"])
    node_ids = {node["node_id"] for node in nodes}
    assert "frontend.request.p99_latency_ms" in node_ids
    assert "paymentservice.cpu.throttled_usec" in node_ids
    assert "unrelatedservice.cpu.usage" not in node_ids


def test_incident_debug_does_not_change_candidates(tmp_path: Path) -> None:
    raw, alert = _make_fake_raw_and_alert(tmp_path)
    output = tmp_path / "candidate"
    summary = build_candidate_subgraphs_for_repeat(str(raw), str(alert), str(output))
    assert "unrelatedservice" not in summary["candidate_services_union"]
    debug = evaluate_candidate_subgraph_for_debug(str(output / "repeat_candidate_summary.json"), str(raw / "incidents.jsonl"))
    assert debug["root_service_hit_rate_debug"] == 0.0
    after = json.loads((output / "repeat_candidate_summary.json").read_text(encoding="utf-8"))
    assert "unrelatedservice" not in after["candidate_services_union"]


def test_build_candidate_subgraph_cli(tmp_path: Path) -> None:
    raw, alert = _make_fake_raw_and_alert(tmp_path)
    output = tmp_path / "candidate_cli"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "proberca.cli.build_candidate_subgraph",
            "--raw-input",
            str(raw),
            "--alert-input",
            str(alert),
            "--output",
            str(output),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "A4 Candidate Subgraph Builder" in result.stdout
    assert (output / "repeat_candidate_summary.json").exists()
    assert (output / "window_01" / "candidate_services.jsonl").exists()


def test_check_a4_candidate_subgraph_minimal(tmp_path: Path) -> None:
    raw, alert = _make_fake_raw_and_alert(tmp_path)
    base = tmp_path / "a4"
    repeat_out = base / "cpu" / "repeat_01"
    build_candidate_subgraphs_for_repeat(str(raw), str(alert), str(repeat_out))
    per_repeat = []
    for index in range(20):
        out = repeat_out if index == 0 else base / "cpu" / f"repeat_copy_{index:02d}"
        if index != 0:
            build_candidate_subgraphs_for_repeat(str(raw), str(alert), str(out))
        per_repeat.append({"candidate_output_dir": str(out), "has_candidate_graph": True})
    summary = {
        "total_repeats": 20,
        "repeats_with_candidate_graph": 20,
        "uses_root_labels_for_building": False,
        "uses_target_config_for_building": False,
        "uses_injected_path_for_building": False,
        "uses_incident_start_end_for_building": False,
        "per_repeat": per_repeat,
    }
    (base / "p2_candidate_preview_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    result = check_a4_candidate_subgraph(str(base))
    assert result["passed"] is True

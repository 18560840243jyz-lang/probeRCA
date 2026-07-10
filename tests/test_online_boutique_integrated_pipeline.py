from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.blind_evidence import generate_blind_evidence_from_alert_windows
from proberca.adapters.online_boutique.integrated_pipeline import (
    build_metric_candidate_table,
    build_service_candidate_table,
    build_path_explanation,
    metric_diagnostic_specificity,
    select_root_metric_within_service,
    select_root_service,
    run_integrated_blind_rca,
)
from proberca.cli.check_b1_integrated_pipeline import check_b1_integrated_pipeline


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _fake_raw(tmp_path: Path, with_incident: bool = False) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    rows: list[dict] = []
    for t in range(12):
        high = t >= 6
        rows.extend([
            {"timestamp": t, "service": "frontend", "metric": "request.p99_latency_ms", "value": 10.0 if not high else 120.0},
            {"timestamp": t, "service": "frontend", "metric": "request.p95_latency_ms", "value": 9.0 if not high else 100.0},
            {"timestamp": t, "service": "checkoutservice", "metric": "request.p99_latency_ms", "value": 8.0 if not high else 40.0},
            {"timestamp": t, "service": "paymentservice", "metric": "cpu.throttled_usec", "value": 1.0 if not high else 80.0},
            {"timestamp": t, "service": "paymentservice", "metric": "request.p99_latency_ms", "value": 7.0 if not high else 30.0},
        ])
    _write_jsonl(raw / "metrics.jsonl", rows)
    _write_jsonl(raw / "service_graph.jsonl", [
        {"src": "frontend", "dst": "checkoutservice"},
        {"src": "checkoutservice", "dst": "paymentservice"},
    ])
    (raw / "metadata.json").write_text(json.dumps({"fixture": True}), encoding="utf-8")
    if with_incident:
        _write_jsonl(raw / "incidents.jsonl", [{
            "incident_id": "fake-1",
            "start_ts": 6,
            "end_ts": 11,
            "root_service": "unrelatedservice",
            "root_metric": "unrelated.metric",
            "root_type": "CPU",
            "injected_path": ["unrelatedservice", "frontend"],
        }])
    return raw


def test_integrated_pipeline_without_incidents(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "out"
    result = run_integrated_blind_rca(str(raw), str(out))
    result_path = out / "09_final_result" / "integrated_rca_results.jsonl"
    metadata_path = out / "09_final_result" / "integrated_rca_metadata.json"
    assert result_path.exists()
    assert metadata_path.exists()
    row = json.loads(result_path.read_text().splitlines()[0])
    assert row["label_safety"]["uses_root_labels"] is False
    assert row["label_safety"]["uses_incident_start_end"] is False
    assert row["label_safety"]["uses_legacy_evidence"] is False
    assert result["metadata"]["runs_old_p1_rca"] is False


def test_integrated_debug_incidents_do_not_change_result(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path, with_incident=True)
    out1 = tmp_path / "out_no_debug"
    out2 = tmp_path / "out_debug"
    run_integrated_blind_rca(str(raw), str(out1), debug_evaluate_incidents=False)
    run_integrated_blind_rca(str(raw), str(out2), debug_evaluate_incidents=True)
    r1 = json.loads((out1 / "09_final_result" / "integrated_rca_results.jsonl").read_text().splitlines()[0])
    r2 = json.loads((out2 / "09_final_result" / "integrated_rca_results.jsonl").read_text().splitlines()[0])
    assert r1["predicted_top1_service"] == r2["predicted_top1_service"]
    assert r1["predicted_top1_metric"] == r2["predicted_top1_metric"]
    assert (out2 / "09_final_result" / "integrated_debug_evaluation.json").exists()


def test_alert_window_blind_evidence_metadata(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    alert_dir = tmp_path / "alerts"
    run_integrated_blind_rca(str(raw), str(tmp_path / "pipeline"))
    alert_windows = tmp_path / "pipeline" / "01_alert_gate" / "alert_windows.jsonl"
    result = generate_blind_evidence_from_alert_windows(str(raw / "metrics.jsonl"), str(alert_windows), str(alert_dir))
    md = json.loads(Path(result["metadata_path"]).read_text())
    assert md["uses_alert_windows"] is True
    assert md["uses_incident_start_end"] is False
    assert md["uses_root_labels"] is False


def test_path_explanation_uses_service_graph(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path, with_incident=True)
    path = build_path_explanation(str(raw / "service_graph.jsonl"), "paymentservice", "frontend")
    assert path["path_status"] == "found"
    assert path["path"][0] == "paymentservice"
    assert path["path"][-1] == "frontend"


def test_integrated_cli_and_check(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "cli_out"
    subprocess.run([
        sys.executable,
        "-m",
        "proberca.cli.run_integrated_blind_rca",
        "--input",
        str(raw),
        "--output",
        str(out),
    ], check=True)
    check = check_b1_integrated_pipeline(str(out))
    assert check["passed"] is True
    subprocess.run([
        sys.executable,
        "-m",
        "proberca.cli.check_b1_integrated_pipeline",
        "--input",
        str(out),
    ], check=True)


def test_metric_candidate_primary_keeps_service_metric_consistent(tmp_path: Path) -> None:
    stage = tmp_path / "stage"
    (stage / "07_graph_sparse").mkdir(parents=True)
    (stage / "08_counterfactual").mkdir(parents=True)
    (stage / "06_evidence_channel").mkdir(parents=True)
    _write_jsonl(stage / "07_graph_sparse" / "metric_scores.jsonl", [
        {"node_id": "redis-cart.memory.usage", "service": "redis-cart", "metric": "memory.usage", "metric_family": "memory", "metric_score": 10.0, "evidence_support": 1.0},
        {"node_id": "adservice.cpu.throttled_usec", "service": "adservice", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 8.0, "evidence_support": 0.0},
    ])
    _write_jsonl(stage / "07_graph_sparse" / "service_scores.jsonl", [
        {"service": "adservice", "service_score": 100.0},
        {"service": "redis-cart", "service_score": 1.0},
    ])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_metric_ranking.jsonl", [])
    _write_jsonl(stage / "06_evidence_channel" / "evidence_vectors.jsonl", [])
    from proberca.adapters.online_boutique.integrated_pipeline import select_primary_candidate, build_top_services_from_candidates
    dirs = {"graph_sparse": str(stage / "07_graph_sparse"), "counterfactual": str(stage / "08_counterfactual"), "evidence_channel": str(stage / "06_evidence_channel")}
    table = build_metric_candidate_table(dirs)
    primary = select_primary_candidate(table)
    top_services = build_top_services_from_candidates(table)
    assert primary["service"] == primary["node_id"].split(".", 1)[0]
    assert top_services[0]["service"] == primary["service"]
    assert "diagnostic_specificity" in primary["score_components"]


def test_integrated_pipeline_writes_per_window_results(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "out_per_window"
    run_integrated_blind_rca(str(raw), str(out))
    md = json.loads((out / "09_final_result" / "integrated_rca_metadata.json").read_text())
    rows = (out / "09_final_result" / "integrated_rca_results.jsonl").read_text().splitlines()
    assert md["per_window_results_count"] == md["alert_windows_count"]
    assert len(rows) == md["alert_windows_count"]
    assert md["primary_candidate_source"] == "service_candidate_table"
    assert md["service_first_enabled"] is True
    assert md["primary_metric_source"] == "metric_candidates_within_root_service"
    assert md["global_top_metrics_primary"] is False
    assert md["top_service_metric_consistent"] is True
    assert (out / "09_final_result" / "integrated_rca_aggregate.json").exists()


def test_root_type_and_path_come_from_primary_candidate(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write_jsonl(raw / "service_graph.jsonl", [{"src": "frontend", "dst": "cartservice"}, {"src": "cartservice", "dst": "redis-cart"}])
    from proberca.adapters.online_boutique.integrated_pipeline import _metric_family_to_root_type, build_path_explanation
    assert _metric_family_to_root_type("memory") == "memory"
    path = build_path_explanation(str(raw / "service_graph.jsonl"), "redis-cart", "frontend")
    assert path["path_root_service"] == "redis-cart"
    assert path["path_uses_injected_path"] is False
    assert path["path_status"] == "found"


def test_b1_check_fails_on_inconsistent_service_metric(tmp_path: Path) -> None:
    root = tmp_path / "bad"
    for rel in ["01_alert_gate", "02_blind_evidence", "03_candidate_subgraph", "04_probe_policy", "05_ipw_rls", "06_evidence_channel", "07_graph_sparse", "08_counterfactual", "09_final_result"]:
        (root / rel).mkdir(parents=True, exist_ok=True)
    _write_jsonl(root / "01_alert_gate" / "alert_windows.jsonl", [{"alert_window_id": "w1"}])
    _write_jsonl(root / "02_blind_evidence" / "blind_evidence.jsonl", [])
    (root / "02_blind_evidence" / "blind_evidence_metadata.json").write_text(json.dumps({"uses_alert_windows": True, "uses_incident_start_end": False}), encoding="utf-8")
    for path in [
        root / "03_candidate_subgraph" / "repeat_candidate_summary.json",
        root / "05_ipw_rls" / "ipw_rls_metadata.json",
        root / "08_counterfactual" / "counterfactual_metadata.json",
        root / "09_final_result" / "integrated_rca_metadata.json",
    ]:
        path.write_text("{}", encoding="utf-8")
    for path in [
        root / "04_probe_policy" / "sampling_log.jsonl",
        root / "04_probe_policy" / "observation_mask.jsonl",
        root / "06_evidence_channel" / "calibrated_residuals.jsonl",
        root / "07_graph_sparse" / "metric_scores.jsonl",
        root / "07_graph_sparse" / "service_scores.jsonl",
        root / "09_final_result" / "metric_candidate_table.jsonl",
        root / "09_final_result" / "top_services.jsonl",
    ]:
        _write_jsonl(path, [])
    bad_md = {"alert_windows_count": 1, "per_window_results_count": 1, "uses_root_labels": False, "uses_target_config": False, "uses_injected_path": False, "uses_incident_start_end": False, "uses_legacy_evidence": False, "runs_old_p1_rca": False, "uses_alert_windows": True, "primary_candidate_source": "metric_candidate_table", "top_service_metric_consistent": False, "per_window_results_match_alert_windows": True}
    (root / "09_final_result" / "integrated_rca_metadata.json").write_text(json.dumps(bad_md), encoding="utf-8")
    _write_jsonl(root / "09_final_result" / "integrated_rca_results.jsonl", [{"predicted_top1_service": "adservice", "predicted_top1_metric": "redis-cart.memory.usage", "predicted_root_type": "memory", "primary_candidate": {"metric_family": "memory"}, "path_explanation": {"path_root_service": "adservice", "path_uses_injected_path": False}, "label_safety": {"uses_root_labels": False, "uses_target_config": False, "uses_injected_path": False, "uses_incident_start_end": False, "uses_legacy_evidence": False, "runs_old_p1_rca": False}}])
    check = check_b1_integrated_pipeline(str(root))
    assert check["passed"] is False
    assert any("mismatch" in item or "top_service_metric_consistent" in item for item in check["failed_checks"])


def test_b1_check_fails_on_window_result_count_mismatch(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "out_mismatch"
    run_integrated_blind_rca(str(raw), str(out))
    md_path = out / "09_final_result" / "integrated_rca_metadata.json"
    md = json.loads(md_path.read_text())
    md["per_window_results_count"] = 1 if md["alert_windows_count"] != 1 else 0
    md["per_window_results_match_alert_windows"] = False
    md_path.write_text(json.dumps(md), encoding="utf-8")
    check = check_b1_integrated_pipeline(str(out))
    assert check["passed"] is False
    assert any("per_window" in item for item in check["failed_checks"])


def test_metric_diagnostic_specificity_static_rules() -> None:
    cpu = metric_diagnostic_specificity("cpu.throttled_usec", "CPU")
    memory = metric_diagnostic_specificity("memory.usage", "memory")
    request = metric_diagnostic_specificity("request.p99_latency_ms", "load")
    assert cpu["specificity_score"] > memory["specificity_score"]
    assert memory["specificity_score"] > request["specificity_score"]


def test_b2r_memory_usage_penalty_and_cpu_boost(tmp_path: Path) -> None:
    stage = tmp_path / "stage_b2r"
    for rel in ["07_graph_sparse", "08_counterfactual", "06_evidence_channel", "02_blind_evidence"]:
        (stage / rel).mkdir(parents=True)
    _write_jsonl(stage / "07_graph_sparse" / "metric_scores.jsonl", [
        {"node_id": "redis-cart.memory.usage", "service": "redis-cart", "metric": "memory.usage", "metric_family": "memory", "metric_score": 10.0},
        {"node_id": "paymentservice.cpu.throttled_usec", "service": "paymentservice", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 8.5},
    ])
    _write_jsonl(stage / "07_graph_sparse" / "service_scores.jsonl", [
        {"service": "redis-cart", "service_score": 10.0},
        {"service": "paymentservice", "service_score": 9.5},
    ])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_metric_ranking.jsonl", [])
    _write_jsonl(stage / "06_evidence_channel" / "evidence_vectors.jsonl", [])
    _write_jsonl(stage / "02_blind_evidence" / "blind_evidence.jsonl", [
        {"node": "paymentservice.cpu.throttled_usec", "service": "paymentservice", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "evidence_score": 1.0},
        {"node": "redis-cart.memory.usage", "service": "redis-cart", "metric": "memory.usage", "evidence_type": "memory", "evidence_score": 1.0},
    ])
    table = build_metric_candidate_table({
        "graph_sparse": str(stage / "07_graph_sparse"),
        "counterfactual": str(stage / "08_counterfactual"),
        "evidence_channel": str(stage / "06_evidence_channel"),
        "blind_evidence": str(stage / "02_blind_evidence"),
    })
    scores = {row["node_id"]: row for row in table}
    assert scores["paymentservice.cpu.throttled_usec"]["final_candidate_score"] > scores["redis-cart.memory.usage"]["final_candidate_score"]
    assert scores["redis-cart.memory.usage"]["score_components"]["weak_memory_usage_penalty_applied"] is True
    assert scores["paymentservice.cpu.throttled_usec"]["score_components"]["cpu_diagnostic_boost_applied"] is True


def test_b2r_strong_memory_evidence_avoids_memory_usage_penalty(tmp_path: Path) -> None:
    stage = tmp_path / "stage_memory"
    for rel in ["07_graph_sparse", "08_counterfactual", "06_evidence_channel", "02_blind_evidence"]:
        (stage / rel).mkdir(parents=True)
    _write_jsonl(stage / "07_graph_sparse" / "metric_scores.jsonl", [
        {"node_id": "redis-cart.memory.usage", "service": "redis-cart", "metric": "memory.usage", "metric_family": "memory", "metric_score": 10.0},
    ])
    _write_jsonl(stage / "07_graph_sparse" / "service_scores.jsonl", [{"service": "redis-cart", "service_score": 10.0}])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_metric_ranking.jsonl", [])
    _write_jsonl(stage / "06_evidence_channel" / "evidence_vectors.jsonl", [])
    _write_jsonl(stage / "02_blind_evidence" / "blind_evidence.jsonl", [
        {"node": "redis-cart.memory.events", "service": "redis-cart", "metric": "memory.events", "evidence_type": "memory", "evidence_score": 0.8},
        {"node": "redis-cart.memory.usage", "service": "redis-cart", "metric": "memory.usage", "evidence_type": "memory", "evidence_score": 0.7},
    ])
    table = build_metric_candidate_table({
        "graph_sparse": str(stage / "07_graph_sparse"),
        "counterfactual": str(stage / "08_counterfactual"),
        "evidence_channel": str(stage / "06_evidence_channel"),
        "blind_evidence": str(stage / "02_blind_evidence"),
    })
    assert table[0]["score_components"]["strong_memory_evidence_available"] is True
    assert table[0]["score_components"]["weak_memory_usage_penalty_applied"] is False


def test_b2s_service_first_selects_metric_within_service(tmp_path: Path) -> None:
    stage = tmp_path / "stage_b2s"
    for rel in ["07_graph_sparse", "08_counterfactual", "06_evidence_channel", "02_blind_evidence"]:
        (stage / rel).mkdir(parents=True)
    _write_jsonl(stage / "07_graph_sparse" / "metric_scores.jsonl", [
        {"node_id": "serviceA.cpu.throttled_usec", "service": "serviceA", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 10.0},
        {"node_id": "serviceB.cpu.throttled_usec", "service": "serviceB", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 8.0},
        {"node_id": "serviceB.cpu.throttle_ratio", "service": "serviceB", "metric": "cpu.throttle_ratio", "metric_family": "CPU", "metric_score": 7.5},
    ])
    _write_jsonl(stage / "07_graph_sparse" / "service_scores.jsonl", [
        {"service": "serviceA", "service_score": 7.0},
        {"service": "serviceB", "service_score": 9.0},
    ])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_metric_ranking.jsonl", [])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_service_ranking.jsonl", [
        {"service": "serviceB", "delta_loss": 10.0, "combined_score": 1.0},
        {"service": "serviceA", "delta_loss": 1.0, "combined_score": 0.1},
    ])
    _write_jsonl(stage / "06_evidence_channel" / "evidence_vectors.jsonl", [])
    _write_jsonl(stage / "06_evidence_channel" / "calibrated_residuals.jsonl", [
        {"service": "frontend", "metric": "request.p99_latency_ms", "metric_family": "load", "node_id": "frontend.request.p99_latency_ms", "calibrated_residual": 8.0},
        {"service": "serviceB", "metric": "cpu.throttled_usec", "metric_family": "CPU", "node_id": "serviceB.cpu.throttled_usec", "calibrated_residual": 5.0},
    ])
    _write_jsonl(stage / "02_blind_evidence" / "blind_evidence.jsonl", [
        {"node": "serviceB.cpu.throttled_usec", "service": "serviceB", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "evidence_score": 1.0},
    ])
    dirs = {"graph_sparse": str(stage / "07_graph_sparse"), "counterfactual": str(stage / "08_counterfactual"), "evidence_channel": str(stage / "06_evidence_channel"), "blind_evidence": str(stage / "02_blind_evidence")}
    metric_table = build_metric_candidate_table(dirs)
    service_graph = {"services": ["serviceA", "serviceB", "frontend"], "edges": [{"src": "frontend", "dst": "serviceA"}, {"src": "frontend", "dst": "serviceB"}]}
    service_table = build_service_candidate_table(metric_table, dirs, "frontend", service_graph)
    root_service = select_root_service(service_table)
    root_metric = select_root_metric_within_service(metric_table, root_service["service"])
    assert root_service["service"] == "serviceB"
    assert root_metric["service"] == "serviceB"
    assert root_metric["node_id"].startswith("serviceB.")


def test_b2s_integrated_primary_top_metrics_are_conditioned_on_service(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "service_first_out"
    run_integrated_blind_rca(str(raw), str(out))
    row = json.loads((out / "09_final_result" / "integrated_rca_results.jsonl").read_text().splitlines()[0])
    top1_service = row["predicted_top1_service"]
    assert row["service_first"] is True
    assert row["primary_metric_conditioned_on_service"] is True
    assert row["global_top_metrics_primary"] is False
    assert all(metric["service"] == top1_service for metric in row["top_metrics"])
    assert row["predicted_top1_metric"].split(".", 1)[0] == top1_service
    assert all(metric.get("auxiliary") is True for metric in row["global_top_metrics_auxiliary"])



def test_b2m_service_local_evidence_prevents_global_cpu_flattening(tmp_path: Path) -> None:
    stage = tmp_path / "stage_b2m_local"
    for rel in ["07_graph_sparse", "08_counterfactual", "06_evidence_channel", "02_blind_evidence"]:
        (stage / rel).mkdir(parents=True)
    _write_jsonl(stage / "07_graph_sparse" / "metric_scores.jsonl", [
        {"node_id": "paymentservice.cpu.throttled_usec", "service": "paymentservice", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 8.0},
        {"node_id": "adservice.cpu.throttled_usec", "service": "adservice", "metric": "cpu.throttled_usec", "metric_family": "CPU", "metric_score": 8.0},
    ])
    _write_jsonl(stage / "07_graph_sparse" / "service_scores.jsonl", [
        {"service": "paymentservice", "service_score": 1.0},
        {"service": "adservice", "service_score": 1.0},
    ])
    _write_jsonl(stage / "08_counterfactual" / "counterfactual_metric_ranking.jsonl", [])
    _write_jsonl(stage / "06_evidence_channel" / "evidence_vectors.jsonl", [])
    _write_jsonl(stage / "02_blind_evidence" / "blind_evidence.jsonl", [
        {"node_id": "paymentservice.cpu.throttled_usec", "service": "paymentservice", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "evidence_score": 1.0},
        {"node_id": "adservice.cpu.usage", "service": "adservice", "metric": "cpu.usage", "evidence_type": "CPU", "evidence_score": 0.2},
    ])
    table = build_metric_candidate_table({
        "graph_sparse": str(stage / "07_graph_sparse"),
        "counterfactual": str(stage / "08_counterfactual"),
        "evidence_channel": str(stage / "06_evidence_channel"),
        "blind_evidence": str(stage / "02_blind_evidence"),
    })
    by_node = {row["node_id"]: row for row in table}
    payment = by_node["paymentservice.cpu.throttled_usec"]
    ad = by_node["adservice.cpu.throttled_usec"]
    assert payment["score_components"]["node_evidence_support"] > ad["score_components"]["node_evidence_support"]
    assert payment["score_components"]["service_family_evidence_support"] > ad["score_components"]["service_family_evidence_support"]
    assert payment["score_components"]["family_global_evidence_support"] == ad["score_components"]["family_global_evidence_support"]
    assert payment["score_components"]["evidence_norm"] > ad["score_components"]["evidence_norm"]
    assert payment["final_candidate_score"] > ad["final_candidate_score"]


def test_b2m_check_fails_on_invalid_primary_ownership(tmp_path: Path) -> None:
    raw = _fake_raw(tmp_path)
    out = tmp_path / "out_invalid_ownership"
    run_integrated_blind_rca(str(raw), str(out))
    result_path = out / "09_final_result" / "integrated_rca_results.jsonl"
    rows = [json.loads(line) for line in result_path.read_text().splitlines()]
    rows[0]["primary_candidate"]["ownership_valid"] = False
    rows[0]["primary_candidate"]["service_matches_node_id"] = False
    _write_jsonl(result_path, rows)
    md_path = out / "09_final_result" / "integrated_rca_metadata.json"
    md = json.loads(md_path.read_text())
    md["primary_candidate_ownership_valid"] = False
    md_path.write_text(json.dumps(md), encoding="utf-8")
    check = check_b1_integrated_pipeline(str(out))
    assert check["passed"] is False
    assert any("ownership" in item for item in check["failed_checks"])

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.adaptive_probe_policy import (
    build_probe_plan_for_window,
    evaluate_probe_policy_for_debug,
    load_blind_evidence_optional,
    load_candidate_graph,
    write_probe_policy_outputs,
)
from proberca.cli.check_a5_probe_policy import check_a5_probe_policy


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _make_fake_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    alert = tmp_path / "alert"
    candidate = tmp_path / "candidate"
    blind = tmp_path / "blind"
    raw = tmp_path / "raw"
    _write_jsonl(alert / "alert_windows.jsonl", [
        {"alert_window_id": "aw1", "symptom_service": "frontend", "severity": "hard", "max_z_score": 8.0, "start_ts": 1.0, "end_ts": 2.0}
    ])
    window = candidate / "window_01"
    _write_jsonl(window / "candidate_services.jsonl", [
        {"service": "frontend"}, {"service": "checkoutservice"}, {"service": "paymentservice"}, {"service": "redis-cart"}
    ])
    _write_jsonl(window / "candidate_metric_nodes.jsonl", [
        {"service": "frontend", "metric": "request.p99_latency_ms", "node_id": "frontend.request.p99_latency_ms"},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "node_id": "paymentservice.cpu.throttled_usec"},
        {"service": "redis-cart", "metric": "io.write_bytes", "node_id": "redis-cart.io.write_bytes"},
    ])
    _write_jsonl(window / "candidate_edges.jsonl", [
        {"src": "paymentservice", "dst": "checkoutservice"},
        {"src": "checkoutservice", "dst": "frontend"},
        {"src": "redis-cart", "dst": "checkoutservice"},
    ])
    (window / "candidate_subgraph_metadata.json").write_text(json.dumps({"uses_root_labels": False, "actual_probe_activation": False}), encoding="utf-8")
    (candidate / "repeat_candidate_summary.json").write_text(json.dumps({"window_summaries": [{"window_output_dir": str(window)}]}), encoding="utf-8")
    _write_jsonl(blind / "blind_evidence.jsonl", [
        {"service": "redis-cart", "metric": "io.write_bytes", "evidence_type": "storage I/O", "evidence_score": 1.0},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "evidence_score": 0.1},
    ])
    _write_jsonl(raw / "incidents.jsonl", [
        {"incident_id": "fake", "root_service": "paymentservice", "root_metric": "cpu.throttled_usec", "root_type": "CPU", "start_ts": 1.0, "end_ts": 2.0}
    ])
    return alert, candidate, blind, raw


def test_build_probe_plan_budget_and_evidence_gain(tmp_path: Path) -> None:
    alert, candidate, blind, _raw = _make_fake_inputs(tmp_path)
    graph = load_candidate_graph(str(candidate))
    evidence = load_blind_evidence_optional(str(blind))
    window = {"alert_window_id": "aw1", "symptom_service": "frontend", "severity": "hard", "max_z_score": 8.0}
    plan = build_probe_plan_for_window(window, graph, evidence, budget=12.0)
    selected = plan["selected_probes"]
    assert any(row["probe_name"] == "request_probe" for row in selected)
    redis_io = [row for row in selected + plan["unselected_probes"] if row["service"] == "redis-cart" and row["probe_name"] == "io_probe"][0]
    payment_io = [row for row in selected + plan["unselected_probes"] if row["service"] == "paymentservice" and row["probe_name"] == "cpu_probe"][0]
    assert redis_io["gain"] > payment_io["gain"]
    assert plan["estimated_cost"] <= plan["budget"]
    assert plan["uses_root_labels"] is False


def test_debug_incident_does_not_change_policy(tmp_path: Path) -> None:
    alert, candidate, blind, raw = _make_fake_inputs(tmp_path)
    out = tmp_path / "out"
    result = write_probe_policy_outputs(str(alert), str(candidate), str(out), str(blind), budget=2.0)
    before = (out / "probe_plan.jsonl").read_text(encoding="utf-8")
    debug = evaluate_probe_policy_for_debug(str(out), str(raw / "incidents.jsonl"))
    after = (out / "probe_plan.jsonl").read_text(encoding="utf-8")
    assert before == after
    assert "debug_root_metric_family_selected_rate" in debug


def test_write_probe_policy_outputs_and_cli(tmp_path: Path) -> None:
    alert, candidate, _blind, _raw = _make_fake_inputs(tmp_path)
    out = tmp_path / "cli_out"
    result = subprocess.run(
        [sys.executable, "-m", "proberca.cli.build_adaptive_probe_policy", "--alert-input", str(alert), "--candidate-input", str(candidate), "--output", str(out)],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "A5 Adaptive Probe Policy" in result.stdout
    assert (out / "probe_plan.jsonl").exists()
    assert (out / "sampling_log.jsonl").exists()
    assert (out / "observation_mask.jsonl").exists()
    metadata = json.loads((out / "adaptive_probe_metadata.json").read_text(encoding="utf-8"))
    assert metadata["actual_probe_activation"] is False


def test_check_a5_probe_policy_minimal(tmp_path: Path) -> None:
    alert, candidate, blind, _raw = _make_fake_inputs(tmp_path)
    base = tmp_path / "a5"
    per_repeat = []
    for index in range(20):
        out = base / "cpu" / f"repeat_{index:02d}"
        write_probe_policy_outputs(str(alert), str(candidate), str(out), str(blind), budget=12.0)
        per_repeat.append({"output_dir": str(out), "has_probe_plan": True})
    summary = {
        "total_repeats": 20,
        "repeats_with_probe_plan": 20,
        "uses_root_labels_for_policy": False,
        "uses_target_config_for_policy": False,
        "uses_injected_path_for_policy": False,
        "uses_incident_start_end_for_policy": False,
        "actual_probe_activation": False,
        "per_repeat": per_repeat,
    }
    (base / "p2_probe_policy_preview_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_a5_probe_policy(str(base))["passed"] is True

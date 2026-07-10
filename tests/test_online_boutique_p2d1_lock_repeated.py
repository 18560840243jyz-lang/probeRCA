import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.lock_fault import parse_lockstress_logs
from proberca.adapters.online_boutique.p2d1_lock_repeat import aggregate_lock_repeat_summary, build_lock_evidence, build_lock_incident, collect_lock_window_metrics
from proberca.cli.check_p2d1_lock_repeated import check_p2d1_lock_repeated


def test_parse_lockstress_logs_p95_and_warning():
    text = "\n".join([
        json.dumps({"timestamp": 1.0, "lock_wait_ms_sum": 10.0, "lock_wait_ms_mean": 1.0, "lock_wait_ms_p95": 3.0, "lock_contention_count": 4, "source": "real_sidecar_lockstress"}),
        json.dumps({"timestamp": 2.0, "lock_wait_ms_sum": 30.0, "lock_wait_ms_mean": 2.0, "lock_wait_p95_ms": 7.0, "lock_contention_count": 9, "source": "real_sidecar_lockstress", "final": True}),
    ])
    parsed = parse_lockstress_logs(text)
    assert parsed["lock_metrics_available"] is True
    assert parsed["lock_wait_ms_p95_max"] == 7.0
    assert parsed["lock_wait_ms_mean_avg"] == 1.5
    assert parsed["p95_parse_warning"] is False

    warning = parse_lockstress_logs(json.dumps({"timestamp": 1.0, "lock_wait_ms_sum": 20.0, "lock_wait_ms_mean": 5.0, "lock_wait_ms_p95": 1.0, "lock_contention_count": 2, "source": "real_sidecar_lockstress", "final": True}))
    assert warning["p95_parse_warning"] is True


def test_collect_lock_window_metrics_distribution(monkeypatch):
    def fake_curl(_url, _requests, _timeout):
        return {"rps": 1.0, "error_rate": 0.0, "p50_latency_ms": 1.0, "p95_latency_ms": 2.0, "p99_latency_ms": 3.0, "http_ok": True}

    monkeypatch.setattr("proberca.adapters.online_boutique.p2d1_lock_repeat.curl_frontend", fake_curl)
    monkeypatch.setattr("proberca.adapters.online_boutique.p2d1_lock_repeat.time.sleep", lambda _seconds: None)
    config = {
        "target": {"service": "cartservice"},
        "experiment": {"frontend_url": "http://127.0.0.1:8080", "requests_per_window": 1, "request_timeout_sec": 1, "window_size_sec": 0, "faulty_windows": 2},
        "_runtime": {"incident_id": "inc", "pod_name_during": "cart-pod"},
    }
    rows = collect_lock_window_metrics(config, "faulty", 1, {"lock_wait_ms_sum_total": 100.0, "lock_wait_ms_mean_avg": 5.0, "lock_contention_count_total": 20})
    metrics = {row["metric"]: row["value"] for row in rows if row["service"] == "cartservice"}
    assert metrics["lock.futex_wait_ms"] == 50.0
    assert metrics["lock.wait_ms"] == 5.0
    assert metrics["lock.contention_count"] == 10.0


def test_build_lock_evidence():
    incident = build_lock_incident(1, 10.0, 20.0)
    metrics = [
        {"service": "cartservice", "metric": "lock.futex_wait_ms", "value": 100.0, "phase": "faulty"},
        {"service": "cartservice", "metric": "lock.contention_count", "value": 5.0, "phase": "faulty"},
    ]
    evidence = build_lock_evidence(metrics, incident)
    assert evidence
    assert evidence[0]["evidence_type"] == "Lock"
    assert evidence[0]["root_type_hint"] == "lock contention"


def test_aggregate_and_check_p2d1(tmp_path: Path):
    config = {"repeat_experiment": {"experiment_group_id": "g", "repeats": 5}}
    rows = []
    for idx in range(5):
        rows.append({"repeat_index": idx + 1, "quality_ok": True, "rca_ok": True, "service_hit_at_1": 1.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 1.0, "metric_mrr": 0.5, "root_type_accuracy": 1.0, "path_fidelity": 1.0, "lock_wait_ms_sum_total": 100.0, "lock_contention_count_total": 10.0, "frontend_latency_lift": 2.0})
    summary = aggregate_lock_repeat_summary(config, rows)
    assert summary["metric_hit_at_3_mean"] == 1.0
    assert summary["metric_hit_at_1_mean"] == 0.0
    base = tmp_path / "ok"
    base.mkdir()
    (base / "p2d1_lock_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2d1_lock_repeated(base)["passed"] is True
    summary["metric_hit_at_3_mean"] = 0.0
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "p2d1_lock_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    result = check_p2d1_lock_repeated(bad)
    assert result["passed"] is False
    assert result["failed_checks"]


def test_cli_imports():
    importlib.import_module("proberca.cli.run_p2d1_lock_repeated")
    importlib.import_module("proberca.cli.check_p2d1_lock_repeated")

from proberca.adapters.online_boutique.lock_fault import build_phaseaware_lockstress_python_command
from proberca.adapters.online_boutique.p2d1r_lock_repeat import collect_phaseaware_lock_metrics_from_logs
from proberca.cli.check_p2d1r_lock_repeated import check_p2d1r_lock_repeated


def test_build_phaseaware_lockstress_python_command_contains_phases():
    command = build_phaseaware_lockstress_python_command(5, 2, 3, 1, 4, 2)
    assert "baseline" in command
    assert "faulty" in command
    assert "recovery" in command
    assert "real_phaseaware_sidecar_lockstress" in command
    assert "sleep 3600" in command


def test_parse_phaseaware_lockstress_logs_counts_and_p95():
    lines = []
    for idx, phase in enumerate(["baseline", "faulty", "recovery"], start=1):
        lines.append(json.dumps({
            "timestamp": float(idx),
            "phase": phase,
            "window_index": idx,
            "lock_wait_ms_sum": 0.0 if phase != "faulty" else 10.0,
            "lock_wait_ms_mean": 0.0 if phase != "faulty" else 1.0,
            "lock_wait_ms_p95": 0.0 if phase != "faulty" else 2.0,
            "lock_contention_count": 0 if phase != "faulty" else 5,
            "workers": 4,
            "lock_active": phase == "faulty",
            "source": "real_phaseaware_sidecar_lockstress",
        }))
    parsed = parse_lockstress_logs("\n".join(lines))
    assert parsed["phaseaware_metrics_available"] is True
    assert parsed["baseline_records_count"] == 1
    assert parsed["faulty_records_count"] == 1
    assert parsed["recovery_records_count"] == 1
    assert parsed["faulty_lock_wait_ms_sum_total"] == 10.0
    assert parsed["faulty_lock_contention_count_total"] == 5
    assert parsed["lock_wait_ms_p95_max"] == 2.0
    assert parsed["p95_parse_warning"] is False


def test_collect_phaseaware_lock_metrics_from_logs_all_phases():
    parsed = {
        "records": [
            {"timestamp": 1.0, "phase": "baseline", "window_index": 1, "lock_wait_ms_sum": 0.0, "lock_wait_ms_mean": 0.0, "lock_wait_ms_p95": 0.0, "lock_contention_count": 0, "lock_active": False},
            {"timestamp": 2.0, "phase": "faulty", "window_index": 2, "lock_wait_ms_sum": 10.0, "lock_wait_ms_mean": 1.0, "lock_wait_ms_p95": 2.0, "lock_contention_count": 5, "lock_active": True},
            {"timestamp": 3.0, "phase": "recovery", "window_index": 3, "lock_wait_ms_sum": 0.0, "lock_wait_ms_mean": 0.0, "lock_wait_ms_p95": 0.0, "lock_contention_count": 0, "lock_active": False},
        ]
    }
    rows = collect_phaseaware_lock_metrics_from_logs(parsed, "cart-pod", incident_id="inc")
    phases = {row["phase"] for row in rows}
    metrics = {row["metric"] for row in rows}
    assert {"baseline", "faulty", "recovery"} <= phases
    assert {"lock.futex_wait_ms", "lock.wait_ms", "lock.wait_p95_ms", "lock.contention_count"} <= metrics
    assert any(row["phase"] == "baseline" and row["metric"] == "lock.futex_wait_ms" and row["value"] == 0.0 for row in rows)


def test_check_p2d1r_lock_repeated_pass_and_fail(tmp_path: Path):
    summary = {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "service_hit_at_1_mean": 0.8,
        "metric_hit_at_3_mean": 0.8,
        "root_type_accuracy_mean": 0.8,
        "path_fidelity_mean": 0.8,
        "lock_wait_lift_mean": 1.0,
        "faulty_lock_contention_count_mean": 1.0,
    }
    ok = tmp_path / "ok"
    ok.mkdir()
    (ok / "p2d1r_lock_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2d1r_lock_repeated(ok)["passed"] is True
    summary["metric_hit_at_3_mean"] = 0.0
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "p2d1r_lock_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    result = check_p2d1r_lock_repeated(bad)
    assert result["passed"] is False
    assert result["failed_checks"]


def test_p2d1r_cli_imports():
    importlib.import_module("proberca.cli.run_p2d1r_lock_repeated")
    importlib.import_module("proberca.cli.check_p2d1r_lock_repeated")

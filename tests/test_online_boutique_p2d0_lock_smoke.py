import importlib
import json

from proberca.adapters.online_boutique.lock_fault import build_lockstress_python_command, evaluate_lock_fault_feasible, parse_lockstress_logs


def test_build_lockstress_python_command_contains_threading_lock():
    command = build_lockstress_python_command(duration_sec=5, workers=4, lock_hold_ms=2)
    assert "threading.Lock" in command
    assert "real_sidecar_lockstress" in command
    assert "sleep 3600" in command


def test_parse_lockstress_logs_json_lines():
    lines = [
        json.dumps({"timestamp": 1.0, "lock_wait_ms_sum": 10.0, "lock_wait_ms_mean": 1.0, "lock_wait_ms_p95": 2.0, "lock_contention_count": 5, "workers": 4, "source": "real_sidecar_lockstress"}),
        "not-json",
        json.dumps({"timestamp": 2.0, "lock_wait_ms_sum": 20.0, "lock_wait_ms_mean": 2.0, "lock_wait_ms_p95": 5.0, "lock_contention_count": 8, "workers": 4, "source": "real_sidecar_lockstress", "final": True}),
    ]
    parsed = parse_lockstress_logs("\n".join(lines))
    assert parsed["lock_metrics_available"] is True
    assert parsed["records_count"] == 2
    assert parsed["lock_wait_ms_sum_total"] == 20.0
    assert parsed["lock_wait_ms_p95_max"] == 5.0
    assert parsed["lock_contention_count_total"] == 8


def test_evaluate_lock_fault_feasible_pass_and_fail():
    summary = {
        "sidecar_injected": True,
        "sidecar_removed": True,
        "frontend_after_http_ok": True,
        "lock_metrics_available": True,
        "lock_contention_count_total": 3,
        "lock_wait_ms_sum_total": 1.5,
    }
    assert evaluate_lock_fault_feasible(summary)["lock_fault_feasible"] is True
    summary["sidecar_removed"] = False
    result = evaluate_lock_fault_feasible(summary)
    assert result["lock_fault_feasible"] is False
    assert result["failed_checks"]


def test_cli_imports():
    importlib.import_module("proberca.cli.run_p2d0_lock_smoke")
    importlib.import_module("proberca.cli.check_p2d0_lock_smoke")


def test_parse_lockstress_logs_p95_warning_aliases():
    payload = {"timestamp": 1.0, "lock_wait_ms_sum": 40.0, "lock_wait_ms_mean": 2.0, "lock_wait_p95_ms": 8.0, "lock_contention_count": 3, "workers": 2, "source": "real_sidecar_lockstress", "final": True}
    parsed = parse_lockstress_logs(json.dumps(payload))
    assert parsed["lock_wait_ms_p95_max"] == 8.0
    assert parsed["p95_parse_warning"] is False

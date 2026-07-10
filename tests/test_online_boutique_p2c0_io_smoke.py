import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.io_fault import build_cadvisor_fs_snapshot, compute_fs_delta, parse_prometheus_text_metrics
from proberca.adapters.online_boutique.p2c0_io_smoke import evaluate_io_fault_feasible
from proberca.cli.check_p2c0_io_smoke import check_p2c0_io_smoke


def test_parse_cadvisor_fs_metrics_and_snapshot():
    text = '''
container_fs_reads_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 10
container_fs_writes_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 20
container_fs_reads_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 1
container_fs_writes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 2
container_fs_io_time_seconds_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 0.5
container_memory_working_set_bytes{namespace="online-boutique",pod="redis-cart-x",container="redis",device="overlay"} 123
'''
    records = parse_prometheus_text_metrics(text)
    assert len(records) == 6
    snapshot = build_cadvisor_fs_snapshot(records, [{"name": "redis-cart-x", "service": "redis-cart"}])
    key = "redis-cart/redis-cart-x/redis/overlay"
    assert snapshot[key]["fs_writes_bytes_total"] == 20.0


def test_compute_fs_delta_and_negative_skip():
    prev = {"k": {"service": "redis-cart", "pod": "p", "container": "redis", "device": "d", "fs_writes_bytes_total": 20.0, "fs_writes_total": 2.0, "fs_io_time_seconds_total": 0.5}}
    curr = {"k": {"service": "redis-cart", "pod": "p", "container": "redis", "device": "d", "fs_writes_bytes_total": 84.0, "fs_writes_total": 6.0, "fs_io_time_seconds_total": 0.7}}
    delta = compute_fs_delta(prev, curr, 5)
    assert delta["k"]["metrics"]["io.write_bytes"] == 64.0
    assert delta["k"]["metrics"]["io.write_ops"] == 4.0
    assert abs(delta["k"]["metrics"]["io.io_time_ms"] - 200.0) < 1e-9
    reset = compute_fs_delta(curr, prev, 5)
    assert "io.write_bytes" not in reset.get("k", {}).get("metrics", {})


def test_io_fault_feasible_and_check_cli_core(tmp_path: Path):
    summary = {"io_fault_feasible": True, "io_stress_started": True, "io_stress_cleaned": True, "frontend_after_http_ok": True, "write_bytes_delta_during": 1.0, "write_ops_delta_during": 0.0}
    assert evaluate_io_fault_feasible(summary)["io_fault_feasible"] is True
    (tmp_path / "p2c0_io_smoke_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2c0_io_smoke(tmp_path)["passed"] is True
    summary["write_bytes_delta_during"] = 0.0
    (tmp_path / "p2c0_io_smoke_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2c0_io_smoke(tmp_path)["passed"] is False


def test_cli_imports():
    importlib.import_module("proberca.cli.run_p2c0_io_smoke")
    importlib.import_module("proberca.cli.check_p2c0_io_smoke")

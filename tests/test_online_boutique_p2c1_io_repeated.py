import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.io_fault import build_cadvisor_fs_snapshot, compute_fs_delta, parse_prometheus_text_metrics
from proberca.adapters.online_boutique.p2c1_io_repeat import _fs_delta_records, aggregate_io_repeat_summary, build_io_incident
from proberca.cli.check_p2c1_io_repeated import check_p2c1_io_repeated


def test_io_write_bytes_delta_from_cadvisor_snapshots():
    before_text = '\ncontainer_fs_writes_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 1000\ncontainer_fs_writes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 10\ncontainer_fs_reads_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 200\ncontainer_fs_reads_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 2\ncontainer_fs_io_time_seconds_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 1.5\n'
    after_text = '\ncontainer_fs_writes_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 7000\ncontainer_fs_writes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 16\ncontainer_fs_reads_bytes_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 500\ncontainer_fs_reads_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 5\ncontainer_fs_io_time_seconds_total{namespace="online-boutique",pod="redis-cart-x",container="redis",device="/dev/dm-0"} 1.8\n'
    pods = [{"name": "redis-cart-x", "service": "redis-cart", "labels": {"app": "redis-cart"}}]
    before = build_cadvisor_fs_snapshot(parse_prometheus_text_metrics(before_text), pods)
    after = build_cadvisor_fs_snapshot(parse_prometheus_text_metrics(after_text), pods)
    delta = compute_fs_delta(before, after, 5)
    rows = _fs_delta_records(delta, "redis-cart", "redis-cart-x", 123.0, "faulty", "inc")
    values = {row["metric"]: row["value"] for row in rows}
    assert values["io.write_bytes"] == 6000.0
    assert values["io.write_ops"] == 6.0
    assert values["io.read_bytes"] == 300.0
    assert values["io.read_ops"] == 3.0
    assert values["io.io_time_ms"] == 300.00000000000006


def test_build_io_incident_repeat_id():
    incident = build_io_incident(4, 10.0, 20.0)
    assert incident["incident_id"] == "ob-io-rediscart-repeat-04"
    assert incident["root_service"] == "redis-cart"
    assert incident["root_metric"] == "io.write_bytes"
    assert incident["root_type"] == "storage I/O"


def test_aggregate_io_repeat_summary():
    config = {"repeat_experiment": {"experiment_group_id": "group", "repeats": 2}}
    rows = [
        {"quality_ok": True, "rca_ok": True, "service_hit_at_1": 1.0, "metric_hit_at_1": 0.0, "metric_hit_at_3": 1.0, "metric_mrr": 0.5, "root_type_accuracy": 1.0, "path_fidelity": 1.0, "write_bytes_lift": 10.0, "write_ops_lift": 3.0, "io_time_lift": 1.0, "frontend_latency_lift": 2.0},
        {"quality_ok": True, "rca_ok": True, "service_hit_at_1": 1.0, "metric_hit_at_1": 1.0, "metric_hit_at_3": 1.0, "metric_mrr": 1.0, "root_type_accuracy": 1.0, "path_fidelity": 1.0, "write_bytes_lift": 30.0, "write_ops_lift": 5.0, "io_time_lift": 3.0, "frontend_latency_lift": 4.0},
    ]
    summary = aggregate_io_repeat_summary(config, rows)
    assert summary["repeats_completed"] == 2
    assert summary["metric_hit_at_3_mean"] == 1.0
    assert summary["metric_hit_at_1_mean"] == 0.5
    assert summary["write_bytes_lift_mean"] == 20.0


def test_check_p2c1_io_repeated_pass_and_fail(tmp_path: Path):
    summary = {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "service_hit_at_1_mean": 0.8,
        "metric_hit_at_3_mean": 0.8,
        "root_type_accuracy_mean": 1.0,
        "path_fidelity_mean": 0.8,
        "write_bytes_lift_mean": 1.0,
    }
    (tmp_path / "p2c1_io_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2c1_io_repeated(tmp_path)["passed"] is True
    summary["write_bytes_lift_mean"] = 0.0
    (tmp_path / "p2c1_io_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    result = check_p2c1_io_repeated(tmp_path)
    assert result["passed"] is False
    assert result["failed_checks"]


def test_cli_imports():
    importlib.import_module("proberca.cli.run_p2c1_io_repeated")
    importlib.import_module("proberca.cli.check_p2c1_io_repeated")

import json
import subprocess
import sys

from proberca.adapters.online_boutique.metrics import (
    build_cadvisor_service_snapshot,
    compute_cadvisor_window_metrics,
    map_container_stats_to_services,
    parse_crictl_stats_json,
    parse_prometheus_text_metrics,
    summarize_http_samples,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import (
    build_data_quality_report,
    build_incident_record,
)


CADVISOR_SAMPLE = """
container_cpu_usage_seconds_total{namespace="online-boutique",pod="paymentservice-x",container="server"} 123.4
container_cpu_cfs_throttled_seconds_total{namespace="online-boutique",pod="paymentservice-x",container="server"} 2.5
container_cpu_cfs_periods_total{namespace="online-boutique",pod="paymentservice-x",container="server"} 100
container_cpu_cfs_throttled_periods_total{namespace="online-boutique",pod="paymentservice-x",container="server"} 10
container_memory_working_set_bytes{namespace="online-boutique",pod="paymentservice-x",container="server"} 123456
container_cpu_usage_seconds_total{namespace="default",pod="ignored",container="server"} 999
container_cpu_usage_seconds_total{namespace="online-boutique",pod="",container="server"} 999
"""


def test_parse_crictl_stats_json():
    payload = {
        "stats": [
            {
                "attributes": {"id": "abcdef123456", "metadata": {"name": "server"}, "labels": {}},
                "cpu": {"usageNanoCores": 12345, "usageCoreNanoSeconds": 999},
                "memory": {"workingSetBytes": 2048},
            }
        ]
    }
    records = parse_crictl_stats_json(json.dumps(payload))
    assert records[0]["id"] == "abcdef123456"
    assert records[0]["cpu_nanocores"] == 12345.0
    assert records[0]["memory_working_set_bytes"] == 2048.0


def test_map_container_stats_to_services():
    pods = [
        {
            "name": "paymentservice-abc",
            "service": "paymentservice",
            "containers": [{"name": "server", "container_id": "containerd://abcdef1234567890"}],
        }
    ]
    crictl = {"containers": [{"id": "abcdef123456", "cpu_nanocores": 10.0, "memory_working_set_bytes": 20.0}]}
    mapped = map_container_stats_to_services(pods, crictl)
    assert mapped["paymentservice"]["cpu.usage"] == 10.0
    assert mapped["paymentservice"]["memory.usage"] == 20.0


def test_summarize_http_samples():
    summary = summarize_http_samples(
        [
            {"http_code": 200, "latency_sec": 0.1},
            {"http_code": 200, "latency_sec": 0.2},
            {"http_code": 500, "latency_sec": 0.3},
        ]
    )
    assert summary["error_rate"] == 1 / 3
    assert summary["p99_latency_ms"] >= summary["p50_latency_ms"]


def test_parse_prometheus_text_metrics_for_cadvisor():
    records = parse_prometheus_text_metrics(CADVISOR_SAMPLE)
    names = {record["name"] for record in records}
    assert "container_cpu_usage_seconds_total" in names
    assert "container_cpu_cfs_throttled_seconds_total" in names
    assert "container_cpu_cfs_periods_total" in names
    assert "container_cpu_cfs_throttled_periods_total" in names
    assert "container_memory_working_set_bytes" in names
    assert all(record["labels"]["namespace"] == "online-boutique" for record in records)
    assert all(record["labels"].get("pod") for record in records)


def test_build_cadvisor_service_snapshot_maps_paymentservice():
    pods = [{"name": "paymentservice-x", "service": "paymentservice", "labels": {"app": "paymentservice"}}]
    records = parse_prometheus_text_metrics(CADVISOR_SAMPLE)
    snapshot = build_cadvisor_service_snapshot(records, pods)
    assert "paymentservice" in snapshot
    assert snapshot["paymentservice"]["paymentservice-x"]["server"]["cpu_usage_seconds_total"] == 123.4
    assert snapshot["paymentservice"]["paymentservice-x"]["server"]["cpu_cfs_throttled_seconds_total"] == 2.5


def test_compute_cadvisor_window_metrics_outputs_resource_metrics():
    prev = {
        "paymentservice": {
            "paymentservice-x": {
                "server": {
                    "cpu_usage_seconds_total": 100.0,
                    "cpu_cfs_throttled_seconds_total": 2.0,
                    "cpu_cfs_periods_total": 90.0,
                    "cpu_cfs_throttled_periods_total": 5.0,
                    "memory_working_set_bytes": 100000.0,
                }
            }
        }
    }
    curr = {
        "paymentservice": {
            "paymentservice-x": {
                "server": {
                    "cpu_usage_seconds_total": 105.0,
                    "cpu_cfs_throttled_seconds_total": 2.5,
                    "cpu_cfs_periods_total": 100.0,
                    "cpu_cfs_throttled_periods_total": 10.0,
                    "memory_working_set_bytes": 123456.0,
                }
            }
        }
    }
    records = compute_cadvisor_window_metrics(prev, curr, 5, timestamp=123.0, incident_id="inc")
    by_metric = {record["metric"]: record for record in records}
    assert by_metric["cpu.usage"]["value"] == 1.0
    assert by_metric["cpu.throttled_usec"]["value"] == 500000.0
    assert by_metric["cpu.throttled_periods"]["value"] == 5.0
    assert by_metric["cpu.throttle_ratio"]["value"] == 0.5
    assert by_metric["memory.usage"]["value"] == 123456.0


def test_compute_cadvisor_window_metrics_skips_negative_deltas():
    prev = {"paymentservice": {"pod": {"server": {"cpu_usage_seconds_total": 10.0}}}}
    curr = {"paymentservice": {"pod": {"server": {"cpu_usage_seconds_total": 9.0}}}}
    records = compute_cadvisor_window_metrics(prev, curr, 5)
    assert all(record["metric"] != "cpu.usage" for record in records)


def test_data_quality_report_root_service_metric_coverage():
    config = {"experiment": {"baseline_windows": 1, "faulty_windows": 1, "recovery_windows": 1}}
    incident = {"incident_id": "ob-cpu-paymentservice-001"}
    metrics = [
        {"service": "paymentservice", "metric": "cpu.usage", "value": 0.1, "phase": "baseline"},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "value": 10.0, "phase": "baseline"},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "value": 20.0, "phase": "faulty"},
        {"service": "frontend", "metric": "request.p99_latency_ms", "value": 100.0, "phase": "faulty"},
    ]
    report = build_data_quality_report(metrics, [{"cadvisor_metrics_available": True}], incident, config, True, True)
    assert report["paymentservice_cpu_metric_present"] is True
    assert report["paymentservice_throttled_metric_present"] is True
    assert report["root_service_metric_coverage_passed"] is True
    assert report["frontend_latency_metric_present"] is True


def test_build_incident_record():
    config = {
        "experiment": {
            "experiment_id": "ob_cpu_paymentservice_001",
            "target_service": "paymentservice",
            "target_metric": "cpu.throttled_usec",
            "target_fault_type": "CPU throttling",
            "symptom_service": "frontend",
        }
    }
    incident = build_incident_record(10.0, 20.0, config)
    assert incident["incident_id"] == "ob-cpu-paymentservice-001"
    assert incident["root_service"] == "paymentservice"
    assert incident["root_metric"] == "cpu.throttled_usec"


def test_p2a1_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.run_p2a1_cpu_fault as m; assert callable(m.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

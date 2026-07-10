import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.p1_bridge import build_real_observation_files, load_real_ob_dataset
from proberca.adapters.online_boutique.p2a2_real_rca import validate_real_cpu_input
from proberca.data.io import read_jsonl, write_jsonl


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _make_fake_real_input(tmp_path: Path, coverage: bool = True) -> Path:
    base = tmp_path / "real"
    base.mkdir()
    metrics = [
        {"timestamp": 1.0, "service": "frontend", "instance": "frontend-a", "node": "frontend", "metric": "request.p99_latency_ms", "value": 10.0, "source": "test"},
        {"timestamp": 1.0, "service": "paymentservice", "instance": "payment-a", "node": "paymentservice", "metric": "cpu.throttled_usec", "value": 0.0, "source": "test"},
        {"timestamp": 1.0, "service": "paymentservice", "instance": "payment-a", "node": "paymentservice", "metric": "cpu.usage", "value": 0.1, "source": "test"},
        {"timestamp": 2.0, "service": "frontend", "instance": "frontend-a", "node": "frontend", "metric": "request.p99_latency_ms", "value": 20.0, "source": "test"},
        {"timestamp": 2.0, "service": "paymentservice", "instance": "payment-a", "node": "paymentservice", "metric": "cpu.throttled_usec", "value": 100.0, "source": "test"},
        {"timestamp": 2.0, "service": "paymentservice", "instance": "payment-a", "node": "paymentservice", "metric": "cpu.usage", "value": 0.2, "source": "test"},
    ]
    incident = {
        "incident_id": "ob-cpu-paymentservice-001_cadvisor",
        "root_service": "paymentservice",
        "root_metric": "cpu.throttled_usec",
        "root_type": "CPU throttling",
        "symptom_service": "frontend",
        "start_ts": 2.0,
        "end_ts": 2.0,
        "injected_path": ["paymentservice.cpu.throttled_usec", "checkoutservice.request.p99_latency_ms", "frontend.request.p99_latency_ms"],
    }
    evidence = [{"incident_id": incident["incident_id"], "service": "paymentservice", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "evidence_score": 1.0, "value": 100.0}]
    graph = [
        {"src": "paymentservice", "dst": "checkoutservice", "source": "paymentservice", "target": "checkoutservice", "edge_type": "call", "weight": 1.0},
        {"src": "checkoutservice", "dst": "frontend", "source": "checkoutservice", "target": "frontend", "edge_type": "call", "weight": 1.0},
    ]
    write_jsonl(base / "metrics.jsonl", metrics)
    write_jsonl(base / "incidents.jsonl", [incident])
    write_jsonl(base / "evidence.jsonl", evidence)
    write_jsonl(base / "service_graph.jsonl", graph)
    _write_json(base / "metadata.json", {"source": "test"})
    _write_json(
        base / "data_quality_report.json",
        {
            "metrics_count": len(metrics),
            "root_service_metric_coverage_passed": coverage,
            "paymentservice_cpu_metric_present": coverage,
            "paymentservice_throttled_metric_present": coverage,
            "frontend_latency_metric_present": True,
            "cadvisor_metrics_available": True,
            "fault_injection_succeeded": True,
            "restore_succeeded": True,
        },
    )
    return base


def test_build_real_observation_files_full_observation(tmp_path):
    input_dir = _make_fake_real_input(tmp_path)
    output_dir = tmp_path / "out"
    result = build_real_observation_files(input_dir, output_dir)
    assert result["observed_ratio"] == 1.0
    for name in ["observed_metrics.jsonl", "sampling_log.jsonl", "observation_mask.jsonl", "adaptive_observation_metadata.json"]:
        assert (output_dir / name).exists()
    observed = read_jsonl(output_dir / "observed_metrics.jsonl")
    sampling = read_jsonl(output_dir / "sampling_log.jsonl")
    mask = read_jsonl(output_dir / "observation_mask.jsonl")
    assert len(observed) == 6
    assert len(sampling) == 6
    assert len(mask) == 6
    assert all(row["sampling_probability"] == 1.0 for row in sampling)
    assert all(row["observed"] is True for row in mask)
    metadata = json.loads((output_dir / "adaptive_observation_metadata.json").read_text())
    assert metadata["observed_ratio"] == 1.0
    assert metadata["partial_observation"] is False


def test_load_real_ob_dataset_and_validate_quality(tmp_path):
    input_dir = _make_fake_real_input(tmp_path)
    dataset = load_real_ob_dataset(input_dir)
    assert len(dataset["metrics"]) == 6
    assert validate_real_cpu_input(input_dir)["data_quality_report"]["root_service_metric_coverage_passed"] is True


def test_validate_real_cpu_input_rejects_failed_coverage(tmp_path):
    input_dir = _make_fake_real_input(tmp_path, coverage=False)
    try:
        validate_real_cpu_input(input_dir)
    except ValueError as exc:
        assert "quality gates" in str(exc)
    else:
        raise AssertionError("expected failed quality gate")


def test_p2a2_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.run_p2a2_real_cpu_rca as r; import proberca.cli.check_p2a2_real_rca as c; assert callable(r.main); assert callable(c.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

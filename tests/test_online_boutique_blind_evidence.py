import importlib
import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.blind_evidence import (
    audit_blind_evidence_code_safety,
    generate_blind_evidence,
    metric_to_evidence_type,
)


def _write_jsonl(path: Path, records):
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _fake_input_dir(base: Path) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    metrics = [
        {"timestamp": 1.0, "service": "paymentservice", "metric": "cpu.throttled_usec", "value": 1.0},
        {"timestamp": 2.0, "service": "currencyservice", "metric": "cpu.throttled_usec", "value": 1.0},
        {"timestamp": 12.0, "service": "paymentservice", "metric": "cpu.throttled_usec", "value": 3.0},
        {"timestamp": 12.0, "service": "currencyservice", "metric": "cpu.throttled_usec", "value": 9.0},
        {"timestamp": 1.0, "service": "frontend", "metric": "request.p99_latency_ms", "value": 20.0},
        {"timestamp": 12.0, "service": "frontend", "metric": "request.p99_latency_ms", "value": 40.0},
    ]
    incidents = [
        {
            "incident_id": "fake-incident-001",
            "start_ts": 10.0,
            "end_ts": 20.0,
            "symptom_service": "frontend",
            "root_service": "paymentservice",
            "root_metric": "cpu.throttled_usec",
            "root_type": "CPU throttling",
            "injected_path": ["paymentservice.cpu.throttled_usec"],
        }
    ]
    _write_jsonl(base / "metrics.jsonl", metrics)
    _write_jsonl(base / "incidents.jsonl", incidents)
    _write_jsonl(base / "service_graph.jsonl", [])
    return base


def test_blind_evidence_does_not_follow_root_label(tmp_path):
    input_dir = _fake_input_dir(tmp_path / "input")
    output_dir = tmp_path / "output"
    result = generate_blind_evidence(str(input_dir), str(output_dir), min_score=0.0, top_k_per_type=20)
    assert result["blind_evidence"] is True

    evidence = [json.loads(line) for line in (output_dir / "blind_evidence.jsonl").read_text().splitlines()]
    cpu = {item["service"]: item for item in evidence if item["evidence_type"] == "CPU"}
    assert cpu["currencyservice"]["evidence_score"] > cpu["paymentservice"]["evidence_score"]
    assert cpu["currencyservice"]["absolute_lift"] > cpu["paymentservice"]["absolute_lift"]


def test_metric_to_evidence_type_mapping():
    assert metric_to_evidence_type("cpu.throttled_usec") == "CPU"
    assert metric_to_evidence_type("net.retrans") == "network"
    assert metric_to_evidence_type("io.write_bytes") == "storage I/O"
    assert metric_to_evidence_type("lock.futex_wait_ms") == "lock contention"
    assert metric_to_evidence_type("memory.usage") == "memory"
    assert metric_to_evidence_type("request.p99_latency_ms") == "load"
    assert metric_to_evidence_type("custom.metric") == "unknown"


def test_generate_blind_evidence_outputs_metadata(tmp_path):
    input_dir = _fake_input_dir(tmp_path / "input")
    output_dir = tmp_path / "output"
    generate_blind_evidence(str(input_dir), str(output_dir), min_score=0.0, top_k_per_type=20)
    assert (output_dir / "blind_evidence.jsonl").exists()
    metadata = json.loads((output_dir / "blind_evidence_metadata.json").read_text(encoding="utf-8"))
    assert metadata["blind_evidence"] is True
    assert metadata["uses_root_labels"] is False
    assert metadata["uses_target_config"] is False
    assert metadata["uses_injected_path"] is False
    assert metadata["evidence_count"] > 0


def test_generate_blind_evidence_cli(tmp_path):
    input_dir = _fake_input_dir(tmp_path / "input")
    output_dir = tmp_path / "output"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "proberca.cli.generate_blind_evidence",
            "--input",
            str(input_dir),
            "--output",
            str(output_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "blind_evidence" in completed.stdout
    assert (output_dir / "blind_evidence.jsonl").exists()


def test_audit_blind_evidence_code_safety():
    result = audit_blind_evidence_code_safety()
    assert "passed" in result
    assert "suspicious_lines" in result
    assert result["passed"] is True


def test_cli_import():
    assert importlib.import_module("proberca.cli.generate_blind_evidence")

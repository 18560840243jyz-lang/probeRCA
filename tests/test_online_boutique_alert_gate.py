import importlib
import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.alert_gate import build_alert_windows, detect_alert_events, write_alert_outputs
from proberca.cli.check_a3_alert_gate import check_a3_alert_gate


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _metric_rows(anomalous=True):
    rows = []
    for idx in range(10):
        ts = float(idx * 5)
        late = 20.0 if idx < 5 or not anomalous else 120.0 + idx
        cpu = 0.0 if idx < 5 or not anomalous else 10000.0
        rows.append({"timestamp": ts, "service": "frontend", "metric": "request.p99_latency_ms", "value": late})
        rows.append({"timestamp": ts, "service": "paymentservice", "metric": "cpu.throttled_usec", "value": cpu})
    return rows


def _fake_input(base: Path, anomalous=True):
    base.mkdir(parents=True, exist_ok=True)
    _write_jsonl(base / "metrics.jsonl", _metric_rows(anomalous=anomalous))
    _write_jsonl(base / "incidents.jsonl", [{
        "incident_id": "fake-001",
        "start_ts": 25.0,
        "end_ts": 50.0,
        "symptom_service": "frontend",
        "root_service": "paymentservice",
        "root_metric": "cpu.throttled_usec",
        "root_type": "CPU",
        "injected_path": ["paymentservice.cpu.throttled_usec"],
    }])
    _write_jsonl(base / "service_graph.jsonl", [])
    return base


def test_detect_alert_events_and_windows_use_symptom_metrics(tmp_path):
    input_dir = _fake_input(tmp_path / "input", anomalous=True)
    events = detect_alert_events(str(input_dir))
    windows = build_alert_windows(events)
    assert events
    assert windows
    assert windows[0]["symptom_service"] == "frontend"
    assert windows[0]["symptom_service"] != "paymentservice"


def test_no_metric_anomaly_does_not_alert_from_root_labels(tmp_path):
    input_dir = _fake_input(tmp_path / "input", anomalous=False)
    events = detect_alert_events(str(input_dir))
    assert events == []


def test_write_alert_outputs_metadata_flags(tmp_path):
    input_dir = _fake_input(tmp_path / "input", anomalous=True)
    output_dir = tmp_path / "out"
    result = write_alert_outputs(str(input_dir), str(output_dir))
    metadata = result["metadata"]
    assert (output_dir / "alert_events.jsonl").exists()
    assert (output_dir / "alert_windows.jsonl").exists()
    assert metadata["uses_root_labels"] is False
    assert metadata["uses_incident_start_end_for_detection"] is False


def test_detect_alert_windows_cli(tmp_path):
    input_dir = _fake_input(tmp_path / "input", anomalous=True)
    output_dir = tmp_path / "out"
    completed = subprocess.run([
        sys.executable,
        "-m",
        "proberca.cli.detect_alert_windows",
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
    ], check=True, capture_output=True, text=True)
    assert "A3 Alert Gate" in completed.stdout
    assert (output_dir / "alert_gate_metadata.json").exists()


def test_check_a3_alert_gate_minimal_summary(tmp_path):
    base = tmp_path / "preview"
    repeat = base / "cpu" / "repeat_01"
    repeat.mkdir(parents=True)
    (repeat / "alert_gate_metadata.json").write_text(json.dumps({"uses_root_labels": False, "uses_incident_start_end_for_detection": False}), encoding="utf-8")
    (repeat / "alert_events.jsonl").write_text("", encoding="utf-8")
    (repeat / "alert_windows.jsonl").write_text("", encoding="utf-8")
    summary = {
        "total_repeats": 20,
        "uses_root_labels_for_detection": False,
        "uses_incident_start_end_for_detection": False,
        "per_repeat": [{"output_dir": str(repeat)}],
    }
    (base / "p2_alert_preview_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_a3_alert_gate(str(base))["passed"] is True


def test_cli_imports():
    assert importlib.import_module("proberca.cli.detect_alert_windows")
    assert importlib.import_module("proberca.cli.run_p2_alert_preview")
    assert importlib.import_module("proberca.cli.check_a3_alert_gate")

import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.blind_rerun import fault_type_sources, prepare_blind_rca_input


def _write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _fake_raw(base: Path) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    metrics = [
        {"timestamp": 1.0, "service": "target", "metric": "cpu.throttled_usec", "value": 1.0},
        {"timestamp": 12.0, "service": "target", "metric": "cpu.throttled_usec", "value": 2.0},
        {"timestamp": 1.0, "service": "other", "metric": "cpu.throttled_usec", "value": 1.0},
        {"timestamp": 12.0, "service": "other", "metric": "cpu.throttled_usec", "value": 9.0},
    ]
    incidents = [
        {
            "incident_id": "fake-001",
            "start_ts": 10.0,
            "end_ts": 20.0,
            "symptom_service": "frontend",
            "root_service": "target",
            "root_metric": "cpu.throttled_usec",
            "root_type": "CPU",
            "injected_path": ["target.cpu.throttled_usec"],
        }
    ]
    legacy = [
        {"incident_id": "fake-001", "service": "target", "metric": "cpu.throttled_usec", "evidence_type": "CPU", "value": 999.0, "source": "legacy_target_aware"}
    ]
    _write_jsonl(base / "metrics.jsonl", metrics)
    _write_jsonl(base / "incidents.jsonl", incidents)
    _write_jsonl(base / "service_graph.jsonl", [])
    _write_jsonl(base / "evidence.jsonl", legacy)
    (base / "metadata.json").write_text(json.dumps({"fake": True}), encoding="utf-8")
    (base / "data_quality_report.json").write_text(json.dumps({"fake": True}), encoding="utf-8")
    return base


def test_prepare_blind_rca_input_replaces_legacy_evidence(tmp_path):
    raw = _fake_raw(tmp_path / "raw")
    out = tmp_path / "blind"
    metadata = prepare_blind_rca_input(str(raw), str(out))
    assert (out / "blind_evidence.jsonl").exists()
    assert (out / "evidence.jsonl").exists()
    evidence = [json.loads(line) for line in (out / "evidence.jsonl").read_text().splitlines()]
    assert evidence
    assert all(item.get("source") == "blind_metric_lift_evidence" for item in evidence)
    assert all(float(item.get("value", 0.0)) != 999.0 for item in evidence)
    assert metadata["uses_legacy_evidence"] is False
    assert metadata["uses_blind_evidence"] is True
    assert metadata["uses_root_labels_for_evidence"] is False
    assert metadata["uses_target_config_for_evidence"] is False


def test_fault_type_sources_returns_four_keys():
    sources = fault_type_sources("base")
    assert sorted(sources) == ["cpu", "io", "lock", "network"]
    assert len(sources["cpu"]) == 5
    assert sources["lock"][0].endswith("lock_cartservice_repeated_phaseaware/repeat_01/raw")


def test_cli_imports():
    assert importlib.import_module("proberca.cli.run_p2_blind_rerun")
    assert importlib.import_module("proberca.cli.check_p2_blind_rerun")

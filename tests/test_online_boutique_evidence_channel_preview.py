from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.cli.check_a7_evidence_channel import check_a7_evidence_channel


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _fake_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    blind = tmp_path / "blind"
    probe = tmp_path / "probe"
    rls = tmp_path / "rls"
    out = tmp_path / "out"
    _write_jsonl(blind / "blind_evidence.jsonl", [
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "node": "paymentservice.cpu.throttled_usec", "evidence_score": 1.0, "evidence_type": "CPU", "source": "blind_metric_lift_evidence"}
    ])
    _write_json(blind / "blind_evidence_metadata.json", {"blind_evidence": True, "uses_root_labels": False, "uses_target_config": False, "uses_injected_path": False})
    _write_jsonl(probe / "sampling_log.jsonl", [
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "probe_name": "cpu_probe", "evidence_type": "CPU", "sampling_probability": 1.0, "selected": True}
    ])
    _write_jsonl(probe / "observation_mask.jsonl", [
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "observed_probability": 1.0, "observed_by_probe": "cpu_probe"}
    ])
    _write_json(probe / "adaptive_probe_metadata.json", {"actual_probe_activation": False})
    _write_jsonl(rls / "ipw_rls_residuals.jsonl", [
        {"timestamp": 1, "target_node": "paymentservice.cpu.throttled_usec", "predicted_z": 0.0, "actual_z": 1.0, "residual": 1.0},
        {"timestamp": 2, "target_node": "paymentservice.cpu.throttled_usec", "predicted_z": 0.0, "actual_z": 3.0, "residual": 3.0},
        {"timestamp": 3, "target_node": "paymentservice.cpu.throttled_usec", "predicted_z": 0.0, "actual_z": 5.0, "residual": 5.0},
    ])
    _write_jsonl(rls / "ipw_rls_predictions.jsonl", [])
    _write_json(rls / "ipw_rls_metadata.json", {"update_mode": "online_rls", "batch_ridge_used": False, "consumes_sampling_probability": True, "consumes_observation_mask": True})
    return blind, probe, rls, out


def test_run_evidence_channel_preview_cli(tmp_path: Path) -> None:
    blind, probe, rls, out = _fake_inputs(tmp_path)
    cmd = [
        sys.executable,
        "-m",
        "proberca.cli.run_evidence_channel_preview",
        "--blind-evidence-input",
        str(blind),
        "--probe-policy-input",
        str(probe),
        "--ipw-rls-input",
        str(rls),
        "--output",
        str(out),
    ]
    completed = subprocess.run(cmd, check=True, text=True, capture_output=True)
    assert "produces_calibrated_residuals=true" in completed.stdout
    assert (out / "evidence_channel_metadata.json").exists()


def test_check_a7_evidence_channel_minimal_summary(tmp_path: Path) -> None:
    repeat = tmp_path / "cpu" / "repeat_01"
    for name in ["evidence_vectors.jsonl", "evidence_effects.jsonl", "calibrated_residuals.jsonl"]:
        _write_jsonl(repeat / name, [])
    _write_json(repeat / "evidence_channel_metadata.json", {"produces_calibrated_residuals": True, "max_abs_calibrated_residual": 1.0, "residual_clip_value": 10.0})
    summary = {
        "total_repeats": 20,
        "repeats_completed": 20,
        "uses_root_labels_for_channel": False,
        "uses_target_config_for_channel": False,
        "uses_injected_path_for_channel": False,
        "uses_incident_start_end_for_channel": False,
        "consumes_blind_evidence": True,
        "consumes_probe_policy": True,
        "consumes_ipw_rls_residuals": True,
        "produces_calibrated_residuals": True,
        "raw_residual_directly_used_for_sparse_inversion": False,
        "per_repeat": [{"output_dir": str(repeat)}],
    }
    _write_json(tmp_path / "p2_evidence_channel_preview_summary.json", summary)
    result = check_a7_evidence_channel(str(tmp_path))
    assert result["passed"] is True

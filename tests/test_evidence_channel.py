from __future__ import annotations

import json
from pathlib import Path

from proberca.evidence.evidence_channel import (
    EvidenceChannelConfig,
    build_evidence_channel,
    calibrate_residuals,
    compute_evidence_vector_for_node,
    load_blind_evidence,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_calibrate_residuals_clips_and_keeps_family_scale() -> None:
    cfg = EvidenceChannelConfig(residual_clip_value=10.0)
    rows = [
        {"timestamp": 1, "node_id": "svc.cpu.throttled_usec", "raw_residual": 999_000.0},
        {"timestamp": 2, "node_id": "svc.cpu.throttled_usec", "raw_residual": 1_000_000.0},
        {"timestamp": 3, "node_id": "svc.cpu.throttled_usec", "raw_residual": 1_002_000.0},
        {"timestamp": 1, "node_id": "redis-cart.io.write_bytes", "raw_residual": 999_999_999_000.0},
        {"timestamp": 2, "node_id": "redis-cart.io.write_bytes", "raw_residual": 1_000_000_000_000.0},
        {"timestamp": 3, "node_id": "redis-cart.io.write_bytes", "raw_residual": 1_000_000_001_000.0},
        {"timestamp": 1, "node_id": "ship.net.retrans", "raw_residual": 8.0},
        {"timestamp": 2, "node_id": "ship.net.retrans", "raw_residual": 10.0},
        {"timestamp": 3, "node_id": "ship.net.retrans", "raw_residual": 12.0},
    ]
    out = calibrate_residuals(rows, cfg)
    assert max(abs(row["calibrated_residual"]) for row in out) <= cfg.residual_clip_value
    assert any(row["metric_family"] == "CPU" and abs(row["calibrated_residual"]) > 0 for row in out)
    assert any(row["metric_family"] == "network" and abs(row["calibrated_residual"]) > 0 for row in out)
    assert all("calibrated_residual" in row for row in out)


def test_compute_evidence_vector_uses_blind_scores_and_probe_weight() -> None:
    cfg = EvidenceChannelConfig(max_evidence_effect=2.0)
    evidence_maps = {
        "evidence_by_node": {"redis-cart.io.write_bytes": {"evidence_score": 1.0}},
        "evidence_by_service_family": {("redis-cart", "storage I/O"): 0.8},
        "evidence_by_family": {"storage I/O": 0.6},
    }
    probe_maps = {
        "sampling_probability_by_node": {"redis-cart.io.write_bytes": 0.25},
        "observed_probability_by_node": {},
        "selected_probe_by_service_family": {("redis-cart", "storage I/O"): "io_probe"},
    }
    vector = compute_evidence_vector_for_node("redis-cart.io.write_bytes", "redis-cart", "io.write_bytes", evidence_maps, probe_maps, cfg)
    assert vector["h_value"] > 0
    assert vector["h_value"] <= cfg.max_evidence_effect
    assert vector["uses_root_labels"] is False
    assert vector["uses_target_config"] is False


def test_load_blind_evidence_marks_legacy_or_nonblind_unsafe(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence.jsonl"
    _write_jsonl(evidence, [{"service": "svc", "metric": "cpu.usage", "evidence_score": 1.0, "source": "legacy_target_aware"}])
    _write_json(tmp_path / "blind_input_metadata.json", {"uses_blind_evidence": False})
    result = load_blind_evidence(str(tmp_path))
    assert result["unsafe_input"] is True
    assert result["unsafe_reasons"]


def test_build_evidence_channel_writes_outputs(tmp_path: Path) -> None:
    blind = tmp_path / "blind"
    probe = tmp_path / "probe"
    rls = tmp_path / "rls"
    out = tmp_path / "out"
    _write_jsonl(blind / "blind_evidence.jsonl", [
        {"service": "redis-cart", "metric": "io.write_bytes", "node": "redis-cart.io.write_bytes", "evidence_score": 1.0, "evidence_type": "storage I/O", "source": "blind_metric_lift_evidence"}
    ])
    _write_json(blind / "blind_evidence_metadata.json", {"blind_evidence": True, "uses_root_labels": False, "uses_target_config": False, "uses_injected_path": False})
    _write_jsonl(probe / "sampling_log.jsonl", [
        {"service": "redis-cart", "metric": "io.write_bytes", "probe_name": "io_probe", "evidence_type": "storage I/O", "sampling_probability": 0.5, "selected": True}
    ])
    _write_jsonl(probe / "observation_mask.jsonl", [
        {"service": "redis-cart", "metric": "io.write_bytes", "observed_probability": 0.5, "observed_by_probe": "io_probe"}
    ])
    _write_json(probe / "adaptive_probe_metadata.json", {"actual_probe_activation": False})
    _write_jsonl(rls / "ipw_rls_residuals.jsonl", [
        {"timestamp": 1, "target_node": "redis-cart.io.write_bytes", "predicted_z": 0.0, "actual_z": 5.0, "residual": 5.0},
        {"timestamp": 2, "target_node": "redis-cart.io.write_bytes", "predicted_z": 0.0, "actual_z": 7.0, "residual": 7.0},
        {"timestamp": 3, "target_node": "redis-cart.io.write_bytes", "predicted_z": 0.0, "actual_z": 9.0, "residual": 9.0},
    ])
    _write_jsonl(rls / "ipw_rls_predictions.jsonl", [])
    _write_json(rls / "ipw_rls_metadata.json", {"update_mode": "online_rls", "batch_ridge_used": False, "consumes_sampling_probability": True, "consumes_observation_mask": True})
    result = build_evidence_channel(str(blind), str(probe), str(rls), str(out))
    assert (out / "evidence_vectors.jsonl").exists()
    assert (out / "evidence_effects.jsonl").exists()
    assert (out / "calibrated_residuals.jsonl").exists()
    assert result["metadata"]["consumes_blind_evidence"] is True
    assert result["metadata"]["produces_calibrated_residuals"] is True
    assert result["metadata"]["raw_residual_directly_used_for_sparse_inversion"] is False

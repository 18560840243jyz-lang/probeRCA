from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.propagation.ipw_rls_online import RLSConfig, run_ipw_rls_preview
from proberca.cli.check_a6_ipw_rls import check_a6_ipw_rls


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _make_fake_dirs(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw = tmp_path / "raw"
    candidate = tmp_path / "candidate"
    probe = tmp_path / "probe"
    metrics = []
    for t in range(8):
        metrics.append({"timestamp": float(t), "service": "frontend", "metric": "request.p99_latency_ms", "value": float(t + 1)})
        metrics.append({"timestamp": float(t), "service": "paymentservice", "metric": "cpu.throttled_usec", "value": float((t + 1) * 2)})
    _write_jsonl(raw / "metrics.jsonl", metrics)
    _write_jsonl(raw / "incidents.jsonl", [{"incident_id": "fake", "root_service": "paymentservice", "root_metric": "cpu.throttled_usec", "start_ts": 1.0, "end_ts": 2.0}])
    window = candidate / "window_01"
    _write_jsonl(window / "candidate_metric_nodes.jsonl", [
        {"service": "frontend", "metric": "request.p99_latency_ms", "node_id": "frontend.request.p99_latency_ms"},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "node_id": "paymentservice.cpu.throttled_usec"},
    ])
    _write_jsonl(window / "candidate_edges.jsonl", [{"src": "paymentservice", "dst": "frontend"}])
    (window / "candidate_subgraph_metadata.json").write_text(json.dumps({"uses_root_labels": False}), encoding="utf-8")
    (candidate / "repeat_candidate_summary.json").write_text(json.dumps({"window_summaries": [{"window_output_dir": str(window)}]}), encoding="utf-8")
    _write_jsonl(probe / "sampling_log.jsonl", [
        {"service": "frontend", "metric": "request.p99_latency_ms", "sampling_probability": 1.0, "selected": True},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "sampling_probability": 0.2, "selected": True},
    ])
    _write_jsonl(probe / "observation_mask.jsonl", [
        {"service": "frontend", "metric": "request.p99_latency_ms", "observed_probability": 1.0, "observed_by_probe": "request_probe"},
        {"service": "paymentservice", "metric": "cpu.throttled_usec", "observed_probability": 0.2, "observed_by_probe": "cpu_probe"},
    ])
    return raw, candidate, probe


def test_run_ipw_rls_preview_outputs_files_and_no_root_labels(tmp_path: Path) -> None:
    raw, candidate, probe = _make_fake_dirs(tmp_path)
    out = tmp_path / "out"
    result = run_ipw_rls_preview(str(raw), str(candidate), str(probe), str(out), RLSConfig(max_parents=4))
    assert (out / "ipw_rls_state.json").exists()
    assert (out / "ipw_rls_edges.jsonl").exists()
    assert (out / "ipw_rls_residuals.jsonl").exists()
    assert (out / "ipw_rls_metadata.json").exists()
    assert result["metadata"]["uses_root_labels"] is False
    assert result["metadata"]["consumes_sampling_probability"] is True
    assert result["metadata"]["batch_ridge_used"] is False


def test_run_ipw_rls_preview_cli(tmp_path: Path) -> None:
    raw, candidate, probe = _make_fake_dirs(tmp_path)
    out = tmp_path / "cli_out"
    result = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_ipw_rls_preview", "--raw-input", str(raw), "--candidate-input", str(candidate), "--probe-policy-input", str(probe), "--output", str(out)],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "A6 True IPW-masked RLS" in result.stdout
    assert (out / "ipw_rls_metadata.json").exists()


def test_check_a6_ipw_rls_minimal(tmp_path: Path) -> None:
    raw, candidate, probe = _make_fake_dirs(tmp_path)
    base = tmp_path / "a6"
    per_repeat = []
    for index in range(20):
        out = base / "cpu" / f"repeat_{index:02d}"
        run_ipw_rls_preview(str(raw), str(candidate), str(probe), str(out), RLSConfig(max_parents=4))
        per_repeat.append({"output_dir": str(out), "completed": True})
    summary = {
        "total_repeats": 20,
        "repeats_completed": 20,
        "uses_root_labels_for_learning": False,
        "uses_target_config_for_learning": False,
        "uses_injected_path_for_learning": False,
        "uses_incident_start_end_for_learning": False,
        "consumes_sampling_probability": True,
        "consumes_observation_mask": True,
        "update_mode": "online_rls",
        "batch_ridge_used": False,
        "per_repeat": per_repeat,
    }
    (base / "p2_ipw_rls_preview_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_a6_ipw_rls(str(base))["passed"] is True

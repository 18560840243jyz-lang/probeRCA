from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.cli.check_a8_graph_sparse import check_a8_graph_sparse


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _fake_dirs(tmp_path: Path) -> tuple[Path, Path, Path]:
    candidate = tmp_path / "candidate"
    evidence = tmp_path / "evidence"
    out = tmp_path / "out"
    _write_jsonl(candidate / "candidate_metric_nodes.jsonl", [
        {"node_id": "svc.cpu.usage", "service": "svc", "metric": "cpu.usage", "metric_family": "CPU"},
        {"node_id": "svc.cpu.throttled_usec", "service": "svc", "metric": "cpu.throttled_usec", "metric_family": "CPU"},
    ])
    _write_jsonl(candidate / "candidate_edges.jsonl", [])
    _write_json(candidate / "candidate_subgraph_metadata.json", {"uses_root_labels": False})
    _write_json(evidence / "evidence_channel_metadata.json", {"produces_calibrated_residuals": True, "raw_residual_directly_used_for_sparse_inversion": False})
    _write_jsonl(evidence / "calibrated_residuals.jsonl", [
        {"node_id": "svc.cpu.usage", "service": "svc", "metric": "cpu.usage", "calibrated_residual": 2.0, "raw_residual": 999999.0, "evidence_effect": 0.0},
        {"node_id": "svc.cpu.throttled_usec", "service": "svc", "metric": "cpu.throttled_usec", "calibrated_residual": 3.0, "raw_residual": 999999.0, "evidence_effect": 0.0},
    ])
    _write_jsonl(evidence / "evidence_vectors.jsonl", [])
    _write_jsonl(evidence / "evidence_effects.jsonl", [])
    return candidate, evidence, out


def test_run_graph_sparse_inversion_cli(tmp_path: Path) -> None:
    candidate, evidence, out = _fake_dirs(tmp_path)
    cmd = [sys.executable, "-m", "proberca.cli.run_graph_sparse_inversion", "--candidate-input", str(candidate), "--evidence-channel-input", str(evidence), "--output", str(out), "--max-iter", "20"]
    completed = subprocess.run(cmd, check=True, text=True, capture_output=True)
    assert "consumes_calibrated_residuals=true" in completed.stdout
    assert (out / "graph_sparse_metadata.json").exists()


def test_check_a8_graph_sparse_minimal_summary(tmp_path: Path) -> None:
    repeat = tmp_path / "cpu" / "repeat_01"
    for name in ["sparse_interventions.jsonl", "metric_scores.jsonl", "service_scores.jsonl", "graph_sparse_objective_trace.jsonl"]:
        _write_jsonl(repeat / name, [])
    _write_json(repeat / "graph_sparse_metadata.json", {"consumes_calibrated_residuals": True, "consumes_raw_residuals": False, "post_sparsify_applied": True, "solver_status": "max_iter_reached"})
    summary = {
        "total_repeats": 20,
        "repeats_completed": 20,
        "average_node_count": 10,
        "average_nonzero_intervention_count": 4,
        "uses_root_labels_for_inversion": False,
        "uses_target_config_for_inversion": False,
        "uses_injected_path_for_inversion": False,
        "uses_incident_start_end_for_inversion": False,
        "consumes_calibrated_residuals": True,
        "consumes_raw_residuals": False,
        "optimization": "admm_graph_sparse_inversion",
        "per_repeat": [{"output_dir": str(repeat)}],
    }
    _write_json(tmp_path / "p2_graph_sparse_preview_summary.json", summary)
    result = check_a8_graph_sparse(str(tmp_path))
    assert result["passed"] is True



def test_check_a8_graph_sparse_fails_high_nonzero_ratio(tmp_path: Path) -> None:
    repeat = tmp_path / "cpu" / "repeat_01"
    for name in ["sparse_interventions.jsonl", "metric_scores.jsonl", "service_scores.jsonl", "graph_sparse_objective_trace.jsonl"]:
        _write_jsonl(repeat / name, [])
    _write_json(repeat / "graph_sparse_metadata.json", {"consumes_calibrated_residuals": True, "consumes_raw_residuals": False, "post_sparsify_applied": True, "solver_status": "converged"})
    summary = {
        "total_repeats": 20,
        "repeats_completed": 20,
        "average_node_count": 10,
        "average_nonzero_intervention_count": 7,
        "uses_root_labels_for_inversion": False,
        "uses_target_config_for_inversion": False,
        "uses_injected_path_for_inversion": False,
        "uses_incident_start_end_for_inversion": False,
        "consumes_calibrated_residuals": True,
        "consumes_raw_residuals": False,
        "optimization": "admm_graph_sparse_inversion",
        "per_repeat": [{"output_dir": str(repeat)}],
    }
    _write_json(tmp_path / "p2_graph_sparse_preview_summary.json", summary)
    result = check_a8_graph_sparse(str(tmp_path))
    assert result["passed"] is False
    assert "nonzero ratio too high for sparse inversion" in result["failed_checks"]

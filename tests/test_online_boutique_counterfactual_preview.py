from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from proberca.cli.check_a9_counterfactual import check_a9_counterfactual
from tests.test_counterfactual_explanation import _fake_inputs, _write_json, _write_jsonl


def test_run_counterfactual_explanation_cli(tmp_path: Path) -> None:
    candidate, evidence, graph, out = _fake_inputs(tmp_path, {"serviceA.cpu.hot": 5.0, "serviceB.cpu.cool": 0.1, "frontend.request.p99_latency_ms": 0.0})
    cmd = [
        sys.executable,
        "-m",
        "proberca.cli.run_counterfactual_explanation",
        "--graph-sparse-input",
        str(graph),
        "--candidate-input",
        str(candidate),
        "--evidence-channel-input",
        str(evidence),
        "--output",
        str(out),
        "--max-reopt-iter",
        "40",
    ]
    completed = subprocess.run(cmd, check=True, text=True, capture_output=True)
    assert "reoptimizes_with_candidate_removed=true" in completed.stdout
    assert (out / "counterfactual_metadata.json").exists()


def test_check_a9_counterfactual_minimal_summary(tmp_path: Path) -> None:
    repeat = tmp_path / "cpu" / "repeat_01"
    for name in ["counterfactual_metric_explanations.jsonl", "counterfactual_service_explanations.jsonl", "counterfactual_metric_ranking.jsonl", "counterfactual_service_ranking.jsonl"]:
        _write_jsonl(repeat / name, [])
    _write_json(repeat / "counterfactual_metadata.json", {"uses_root_labels": False, "reoptimizes_with_candidate_removed": True})
    summary = {
        "total_repeats": 20,
        "repeats_completed": 20,
        "uses_root_labels_for_counterfactual": False,
        "uses_target_config_for_counterfactual": False,
        "uses_injected_path_for_counterfactual": False,
        "uses_incident_start_end_for_counterfactual": False,
        "consumes_a8r_sparse_interventions": True,
        "reoptimizes_with_candidate_removed": True,
        "per_repeat": [{"output_dir": str(repeat)}],
    }
    _write_json(tmp_path / "p2_counterfactual_preview_summary.json", summary)
    result = check_a9_counterfactual(str(tmp_path))
    assert result["passed"] is True

import json
import subprocess
import sys
from pathlib import Path

from proberca.data.io import read_jsonl
from proberca.eval.p0_experiment import run_p0_experiment


def test_p0_experiment_outputs_and_metrics(tmp_path):
    output_dir = tmp_path / "demo"
    run_p0_experiment(str(output_dir), seed=7)

    assert (output_dir / "p0_results.jsonl").exists()
    assert (output_dir / "p0_results_metadata.json").exists()
    assert (output_dir / "p0_evaluation_summary.json").exists()
    assert (output_dir / "p0_experiment_metadata.json").exists()

    results = read_jsonl(output_dir / "p0_results.jsonl")
    assert len(results) == 4
    for result in results:
        assert "top_services" in result
        assert "top_metrics" in result
        assert "root_type" in result
        assert "path" in result
        assert "evidence" in result
        assert "adaptive_sampling" not in result
        assert "drift" not in result
        assert "shapley" not in result

    summary = json.loads((output_dir / "p0_evaluation_summary.json").read_text(encoding="utf-8"))
    assert summary["incidents_count"] == 4
    assert summary["service_hit_at_1"] >= 0.75
    assert summary["service_hit_at_3"] == 1.0
    assert summary["metric_hit_at_1"] >= 0.75
    assert summary["metric_hit_at_3"] == 1.0
    assert summary["root_type_accuracy"] == 1.0
    assert summary["path_fidelity"] >= 0.75


def test_p0_experiment_cli(tmp_path):
    output_dir = tmp_path / "demo-cli"
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_p0_experiment", "--output", str(output_dir), "--seed", "7"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "P0 端到端实验完成" in completed.stdout
    assert "不包含真实 eBPF、Kubernetes 或分布式部署" in completed.stdout

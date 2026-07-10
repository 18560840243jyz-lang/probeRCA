import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.p2a4_top3_acceptance import (
    evaluate_cpu_repeat_top3_acceptance,
    write_p2a4_cpu_top3_acceptance,
)


def _passing_summary():
    return {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "service_hit_at_1_mean": 1.0,
        "service_hit_at_1_min": 1.0,
        "metric_hit_at_1_mean": 0.2,
        "metric_hit_at_1_min": 0.0,
        "metric_hit_at_3_mean": 1.0,
        "metric_hit_at_3_min": 1.0,
        "metric_mrr_mean": 0.6,
        "metric_mrr_min": 0.5,
        "root_type_accuracy_mean": 1.0,
        "root_type_accuracy_min": 1.0,
        "path_fidelity_mean": 1.0,
        "path_fidelity_min": 1.0,
    }


def test_top3_acceptance_passes_with_low_metric_hit_at_1():
    result = evaluate_cpu_repeat_top3_acceptance(_passing_summary())
    assert result["p2a4_passed"] is True
    assert result["decision"] == "P2A4_CPU_TOP3_PASS"
    assert result["auxiliary_metrics"]["metric_hit_at_1_mean"] == 0.2


def test_top3_acceptance_fails_when_metric_hit_at_3_min_low():
    summary = _passing_summary()
    summary["metric_hit_at_3_min"] = 0.5
    result = evaluate_cpu_repeat_top3_acceptance(summary)
    assert result["p2a4_passed"] is False
    assert result["decision"] == "P2A4_CPU_TOP3_FAIL"
    assert "metric_hit_at_3_min < 1.0" in result["failed_checks"]


def test_write_p2a4_cpu_top3_acceptance(tmp_path):
    input_dir = tmp_path / "repeat"
    input_dir.mkdir()
    (input_dir / "p2a3_cpu_repeat_summary.json").write_text(json.dumps(_passing_summary()), encoding="utf-8")
    payload = write_p2a4_cpu_top3_acceptance(str(input_dir))
    out = Path(payload["output_path"])
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["p2a4_passed"] is True


def test_p2a4_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.check_p2a4_cpu_top3_acceptance as c; assert callable(c.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

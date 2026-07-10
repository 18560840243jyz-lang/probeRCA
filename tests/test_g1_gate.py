import json
import subprocess
import sys

from proberca.eval.g1_gate import evaluate_g1_gate, write_g1_decision


def _passing_summary():
    return {
        "label_leakage_passed": True,
        "multi_seed_mean_service_hit_at_1": 1.0,
        "multi_seed_min_service_hit_at_1": 0.75,
        "multi_seed_mean_metric_hit_at_1": 0.9,
        "multi_seed_min_metric_hit_at_1": 0.75,
        "full_vs_no_semantic": {
            "full": {"metric_hit_at_1": 1.0},
            "no_semantic_evidence": {"metric_hit_at_1": 0.25},
        },
        "noise_sensitivity": {
            "runs": [
                {"noise_std": 0.05, "metric_hit_at_1": 1.0},
                {"noise_std": 0.1, "metric_hit_at_1": 0.75},
                {"noise_std": 0.2, "metric_hit_at_1": 0.5},
            ]
        },
        "audit_passed": True,
    }


def test_evaluate_g1_gate_passes():
    result = evaluate_g1_gate(_passing_summary())
    assert result["g1_passed"] is True
    assert result["decision"] == "G1_PASS"
    assert result["failed_checks"] == []


def test_evaluate_g1_gate_fails_on_label_leakage():
    summary = _passing_summary()
    summary["label_leakage_passed"] = False
    result = evaluate_g1_gate(summary)
    assert result["g1_passed"] is False
    assert result["decision"] == "G1_FAIL"
    assert result["failed_checks"]


def test_write_g1_decision(tmp_path):
    summary_path = tmp_path / "p0_audit_summary.json"
    summary_path.write_text(json.dumps(_passing_summary()), encoding="utf-8")
    result = write_g1_decision(str(summary_path), str(tmp_path))
    assert (tmp_path / "g1_decision.json").exists()
    assert result["decision"]["g1_passed"] is True


def test_g1_gate_cli(tmp_path):
    summary_path = tmp_path / "p0_audit_summary.json"
    summary_path.write_text(json.dumps(_passing_summary()), encoding="utf-8")
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_g1_gate", "--audit-dir", str(tmp_path)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "G1 gate 决策完成" in completed.stdout
    assert "G1_PASS" in completed.stdout

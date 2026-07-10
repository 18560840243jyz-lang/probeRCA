import json
import subprocess
import sys

from proberca.eval.p1_audit import run_p1_audit
from proberca.eval.p1_gate import evaluate_p1_gate, write_p1_gate_decision


def _passing_summary():
    return {
        "label_leakage_passed": True,
        "multi_seed_min_service_hit_at_1": 0.75,
        "multi_seed_mean_metric_hit_at_1": 0.75,
        "multi_seed_min_metric_hit_at_3": 0.75,
        "multi_seed_mean_metric_mrr": 0.80,
        "multi_seed_min_root_type_accuracy": 0.75,
        "multi_seed_min_path_fidelity": 0.75,
        "observation_audit_passed": True,
        "audit_passed": True,
        "observed_ratio_mean": 0.60,
        "observed_ratio_min": 0.55,
        "observed_ratio_max": 0.65,
    }


def test_p1_gate_pass_and_fail():
    passed = evaluate_p1_gate(_passing_summary())
    assert passed["p1_gate_passed"] is True
    assert passed["decision"] == "P1_PASS"

    failed_summary = _passing_summary()
    failed_summary["label_leakage_passed"] = False
    failed = evaluate_p1_gate(failed_summary)
    assert failed["p1_gate_passed"] is False
    assert failed["decision"] == "P1_FAIL"
    assert failed["failed_checks"]


def test_write_p1_gate_decision(tmp_path):
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    summary_path = audit_dir / "p1_audit_summary.json"
    summary_path.write_text(json.dumps(_passing_summary()), encoding="utf-8")

    result = write_p1_gate_decision(summary_path, audit_dir)

    assert (audit_dir / "p1_gate_decision.json").exists()
    assert result["decision"]["p1_gate_passed"] is True


def test_run_p1_audit_quick(tmp_path):
    audit_dir = tmp_path / "p1_audit_quick"
    result = run_p1_audit(audit_dir, quick=True)

    assert (audit_dir / "p1_audit_summary.json").exists()
    assert (audit_dir / "p1_failure_analysis.json").exists()
    assert "label_leakage_passed" in result["summary"]
    assert "observed_ratio_mean" in result["summary"]


def test_p1_audit_and_gate_cli(tmp_path):
    audit_dir = tmp_path / "p1_cli_audit"
    audit = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_p1_audit", "--output", str(audit_dir), "--quick"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert audit.returncode == 0, audit.stderr + audit.stdout
    assert (audit_dir / "p1_audit_summary.json").exists()

    gate = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_p1_gate", "--audit-dir", str(audit_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert (audit_dir / "p1_gate_decision.json").exists()
    assert "P1 gate" in gate.stdout or "P1 gate" in gate.stderr

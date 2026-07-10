import subprocess
import sys

from proberca.eval.p0_audit import DEFAULT_SCAN_FILES, run_multi_seed_audit, run_p0_audit, scan_for_label_leakage


def test_scan_for_label_leakage_passes():
    result = scan_for_label_leakage(DEFAULT_SCAN_FILES)
    assert result["passed"] is True
    assert result["suspicious_files"] == []


def test_multi_seed_audit_quick_metrics(tmp_path):
    result = run_multi_seed_audit(str(tmp_path / "multi_seed"), seeds=[1, 2])
    assert "service_hit_at_1" in result["aggregate"]
    assert "metric_hit_at_1" in result["aggregate"]
    assert result["aggregate"]["service_hit_at_1"]["min"] >= 0.75
    assert result["aggregate"]["metric_hit_at_1"]["min"] >= 0.75


def test_run_p0_audit_quick(tmp_path):
    result = run_p0_audit(str(tmp_path / "audit"), quick=True)
    assert result["summary"]["label_leakage_passed"] is True
    assert "full_vs_no_semantic" in result["summary"]
    assert "noise_sensitivity" in result["summary"]
    assert result["summary"]["audit_passed"] is True


def test_p0_audit_cli_quick(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.run_p0_audit", "--output", str(tmp_path / "audit-cli"), "--quick"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P0 sanity audit 完成" in completed.stdout
    assert "不包含 P1 adaptive sampling" in completed.stdout

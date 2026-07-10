import json
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from proberca.cli.check_p1_freeze import check_p1_freeze


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEANUP_SCRIPT = PROJECT_ROOT / "scripts" / "cleanup_p1_artifacts.py"


def _write_freeze_files(freeze_dir: Path, *, p1_passed: bool = True) -> None:
    freeze_dir.mkdir(parents=True, exist_ok=True)
    (freeze_dir / "p1_audit_summary.json").write_text(
        json.dumps(
            {
                "label_leakage_passed": True,
                "multi_seed_min_service_hit_at_1": 1.0,
                "multi_seed_mean_metric_hit_at_1": 0.925,
                "multi_seed_min_metric_hit_at_3": 0.75,
                "multi_seed_mean_metric_mrr": 0.955,
                "observation_audit_passed": True,
                "audit_passed": True,
            }
        ),
        encoding="utf-8",
    )
    (freeze_dir / "p1_gate_decision.json").write_text(
        json.dumps(
            {
                "p1_gate_passed": p1_passed,
                "decision": "P1_PASS" if p1_passed else "P1_FAIL",
                "failed_checks": [] if p1_passed else ["forced_failure"],
            }
        ),
        encoding="utf-8",
    )


def _load_cleanup_module():
    spec = spec_from_file_location("cleanup_p1_artifacts", CLEANUP_SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_check_p1_freeze_passes(tmp_path):
    freeze_dir = tmp_path / "freeze"
    _write_freeze_files(freeze_dir)
    result = check_p1_freeze(freeze_dir)
    assert result["passed"] is True
    assert result["decision"] == "P1_PASS"


def test_check_p1_freeze_fails_on_gate_failure(tmp_path):
    freeze_dir = tmp_path / "freeze"
    _write_freeze_files(freeze_dir, p1_passed=False)
    result = check_p1_freeze(freeze_dir)
    assert result["passed"] is False
    assert result["failures"]


def test_cleanup_p1_artifacts_dry_run_and_apply(tmp_path):
    cleanup = _load_cleanup_module()
    base = tmp_path / "data" / "p1_single_vm"
    demo_dir = base / "demo"
    demo_dir.mkdir(parents=True)
    metrics_path = demo_dir / "metrics.jsonl"
    results_path = demo_dir / "p1_results.jsonl"
    metrics_path.write_text("large\n", encoding="utf-8")
    results_path.write_text("keep\n", encoding="utf-8")

    dry_run = cleanup.cleanup_p1_artifacts(base, apply=False)
    assert str(metrics_path) in dry_run["candidate_files"]
    assert metrics_path.exists()
    assert results_path.exists()

    applied = cleanup.cleanup_p1_artifacts(base, apply=True)
    assert str(metrics_path) in applied["deleted_files"]
    assert not metrics_path.exists()
    assert results_path.exists()


def test_check_p1_freeze_cli(tmp_path):
    freeze_dir = tmp_path / "freeze"
    _write_freeze_files(freeze_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.check_p1_freeze", "--freeze-dir", str(freeze_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1 freeze check passed" in completed.stdout

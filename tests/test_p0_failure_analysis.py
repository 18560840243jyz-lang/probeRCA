import json
import subprocess
import sys
from pathlib import Path

from proberca.data.io import write_jsonl
from proberca.eval.p0_failure_analysis import analyze_p0_failures


def _write_seed(seed_dir: Path, metric_hit: float = 1.0) -> None:
    seed_dir.mkdir(parents=True)
    incident = {
        "incident_id": "inc-1",
        "root_service": "paymentservice",
        "root_metric": "cpu.throttled_usec",
        "root_type": "CPU throttling",
        "symptom_service": "frontend",
        "start_ts": 0.0,
        "end_ts": 1.0,
        "injected_path": [],
    }
    result = {
        "incident_id": "inc-1",
        "symptom_service": "frontend",
        "top_services": [{"service": "paymentservice", "score": 1.0, "best_metric": "cpu.pressure"}],
        "top_metrics": [
            {"service": "paymentservice", "metric": "cpu.pressure", "score": 2.0},
            {"service": "paymentservice", "metric": "cpu.throttled_usec", "score": 1.0},
        ],
        "root_type": "CPU",
        "evidence": [],
        "path": ["paymentservice", "frontend"],
        "latency_ms": None,
    }
    eval_summary = {
        "metric_hit_at_1": metric_hit,
        "per_incident": [
            {"incident_id": "inc-1", "metric_hit_at_1": metric_hit}
        ],
    }
    (seed_dir / "p0_evaluation_summary.json").write_text(json.dumps(eval_summary), encoding="utf-8")
    write_jsonl(seed_dir / "incidents.jsonl", [incident])
    write_jsonl(seed_dir / "p0_results.jsonl", [result])
    write_jsonl(seed_dir / "semantic_interventions.jsonl", [])
    write_jsonl(seed_dir / "sparse_interventions.jsonl", [])


def test_analyze_p0_failures_handles_no_failures(tmp_path):
    audit_dir = tmp_path / "audit"
    _write_seed(audit_dir / "multi_seed" / "seed_1", metric_hit=1.0)
    result = analyze_p0_failures(str(audit_dir))
    assert result["failed_seeds"] == []
    assert (audit_dir / "p0_failure_analysis.json").exists()


def test_analyze_p0_failures_detects_wrong_metric(tmp_path):
    audit_dir = tmp_path / "audit"
    _write_seed(audit_dir / "multi_seed" / "seed_1", metric_hit=0.0)
    result = analyze_p0_failures(str(audit_dir))
    assert result["failed_seeds"] == [1]
    assert result["per_incident_failures"]
    assert "same_service_wrong_metric" in result["per_incident_failures"][0]["failure_patterns"]


def test_analyze_p0_failures_cli(tmp_path):
    audit_dir = tmp_path / "audit-cli"
    _write_seed(audit_dir / "multi_seed" / "seed_1", metric_hit=1.0)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.analyze_p0_failures", "--audit-dir", str(audit_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P0 失败分析完成" in completed.stdout

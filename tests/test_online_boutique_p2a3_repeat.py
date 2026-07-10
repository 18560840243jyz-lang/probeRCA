import json
import subprocess
import sys
from pathlib import Path

from proberca.adapters.online_boutique.p2a3_cpu_repeat import aggregate_repeat_summary, make_repeat_fault_config, write_yaml
from proberca.cli.check_p2a3_cpu_repeated import check_p2a3_cpu_repeated


def _base_fault_config(tmp_path: Path) -> Path:
    path = tmp_path / "base.yaml"
    path.write_text(
        """
project_mode: single_vm_pseudo_distributed
phase: P2A-1R
system: google_online_boutique
kubernetes:
  cluster_name: proberca-ob
  context: kind-proberca-ob
  namespace: online-boutique
experiment:
  experiment_id: ob_cpu_paymentservice_001_cadvisor
  target_service: paymentservice
  target_metric: cpu.throttled_usec
  target_fault_type: CPU throttling
  symptom_service: frontend
  window_size_sec: 5
  baseline_windows: 12
  faulty_windows: 12
  recovery_windows: 4
  output_dir: old
fault_injection:
  target_deployment: paymentservice
  target_container: server
  cpu_limit_during_fault: "50m"
  memory_limit_during_fault: "128Mi"
traffic:
  frontend_url: http://127.0.0.1:8080
  requests_per_window: 20
  request_timeout_sec: 3
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def test_make_repeat_fault_config_changes_output_and_incident_id(tmp_path):
    base = _base_fault_config(tmp_path)
    cfg1 = make_repeat_fault_config(base, 1, tmp_path / "repeat_01")
    cfg2 = make_repeat_fault_config(base, 2, tmp_path / "repeat_02")
    assert cfg1["experiment"]["experiment_id"] == "ob_cpu_paymentservice_repeat_01"
    assert cfg2["experiment"]["experiment_id"] == "ob_cpu_paymentservice_repeat_02"
    assert cfg1["experiment"]["output_dir"].endswith("repeat_01/raw")
    assert cfg2["experiment"]["output_dir"].endswith("repeat_02/raw")


def test_aggregate_repeat_summary_metrics():
    config = {"repeat_experiment": {"experiment_group_id": "g", "repeats": 2}}
    rows = [
        {"status": "success", "root_service_metric_coverage_passed": True, "paymentservice_throttled_metric_present": True, "fault_injection_succeeded": True, "restore_succeeded": True, "service_hit_at_1": 1, "metric_hit_at_1": 1, "metric_hit_at_3": 1, "metric_mrr": 1, "root_type_accuracy": 1, "path_fidelity": 1, "paymentservice_throttling_lift_debug": 10, "frontend_latency_lift_debug": 2},
        {"status": "success", "root_service_metric_coverage_passed": True, "paymentservice_throttled_metric_present": True, "fault_injection_succeeded": True, "restore_succeeded": True, "service_hit_at_1": 1, "metric_hit_at_1": 0, "metric_hit_at_3": 1, "metric_mrr": 0.5, "root_type_accuracy": 1, "path_fidelity": 1, "paymentservice_throttling_lift_debug": 20, "frontend_latency_lift_debug": 4},
    ]
    summary = aggregate_repeat_summary(config, rows)
    assert summary["repeats_completed"] == 2
    assert summary["metric_hit_at_1_mean"] == 0.5
    assert summary["metric_hit_at_3_mean"] == 1.0
    assert summary["paymentservice_throttling_lift_mean"] == 15.0


def test_check_p2a3_cpu_repeated_pass_and_fail(tmp_path):
    passed_dir = tmp_path / "passed"
    passed_dir.mkdir()
    summary = {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "metric_hit_at_3_mean": 1.0,
        "metric_hit_at_1_mean": 0.8,
        "service_hit_at_1_min": 1.0,
        "root_type_accuracy_min": 1.0,
    }
    (passed_dir / "p2a3_cpu_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2a3_cpu_repeated(passed_dir)["passed"] is True
    summary["metric_hit_at_1_mean"] = 0.5
    (passed_dir / "p2a3_cpu_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    failed = check_p2a3_cpu_repeated(passed_dir)
    assert failed["passed"] is False
    assert failed["failed_checks"]


def test_yaml_writer_roundtrip_shape(tmp_path):
    path = tmp_path / "out.yaml"
    write_yaml(path, {"a": 1, "b": {"c": True}, "d": ["x"]})
    text = path.read_text()
    assert "a: 1" in text
    assert "c: true" in text
    assert "- x" in text


def test_p2a3_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.run_p2a3_cpu_repeated as r; import proberca.cli.check_p2a3_cpu_repeated as c; assert callable(r.main); assert callable(c.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

from proberca.adapters.online_boutique.p2a3_failure_diagnosis import diagnose_p2a3_cpu_repeat_failures


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _fake_repeat(base: Path, index: int, predicted: str, rank: int):
    repeat = base / f"repeat_{index:02d}"
    raw = repeat / "raw"
    p1 = repeat / "p1rca"
    raw.mkdir(parents=True, exist_ok=True)
    p1.mkdir(parents=True, exist_ok=True)
    metrics = []
    for phase, pay_thr, pay_usage, cur_thr in [
        ("baseline", 0.0, 0.1, 0.0),
        ("faulty", 50_000.0, 0.8, 90_000.0 if predicted.startswith("currencyservice") else 1_000.0),
    ]:
        metrics.extend([
            {"phase": phase, "service": "paymentservice", "metric": "cpu.throttled_usec", "value": pay_thr},
            {"phase": phase, "service": "paymentservice", "metric": "cpu.usage", "value": pay_usage},
            {"phase": phase, "service": "currencyservice", "metric": "cpu.throttled_usec", "value": cur_thr},
        ])
    _write_jsonl(raw / "metrics.jsonl", metrics)
    _write_jsonl(raw / "incidents.jsonl", [{"root_service": "paymentservice", "root_metric": "cpu.throttled_usec"}])
    _write_jsonl(
        p1 / "ipw_semantic_interventions.jsonl",
        [
            {"node": predicted, "semantic_rank": 1, "semantic_score": 10, "sparse_score": 9, "evidence_type": "CPU", "confidence": 1},
            {"node": "paymentservice.cpu.throttled_usec", "semantic_rank": rank, "semantic_score": 5, "sparse_score": 4, "evidence_type": "CPU", "confidence": 1},
        ],
    )
    (p1 / "p1_evaluation_summary.json").write_text(json.dumps({"per_incident": [{"true_root_metric_debug": "paymentservice.cpu.throttled_usec", "predicted_top1_metric": predicted, "predicted_top1_service": predicted.split('.')[0], "metric_rank_debug": rank}]}), encoding="utf-8")


def test_diagnose_p2a3_cpu_repeat_failures_fake_outputs(tmp_path):
    base = tmp_path / "repeat"
    summary = {
        "per_repeat": [
            {"repeat_index": 1, "predicted_top1_metric": "paymentservice.cpu.usage", "predicted_top1_service": "paymentservice", "metric_rank_debug": 2},
            {"repeat_index": 2, "predicted_top1_metric": "currencyservice.cpu.throttled_usec", "predicted_top1_service": "currencyservice", "metric_rank_debug": 2},
        ]
    }
    base.mkdir()
    (base / "p2a3_cpu_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    _fake_repeat(base, 1, "paymentservice.cpu.usage", 2)
    _fake_repeat(base, 2, "currencyservice.cpu.throttled_usec", 2)
    result = diagnose_p2a3_cpu_repeat_failures(str(base))
    assert (base / "p2a3_failure_diagnosis.json").exists()
    assert "same_service_usage_over_throttling" in result["failure_patterns"]
    assert "cross_service_cpu_noise" in result["failure_patterns"]


def test_make_repeat_fault_config_applies_controlled_overrides(tmp_path):
    base = _base_fault_config(tmp_path)
    repeat_config = {
        "phase": "P2A-3R",
        "repeat_experiment": {
            "controlled_fault_overrides": {
                "cpu_limit_during_fault": "25m",
                "memory_limit_during_fault": "128Mi",
                "baseline_windows": 14,
                "faulty_windows": 16,
                "recovery_windows": 6,
                "requests_per_window": 40,
            }
        },
    }
    cfg = make_repeat_fault_config(base, 1, tmp_path / "repeat_01", repeat_config)
    assert cfg["phase"] == "P2A-3R"
    assert cfg["fault_injection"]["cpu_limit_during_fault"] == "25m"
    assert cfg["experiment"]["baseline_windows"] == 14
    assert cfg["traffic"]["requests_per_window"] == 40


def test_p2a3r_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.check_p2a3r_cpu_repeated as c; import proberca.cli.diagnose_p2a3_cpu_repeated as d; assert callable(c.main); assert callable(d.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

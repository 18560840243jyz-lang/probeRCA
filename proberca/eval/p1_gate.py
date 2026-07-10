"""P1 gate decision helpers."""

from __future__ import annotations

import json
from pathlib import Path


_GATE_CHECKS = [
    ("label_leakage_passed", "label_leakage_passed == True"),
    ("multi_seed_min_service_hit_at_1", "multi_seed_min_service_hit_at_1 >= 0.75"),
    ("multi_seed_mean_metric_hit_at_1", "multi_seed_mean_metric_hit_at_1 >= 0.75"),
    ("multi_seed_min_metric_hit_at_3", "multi_seed_min_metric_hit_at_3 >= 0.75"),
    ("multi_seed_mean_metric_mrr", "multi_seed_mean_metric_mrr >= 0.80"),
    ("multi_seed_min_root_type_accuracy", "multi_seed_min_root_type_accuracy >= 0.75"),
    ("multi_seed_min_path_fidelity", "multi_seed_min_path_fidelity >= 0.75"),
    ("observation_audit_passed", "observation_audit_passed == True"),
    ("audit_passed", "audit_passed == True"),
]


def evaluate_p1_gate(audit_summary: dict) -> dict:
    """Evaluate whether a P1 audit summary passes the P1 gate."""

    failed_checks: list[str] = []
    reasons: list[str] = []
    if audit_summary.get("label_leakage_passed") is not True:
        failed_checks.append("label_leakage_passed")
        reasons.append("label_leakage_passed must be True")
    if float(audit_summary.get("multi_seed_min_service_hit_at_1", 0.0)) < 0.75:
        failed_checks.append("multi_seed_min_service_hit_at_1")
        reasons.append("multi_seed_min_service_hit_at_1 must be >= 0.75")
    if float(audit_summary.get("multi_seed_mean_metric_hit_at_1", 0.0)) < 0.75:
        failed_checks.append("multi_seed_mean_metric_hit_at_1")
        reasons.append("multi_seed_mean_metric_hit_at_1 must be >= 0.75")
    if float(audit_summary.get("multi_seed_min_metric_hit_at_3", 0.0)) < 0.75:
        failed_checks.append("multi_seed_min_metric_hit_at_3")
        reasons.append("multi_seed_min_metric_hit_at_3 must be >= 0.75")
    if float(audit_summary.get("multi_seed_mean_metric_mrr", 0.0)) < 0.80:
        failed_checks.append("multi_seed_mean_metric_mrr")
        reasons.append("multi_seed_mean_metric_mrr must be >= 0.80")
    if float(audit_summary.get("multi_seed_min_root_type_accuracy", 0.0)) < 0.75:
        failed_checks.append("multi_seed_min_root_type_accuracy")
        reasons.append("multi_seed_min_root_type_accuracy must be >= 0.75")
    if float(audit_summary.get("multi_seed_min_path_fidelity", 0.0)) < 0.75:
        failed_checks.append("multi_seed_min_path_fidelity")
        reasons.append("multi_seed_min_path_fidelity must be >= 0.75")
    if audit_summary.get("observation_audit_passed") is not True:
        failed_checks.append("observation_audit_passed")
        reasons.append("observation_audit_passed must be True")
    if audit_summary.get("audit_passed") is not True:
        failed_checks.append("audit_passed")
        reasons.append("audit_passed must be True")

    key_metrics = {
        "multi_seed_min_service_hit_at_1": audit_summary.get("multi_seed_min_service_hit_at_1"),
        "multi_seed_mean_metric_hit_at_1": audit_summary.get("multi_seed_mean_metric_hit_at_1"),
        "multi_seed_min_metric_hit_at_3": audit_summary.get("multi_seed_min_metric_hit_at_3"),
        "multi_seed_mean_metric_mrr": audit_summary.get("multi_seed_mean_metric_mrr"),
        "multi_seed_min_root_type_accuracy": audit_summary.get("multi_seed_min_root_type_accuracy"),
        "multi_seed_min_path_fidelity": audit_summary.get("multi_seed_min_path_fidelity"),
        "observed_ratio_mean": audit_summary.get("observed_ratio_mean"),
        "observed_ratio_min": audit_summary.get("observed_ratio_min"),
        "observed_ratio_max": audit_summary.get("observed_ratio_max"),
    }
    passed = len(failed_checks) == 0
    return {
        "p1_gate_passed": passed,
        "decision": "P1_PASS" if passed else "P1_FAIL",
        "failed_checks": failed_checks,
        "reasons": reasons,
        "key_metrics": key_metrics,
    }


def write_p1_gate_decision(audit_summary_path: str | Path, output_dir: str | Path) -> dict:
    """Read p1_audit_summary.json and write p1_gate_decision.json."""

    summary_path = Path(audit_summary_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    audit_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    decision = evaluate_p1_gate(audit_summary)
    path = output_path / "p1_gate_decision.json"
    path.write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"p1_gate_decision_path": str(path), "decision": decision}

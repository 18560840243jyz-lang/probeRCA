"""G1 gate decision for freezing probeRCA P0."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _metric(summary: dict, path: list[str], default: float = 0.0) -> float:
    value: Any = summary
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_g1_gate(audit_summary: dict) -> dict:
    """Evaluate whether P0 passes the G1 freeze gate."""

    failed_checks: list[str] = []
    reasons: list[str] = []
    full_metric_hit_at_1 = _metric(audit_summary, ["full_vs_no_semantic", "full", "metric_hit_at_1"])
    no_semantic_metric_hit_at_1 = _metric(audit_summary, ["full_vs_no_semantic", "no_semantic_evidence", "metric_hit_at_1"])
    noise_runs = audit_summary.get("noise_sensitivity", {}).get("runs", [])

    checks = [
        ("label_leakage_passed", bool(audit_summary.get("label_leakage_passed")) is True, "标签泄漏检查必须通过。"),
        (
            "multi_seed_min_service_hit_at_1",
            float(audit_summary.get("multi_seed_min_service_hit_at_1", 0.0)) >= 0.75,
            "多 seed 下服务级 Hit@1 最低值必须至少 0.75。",
        ),
        (
            "multi_seed_min_metric_hit_at_1",
            float(audit_summary.get("multi_seed_min_metric_hit_at_1", 0.0)) >= 0.75,
            "多 seed 下指标级 Hit@1 最低值必须至少 0.75。",
        ),
        (
            "full_metric_not_worse_than_no_semantic",
            full_metric_hit_at_1 >= no_semantic_metric_hit_at_1,
            "完整方法的指标级 Hit@1 不能低于去掉语义证据的版本。",
        ),
        (
            "semantic_evidence_contributes",
            no_semantic_metric_hit_at_1 < full_metric_hit_at_1,
            "语义证据消融应证明 semantic evidence 有贡献。",
        ),
        ("audit_passed", bool(audit_summary.get("audit_passed")) is True, "P0 audit_passed 必须为 True。"),
    ]

    for row in noise_runs:
        try:
            noise_std = float(row.get("noise_std"))
            metric_hit_at_1 = float(row.get("metric_hit_at_1", 0.0))
        except (TypeError, ValueError):
            noise_std = 0.0
            metric_hit_at_1 = 0.0
        if noise_std <= 0.1 and metric_hit_at_1 < 0.75:
            checks.append(
                (
                    f"noise_metric_hit_at_1_noise_{noise_std:g}",
                    False,
                    f"noise_std={noise_std:g} 时 metric_hit_at_1 必须至少 0.75。",
                )
            )

    for name, passed, reason in checks:
        if not passed:
            failed_checks.append(name)
            reasons.append(reason)

    key_metrics = {
        "label_leakage_passed": bool(audit_summary.get("label_leakage_passed")),
        "multi_seed_mean_service_hit_at_1": float(audit_summary.get("multi_seed_mean_service_hit_at_1", 0.0)),
        "multi_seed_min_service_hit_at_1": float(audit_summary.get("multi_seed_min_service_hit_at_1", 0.0)),
        "multi_seed_mean_metric_hit_at_1": float(audit_summary.get("multi_seed_mean_metric_hit_at_1", 0.0)),
        "multi_seed_min_metric_hit_at_1": float(audit_summary.get("multi_seed_min_metric_hit_at_1", 0.0)),
        "full_metric_hit_at_1": full_metric_hit_at_1,
        "no_semantic_metric_hit_at_1": no_semantic_metric_hit_at_1,
        "audit_passed": bool(audit_summary.get("audit_passed")),
        "noise_runs": [
            {"noise_std": float(row.get("noise_std", 0.0)), "metric_hit_at_1": float(row.get("metric_hit_at_1", 0.0))}
            for row in noise_runs
        ],
    }
    g1_passed = not failed_checks
    return {
        "g1_passed": g1_passed,
        "decision": "G1_PASS" if g1_passed else "G1_FAIL",
        "reasons": reasons,
        "failed_checks": failed_checks,
        "key_metrics": key_metrics,
    }


def write_g1_decision(audit_summary_path: str, output_dir: str) -> dict:
    """Read p0_audit_summary.json and write g1_decision.json."""

    summary_path = Path(audit_summary_path)
    if not summary_path.exists():
        raise FileNotFoundError(f"missing audit summary file: {summary_path}")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    audit_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    decision = evaluate_g1_gate(audit_summary)
    decision_path = output_path / "g1_decision.json"
    with decision_path.open("w", encoding="utf-8") as handle:
        json.dump(decision, handle, ensure_ascii=False, indent=2)
    return {"g1_decision_path": str(decision_path), "decision": decision}

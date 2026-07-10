"""Diagnose P2A-3 repeated real CPU fault injection failures."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _std(values: list[float]) -> float:
    return float(np.std(np.asarray(values, dtype=float))) if values else 0.0


def _phase_values(metrics: list[dict[str, Any]], service: str, metric: str, phase: str) -> list[float]:
    return [float(row.get("value", 0.0)) for row in metrics if row.get("service") == service and row.get("metric") == metric and row.get("phase") == phase]


def _metric_lift(metrics: list[dict[str, Any]], service: str, metric: str) -> dict[str, float]:
    baseline = _phase_values(metrics, service, metric, "baseline")
    faulty = _phase_values(metrics, service, metric, "faulty")
    b_mean = _mean(baseline)
    f_mean = _mean(faulty)
    return {
        "baseline_mean": b_mean,
        "faulty_mean": f_mean,
        "lift": f_mean - b_mean,
        "baseline_std": _std(baseline),
        "faulty_std": _std(faulty),
    }


def _all_service_throttling_lift_ranking(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    services = sorted({str(row.get("service")) for row in metrics if row.get("metric") == "cpu.throttled_usec"})
    ranking = []
    for service in services:
        stats = _metric_lift(metrics, service, "cpu.throttled_usec")
        ranking.append({"service": service, **stats})
    ranking.sort(key=lambda row: (-float(row["lift"]), str(row["service"])))
    return ranking


def _top5_semantic(records: list[dict[str, Any]]) -> dict[str, list[Any]]:
    ranked = sorted(records, key=lambda row: (int(row.get("semantic_rank", 10**9)), -float(row.get("semantic_score", 0.0)), str(row.get("node", ""))))[:5]
    return {
        "top5_metrics": [row.get("node") for row in ranked],
        "top5_semantic_scores": [float(row.get("semantic_score", 0.0)) for row in ranked],
        "top5_sparse_scores": [float(row.get("sparse_score", 0.0)) for row in ranked],
        "top5_evidence_types": [row.get("evidence_type") for row in ranked],
        "top5_confidence": [float(row.get("confidence", 0.0)) for row in ranked],
    }


def _detect_patterns(predicted: str, true_metric: str, target_lift: dict[str, float], currency_lift: dict[str, float], all_noise: list[dict[str, Any]]) -> list[str]:
    patterns: list[str] = []
    if predicted == "paymentservice.cpu.usage" and true_metric == "paymentservice.cpu.throttled_usec":
        patterns.append("same_service_usage_over_throttling")
    if predicted.startswith("currencyservice.") or (not predicted.startswith("paymentservice.") and predicted.endswith("cpu.throttled_usec")):
        patterns.append("cross_service_cpu_noise")
    if float(target_lift.get("lift", 0.0)) < 100000.0:
        patterns.append("weak_target_lift")
    if float(target_lift.get("baseline_std", 0.0)) > max(10000.0, abs(float(target_lift.get("baseline_mean", 0.0))) * 2.0):
        patterns.append("unstable_baseline")
    if float(target_lift.get("baseline_mean", 0.0)) > 10000.0:
        patterns.append("recovery_carryover")
    top_noise = all_noise[0] if all_noise else {}
    if top_noise and top_noise.get("service") != "paymentservice" and float(top_noise.get("lift", 0.0)) >= float(target_lift.get("lift", 0.0)):
        if "cross_service_cpu_noise" not in patterns:
            patterns.append("cross_service_cpu_noise")
    if currency_lift.get("lift", 0.0) > target_lift.get("lift", 0.0):
        if "cross_service_cpu_noise" not in patterns:
            patterns.append("cross_service_cpu_noise")
    return patterns or ["unknown"]


def diagnose_p2a3_cpu_repeat_failures(input_dir: str = "data/p2_online_boutique/cpu_paymentservice_repeated") -> dict[str, Any]:
    """Diagnose existing P2A-3 real CPU repeat outputs without changing scoring."""

    base = Path(input_dir)
    summary = _read_json(base / "p2a3_cpu_repeat_summary.json")
    per_repeat_summary = {int(row.get("repeat_index", 0)): row for row in summary.get("per_repeat", [])}
    repeat_dirs = sorted(path for path in base.glob("repeat_*") if path.is_dir())

    per_repeat_top5: list[dict[str, Any]] = []
    per_repeat_metric_lift: list[dict[str, Any]] = []
    failed_repeats: list[int] = []
    pattern_counts: dict[str, int] = defaultdict(int)

    for repeat_dir in repeat_dirs:
        try:
            repeat_index = int(repeat_dir.name.split("_")[-1])
        except ValueError:
            continue
        raw_dir = repeat_dir / "raw"
        p1_dir = repeat_dir / "p1rca"
        metrics = _read_jsonl(raw_dir / "metrics.jsonl")
        incidents = _read_jsonl(raw_dir / "incidents.jsonl")
        semantic = _read_jsonl(p1_dir / "ipw_semantic_interventions.jsonl")
        evaluation = _read_json(p1_dir / "p1_evaluation_summary.json")
        per_eval = evaluation.get("per_incident", [{}])[0] if evaluation.get("per_incident") else {}
        row_summary = per_repeat_summary.get(repeat_index, {})
        true_metric = str(per_eval.get("true_root_metric_debug") or "paymentservice.cpu.throttled_usec")
        predicted = str(row_summary.get("predicted_top1_metric") or per_eval.get("predicted_top1_metric") or "")
        metric_rank = per_eval.get("metric_rank_debug", row_summary.get("metric_rank_debug"))
        if metric_rank != 1:
            failed_repeats.append(repeat_index)
        top5 = _top5_semantic(semantic)
        per_repeat_top5.append({
            "repeat_index": repeat_index,
            "predicted_top1_service": row_summary.get("predicted_top1_service") or per_eval.get("predicted_top1_service"),
            "predicted_top1_metric": predicted,
            "true_root_metric_debug": true_metric,
            "metric_rank_debug": metric_rank,
            **top5,
        })
        target_lift = _metric_lift(metrics, "paymentservice", "cpu.throttled_usec")
        usage_lift = _metric_lift(metrics, "paymentservice", "cpu.usage")
        currency_lift = _metric_lift(metrics, "currencyservice", "cpu.throttled_usec")
        noise_ranking = _all_service_throttling_lift_ranking(metrics)
        patterns = _detect_patterns(predicted, true_metric, target_lift, currency_lift, noise_ranking)
        failed = metric_rank != 1
        if failed:
            for pattern in patterns:
                pattern_counts[pattern] += 1
        per_repeat_metric_lift.append({
            "repeat_index": repeat_index,
            "paymentservice_cpu_throttled_usec": target_lift,
            "paymentservice_cpu_usage": usage_lift,
            "currencyservice_cpu_throttled_usec": currency_lift,
            "all_services_cpu_throttled_usec_faulty_lift_ranking": noise_ranking[:10],
            "failure_patterns": patterns if failed else [],
        })

    recommendation = [
        "Use a stronger CPU limit such as 25m to increase target throttling lift.",
        "Increase faulty windows and frontend requests per window to reduce weak-signal repeats.",
        "Add pre-repeat target throttling checks and longer cooldown to reduce recovery carryover.",
        "Record non-target service CPU throttling noise and keep P1 scoring unchanged.",
    ]
    result = {
        "input_dir": str(base),
        "failed_repeats": failed_repeats,
        "failure_patterns": dict(sorted(pattern_counts.items())),
        "per_repeat_top5": per_repeat_top5,
        "per_repeat_metric_lift": per_repeat_metric_lift,
        "recommendation": recommendation,
    }
    out = base / "p2a3_failure_diagnosis.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result

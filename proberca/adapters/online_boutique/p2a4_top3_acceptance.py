"""P2A-4 Top3 acceptance for repeated real CPU fault injection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT_DIR = "data/p2_online_boutique/cpu_paymentservice_repeated_controlled"


def load_cpu_repeat_summary(input_dir: str = DEFAULT_INPUT_DIR) -> dict[str, Any]:
    """Load existing P2A-3R repeat summary without rerunning experiments."""

    summary_path = Path(input_dir) / "p2a3_cpu_repeat_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"missing required P2A-3R summary: {summary_path}")
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"summary must be a JSON object: {summary_path}")
    return data


def _f(summary: dict[str, Any], key: str) -> float:
    return float(summary.get(key, 0.0) or 0.0)


def _i(summary: dict[str, Any], key: str) -> int:
    return int(summary.get(key, 0) or 0)


def evaluate_cpu_repeat_top3_acceptance(summary: dict[str, Any]) -> dict[str, Any]:
    """Evaluate CPU repeat acceptance using metric Hit@3 as the primary metric."""

    checks = [
        (_i(summary, "repeats_completed") >= 5, "repeats_completed < 5"),
        (_i(summary, "repeats_successful_quality") >= 5, "repeats_successful_quality < 5"),
        (_i(summary, "repeats_successful_rca") >= 5, "repeats_successful_rca < 5"),
        (_f(summary, "service_hit_at_1_mean") >= 1.0, "service_hit_at_1_mean < 1.0"),
        (_f(summary, "service_hit_at_1_min") >= 1.0, "service_hit_at_1_min < 1.0"),
        (_f(summary, "metric_hit_at_3_mean") >= 1.0, "metric_hit_at_3_mean < 1.0"),
        (_f(summary, "metric_hit_at_3_min") >= 1.0, "metric_hit_at_3_min < 1.0"),
        (_f(summary, "root_type_accuracy_mean") >= 1.0, "root_type_accuracy_mean < 1.0"),
        (_f(summary, "root_type_accuracy_min") >= 1.0, "root_type_accuracy_min < 1.0"),
        (_f(summary, "path_fidelity_mean") >= 1.0, "path_fidelity_mean < 1.0"),
        (_f(summary, "path_fidelity_min") >= 1.0, "path_fidelity_min < 1.0"),
    ]
    failed = [reason for ok, reason in checks if not ok]
    key_metrics = {
        "repeats_completed": _i(summary, "repeats_completed"),
        "repeats_successful_quality": _i(summary, "repeats_successful_quality"),
        "repeats_successful_rca": _i(summary, "repeats_successful_rca"),
        "service_hit_at_1_mean": _f(summary, "service_hit_at_1_mean"),
        "service_hit_at_1_min": _f(summary, "service_hit_at_1_min"),
        "metric_hit_at_3_mean": _f(summary, "metric_hit_at_3_mean"),
        "metric_hit_at_3_min": _f(summary, "metric_hit_at_3_min"),
        "root_type_accuracy_mean": _f(summary, "root_type_accuracy_mean"),
        "root_type_accuracy_min": _f(summary, "root_type_accuracy_min"),
        "path_fidelity_mean": _f(summary, "path_fidelity_mean"),
        "path_fidelity_min": _f(summary, "path_fidelity_min"),
    }
    auxiliary_metrics = {
        "metric_hit_at_1_mean": _f(summary, "metric_hit_at_1_mean"),
        "metric_hit_at_1_min": _f(summary, "metric_hit_at_1_min"),
        "metric_mrr_mean": _f(summary, "metric_mrr_mean"),
        "metric_mrr_min": _f(summary, "metric_mrr_min"),
    }
    passed = not failed
    return {
        "p2a4_passed": passed,
        "decision": "P2A4_CPU_TOP3_PASS" if passed else "P2A4_CPU_TOP3_FAIL",
        "failed_checks": failed,
        "key_metrics": key_metrics,
        "auxiliary_metrics": auxiliary_metrics,
        "note": "metric_hit_at_1 is reported as an auxiliary metric and is not a P2A-4 Top3 acceptance gate.",
    }


def write_p2a4_cpu_top3_acceptance(input_dir: str = DEFAULT_INPUT_DIR, output_dir: str | None = None) -> dict[str, Any]:
    """Write P2A-4 Top3 acceptance result next to the existing repeat summary."""

    summary = load_cpu_repeat_summary(input_dir)
    result = evaluate_cpu_repeat_top3_acceptance(summary)
    output_path = Path(output_dir) if output_dir is not None else Path(input_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / "p2a4_cpu_top3_acceptance.json"
    out_file.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"input_dir": str(input_dir), "output_dir": str(output_path), "output_path": str(out_file), "acceptance": result}

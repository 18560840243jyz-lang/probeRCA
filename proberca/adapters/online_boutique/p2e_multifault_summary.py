"""P2E real multi-fault summary for Online Boutique experiments."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

FAULT_CONFIG = {
    "cpu": {
        "fault_type": "CPU",
        "summary_path": "cpu_paymentservice_repeated_controlled/p2a3_cpu_repeat_summary.json",
        "acceptance_path": "cpu_paymentservice_repeated_controlled/p2a4_cpu_top3_acceptance.json",
        "target_service": "paymentservice",
        "target_metric": "cpu.throttled_usec",
        "limitations": ["CPU exact metric Hit@1 is unstable; cpu.usage can rank above cpu.throttled_usec, but metric Hit@3 is stable."],
    },
    "network": {
        "fault_type": "Network",
        "summary_path": "network_shippingservice_repeated/p2b1_network_repeat_summary.json",
        "target_service": "shippingservice",
        "target_metric": "net.retrans",
        "limitations": [],
    },
    "io": {
        "fault_type": "I/O",
        "summary_path": "io_rediscart_repeated/p2c1_io_repeat_summary.json",
        "target_service": "redis-cart",
        "target_metric": "io.write_bytes",
        "limitations": [],
    },
    "lock": {
        "fault_type": "Lock",
        "summary_path": "lock_cartservice_repeated_phaseaware/p2d1r_lock_repeat_summary.json",
        "target_service": "cartservice",
        "target_metric": "lock.futex_wait_ms",
        "limitations": [
            "Lock fault comes from a cartservice Pod sidecar lock-stress container, not an original cartservice business-code bug.",
            "Baseline lock metrics are real idle sidecar measurements, not fake baseline zeros.",
        ],
    },
}

STANDARD_FIELDS = [
    "repeats_completed",
    "repeats_successful_quality",
    "repeats_successful_rca",
    "service_hit_at_1_mean",
    "service_hit_at_1_min",
    "metric_hit_at_1_mean",
    "metric_hit_at_1_min",
    "metric_hit_at_3_mean",
    "metric_hit_at_3_min",
    "metric_mrr_mean",
    "metric_mrr_min",
    "root_type_accuracy_mean",
    "root_type_accuracy_min",
    "path_fidelity_mean",
    "path_fidelity_min",
]

UNKNOWN = "unknown"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"required P2E input summary is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isfinite(float(value)):
            return float(value)
        return None
    return None


def _weighted_mean(fault_summaries: dict[str, dict[str, Any]], field: str) -> float | str:
    numerator = 0.0
    denominator = 0.0
    for item in fault_summaries.values():
        repeats = _to_float(item.get("repeats_completed"))
        value = _to_float(item.get(field))
        if repeats is None or value is None:
            continue
        numerator += repeats * value
        denominator += repeats
    if denominator <= 0.0:
        return UNKNOWN
    return numerator / denominator


def _sum_field(fault_summaries: dict[str, dict[str, Any]], field: str) -> int | str:
    total = 0
    found = False
    for item in fault_summaries.values():
        value = _to_float(item.get(field))
        if value is None:
            continue
        total += int(value)
        found = True
    return total if found else UNKNOWN


def _min_field(fault_summaries: dict[str, dict[str, Any]], field: str) -> float | str:
    values = [_to_float(item.get(field)) for item in fault_summaries.values()]
    clean = [value for value in values if value is not None]
    if not clean:
        return UNKNOWN
    return min(clean)


def _normalize_summary(name: str, raw: dict[str, Any], acceptance: dict[str, Any] | None = None) -> dict[str, Any]:
    config = FAULT_CONFIG[name]
    missing_fields: list[str] = []
    normalized: dict[str, Any] = {
        "fault_type": config["fault_type"],
        "target_service": config["target_service"],
        "target_metric": config["target_metric"],
        "limitations": list(config.get("limitations", [])),
        "per_repeat": raw.get("per_repeat", []),
        "missing_fields": missing_fields,
        "source_summary_path": config["summary_path"],
    }
    for field in STANDARD_FIELDS:
        if field in raw:
            normalized[field] = raw[field]
        else:
            normalized[field] = UNKNOWN
            missing_fields.append(field)
    raw_limit = raw.get("limitation") or raw.get("limitations")
    if isinstance(raw_limit, str) and raw_limit not in normalized["limitations"]:
        normalized["limitations"].append(raw_limit)
    elif isinstance(raw_limit, list):
        for item in raw_limit:
            if item not in normalized["limitations"]:
                normalized["limitations"].append(item)
    if acceptance is not None:
        normalized["acceptance"] = acceptance
    return normalized


def load_fault_type_summaries(base_dir: str = "data/p2_online_boutique") -> dict[str, dict[str, Any]]:
    base = Path(base_dir)
    result: dict[str, dict[str, Any]] = {}
    for name, config in FAULT_CONFIG.items():
        raw = _read_json(base / str(config["summary_path"]))
        acceptance = None
        if config.get("acceptance_path"):
            acceptance = _read_json(base / str(config["acceptance_path"]))
        result[name] = _normalize_summary(name, raw, acceptance=acceptance)
    return result


def compute_weighted_overall_metrics(fault_summaries: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_repeats": _sum_field(fault_summaries, "repeats_completed"),
        "total_successful_quality": _sum_field(fault_summaries, "repeats_successful_quality"),
        "total_successful_rca": _sum_field(fault_summaries, "repeats_successful_rca"),
        "service_hit_at_1_overall": _weighted_mean(fault_summaries, "service_hit_at_1_mean"),
        "metric_hit_at_3_overall": _weighted_mean(fault_summaries, "metric_hit_at_3_mean"),
        "root_type_accuracy_overall": _weighted_mean(fault_summaries, "root_type_accuracy_mean"),
        "path_fidelity_overall": _weighted_mean(fault_summaries, "path_fidelity_mean"),
        "metric_hit_at_1_overall_auxiliary": _weighted_mean(fault_summaries, "metric_hit_at_1_mean"),
        "metric_mrr_overall_auxiliary": _weighted_mean(fault_summaries, "metric_mrr_mean"),
        "service_hit_at_1_min_across_faults": _min_field(fault_summaries, "service_hit_at_1_min"),
        "metric_hit_at_3_min_across_faults": _min_field(fault_summaries, "metric_hit_at_3_min"),
        "root_type_accuracy_min_across_faults": _min_field(fault_summaries, "root_type_accuracy_min"),
        "path_fidelity_min_across_faults": _min_field(fault_summaries, "path_fidelity_min"),
        "metric_hit_at_1_min_across_faults_auxiliary": _min_field(fault_summaries, "metric_hit_at_1_min"),
        "metric_mrr_min_across_faults_auxiliary": _min_field(fault_summaries, "metric_mrr_min"),
    }


def _check_at_least(failed: list[str], data: dict[str, Any], key: str, threshold: float) -> None:
    value = _to_float(data.get(key))
    if value is None or value < threshold:
        failed.append(f"{key} < {threshold}")


def evaluate_p2e_multifault_acceptance(overall: dict[str, Any], fault_summaries: dict[str, dict[str, Any]]) -> dict[str, Any]:
    failed: list[str] = []
    _check_at_least(failed, overall, "total_repeats", 20)
    _check_at_least(failed, overall, "total_successful_quality", 20)
    _check_at_least(failed, overall, "total_successful_rca", 20)
    _check_at_least(failed, overall, "service_hit_at_1_overall", 0.8)
    _check_at_least(failed, overall, "metric_hit_at_3_overall", 0.8)
    _check_at_least(failed, overall, "root_type_accuracy_overall", 0.8)
    _check_at_least(failed, overall, "path_fidelity_overall", 0.8)

    per_fault_type_status: dict[str, dict[str, Any]] = {}
    limitations: list[str] = []
    for name, item in fault_summaries.items():
        item_failed: list[str] = []
        metric_hit_at_3 = _to_float(item.get("metric_hit_at_3_mean"))
        service_hit_at_1 = _to_float(item.get("service_hit_at_1_mean"))
        if metric_hit_at_3 is None or metric_hit_at_3 < 0.8:
            item_failed.append("metric_hit_at_3_mean < 0.8")
            failed.append(f"{name}.metric_hit_at_3_mean < 0.8")
        if service_hit_at_1 is None or service_hit_at_1 < 0.8:
            item_failed.append("service_hit_at_1_mean < 0.8")
            failed.append(f"{name}.service_hit_at_1_mean < 0.8")
        per_fault_type_status[name] = {
            "passed": not item_failed,
            "failed_checks": item_failed,
            "fault_type": item.get("fault_type", name),
            "service_hit_at_1_mean": item.get("service_hit_at_1_mean"),
            "metric_hit_at_3_mean": item.get("metric_hit_at_3_mean"),
            "metric_hit_at_1_mean_auxiliary": item.get("metric_hit_at_1_mean"),
        }
        for limitation in item.get("limitations", []):
            if limitation not in limitations:
                limitations.append(limitation)

    passed = not failed
    return {
        "p2e_passed": passed,
        "decision": "P2E_REAL_MULTIFAULT_PASS" if passed else "P2E_REAL_MULTIFAULT_FAIL",
        "failed_checks": failed,
        "primary_metrics": {
            "total_repeats": overall.get("total_repeats"),
            "total_successful_quality": overall.get("total_successful_quality"),
            "total_successful_rca": overall.get("total_successful_rca"),
            "service_hit_at_1_overall": overall.get("service_hit_at_1_overall"),
            "metric_hit_at_3_overall": overall.get("metric_hit_at_3_overall"),
            "root_type_accuracy_overall": overall.get("root_type_accuracy_overall"),
            "path_fidelity_overall": overall.get("path_fidelity_overall"),
        },
        "auxiliary_metrics": {
            "metric_hit_at_1_overall_auxiliary": overall.get("metric_hit_at_1_overall_auxiliary"),
            "metric_mrr_overall_auxiliary": overall.get("metric_mrr_overall_auxiliary"),
            "metric_hit_at_1_min_across_faults_auxiliary": overall.get("metric_hit_at_1_min_across_faults_auxiliary"),
            "metric_mrr_min_across_faults_auxiliary": overall.get("metric_mrr_min_across_faults_auxiliary"),
        },
        "per_fault_type_status": per_fault_type_status,
        "limitations": limitations,
    }


def _format_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def _build_markdown(summary: dict[str, Any], acceptance: dict[str, Any]) -> str:
    overall = summary["overall"]
    faults = summary["fault_summaries"]
    lines = [
        "# P2E Real Multi-Fault Summary",
        "",
        "## Scope",
        "- CPU / Network / I/O / Lock four real repeated experiments.",
        "- Each fault type has 5 repeats, for 20 total repeats.",
        "- All repeats are real injections in the Online Boutique single-VM kind cluster.",
        "- This is not synthetic data.",
        "",
        "## Primary Metrics",
        f"- service_hit_at_1_overall: {_format_value(overall.get('service_hit_at_1_overall'))}",
        f"- metric_hit_at_3_overall: {_format_value(overall.get('metric_hit_at_3_overall'))}",
        f"- root_type_accuracy_overall: {_format_value(overall.get('root_type_accuracy_overall'))}",
        f"- path_fidelity_overall: {_format_value(overall.get('path_fidelity_overall'))}",
        "",
        "## Auxiliary Metrics",
        f"- metric_hit_at_1_overall_auxiliary: {_format_value(overall.get('metric_hit_at_1_overall_auxiliary'))}",
        f"- metric_mrr_overall_auxiliary: {_format_value(overall.get('metric_mrr_overall_auxiliary'))}",
        "",
        "metric Hit@1 is auxiliary.",
        "中文解释：指标级 Top1 是辅助指标，不作为 P2 真实实验通过门槛。",
        "",
        "## Per Fault Type",
    ]
    for name in ["cpu", "network", "io", "lock"]:
        item = faults[name]
        limitation = "; ".join(item.get("limitations", [])) or "none"
        lines.extend([
            f"### {item.get('fault_type', name)}",
            f"- repeats: {item.get('repeats_completed')}",
            f"- target_service: {item.get('target_service')}",
            f"- target_metric: {item.get('target_metric')}",
            f"- service_hit_at_1: {item.get('service_hit_at_1_mean')}",
            f"- metric_hit_at_3: {item.get('metric_hit_at_3_mean')}",
            f"- metric_hit_at_1 auxiliary: {item.get('metric_hit_at_1_mean')}",
            f"- root_type_accuracy: {item.get('root_type_accuracy_mean')}",
            f"- path_fidelity: {item.get('path_fidelity_mean')}",
            f"- limitation: {limitation}",
            "",
        ])
    lines.extend([
        "## Known Limitations",
        "- CPU exact metric Hit@1 is unstable: cpu.usage often ranks above cpu.throttled_usec, but metric Hit@3 is stable.",
        "- Lock fault comes from sidecar lock-stress, not an original cartservice business-code bug.",
        "- Current experiments do not use Prometheus/Beyla/ClickHouse.",
        "- Current deployment is single-VM pseudo-distributed deployment.",
        "  中文解释：单机伪分布式部署。",
        "",
        "## Decision",
        str(acceptance.get("decision")),
        "",
    ])
    return "\n".join(lines)


def write_p2e_multifault_summary(
    output_dir: str = "data/p2_online_boutique/multifault_summary",
    base_dir: str = "data/p2_online_boutique",
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    fault_summaries = load_fault_type_summaries(base_dir=base_dir)
    overall = compute_weighted_overall_metrics(fault_summaries)
    acceptance = evaluate_p2e_multifault_acceptance(overall, fault_summaries)
    summary = {
        "summary_type": "P2E real multi-fault experiment summary",
        "base_dir": base_dir,
        "overall": overall,
        "fault_summaries": fault_summaries,
        "evaluation_policy": {
            "primary_metrics": ["service_hit_at_1", "metric_hit_at_3", "root_type_accuracy", "path_fidelity"],
            "auxiliary_metrics": ["metric_hit_at_1", "metric_mrr"],
            "metric_hit_at_1_is_auxiliary": True,
        },
    }
    metadata = {
        "phase": "P2E",
        "real_collection": True,
        "reused_existing_results_only": True,
        "reran_fault_injection": False,
        "reran_rca_pipeline": False,
        "output_dir": str(output),
        "inputs": {name: item.get("source_summary_path") for name, item in fault_summaries.items()},
    }
    (output / "p2e_multifault_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "p2e_multifault_acceptance.json").write_text(json.dumps(acceptance, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "p2e_multifault_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "p2e_multifault_report.md").write_text(_build_markdown(summary, acceptance), encoding="utf-8")
    return {"summary": summary, "acceptance": acceptance, "metadata": metadata, "output_dir": str(output)}

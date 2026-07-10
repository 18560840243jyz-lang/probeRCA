"""Check whether the P1 freeze snapshot is ready for the next phase."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SERVICE_HIT_AT_1_THRESHOLD = 0.75
MEAN_METRIC_HIT_AT_1_THRESHOLD = 0.75
METRIC_HIT_AT_3_THRESHOLD = 0.75
MEAN_METRIC_MRR_THRESHOLD = 0.80


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"缺少冻结快照文件：{path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"冻结快照文件不是 JSON object：{path}")
    return data


def _number_at_least(value: object, threshold: float) -> bool:
    return isinstance(value, (int, float)) and value >= threshold


def check_p1_freeze(freeze_dir: str | Path = "docs/p1_freeze_snapshot") -> dict:
    """Validate a P1 freeze snapshot against the P1 gate requirements."""

    freeze_path = Path(freeze_dir)
    audit = _load_json(freeze_path / "p1_audit_summary.json")
    gate = _load_json(freeze_path / "p1_gate_decision.json")

    failures: list[str] = []
    p1_gate_passed = gate.get("p1_gate_passed") is True
    decision_ok = gate.get("decision") == "P1_PASS"
    label_ok = audit.get("label_leakage_passed") is True
    min_service = audit.get("multi_seed_min_service_hit_at_1")
    mean_metric_hit1 = audit.get("multi_seed_mean_metric_hit_at_1")
    min_metric_hit3 = audit.get("multi_seed_min_metric_hit_at_3")
    mean_metric_mrr = audit.get("multi_seed_mean_metric_mrr")
    observation_ok = audit.get("observation_audit_passed") is True
    audit_ok = audit.get("audit_passed") is True

    if not p1_gate_passed:
        failures.append("p1_gate_passed 不是 True。")
    if not decision_ok:
        failures.append("decision 不是 P1_PASS。")
    if not label_ok:
        failures.append("label_leakage_passed 不是 True。")
    if not _number_at_least(min_service, SERVICE_HIT_AT_1_THRESHOLD):
        failures.append("multi_seed_min_service_hit_at_1 低于 0.75。")
    if not _number_at_least(mean_metric_hit1, MEAN_METRIC_HIT_AT_1_THRESHOLD):
        failures.append("multi_seed_mean_metric_hit_at_1 低于 0.75。")
    if not _number_at_least(min_metric_hit3, METRIC_HIT_AT_3_THRESHOLD):
        failures.append("multi_seed_min_metric_hit_at_3 低于 0.75。")
    if not _number_at_least(mean_metric_mrr, MEAN_METRIC_MRR_THRESHOLD):
        failures.append("multi_seed_mean_metric_mrr 低于 0.80。")
    if not observation_ok:
        failures.append("observation_audit_passed 不是 True。")
    if not audit_ok:
        failures.append("audit_passed 不是 True。")

    return {
        "passed": not failures,
        "freeze_dir": str(freeze_path),
        "failures": failures,
        "p1_gate_passed": gate.get("p1_gate_passed"),
        "decision": gate.get("decision"),
        "label_leakage_passed": audit.get("label_leakage_passed"),
        "multi_seed_min_service_hit_at_1": min_service,
        "multi_seed_mean_metric_hit_at_1": mean_metric_hit1,
        "multi_seed_min_metric_hit_at_3": min_metric_hit3,
        "multi_seed_mean_metric_mrr": mean_metric_mrr,
        "observation_audit_passed": audit.get("observation_audit_passed"),
        "audit_passed": audit.get("audit_passed"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check probeRCA P1 freeze snapshot.")
    parser.add_argument("--freeze-dir", default="docs/p1_freeze_snapshot")
    args = parser.parse_args(argv)

    try:
        result = check_p1_freeze(args.freeze_dir)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"P1 freeze check failed：{exc}")
        return 1

    print("probeRCA P1 freeze 检查")
    print(f"freeze_dir：{result['freeze_dir']}")
    print(f"p1_gate_passed：{result['p1_gate_passed']}")
    print(f"decision：{result['decision']}")
    print(f"label_leakage_passed：{result['label_leakage_passed']}")
    print(f"multi_seed_min_service_hit_at_1：{result['multi_seed_min_service_hit_at_1']}")
    print(f"multi_seed_mean_metric_hit_at_1：{result['multi_seed_mean_metric_hit_at_1']}")
    print(f"multi_seed_min_metric_hit_at_3：{result['multi_seed_min_metric_hit_at_3']}")
    print(f"multi_seed_mean_metric_mrr：{result['multi_seed_mean_metric_mrr']}")
    print(f"observation_audit_passed：{result['observation_audit_passed']}")
    print(f"audit_passed：{result['audit_passed']}")

    if result["passed"]:
        print("P1 freeze check passed: ready for next phase.")
        print("中文解释：P1 冻结检查通过，可以进入下一阶段。")
        return 0

    print("P1 freeze check failed：")
    for failure in result["failures"]:
        print(f"- {failure}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

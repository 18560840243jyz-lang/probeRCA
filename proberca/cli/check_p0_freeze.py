"""Check whether the P0 freeze snapshot is ready for P1."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SERVICE_HIT_AT_1_THRESHOLD = 0.75
METRIC_HIT_AT_1_THRESHOLD = 0.75


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"缺少冻结快照文件：{path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"冻结快照文件不是 JSON object：{path}")
    return data


def check_p0_freeze(freeze_dir: str | Path = "docs/p0_freeze_snapshot") -> dict:
    """Validate a P0 freeze snapshot against the G1 gate requirements."""

    freeze_path = Path(freeze_dir)
    audit = _load_json(freeze_path / "p0_audit_summary.json")
    g1 = _load_json(freeze_path / "g1_decision.json")

    failures: list[str] = []
    label_ok = audit.get("label_leakage_passed") is True
    g1_passed = g1.get("g1_passed") is True
    decision_ok = g1.get("decision") == "G1_PASS"
    min_service = audit.get("multi_seed_min_service_hit_at_1")
    min_metric = audit.get("multi_seed_min_metric_hit_at_1")

    if not g1_passed:
        failures.append("g1_passed 不是 True。")
    if not decision_ok:
        failures.append("decision 不是 G1_PASS。")
    if not label_ok:
        failures.append("label_leakage_passed 不是 True。")
    if not isinstance(min_service, (int, float)) or min_service < SERVICE_HIT_AT_1_THRESHOLD:
        failures.append("multi_seed_min_service_hit_at_1 低于 0.75。")
    if not isinstance(min_metric, (int, float)) or min_metric < METRIC_HIT_AT_1_THRESHOLD:
        failures.append("multi_seed_min_metric_hit_at_1 低于 0.75。")

    return {
        "passed": not failures,
        "freeze_dir": str(freeze_path),
        "failures": failures,
        "label_leakage_passed": audit.get("label_leakage_passed"),
        "multi_seed_min_service_hit_at_1": min_service,
        "multi_seed_min_metric_hit_at_1": min_metric,
        "g1_passed": g1.get("g1_passed"),
        "decision": g1.get("decision"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check probeRCA P0 freeze snapshot.")
    parser.add_argument("--freeze-dir", default="docs/p0_freeze_snapshot")
    args = parser.parse_args(argv)

    try:
        result = check_p0_freeze(args.freeze_dir)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"P0 freeze check failed：{exc}")
        return 1

    print("probeRCA P0 freeze 检查")
    print(f"freeze_dir：{result['freeze_dir']}")
    print(f"g1_passed：{result['g1_passed']}")
    print(f"decision：{result['decision']}")
    print(f"label_leakage_passed：{result['label_leakage_passed']}")
    print(f"multi_seed_min_service_hit_at_1：{result['multi_seed_min_service_hit_at_1']}")
    print(f"multi_seed_min_metric_hit_at_1：{result['multi_seed_min_metric_hit_at_1']}")

    if result["passed"]:
        print("P0 freeze check passed: ready for P1.")
        print("中文解释：P0 冻结检查通过，可以进入 P1。")
        return 0

    print("P0 freeze check failed：")
    for failure in result["failures"]:
        print(f"- {failure}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

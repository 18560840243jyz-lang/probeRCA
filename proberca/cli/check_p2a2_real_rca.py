"""Check P2A-2 single real CPU RCA outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def check_p2a2_real_rca(input_dir: str | Path) -> dict:
    input_path = Path(input_dir)
    required = ["p1_results.jsonl", "p1_evaluation_summary.json", "real_p1_rca_summary.json"]
    missing = [name for name in required if not (input_path / name).exists()]
    if missing:
        return {"passed": False, "failed_checks": [f"missing:{name}" for name in missing], "input_dir": str(input_path)}
    summary = json.loads((input_path / "real_p1_rca_summary.json").read_text(encoding="utf-8"))
    failed: list[str] = []
    if int(summary.get("incident_count", 0)) != 1:
        failed.append("incident_count != 1")
    if summary.get("root_service_metric_coverage_passed") is not True:
        failed.append("root_service_metric_coverage_passed != true")
    if summary.get("paymentservice_throttled_metric_present") is not True:
        failed.append("paymentservice_throttled_metric_present != true")
    metric_rank = summary.get("metric_rank_debug")
    metric_hit_at_3 = float(summary.get("metric_hit_at_3", 0.0))
    if not (metric_hit_at_3 >= 1.0 or (metric_rank is not None and int(metric_rank) <= 3)):
        failed.append("metric_hit_at_3 < 1.0 and metric_rank_debug > 3")
    if float(summary.get("service_hit_at_1", 0.0)) < 1.0:
        failed.append("service_hit_at_1 < 1.0")
    if float(summary.get("root_type_accuracy", 0.0)) < 1.0:
        failed.append("root_type_accuracy < 1.0")
    return {"passed": not failed, "failed_checks": failed, "input_dir": str(input_path), "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2A-2 real CPU RCA outputs.")
    parser.add_argument("--input", default="data/p2_online_boutique/cpu_paymentservice_001_p1rca")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2a2_real_rca(args.input)
    summary = result.get("summary", {})
    print("probeRCA P2A-2 real RCA 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    for key in ["incident_count", "service_hit_at_1", "metric_hit_at_3", "metric_rank_debug", "root_type_accuracy"]:
        if key in summary:
            print(f"{key}：{summary.get(key)}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

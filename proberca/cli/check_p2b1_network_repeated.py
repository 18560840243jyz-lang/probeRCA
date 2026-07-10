"""Check P2B-1 repeated real network Top3 outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_p2b1_network_repeated(input_dir: str | Path) -> dict[str, Any]:
    input_path = Path(input_dir)
    summary_path = input_path / "p2b1_network_repeat_summary.json"
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["missing:p2b1_network_repeat_summary.json"], "input_dir": str(input_path)}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    failed: list[str] = []
    checks = [
        (int(summary.get("repeats_completed", 0)) >= 5, "repeats_completed < 5"),
        (int(summary.get("repeats_successful_quality", 0)) >= 5, "repeats_successful_quality < 5"),
        (int(summary.get("repeats_successful_rca", 0)) >= 5, "repeats_successful_rca < 5"),
        (float(summary.get("service_hit_at_1_mean", 0.0)) >= 0.8, "service_hit_at_1_mean < 0.8"),
        (float(summary.get("metric_hit_at_3_mean", 0.0)) >= 0.8, "metric_hit_at_3_mean < 0.8"),
        (float(summary.get("root_type_accuracy_mean", 0.0)) >= 0.8, "root_type_accuracy_mean < 0.8"),
        (float(summary.get("path_fidelity_mean", 0.0)) >= 0.8, "path_fidelity_mean < 0.8"),
    ]
    for ok, reason in checks:
        if not ok:
            failed.append(reason)
    return {"passed": not failed, "failed_checks": failed, "input_dir": str(input_path), "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2B-1 repeated real network output.")
    parser.add_argument("--input", default="data/p2_online_boutique/network_shippingservice_repeated")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2b1_network_repeated(args.input)
    print("probeRCA P2B-1 network repeated real injection Top3 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    if result["passed"]:
        print("P2B-1 network repeated real injection Top3 check passed.")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

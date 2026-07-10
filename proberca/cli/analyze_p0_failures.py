"""CLI for analyzing P0 audit failures."""

from __future__ import annotations

import argparse
import sys

from proberca.eval.p0_failure_analysis import analyze_p0_failures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze P0 full-audit failures.")
    parser.add_argument("--audit-dir", default="data/p0_single_vm/audit_full")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = analyze_p0_failures(args.audit_dir)
    except FileNotFoundError as exc:
        print(f"P0 失败分析失败：{exc}", file=sys.stderr)
        return 1
    print("probeRCA P0 失败分析完成")
    print(f"failed_seeds：{result['failed_seeds']}")
    print(f"failed_incidents：{result['failed_incidents']}")
    print(f"per_seed_metric_hit_at_1：{result['per_seed_metric_hit_at_1']}")
    print(f"failure_patterns：{result['failure_patterns']}")
    for item in result["per_incident_failures"]:
        print(
            f"- seed={item['seed']}, incident_id={item['incident_id']}, "
            f"root_metric={item['root_metric']}, predicted_top1_metric={item['predicted_top1_metric']}, "
            f"patterns={item['failure_patterns']}, top5_metrics={item['top5_metrics']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""CLI for P1 full or quick audit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.eval.p1_audit import run_p1_audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P1 audit.")
    parser.add_argument("--output", default="data/p1_single_vm/audit_full")
    parser.add_argument("--quick", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_p1_audit(Path(args.output), quick=args.quick)
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1 audit 运行失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    print("probeRCA P1 audit 完成")
    print(f"output：{args.output}")
    print(f"quick：{args.quick}")
    print(f"label_leakage_passed：{summary['label_leakage_passed']}")
    print(f"multi_seed_mean_service_hit_at_1：{summary['multi_seed_mean_service_hit_at_1']}")
    print(f"multi_seed_min_service_hit_at_1：{summary['multi_seed_min_service_hit_at_1']}")
    print(f"multi_seed_mean_metric_hit_at_1：{summary['multi_seed_mean_metric_hit_at_1']}")
    print(f"multi_seed_min_metric_hit_at_1：{summary['multi_seed_min_metric_hit_at_1']}")
    print(f"multi_seed_mean_metric_hit_at_3：{summary['multi_seed_mean_metric_hit_at_3']}")
    print(f"multi_seed_min_metric_hit_at_3：{summary['multi_seed_min_metric_hit_at_3']}")
    print(f"multi_seed_mean_metric_mrr：{summary['multi_seed_mean_metric_mrr']}")
    print(f"observed_ratio_mean：{summary['observed_ratio_mean']}")
    print(f"observation_audit_passed：{summary['observation_audit_passed']}")
    print(f"audit_passed：{summary['audit_passed']}")
    print("注意：当前是 P1 audit，不实现 adaptive sampling bandit 或 optional drift。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

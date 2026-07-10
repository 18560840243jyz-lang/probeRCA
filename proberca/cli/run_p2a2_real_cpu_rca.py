"""CLI for P2A-2 real CPU injection data to P1 RCA pipeline."""

from __future__ import annotations

import argparse
import sys

from proberca.adapters.online_boutique.p2a2_real_rca import run_p2a2_real_cpu_rca


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2A-2 real CPU injection data through P1 RCA pipeline.")
    parser.add_argument("--input", default="data/p2_online_boutique/cpu_paymentservice_001_cadvisor")
    parser.add_argument("--output", default="data/p2_online_boutique/cpu_paymentservice_001_p1rca")
    parser.add_argument("--top-k", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_p2a2_real_cpu_rca(input_dir=args.input, output_dir=args.output, top_k=args.top_k)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"P2A-2 real CPU RCA 运行失败：{exc}", file=sys.stderr)
        return 1
    summary = result["summary"]
    print("probeRCA P2A-2 real CPU injection data to P1 RCA pipeline 完成")
    for key in [
        "input_dir",
        "output_dir",
        "incident_count",
        "service_hit_at_1",
        "metric_hit_at_1",
        "metric_hit_at_3",
        "metric_mrr",
        "root_type_accuracy",
        "path_fidelity",
        "predicted_top1_service",
        "predicted_top1_metric",
        "predicted_root_type",
        "metric_rank_debug",
        "root_service_metric_coverage_passed",
        "paymentservice_throttled_metric_present",
        "observed_ratio",
    ]:
        print(f"{key}：{summary.get(key)}")
    print("注意：当前是 P2A-2 real CPU injection data to P1 RCA pipeline，只评估单个真实 CPU 注入事件，不代表多故障总体准确率。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

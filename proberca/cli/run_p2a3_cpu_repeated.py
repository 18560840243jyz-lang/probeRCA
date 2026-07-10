"""CLI for P2A-3 repeated real CPU fault injection experiments."""

from __future__ import annotations

import argparse
import sys

from proberca.adapters.online_boutique.p2a3_cpu_repeat import run_p2a3_cpu_repeated_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2A-3 repeated real CPU fault injection experiments.")
    parser.add_argument("--config", default="configs/p2a3_online_boutique_cpu_repeated.yaml")
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--sleep-between-repeats-sec", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_p2a3_cpu_repeated_experiment(args.config, repeats=args.repeats, sleep_between_repeats_sec=args.sleep_between_repeats_sec)
    except Exception as exc:  # noqa: BLE001 - real experiment CLI should report clear failure.
        print(f"P2A-3 repeated CPU 运行失败：{exc}", file=sys.stderr)
        return 1
    summary = result["summary"]
    print("probeRCA P2A-3 repeated real CPU fault injection 完成")
    for key in [
        "repeats_requested",
        "repeats_completed",
        "repeats_successful_quality",
        "repeats_successful_rca",
        "service_hit_at_1_mean",
        "metric_hit_at_1_mean",
        "metric_hit_at_3_mean",
        "metric_mrr_mean",
        "root_type_accuracy_mean",
        "path_fidelity_mean",
    ]:
        print(f"{key}：{summary.get(key)}")
    for row in summary.get("per_repeat", []):
        print(f"repeat_{int(row['repeat_index']):02d}_predicted_top1_metric：{row.get('predicted_top1_metric')}")
    print("注意：当前是 P2A-3 repeated real CPU fault injection，只代表 CPU 故障重复实验，不代表多故障总体准确率。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

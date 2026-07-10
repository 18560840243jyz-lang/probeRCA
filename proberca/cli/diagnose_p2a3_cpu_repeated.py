"""CLI for P2A-3 repeated CPU failure diagnosis."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2a3_failure_diagnosis import diagnose_p2a3_cpu_repeat_failures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose P2A-3 repeated real CPU failures.")
    parser.add_argument("--input", default="data/p2_online_boutique/cpu_paymentservice_repeated")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = diagnose_p2a3_cpu_repeat_failures(args.input)
    print("probeRCA P2A-3 真实 CPU 重复实验失败诊断")
    print(f"input_dir：{result['input_dir']}")
    print(f"failed_repeats：{result['failed_repeats']}")
    print(f"failure_patterns：{result['failure_patterns']}")
    for row in result.get("per_repeat_top5", []):
        print(f"repeat_{int(row['repeat_index']):02d}_top5：{row.get('top5_metrics', [])}")
    for row in result.get("per_repeat_metric_lift", []):
        target = row.get("paymentservice_cpu_throttled_usec", {})
        currency = row.get("currencyservice_cpu_throttled_usec", {})
        print(f"repeat_{int(row['repeat_index']):02d}_paymentservice_throttling_lift：{target.get('lift')}")
        print(f"repeat_{int(row['repeat_index']):02d}_currencyservice_throttling_lift：{currency.get('lift')}")
    print(f"recommendation：{result.get('recommendation', [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

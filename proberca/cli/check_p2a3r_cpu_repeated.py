"""Check P2A-3R controlled repeated real CPU experiment outputs."""

from __future__ import annotations

import argparse

from proberca.cli.check_p2a3_cpu_repeated import check_p2a3_cpu_repeated


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2A-3R controlled repeated real CPU output.")
    parser.add_argument("--input", default="data/p2_online_boutique/cpu_paymentservice_repeated_controlled")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2a3_cpu_repeated(args.input)
    print("probeRCA P2A-3R controlled CPU repeated real injection 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    if result["passed"]:
        print("P2A-3R controlled CPU repeated real injection check passed.")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

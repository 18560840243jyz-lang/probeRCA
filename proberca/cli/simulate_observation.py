"""CLI for probeRCA P1A adaptive observation simulation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simulate P1A adaptive observation masks.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--normal-sampling-rate", type=float, default=0.10)
    parser.add_argument("--soft-alert-sampling-rate", type=float, default=0.40)
    parser.add_argument("--hard-alert-sampling-rate", type=float, default=1.00)
    parser.add_argument("--min-sampling-probability", type=float, default=0.05)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = simulate_adaptive_observation(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            config=ObservationPolicyConfig(
                seed=args.seed,
                normal_sampling_rate=args.normal_sampling_rate,
                soft_alert_sampling_rate=args.soft_alert_sampling_rate,
                hard_alert_sampling_rate=args.hard_alert_sampling_rate,
                min_sampling_probability=args.min_sampling_probability,
            ),
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"自适应观测模拟失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P1A 自适应观测模拟完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"total_records：{metadata['total_records']}")
    print(f"observed_records：{metadata['observed_records']}")
    print(f"observed_ratio：{metadata['observed_ratio']:.6f}")
    print(f"always_on_count：{metadata['always_on_count']}")
    print(f"fine_metric_count：{metadata['fine_metric_count']}")
    print(f"normal_sampled_count：{metadata['normal_sampled_count']}")
    print(f"soft_alert_burst_count：{metadata['soft_alert_burst_count']}")
    print(f"hard_alert_burst_count：{metadata['hard_alert_burst_count']}")
    print("注意：当前是 P1A adaptive observation simulator，不包含 IPW-masked RLS。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

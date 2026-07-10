"""Run the probeRCA P1B IPW-masked propagation pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.propagation.ipw import IPWPropagationConfig, run_p1b_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P1B IPW-masked propagation pipeline.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    parser.add_argument("--ridge-lambda", type=float, default=1.0)
    parser.add_argument("--min-sampling-probability", type=float, default=0.05)
    parser.add_argument("--max-ipw-weight", type=float, default=20.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = IPWPropagationConfig(
        ridge_lambda=args.ridge_lambda,
        min_sampling_probability=args.min_sampling_probability,
        max_ipw_weight=args.max_ipw_weight,
    )
    try:
        result = run_p1b_pipeline(
            output_dir=Path(args.output),
            seed=args.seed,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1B IPW-masked propagation pipeline 失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P1B IPW-masked propagation pipeline 完成")
    print(f"output_dir：{args.output}")
    print(f"incidents_count：{metadata['incidents_count']}")
    print(f"coefficients_count：{metadata['coefficients_count']}")
    print(f"residuals_count：{metadata['residuals_count']}")
    print(f"mean_sampling_probability：{metadata['mean_sampling_probability']:.6f}")
    print(f"mean_ipw_weight：{metadata['mean_ipw_weight']:.6f}")
    print("注意：当前只运行 generate_dataset、normalize_dataset、simulate_adaptive_observation、train_ipw_masked_propagation；不包含 sparse inversion、semantic evidence、path explanation 或 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

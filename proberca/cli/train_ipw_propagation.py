"""CLI for P1B IPW-masked stable propagation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train P1B IPW-masked stable propagation.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
    parser.add_argument("--ridge-lambda", type=float, default=1.0)
    parser.add_argument("--min-sampling-probability", type=float, default=0.05)
    parser.add_argument("--max-ipw-weight", type=float, default=20.0)
    parser.add_argument("--no-ipw", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = IPWPropagationConfig(
        ridge_lambda=args.ridge_lambda,
        min_sampling_probability=args.min_sampling_probability,
        max_ipw_weight=args.max_ipw_weight,
        use_ipw=not args.no_ipw,
        use_parent_ipw=not args.no_ipw,
        use_target_ipw=not args.no_ipw,
    )
    try:
        result = train_ipw_masked_propagation(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"IPW-masked 稳定传播训练失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P1B IPW-masked stable propagation 完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"coefficients 数量：{metadata['coefficients_count']}")
    print(f"residuals 数量：{metadata['residuals_count']}")
    print(f"mean_sampling_probability：{metadata['mean_sampling_probability']:.6f}")
    print(f"mean_ipw_weight：{metadata['mean_ipw_weight']:.6f}")
    print(f"use_ipw：{metadata['use_ipw']}")
    print("incident 摘要：")
    for summary in result["summaries"]:
        print(
            "- "
            f"incident_id={summary['incident_id']}, "
            f"baseline_rmse={summary['baseline_rmse']:.6f}, "
            f"faulty_rmse={summary['faulty_rmse']:.6f}, "
            f"residual_count={summary['residual_count']}, "
            f"observed_cells={summary['observed_cells']}"
        )
    print("注意：当前是 P1B IPW-masked stable propagation，不包含 sparse inversion 或最终 RCA。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

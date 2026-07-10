"""CLI for training probeRCA P0 stable propagation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.propagation.stable import PropagationConfig, train_stable_propagation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train probeRCA P0 stable propagation learner.")
    parser.add_argument("--input", default="data/p0_single_vm/demo", help="Input dataset directory.")
    parser.add_argument("--output", default=None, help="Output directory. Defaults to input directory.")
    parser.add_argument("--ridge-lambda", type=float, default=1.0, help="Ridge regression regularization coefficient.")
    parser.add_argument("--coefficient-threshold", type=float, default=1e-10, help="Minimum absolute coefficient to write.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output is not None else input_dir
    config = PropagationConfig(
        ridge_lambda=args.ridge_lambda,
        coefficient_threshold=args.coefficient_threshold,
    )

    try:
        result = train_stable_propagation(input_dir=input_dir, output_dir=output_dir, config=config)
    except (FileNotFoundError, ValueError) as exc:
        print(f"稳定传播学习失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P0 稳定传播学习完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"coefficients 数量：{metadata['coefficients_count']}")
    print(f"residuals 数量：{metadata['residuals_count']}")
    print(f"expected residuals 数量：{metadata['expected_residuals_count']}")
    print(f"residuals_count_matches_expected：{metadata['residuals_count_matches_expected']}")
    print(f"ridge_lambda：{metadata['ridge_lambda']}")
    print(f"coefficient_threshold：{metadata['coefficient_threshold']}")
    print("incident RMSE 摘要：")
    for summary in result["summaries"]:
        print(
            "- "
            f"incident_id={summary['incident_id']}, "
            f"timestamp_count={summary['timestamp_count']}, "
            f"node_count={summary['node_count']}, "
            f"train_pairs={summary['train_pairs']}, "
            f"residual_count={summary['residual_count']}, "
            f"baseline_rmse={summary['baseline_rmse']:.6f}, "
            f"faulty_rmse={summary['faulty_rmse']:.6f}"
        )
    if not metadata["residuals_count_matches_expected"]:
        print("稳定传播学习失败：residuals_count 与 expected_residuals_count 不一致", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""CLI for P1C sparse inversion on IPW residuals."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Solve P1C sparse inversion on IPW residuals.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
    parser.add_argument("--l1-lambda", type=float, default=0.5)
    parser.add_argument("--no-ipw-weighted-mean", action="store_true")
    parser.add_argument("--min-baseline-observations", type=int, default=2)
    parser.add_argument("--min-faulty-observations", type=int, default=2)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = IPWSparseInversionConfig(
        l1_lambda=args.l1_lambda,
        use_ipw_weighted_mean=not args.no_ipw_weighted_mean,
        min_baseline_observations=args.min_baseline_observations,
        min_faulty_observations=args.min_faulty_observations,
    )
    try:
        result = solve_ipw_sparse_inversion(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1C IPW 残差稀疏反演失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    summary = result["summary"]
    print("probeRCA P1C IPW residual sparse inversion 完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{summary['incidents_count']}")
    print(f"candidates 数量：{summary['candidates_count']}")
    print(f"nonzero candidates 数量：{summary['nonzero_candidates_count']}")
    print(f"mean_true_root_rank_debug：{summary['mean_true_root_rank_debug']}")
    print("incident 摘要：")
    for item in summary["per_incident"]:
        print(
            "- "
            f"incident_id={item['incident_id']}, "
            f"top_candidate={item['top_candidate']}, "
            f"top_score={item['top_score']:.6f}, "
            f"true_root_rank_debug={item['true_root_rank_debug']}, "
            f"low_confidence_candidates_count={item['low_confidence_candidates_count']}"
        )
    print("注意：当前是 P1C IPW residual sparse inversion，不包含 semantic evidence、path explanation 或最终 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""CLI for building final P1 RCAResult records."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.eval.p1_result import build_p1_results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build final P1 RCAResult records.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
    parser.add_argument("--top-k", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = build_p1_results(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            top_k=args.top_k,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1 结果构建失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P1 RCAResult 构建完成")
    print(f"input_dir：{metadata['input_dir']}")
    print(f"output_dir：{metadata['output_dir']}")
    print(f"results_count：{metadata['results_count']}")
    print(f"observed_ratio：{metadata['observed_ratio']}")
    print(f"mean_sampling_probability：{metadata['mean_sampling_probability']}")
    print(f"mean_ipw_weight：{metadata['mean_ipw_weight']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

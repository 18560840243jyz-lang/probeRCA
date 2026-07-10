"""CLI for robust normalization of probeRCA metric data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.features.robust import normalize_dataset


def _clip_value(raw: str) -> float | None:
    if raw.lower() in {"none", "null"}:
        return None
    return float(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize probeRCA metric records with robust statistics.")
    parser.add_argument("--input", default="data/p0_single_vm/demo", help="Input dataset directory.")
    parser.add_argument("--output", default=None, help="Output directory. Defaults to input directory.")
    parser.add_argument("--eps", type=float, default=1e-6, help="Small positive value added to the MAD scale.")
    parser.add_argument("--clip", type=_clip_value, default=20.0, help="Optional z_value clipping threshold, or none.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output is not None else input_dir

    try:
        result = normalize_dataset(input_dir=input_dir, output_dir=output_dir, eps=args.eps, clip=args.clip)
    except (FileNotFoundError, ValueError) as exc:
        print(f"归一化失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P0 鲁棒归一化完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"normalized_metrics 数量：{metadata['normalized_count']}")
    print(f"robust_stats 数量：{metadata['stats_count']}")
    print(f"eps：{metadata['eps']}")
    print(f"clip：{metadata['clip']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

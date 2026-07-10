"""CLI for running P1E pipeline through IPW semantic path explanation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.explain.ipw_path import IPWPathExplanationConfig, run_p1e_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P1E path explanation pipeline.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    parser.add_argument("--top-k-candidates", type=int, default=5)
    parser.add_argument("--max-path-length", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = IPWPathExplanationConfig(
        top_k_candidates=args.top_k_candidates,
        max_path_length=args.max_path_length,
    )
    try:
        result = run_p1e_pipeline(
            output_dir=Path(args.output),
            seed=args.seed,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1E pipeline 失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    print("probeRCA P1E path explanation pipeline 完成")
    print(f"output_dir：{args.output}")
    print(f"incidents_count：{summary['incidents_count']}")
    print(f"path_records_count：{summary['path_records_count']}")
    print(f"candidates_explained_count：{summary['candidates_explained_count']}")
    print(f"paths_missing_count：{summary['paths_missing_count']}")
    print(f"path_fidelity_debug：{summary['path_fidelity_debug']}")
    print("注意：当前只运行到 P1E path explanation；不包含最终 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run the probeRCA P1C sparse inversion pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.inference.ipw_sparse import IPWSparseInversionConfig, run_p1c_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P1C IPW residual sparse inversion pipeline.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    parser.add_argument("--l1-lambda", type=float, default=0.5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_p1c_pipeline(
            output_dir=Path(args.output),
            seed=args.seed,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
            config=IPWSparseInversionConfig(l1_lambda=args.l1_lambda),
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1C sparse inversion pipeline 失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    print("probeRCA P1C sparse inversion pipeline 完成")
    print(f"output_dir：{args.output}")
    print(f"incidents_count：{summary['incidents_count']}")
    print(f"candidates_count：{summary['candidates_count']}")
    print(f"nonzero_candidates_count：{summary['nonzero_candidates_count']}")
    print(f"mean_true_root_rank_debug：{summary['mean_true_root_rank_debug']}")
    print("注意：当前只运行 generate_dataset、normalize_dataset、simulate_adaptive_observation、train_ipw_masked_propagation、solve_ipw_sparse_inversion；不包含 semantic evidence、path explanation 或 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run the probeRCA P1D semantic evidence pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, run_p1d_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P1D semantic evidence pipeline.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    parser.add_argument("--evidence-weight", type=float, default=3.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_p1d_pipeline(
            output_dir=Path(args.output),
            seed=args.seed,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
            config=IPWSemanticEvidenceConfig(evidence_weight=args.evidence_weight),
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1D semantic evidence pipeline 失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    print("probeRCA P1D semantic evidence pipeline 完成")
    print(f"output_dir：{args.output}")
    print(f"incidents_count：{summary['incidents_count']}")
    print(f"candidates_count：{summary['candidates_count']}")
    print(f"type_scores_count：{summary['type_scores_count']}")
    print(f"mean_true_root_semantic_rank_debug：{summary['mean_true_root_semantic_rank_debug']}")
    print("注意：当前只运行到 score_ipw_semantic_evidence；不包含 path explanation 或 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

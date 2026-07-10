"""CLI for probeRCA P0 sparse inversion."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.inference.sparse import SparseInversionConfig, solve_sparse_inversion


def _clip_value(raw: str) -> float | None:
    if raw.lower() in {"none", "null"}:
        return None
    return float(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Solve probeRCA P0 sparse intervention candidates.")
    parser.add_argument("--input", default="data/p0_single_vm/demo", help="Input dataset directory.")
    parser.add_argument("--output", default=None, help="Output directory. Defaults to input directory.")
    parser.add_argument("--l1-lambda", type=float, default=0.5, help="L1 sparse threshold.")
    parser.add_argument("--group-lambda", type=float, default=0.1, help="Service group shrinkage strength.")
    parser.add_argument("--graph-lambda", type=float, default=0.0, help="Graph smoothness strength, disabled by default in P0.")
    parser.add_argument("--clip-score", type=_clip_value, default=100.0, help="Optional score clipping threshold, or none.")
    parser.add_argument("--top-k-debug", type=int, default=5, help="Number of debug candidates to print per incident.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output is not None else input_dir
    config = SparseInversionConfig(
        l1_lambda=args.l1_lambda,
        group_lambda=args.group_lambda,
        graph_lambda=args.graph_lambda,
        clip_score=args.clip_score,
        top_k_debug=args.top_k_debug,
    )

    try:
        result = solve_sparse_inversion(input_dir=input_dir, output_dir=output_dir, config=config)
    except (FileNotFoundError, ValueError) as exc:
        print(f"稀疏反演失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P0 稀疏反演完成")
    print("注意：当前输出是 Step 5 intervention candidates，不是最终 RCA 结果。")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"candidates 数量：{metadata['candidates_count']}")
    print(f"expected candidates 数量：{metadata['expected_candidates_count']}")
    print(f"candidates_count_matches_expected：{metadata['candidates_count_matches_expected']}")
    print(f"nonzero candidates 数量：{metadata['nonzero_candidates_count']}")
    print(f"l1_lambda：{metadata['l1_lambda']}")
    print(f"group_lambda：{metadata['group_lambda']}")
    print(f"graph_lambda：{metadata['graph_lambda']}")
    print("incident sparse intervention debug 摘要：")
    for summary in result["summaries"]:
        print(
            "- "
            f"incident_id={summary['incident_id']}, "
            f"true_root_rank={summary['true_root_rank']}, "
            f"true_root_score={summary['true_root_score']}"
        )
        for candidate in summary["top_debug_candidates"]:
            print(
                "  * "
                f"rank={candidate['rank']}, "
                f"node={candidate['node']}, "
                f"intervention_score={candidate['intervention_score']}, "
                f"signed_intervention={candidate['signed_intervention']}"
            )

    if not metadata["candidates_count_matches_expected"]:
        print("稀疏反演失败：candidates_count 与 expected_candidates_count 不一致", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

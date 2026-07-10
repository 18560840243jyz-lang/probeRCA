"""CLI for P1D semantic evidence on IPW sparse candidates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score P1D IPW semantic evidence.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
    parser.add_argument("--evidence-weight", type=float, default=3.0)
    parser.add_argument("--disable-specificity", action="store_true")
    parser.add_argument("--disable-semantic-anchor", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = IPWSemanticEvidenceConfig(
        evidence_weight=args.evidence_weight,
        specificity_weight_enabled=not args.disable_specificity,
        semantic_anchor_enabled=not args.disable_semantic_anchor,
    )
    try:
        result = score_ipw_semantic_evidence(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1D IPW 语义证据打分失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    summary = result["summary"]
    print("probeRCA P1D IPW semantic evidence 完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{summary['incidents_count']}")
    print(f"candidates 数量：{summary['candidates_count']}")
    print(f"type scores 数量：{summary['type_scores_count']}")
    print(f"mean_true_root_semantic_rank_debug：{summary['mean_true_root_semantic_rank_debug']}")
    print(f"metric_hit_at_1_debug：{summary.get('metric_hit_at_1_debug')}")
    print(f"metric_hit_at_3_debug：{summary.get('metric_hit_at_3_debug')}")
    print("incident 摘要：")
    for item in summary["per_incident"]:
        print(
            "- "
            f"incident_id={item['incident_id']}, "
            f"top_candidate={item['top_candidate']}, "
            f"top_type_candidate={item['top_type_candidate']}, "
            f"true_root_sparse_rank_debug={item['true_root_sparse_rank_debug']}, "
            f"true_root_semantic_rank_debug={item['true_root_semantic_rank_debug']}"
        )
    print("注意：当前是 P1D IPW semantic evidence，不包含 path explanation 或最终 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

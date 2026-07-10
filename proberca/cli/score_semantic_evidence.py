"""CLI for probeRCA P0 semantic evidence scoring."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.evidence.semantic import SemanticEvidenceConfig, score_semantic_evidence


def _clip_value(raw: str) -> float | None:
    if raw.lower() in {"none", "null"}:
        return None
    return float(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score probeRCA P0 semantic evidence candidates.")
    parser.add_argument("--input", default="data/p0_single_vm/demo", help="Input dataset directory.")
    parser.add_argument("--output", default=None, help="Output directory. Defaults to input directory.")
    parser.add_argument("--evidence-weight", type=float, default=3.0, help="Evidence fusion weight.")
    parser.add_argument("--exact-metric-bonus", type=float, default=2.0, help="Exact metric evidence bonus.")
    parser.add_argument("--same-type-bonus", type=float, default=1.0, help="Same type evidence bonus.")
    parser.add_argument("--service-level-bonus", type=float, default=0.5, help="Same service evidence bonus.")
    parser.add_argument("--clip-semantic-score", type=_clip_value, default=500.0, help="Optional semantic score clipping threshold, or none.")
    parser.add_argument("--top-k-debug", type=int, default=5, help="Debug candidates to print per incident.")
    parser.add_argument("--disable-metric-specificity", action="store_true", help="Disable label-free metric specificity prior.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output is not None else input_dir
    config = SemanticEvidenceConfig(
        evidence_weight=args.evidence_weight,
        exact_metric_bonus=args.exact_metric_bonus,
        same_type_bonus=args.same_type_bonus,
        service_level_bonus=args.service_level_bonus,
        clip_semantic_score=args.clip_semantic_score,
        top_k_debug=args.top_k_debug,
        use_metric_specificity=not args.disable_metric_specificity,
    )

    try:
        result = score_semantic_evidence(input_dir=input_dir, output_dir=output_dir, config=config)
    except (FileNotFoundError, ValueError) as exc:
        print(f"语义证据打分失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P0 语义证据打分完成")
    print("注意：当前输出是 Step 6 semantic candidates，不是最终 RCAResult。")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"semantic records 数量：{metadata['semantic_records_count']}")
    print(f"type scores 数量：{metadata['type_scores_count']}")
    print(f"expected candidates 数量：{metadata['expected_candidates_count']}")
    print(f"candidates_count_matches_expected：{metadata['candidates_count_matches_expected']}")
    print("incident semantic evidence debug 摘要：")
    for summary in result["summaries"]:
        print(
            "- "
            f"incident_id={summary['incident_id']}, "
            f"true_root_sparse_rank={summary['true_root_sparse_rank']}, "
            f"true_root_semantic_rank={summary['true_root_semantic_rank']}, "
            f"true_root_sparse_score={summary['true_root_sparse_score']}, "
            f"true_root_semantic_score={summary['true_root_semantic_score']}"
        )
        print("  top_debug_candidates:")
        for candidate in summary["top_debug_candidates"]:
            print(
                "  * "
                f"semantic_rank={candidate['semantic_rank']}, "
                f"node={candidate['node']}, "
                f"semantic_score={candidate['semantic_score']}, "
                f"sparse_rank={candidate['sparse_rank']}, "
                f"evidence_type={candidate['evidence_type']}, "
                f"evidence_score={candidate['evidence_score']}"
            )
        print("  top_type_candidates:")
        for type_candidate in summary["top_type_candidates"]:
            print(
                "  * "
                f"rank={type_candidate['rank']}, "
                f"root_type_candidate={type_candidate['root_type_candidate']}, "
                f"type_score={type_candidate['type_score']}"
            )

    if not metadata["candidates_count_matches_expected"]:
        print("语义证据打分失败：candidates_count 与 expected_candidates_count 不一致", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

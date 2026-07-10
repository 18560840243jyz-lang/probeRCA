"""CLI for probeRCA P0 Step 7 path explanation."""

from __future__ import annotations

import argparse
import sys

from proberca.explain.path import PathExplanationConfig, explain_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P0 path explanation.")
    parser.add_argument("--input", default="data/p0_single_vm/demo", help="Input dataset directory.")
    parser.add_argument("--output", default=None, help="Output directory. Defaults to input directory.")
    parser.add_argument("--top-k-candidates-per-incident", type=int, default=5)
    parser.add_argument("--max-path-length", type=int, default=5)
    parser.add_argument("--top-k-paths-per-candidate", type=int, default=3)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = PathExplanationConfig(
        top_k_candidates_per_incident=args.top_k_candidates_per_incident,
        max_path_length=args.max_path_length,
        top_k_paths_per_candidate=args.top_k_paths_per_candidate,
    )
    try:
        result = explain_paths(args.input, args.output, config)
    except (FileNotFoundError, ValueError) as exc:
        print(f"路径解释失败：{exc}", file=sys.stderr)
        return 1

    metadata = result["metadata"]
    print("probeRCA P0 路径解释完成")
    print("注意：当前输出是 Step 7 path explanations，不是最终 RCAResult。")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"path records 数量：{metadata['path_records_count']}")
    print(f"candidates explained 数量：{metadata['candidates_explained_count']}")
    print(f"paths missing 数量：{metadata['paths_missing_count']}")
    print("incident path explanation debug 摘要：")
    for summary in result["summaries"]:
        print(
            f"- incident_id={summary['incident_id']}, "
            f"top_path_candidate={summary['top_path_candidate']}, "
            f"top_path_score={summary['top_path_score']}, "
            f"path_fidelity_debug={summary['path_fidelity_debug']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

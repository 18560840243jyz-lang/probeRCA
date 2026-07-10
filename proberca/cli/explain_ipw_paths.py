"""CLI for P1E IPW semantic path explanation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.explain.ipw_path import IPWPathExplanationConfig, explain_ipw_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Explain paths for P1E IPW semantic candidates.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    parser.add_argument("--output", default=None)
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
        result = explain_ipw_paths(
            input_dir=Path(args.input),
            output_dir=Path(args.output) if args.output is not None else None,
            config=config,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1E IPW 路径解释失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    metadata = result["metadata"]
    print("probeRCA P1E IPW semantic path explanation 完成")
    print(f"输入目录：{metadata['input_dir']}")
    print(f"输出目录：{metadata['output_dir']}")
    print(f"path records 数量：{summary['path_records_count']}")
    print(f"candidates explained 数量：{summary['candidates_explained_count']}")
    print(f"paths missing 数量：{summary['paths_missing_count']}")
    print(f"path_fidelity_debug：{summary['path_fidelity_debug']}")
    print("incident 摘要：")
    for item in summary["per_incident"]:
        print(
            "- "
            f"incident_id={item['incident_id']}, "
            f"top_path_candidate={item['top_path_candidate']}, "
            f"top_path_score={item['top_path_score']}, "
            f"top_path_services={item['top_path_services']}, "
            f"top_path_missing={item['top_path_missing']}"
        )
    print("注意：当前是 P1E IPW semantic path explanation，不包含最终 RCAResult。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

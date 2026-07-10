"""CLI for diagnosing P1D semantic sibling metric errors."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.evidence.ipw_semantic_diagnosis import diagnose_ipw_semantic_sibling_errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose P1D IPW semantic sibling errors.")
    parser.add_argument("--input", default="data/p1_single_vm/demo")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = diagnose_ipw_semantic_sibling_errors(Path(args.input))
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1D sibling diagnosis 失败：{exc}", file=sys.stderr)
        return 1

    print("probeRCA P1D sibling diagnosis 完成")
    print(f"输入目录：{args.input}")
    print(f"输出文件：{result['output_path']}")
    print(f"failed_top1_incidents：{result['failed_top1_incidents']}")
    print(f"same_service_sibling_errors：{len(result['same_service_sibling_errors'])}")
    print(f"same_type_sibling_errors：{len(result['same_type_sibling_errors'])}")
    print("per_incident_top5：")
    for item in result["per_incident_top5"]:
        print(f"- incident_id={item['incident_id']}, top5_metrics={item['top5_metrics']}")
    print("注意：当前诊断只使用 synthetic debug labels，不参与 P1D 打分。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

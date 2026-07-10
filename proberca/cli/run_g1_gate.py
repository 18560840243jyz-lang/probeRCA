"""CLI for probeRCA G1 gate decision."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.eval.g1_gate import write_g1_decision


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA G1 gate from a P0 audit directory.")
    parser.add_argument("--audit-dir", default="data/p0_single_vm/audit_full", help="Directory containing p0_audit_summary.json.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    audit_dir = Path(args.audit_dir)
    try:
        result = write_g1_decision(str(audit_dir / "p0_audit_summary.json"), str(audit_dir))
    except FileNotFoundError as exc:
        print(f"G1 gate 失败：{exc}", file=sys.stderr)
        return 1

    decision = result["decision"]
    print("probeRCA G1 gate 决策完成")
    print(f"g1_passed：{decision['g1_passed']}")
    print(f"decision：{decision['decision']}")
    print(f"failed_checks：{decision['failed_checks']}")
    print(f"key_metrics：{decision['key_metrics']}")
    return 0 if decision["g1_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

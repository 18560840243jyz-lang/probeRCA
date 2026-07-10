"""CLI for P1 gate decision."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.eval.p1_gate import write_p1_gate_decision


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P1 gate.")
    parser.add_argument("--audit-dir", default="data/p1_single_vm/audit_full")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    audit_dir = Path(args.audit_dir)
    try:
        result = write_p1_gate_decision(audit_dir / "p1_audit_summary.json", audit_dir)
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1 gate 运行失败：{exc}", file=sys.stderr)
        return 1

    decision = result["decision"]
    print("probeRCA P1 gate 完成")
    print(f"audit_dir：{audit_dir}")
    print(f"p1_gate_passed：{decision['p1_gate_passed']}")
    print(f"decision：{decision['decision']}")
    print(f"failed_checks：{decision['failed_checks']}")
    print(f"key_metrics：{decision['key_metrics']}")
    return 0 if decision["p1_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

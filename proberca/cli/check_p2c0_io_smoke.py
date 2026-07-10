"""Check P2C-0 IO smoke outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_p2c0_io_smoke(input_dir: str | Path) -> dict[str, Any]:
    input_path = Path(input_dir)
    summary_path = input_path / "p2c0_io_smoke_summary.json"
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["missing:p2c0_io_smoke_summary.json"], "input_dir": str(input_path)}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    failed: list[str] = []
    checks = [
        (summary.get("io_fault_feasible") is True, "io_fault_feasible != true"),
        (summary.get("io_stress_started") is True, "io_stress_started != true"),
        (summary.get("io_stress_cleaned") is True, "io_stress_cleaned != true"),
        (summary.get("frontend_after_http_ok") is True, "frontend_after_http_ok != true"),
        (float(summary.get("write_bytes_delta_during", 0.0)) > 0.0 or float(summary.get("write_ops_delta_during", 0.0)) > 0.0, "write delta not positive"),
    ]
    for ok, reason in checks:
        if not ok:
            failed.append(reason)
    return {"passed": not failed, "failed_checks": failed, "input_dir": str(input_path), "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2C-0 IO smoke output.")
    parser.add_argument("--input", default="data/p2_online_boutique/io_rediscart_smoke_001")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2c0_io_smoke(args.input)
    print("probeRCA P2C-0 IO fault feasibility smoke 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    if result["passed"]:
        print("P2C-0 IO fault feasibility smoke passed.")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

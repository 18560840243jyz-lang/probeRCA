"""Check P2D-0 real lock contention smoke output."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_p2d0_lock_smoke(input_dir: str | Path) -> dict[str, Any]:
    input_path = Path(input_dir)
    summary_path = input_path / "p2d0_lock_smoke_summary.json"
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["missing:p2d0_lock_smoke_summary.json"], "input_dir": str(input_path)}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    failed: list[str] = []
    checks = [
        (summary.get("lock_fault_feasible") is True, "lock_fault_feasible != true"),
        (summary.get("sidecar_injected") is True, "sidecar_injected != true"),
        (summary.get("sidecar_removed") is True, "sidecar_removed != true"),
        (summary.get("frontend_after_http_ok") is True, "frontend_after_http_ok != true"),
        (summary.get("lock_metrics_available") is True, "lock_metrics_available != true"),
        (float(summary.get("lock_contention_count_total", 0.0)) > 0.0, "lock_contention_count_total <= 0"),
        (float(summary.get("lock_wait_ms_sum_total", 0.0)) > 0.0, "lock_wait_ms_sum_total <= 0"),
    ]
    for ok, reason in checks:
        if not ok:
            failed.append(reason)
    return {"passed": not failed, "failed_checks": failed, "input_dir": str(input_path), "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2D-0 lock smoke output.")
    parser.add_argument("--input", default="data/p2_online_boutique/lock_cartservice_smoke_001")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2d0_lock_smoke(args.input)
    print("probeRCA P2D-0 lock contention feasibility smoke 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    if result["passed"]:
        print("P2D-0 lock contention feasibility smoke passed.")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

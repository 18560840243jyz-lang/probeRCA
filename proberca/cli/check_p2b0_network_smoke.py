"""Check P2B-0 network smoke output."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def check_p2b0_network_smoke(input_dir: str | Path) -> dict:
    path = Path(input_dir)
    summary_path = path / "p2b0_network_smoke_summary.json"
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["missing:p2b0_network_smoke_summary.json"], "input_dir": str(path)}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    checks = [
        (summary.get("network_fault_feasible") is True, "network_fault_feasible != true"),
        (summary.get("netem_applied") is True, "netem_applied != true"),
        (summary.get("netem_restored") is True, "netem_restored != true"),
        (summary.get("frontend_after_http_ok") is True, "frontend_after_http_ok != true"),
    ]
    failed = [reason for ok, reason in checks if not ok]
    return {"passed": not failed, "failed_checks": failed, "input_dir": str(path), "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2B-0 network smoke output.")
    parser.add_argument("--input", default="data/p2_online_boutique/network_shippingservice_smoke_001")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = check_p2b0_network_smoke(args.input)
    print("probeRCA P2B-0 network fault feasibility smoke 检查")
    print(f"input_dir：{result['input_dir']}")
    print(f"passed：{result['passed']}")
    print(f"failed_checks：{result['failed_checks']}")
    if result["passed"]:
        print("P2B-0 network fault feasibility smoke passed.")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

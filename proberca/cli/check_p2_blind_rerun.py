"""Structural check for A2 blind P2 rerun outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def check_p2_blind_rerun(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_blind_rerun_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_blind_rerun_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if _num(summary.get("total_repeats")) is None or float(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if _num(summary.get("total_successful_rca")) is None or float(summary.get("total_successful_rca", 0)) < 20:
        failed.append("total_successful_rca < 20")
    for key in ["service_hit_at_1_overall", "metric_hit_at_3_overall", "root_type_accuracy_overall", "path_fidelity_overall"]:
        if key not in summary:
            failed.append(f"{key} missing")
    for index, row in enumerate(summary.get("per_repeat", []), start=1):
        if row.get("uses_blind_evidence") is not True:
            failed.append(f"repeat {index} uses_blind_evidence != true")
        if row.get("uses_legacy_evidence") is not False:
            failed.append(f"repeat {index} uses_legacy_evidence != false")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A2 blind P2 rerun structure.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_p2_blind_rerun(args.input)
    print("probeRCA A2 blind rerun 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("P2 blind rerun structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Check P2E real multi-fault summary acceptance."""

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


def check_p2e_multifault_summary(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2e_multifault_summary.json"
    acceptance_path = base / "p2e_multifault_acceptance.json"
    failed: list[str] = []
    if not summary_path.exists():
        failed.append("p2e_multifault_summary.json missing")
    if not acceptance_path.exists():
        failed.append("p2e_multifault_acceptance.json missing")
    if failed:
        return {"passed": False, "failed_checks": failed}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
    overall = summary.get("overall", {})
    checks = [
        ("total_repeats", 20),
        ("service_hit_at_1_overall", 0.8),
        ("metric_hit_at_3_overall", 0.8),
        ("root_type_accuracy_overall", 0.8),
        ("path_fidelity_overall", 0.8),
    ]
    for key, threshold in checks:
        value = _num(overall.get(key))
        if value is None or value < threshold:
            failed.append(f"{key} < {threshold}")
    if acceptance.get("p2e_passed") is not True:
        failed.append("p2e_passed != true")
    if acceptance.get("decision") != "P2E_REAL_MULTIFAULT_PASS":
        failed.append("decision != P2E_REAL_MULTIFAULT_PASS")
    return {
        "passed": not failed,
        "failed_checks": failed,
        "summary": summary,
        "acceptance": acceptance,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check P2E real multi-fault summary.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_p2e_multifault_summary(args.input)
    print("probeRCA P2E real multi-fault summary 检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("P2E real multi-fault summary check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

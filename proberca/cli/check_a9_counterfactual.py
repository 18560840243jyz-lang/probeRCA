"""Structural check for A9 counterfactual explanation preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a9_counterfactual(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_counterfactual_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_counterfactual_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_completed", 0)) < 20:
        failed.append("repeats_completed < 20")
    checks = {
        "uses_root_labels_for_counterfactual": False,
        "uses_target_config_for_counterfactual": False,
        "uses_injected_path_for_counterfactual": False,
        "uses_incident_start_end_for_counterfactual": False,
        "consumes_a8r_sparse_interventions": True,
        "reoptimizes_with_candidate_removed": True,
    }
    for key, expected in checks.items():
        if summary.get(key) is not expected:
            failed.append(f"{key} != {str(expected).lower()}")
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["counterfactual_metric_explanations.jsonl", "counterfactual_service_explanations.jsonl", "counterfactual_metric_ranking.jsonl", "counterfactual_service_ranking.jsonl", "counterfactual_metadata.json"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "counterfactual_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("uses_root_labels") is not False:
                failed.append(f"metadata uses_root_labels != false: {metadata_path}")
            if metadata.get("reoptimizes_with_candidate_removed") is not True:
                failed.append(f"metadata reoptimizes_with_candidate_removed != true: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A9 counterfactual explanation preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a9_counterfactual(args.input)
    print("probeRCA A9 counterfactual explanation 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A9 counterfactual explanation structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

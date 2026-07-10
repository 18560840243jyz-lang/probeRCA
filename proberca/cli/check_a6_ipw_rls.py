"""Structural check for A6 IPW-masked RLS preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a6_ipw_rls(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_ipw_rls_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_ipw_rls_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_completed", 0)) < 20:
        failed.append("repeats_completed < 20")
    checks = {
        "uses_root_labels_for_learning": False,
        "uses_target_config_for_learning": False,
        "uses_injected_path_for_learning": False,
        "uses_incident_start_end_for_learning": False,
        "consumes_sampling_probability": True,
        "consumes_observation_mask": True,
        "batch_ridge_used": False,
    }
    for key, expected in checks.items():
        if summary.get(key) is not expected:
            failed.append(f"{key} != {str(expected).lower()}")
    if summary.get("update_mode") != "online_rls":
        failed.append("update_mode != online_rls")
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["ipw_rls_state.json", "ipw_rls_edges.jsonl", "ipw_rls_residuals.jsonl", "ipw_rls_metadata.json"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "ipw_rls_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("consumes_sampling_probability") is not True:
                failed.append(f"metadata consumes_sampling_probability != true: {metadata_path}")
            if metadata.get("batch_ridge_used") is not False:
                failed.append(f"metadata batch_ridge_used != false: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A6 IPW-masked RLS preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a6_ipw_rls(args.input)
    print("probeRCA A6 IPW-masked RLS 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A6 IPW-masked RLS structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

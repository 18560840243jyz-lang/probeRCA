"""Structural check for A5 adaptive probe policy preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a5_probe_policy(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_probe_policy_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_probe_policy_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_with_probe_plan", 0)) < 20:
        failed.append("repeats_with_probe_plan < 20")
    for flag in ["uses_root_labels_for_policy", "uses_target_config_for_policy", "uses_injected_path_for_policy", "uses_incident_start_end_for_policy", "actual_probe_activation"]:
        expected = False
        if summary.get(flag) is not expected:
            failed.append(f"{flag} != false")
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["adaptive_probe_metadata.json", "probe_plan.jsonl", "sampling_log.jsonl", "observation_mask.jsonl"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "adaptive_probe_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("uses_root_labels") is not False:
                failed.append(f"metadata uses_root_labels != false: {metadata_path}")
            if metadata.get("actual_probe_activation") is not False:
                failed.append(f"metadata actual_probe_activation != false: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A5 adaptive probe policy preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a5_probe_policy(args.input)
    print("probeRCA A5 adaptive probe policy 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A5 adaptive probe policy structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

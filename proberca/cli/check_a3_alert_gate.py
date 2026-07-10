"""Structural check for A3 alert gate preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a3_alert_gate(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_alert_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_alert_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if summary.get("uses_root_labels_for_detection") is not False:
        failed.append("uses_root_labels_for_detection != false")
    if summary.get("uses_incident_start_end_for_detection") is not False:
        failed.append("uses_incident_start_end_for_detection != false")
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["alert_gate_metadata.json", "alert_events.jsonl", "alert_windows.jsonl"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "alert_gate_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("uses_root_labels") is not False:
                failed.append(f"metadata uses_root_labels != false: {metadata_path}")
            if metadata.get("uses_incident_start_end_for_detection") is not False:
                failed.append(f"metadata uses_incident_start_end_for_detection != false: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A3 alert gate preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a3_alert_gate(args.input)
    print("probeRCA A3 alert gate 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A3 alert gate structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

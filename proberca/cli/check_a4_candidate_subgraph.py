"""Structural check for A4 candidate subgraph preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a4_candidate_subgraph(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_candidate_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_candidate_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_with_candidate_graph", 0)) < 20:
        failed.append("repeats_with_candidate_graph < 20")
    for flag in [
        "uses_root_labels_for_building",
        "uses_target_config_for_building",
        "uses_injected_path_for_building",
        "uses_incident_start_end_for_building",
    ]:
        if summary.get(flag) is not False:
            failed.append(f"{flag} != false")

    for row in summary.get("per_repeat", []):
        repeat_out = Path(row["candidate_output_dir"])
        repeat_summary_path = repeat_out / "repeat_candidate_summary.json"
        if not repeat_summary_path.exists():
            failed.append(f"missing repeat_candidate_summary.json: {repeat_out}")
            continue
        repeat_summary = json.loads(repeat_summary_path.read_text(encoding="utf-8"))
        for window in repeat_summary.get("window_summaries", []):
            window_out = Path(window["window_output_dir"])
            for name in ["candidate_subgraph_metadata.json", "candidate_services.jsonl", "candidate_metric_nodes.jsonl", "candidate_edges.jsonl"]:
                if not (window_out / name).exists():
                    failed.append(f"missing {name}: {window_out}")
            metadata_path = window_out / "candidate_subgraph_metadata.json"
            if metadata_path.exists():
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                if metadata.get("uses_root_labels") is not False:
                    failed.append(f"metadata uses_root_labels != false: {metadata_path}")
                if metadata.get("uses_target_config") is not False:
                    failed.append(f"metadata uses_target_config != false: {metadata_path}")
                if metadata.get("uses_injected_path") is not False:
                    failed.append(f"metadata uses_injected_path != false: {metadata_path}")
                if metadata.get("uses_incident_start_end") is not False:
                    failed.append(f"metadata uses_incident_start_end != false: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A4 candidate subgraph preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a4_candidate_subgraph(args.input)
    print("probeRCA A4 candidate subgraph 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A4 candidate subgraph structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

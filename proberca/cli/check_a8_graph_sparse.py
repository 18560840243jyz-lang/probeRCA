"""Structural check for A8/A8R graph sparse inversion preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _ratio(summary: dict[str, Any]) -> float:
    nodes = float(summary.get("average_node_count") or 0.0)
    nonzero = float(summary.get("average_nonzero_intervention_count") or 0.0)
    return nonzero / nodes if nodes > 0 else 0.0


def check_a8_graph_sparse(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_graph_sparse_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_graph_sparse_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_completed", 0)) < 20:
        failed.append("repeats_completed < 20")
    checks = {
        "uses_root_labels_for_inversion": False,
        "uses_target_config_for_inversion": False,
        "uses_injected_path_for_inversion": False,
        "uses_incident_start_end_for_inversion": False,
        "consumes_calibrated_residuals": True,
        "consumes_raw_residuals": False,
    }
    for key, expected in checks.items():
        if summary.get(key) is not expected:
            failed.append(f"{key} != {str(expected).lower()}")
    if summary.get("optimization") != "admm_graph_sparse_inversion":
        failed.append("optimization != admm_graph_sparse_inversion")
    nonzero_ratio = _ratio(summary)
    if nonzero_ratio > 0.60:
        failed.append("nonzero ratio too high for sparse inversion")
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["sparse_interventions.jsonl", "metric_scores.jsonl", "service_scores.jsonl", "graph_sparse_objective_trace.jsonl", "graph_sparse_metadata.json"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "graph_sparse_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("consumes_calibrated_residuals") is not True:
                failed.append(f"metadata consumes_calibrated_residuals != true: {metadata_path}")
            if metadata.get("consumes_raw_residuals") is not False:
                failed.append(f"metadata consumes_raw_residuals != false: {metadata_path}")
            if metadata.get("post_sparsify_applied") is not True:
                failed.append(f"metadata post_sparsify_applied != true: {metadata_path}")
            if metadata.get("solver_status") == "failed_numeric":
                failed.append(f"solver_status failed_numeric: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary, "nonzero_ratio": nonzero_ratio}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A8/A8R graph sparse inversion preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a8_graph_sparse(args.input)
    print("probeRCA A8/A8R graph sparse inversion 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"nonzero_ratio：{result.get('nonzero_ratio')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A8 graph sparse inversion structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

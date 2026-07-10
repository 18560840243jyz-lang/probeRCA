"""Structural check for A7 evidence channel preview outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def check_a7_evidence_channel(input_dir: str) -> dict[str, Any]:
    base = Path(input_dir)
    summary_path = base / "p2_evidence_channel_preview_summary.json"
    failed: list[str] = []
    if not summary_path.exists():
        return {"passed": False, "failed_checks": ["p2_evidence_channel_preview_summary.json missing"]}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if int(summary.get("total_repeats", 0)) < 20:
        failed.append("total_repeats < 20")
    if int(summary.get("repeats_completed", 0)) < 20:
        failed.append("repeats_completed < 20")
    checks = {
        "uses_root_labels_for_channel": False,
        "uses_target_config_for_channel": False,
        "uses_injected_path_for_channel": False,
        "uses_incident_start_end_for_channel": False,
        "consumes_blind_evidence": True,
        "consumes_probe_policy": True,
        "consumes_ipw_rls_residuals": True,
        "produces_calibrated_residuals": True,
        "raw_residual_directly_used_for_sparse_inversion": False,
    }
    for key, expected in checks.items():
        if summary.get(key) is not expected:
            failed.append(f"{key} != {str(expected).lower()}")
    eps = 1e-6
    for row in summary.get("per_repeat", []):
        out = Path(row["output_dir"])
        for name in ["evidence_vectors.jsonl", "evidence_effects.jsonl", "calibrated_residuals.jsonl", "evidence_channel_metadata.json"]:
            if not (out / name).exists():
                failed.append(f"missing {name}: {out}")
        metadata_path = out / "evidence_channel_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("produces_calibrated_residuals") is not True:
                failed.append(f"metadata produces_calibrated_residuals != true: {metadata_path}")
            clip = float(metadata.get("residual_clip_value", 10.0))
            max_cal = float(metadata.get("max_abs_calibrated_residual", 0.0))
            if max_cal > clip + eps:
                failed.append(f"max_abs_calibrated_residual exceeds clip: {metadata_path}")
    return {"passed": not failed, "failed_checks": failed, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A7 evidence channel preview outputs.")
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    result = check_a7_evidence_channel(args.input)
    print("probeRCA A7 evidence channel 结构检查")
    print(f"input_dir：{args.input}")
    print(f"passed：{result.get('passed')}")
    print(f"failed_checks：{result.get('failed_checks')}")
    if result.get("passed"):
        print("A7 evidence channel structural check passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

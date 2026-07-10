"""CLI for A3 metrics-driven alert window detection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from proberca.adapters.online_boutique.alert_gate import (
    evaluate_alert_windows_for_debug,
    write_alert_outputs,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect alert windows from metrics only.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--soft-threshold", type=float, default=3.0)
    parser.add_argument("--hard-threshold", type=float, default=6.0)
    parser.add_argument("--consecutive-windows", type=int, default=2)
    parser.add_argument("--baseline-ratio", type=float, default=0.3)
    parser.add_argument("--pre-window-sec", type=float, default=30.0)
    parser.add_argument("--post-window-sec", type=float, default=60.0)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    config = {
        "soft_threshold": args.soft_threshold,
        "hard_threshold": args.hard_threshold,
        "consecutive_windows": args.consecutive_windows,
        "baseline_ratio": args.baseline_ratio,
        "pre_window_sec": args.pre_window_sec,
        "post_window_sec": args.post_window_sec,
    }
    result = write_alert_outputs(args.input, args.output, config)
    debug = None
    incidents = Path(args.input) / "incidents.jsonl"
    if args.debug_evaluate_incidents and incidents.exists():
        debug = evaluate_alert_windows_for_debug(str(Path(args.output) / "alert_windows.jsonl"), str(incidents))
        (Path(args.output) / "alert_debug_evaluation.json").write_text(
            json.dumps(debug, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    metadata = result["metadata"]
    print("probeRCA A3 Alert Gate 摘要")
    print(f"input_dir：{args.input}")
    print(f"output_dir：{args.output}")
    print(f"alert_events_count：{metadata['alert_events_count']}")
    print(f"alert_windows_count：{metadata['alert_windows_count']}")
    print(f"soft_alert_count：{metadata['soft_alert_count']}")
    print(f"hard_alert_count：{metadata['hard_alert_count']}")
    print("uses_root_labels：False")
    print("uses_incident_start_end_for_detection：False")
    if debug is not None:
        print(f"debug_incident_window_recall：{debug['incident_window_recall']}")
    print("注意：当前是 A3 Alert Gate，只检测告警窗口，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

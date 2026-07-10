from __future__ import annotations

import argparse
import json

from proberca.adapters.online_boutique.integrated_pipeline import run_integrated_blind_rca


def main() -> int:
    parser = argparse.ArgumentParser(description="Run B1 integrated blind RCA smoke.")
    parser.add_argument("--input", required=True, help="raw input directory containing metrics.jsonl and service_graph.jsonl")
    parser.add_argument("--output", required=True, help="output directory")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()

    result = run_integrated_blind_rca(
        raw_input_dir=args.input,
        output_dir=args.output,
        config={"top_k": args.top_k},
        debug_evaluate_incidents=args.debug_evaluate_incidents,
    )
    md = result["metadata"]
    rows = result.get("results", [])
    first = rows[0] if rows else {}
    print("注意：当前是 B1 Integrated Blind RCA smoke，只跑集成链路，不重新注入故障，不运行旧 P1 RCA，不进入 B2/B3。")
    print(json.dumps({
        "alert_windows_count": md.get("alert_windows_count"),
        "final_results_count": md.get("final_results_count"),
        "per_window_results_count": md.get("per_window_results_count"),
        "aggregate_result_count": md.get("aggregate_result_count"),
        "top1_service": md.get("top1_service"),
        "top1_metric": md.get("top1_metric"),
        "predicted_root_type": md.get("predicted_root_type"),
        "path_status": md.get("path_status"),
        "uses_root_labels": md.get("uses_root_labels"),
        "uses_incident_start_end": md.get("uses_incident_start_end"),
        "uses_legacy_evidence": md.get("uses_legacy_evidence"),
        "runs_old_p1_rca": md.get("runs_old_p1_rca"),
        "top_service_metric_consistent": md.get("top_service_metric_consistent"),
        "per_window_results_match_alert_windows": md.get("per_window_results_match_alert_windows"),
        "confidence": first.get("confidence"),
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

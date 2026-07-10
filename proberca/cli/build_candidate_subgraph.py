"""Build A4 candidate subgraph for one P2 repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.candidate_subgraph import (
    build_candidate_subgraphs_for_repeat,
    evaluate_candidate_subgraph_for_debug,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build A4 candidate subgraph for one repeat.")
    parser.add_argument("--raw-input", required=True)
    parser.add_argument("--alert-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--reverse-hops", type=int, default=2)
    parser.add_argument("--forward-hops", type=int, default=1)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()

    summary = build_candidate_subgraphs_for_repeat(
        args.raw_input,
        args.alert_input,
        args.output,
        reverse_hops=args.reverse_hops,
        forward_hops=args.forward_hops,
    )
    debug = None
    incidents_path = Path(args.raw_input) / "incidents.jsonl"
    if args.debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_candidate_subgraph_for_debug(str(Path(args.output) / "repeat_candidate_summary.json"), str(incidents_path))
        _write_json(Path(args.output) / "candidate_debug_evaluation.json", debug)

    print("probeRCA A4 candidate subgraph 摘要")
    print(f"raw_input_dir：{args.raw_input}")
    print(f"alert_input_dir：{args.alert_input}")
    print(f"output_dir：{args.output}")
    print(f"alert_windows_count：{summary['alert_windows_count']}")
    print(f"candidate_service_count：{summary['candidate_service_count']}")
    print(f"candidate_metric_node_count：{summary['candidate_metric_node_count']}")
    if debug is not None:
        print(f"debug_root_service_candidate_hit_rate：{debug['root_service_hit_rate_debug']}")
        print(f"debug_root_metric_candidate_hit_rate：{debug['root_metric_hit_rate_debug']}")
    print("uses_root_labels=false")
    print("uses_incident_start_end=false")
    print("注意：当前是 A4 Candidate Subgraph Builder，只构建候选子图，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

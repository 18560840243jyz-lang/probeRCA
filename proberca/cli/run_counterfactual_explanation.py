"""Run A9 counterfactual explanation preview for one repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.explain.counterfactual_explanation import CounterfactualConfig, evaluate_counterfactual_debug, run_counterfactual_explanation


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _guess_incidents_path(graph_sparse_input: str) -> Path | None:
    path = Path(graph_sparse_input)
    parts = path.parts
    if "a8r_graph_sparse_preview" not in parts:
        return None
    try:
        idx = parts.index("a8r_graph_sparse_preview")
        key = parts[idx + 1]
        repeat = parts[idx + 2]
    except (ValueError, IndexError):
        return None
    raw_roots = {
        "cpu": "cpu_paymentservice_repeated_controlled",
        "network": "network_shippingservice_repeated",
        "io": "io_rediscart_repeated",
        "lock": "lock_cartservice_repeated_phaseaware",
    }
    raw_root = raw_roots.get(key)
    if not raw_root:
        return None
    candidate = Path("data/p2_online_boutique") / raw_root / repeat / "raw" / "incidents.jsonl"
    return candidate if candidate.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A9 counterfactual explanation preview.")
    parser.add_argument("--graph-sparse-input", required=True)
    parser.add_argument("--candidate-input", required=True)
    parser.add_argument("--evidence-channel-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--top-k-metrics", type=int, default=10)
    parser.add_argument("--top-k-services", type=int, default=5)
    parser.add_argument("--max-reopt-iter", type=int, default=500)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = CounterfactualConfig(top_k_metrics=args.top_k_metrics, top_k_services=args.top_k_services, max_reopt_iter=args.max_reopt_iter)
    result = run_counterfactual_explanation(args.graph_sparse_input, args.candidate_input, args.evidence_channel_input, args.output, cfg)
    debug = None
    incidents = _guess_incidents_path(args.graph_sparse_input)
    if args.debug_evaluate_incidents and incidents:
        debug = evaluate_counterfactual_debug(args.output, str(incidents))
        _write_json(Path(args.output) / "counterfactual_debug_evaluation.json", debug)
    metadata = result["metadata"]
    print("probeRCA A9 Counterfactual Explanation preview 摘要")
    print(f"metric_counterfactual_count：{metadata['metric_counterfactual_count']}")
    print(f"service_counterfactual_count：{metadata['service_counterfactual_count']}")
    print(f"average_metric_delta_loss：{metadata['average_metric_delta_loss']}")
    print(f"average_service_delta_loss：{metadata['average_service_delta_loss']}")
    if debug:
        print(f"debug_counterfactual_service_hit_at_1：{debug.get('debug_counterfactual_service_hit_at_1')}")
        print(f"debug_counterfactual_metric_hit_at_3：{debug.get('debug_counterfactual_metric_hit_at_3')}")
        print(f"debug_counterfactual_root_type_accuracy：{debug.get('debug_root_type_by_top_metric_family')}")
    print("reoptimizes_with_candidate_removed=true")
    print("uses_root_labels=false")
    print("注意：当前是 A9 Counterfactual Explanation，只生成反事实解释 preview，不运行旧 P1 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run A8 graph sparse inversion preview for one repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.inference.graph_sparse_inversion import GraphSparseConfig, evaluate_graph_sparse_debug, run_graph_sparse_inversion


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _guess_incidents_path(candidate_input: str) -> Path | None:
    path = Path(candidate_input)
    parts = path.parts
    if "a4_candidate_preview" not in parts:
        return None
    try:
        idx = parts.index("a4_candidate_preview")
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
    parser = argparse.ArgumentParser(description="Run A8 graph sparse inversion preview.")
    parser.add_argument("--candidate-input", required=True)
    parser.add_argument("--evidence-channel-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--lambda-l1", type=float, default=0.15)
    parser.add_argument("--lambda-graph-tv", type=float, default=0.08)
    parser.add_argument("--lambda-group", type=float, default=0.05)
    parser.add_argument("--rho", type=float, default=1.0)
    parser.add_argument("--max-iter", type=int, default=1000)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = GraphSparseConfig(args.lambda_l1, args.lambda_graph_tv, args.lambda_group, args.rho, args.max_iter)
    result = run_graph_sparse_inversion(args.candidate_input, args.evidence_channel_input, args.output, cfg)
    debug = None
    incidents = _guess_incidents_path(args.candidate_input)
    if args.debug_evaluate_incidents and incidents:
        debug = evaluate_graph_sparse_debug(args.output, str(incidents))
        _write_json(Path(args.output) / "graph_sparse_debug_evaluation.json", debug)
    metadata = result["metadata"]
    print("probeRCA A8 Graph Sparse Inversion preview 摘要")
    print(f"node_count：{metadata['node_count']}")
    print(f"edge_count：{metadata['edge_count']}")
    print(f"nonzero_intervention_count：{metadata['nonzero_intervention_count']}")
    print(f"solver_status：{metadata['solver_status']}")
    print(f"iterations：{metadata['iterations']}")
    print(f"final_objective：{metadata['final_objective']}")
    if debug:
        print(f"debug_service_hit_at_1：{debug.get('debug_service_hit_at_1')}")
        print(f"debug_metric_hit_at_3：{debug.get('debug_metric_hit_at_3')}")
        print(f"debug_root_type_accuracy：{debug.get('debug_root_type_match_by_metric_family')}")
    print("consumes_calibrated_residuals=true")
    print("consumes_raw_residuals=false")
    print("注意：当前是 A8 Graph Sparse Inversion，只生成 sparse intervention preview，不运行旧 P1 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

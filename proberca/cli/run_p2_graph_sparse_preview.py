"""Run A8 graph sparse inversion preview over all existing P2 repeats."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.graph_sparse_preview import run_p2_graph_sparse_preview
from proberca.inference.graph_sparse_inversion import GraphSparseConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P2 A8 graph sparse inversion preview.")
    parser.add_argument("--output", default="data/p2_online_boutique/a8_graph_sparse_preview")
    parser.add_argument("--lambda-l1", type=float, default=0.15)
    parser.add_argument("--lambda-graph-tv", type=float, default=0.08)
    parser.add_argument("--lambda-group", type=float, default=0.05)
    parser.add_argument("--rho", type=float, default=1.0)
    parser.add_argument("--max-iter", type=int, default=1000)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = GraphSparseConfig(args.lambda_l1, args.lambda_graph_tv, args.lambda_group, args.rho, args.max_iter)
    summary = run_p2_graph_sparse_preview(args.output, cfg, args.debug_evaluate_incidents)
    print("probeRCA A8 P2 graph sparse inversion preview 摘要")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_completed：{summary['repeats_completed']}")
    print(f"average_node_count：{summary['average_node_count']}")
    print(f"average_edge_count：{summary['average_edge_count']}")
    print(f"average_nonzero_intervention_count：{summary['average_nonzero_intervention_count']}")
    if "debug_service_hit_at_1_overall" in summary:
        print(f"debug_service_hit_at_1_overall：{summary['debug_service_hit_at_1_overall']}")
        print(f"debug_metric_hit_at_3_overall：{summary['debug_metric_hit_at_3_overall']}")
        print(f"debug_root_type_accuracy_overall：{summary['debug_root_type_accuracy_overall']}")
    print("optimization=admm_graph_sparse_inversion")
    print("consumes_calibrated_residuals=true")
    print("consumes_raw_residuals=false")
    print("注意：当前是 A8 Graph Sparse Inversion preview，不运行旧 P1 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

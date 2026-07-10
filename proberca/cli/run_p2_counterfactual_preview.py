"""Run A9 counterfactual explanation preview over all P2 repeats."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.counterfactual_preview import run_p2_counterfactual_preview
from proberca.explain.counterfactual_explanation import CounterfactualConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P2 A9 counterfactual explanation preview.")
    parser.add_argument("--output", default="data/p2_online_boutique/a9_counterfactual_preview")
    parser.add_argument("--top-k-metrics", type=int, default=10)
    parser.add_argument("--top-k-services", type=int, default=5)
    parser.add_argument("--max-reopt-iter", type=int, default=500)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = CounterfactualConfig(top_k_metrics=args.top_k_metrics, top_k_services=args.top_k_services, max_reopt_iter=args.max_reopt_iter)
    summary = run_p2_counterfactual_preview(args.output, cfg, args.debug_evaluate_incidents)
    print("probeRCA A9 P2 counterfactual explanation preview 摘要")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_completed：{summary['repeats_completed']}")
    print(f"average_metric_counterfactual_count：{summary['average_metric_counterfactual_count']}")
    print(f"average_service_counterfactual_count：{summary['average_service_counterfactual_count']}")
    print(f"average_metric_delta_loss：{summary['average_metric_delta_loss']}")
    print(f"average_service_delta_loss：{summary['average_service_delta_loss']}")
    if "debug_counterfactual_service_hit_at_1_overall" in summary:
        print(f"debug_counterfactual_service_hit_at_1_overall：{summary['debug_counterfactual_service_hit_at_1_overall']}")
        print(f"debug_counterfactual_metric_hit_at_3_overall：{summary['debug_counterfactual_metric_hit_at_3_overall']}")
        print(f"debug_counterfactual_root_type_accuracy_overall：{summary['debug_counterfactual_root_type_accuracy_overall']}")
    print("reoptimizes_with_candidate_removed=true")
    print("uses_root_labels=false")
    print("注意：当前是 A9 Counterfactual Explanation preview，不运行旧 P1 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

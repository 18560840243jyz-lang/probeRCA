"""Run A6 IPW-masked RLS preview over all existing P2 repeats."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.ipw_rls_preview import run_p2_ipw_rls_preview
from proberca.propagation.ipw_rls_online import RLSConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P2 A6 IPW-masked RLS preview.")
    parser.add_argument("--output", default="data/p2_online_boutique/a6_ipw_rls_preview")
    parser.add_argument("--forgetting-factor", type=float, default=0.98)
    parser.add_argument("--ridge-init", type=float, default=100.0)
    parser.add_argument("--min-sampling-probability", type=float, default=0.05)
    parser.add_argument("--max-ipw-weight", type=float, default=20.0)
    parser.add_argument("--max-parents", type=int, default=32)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = RLSConfig(args.forgetting_factor, args.ridge_init, args.min_sampling_probability, args.max_ipw_weight, args.max_parents)
    summary = run_p2_ipw_rls_preview(args.output, cfg, args.debug_evaluate_incidents)
    print("probeRCA A6 P2 IPW-masked RLS preview 摘要")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_completed：{summary['repeats_completed']}")
    print(f"average_node_count：{summary['average_node_count']}")
    print(f"average_total_updates：{summary['average_total_updates']}")
    print(f"average_skipped_updates：{summary['average_skipped_updates']}")
    print(f"average_abs_residual：{summary['average_abs_residual']}")
    if "debug_root_metric_residual_rank_mean" in summary:
        print(f"debug_root_metric_residual_rank_mean：{summary['debug_root_metric_residual_rank_mean']}")
        print(f"debug_root_service_residual_rank_mean：{summary['debug_root_service_residual_rank_mean']}")
    print("update_mode=online_rls")
    print("batch_ridge_used=false")
    print("注意：当前是 A6 True IPW-masked RLS preview，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

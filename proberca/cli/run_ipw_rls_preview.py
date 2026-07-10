"""Run A6 IPW-masked RLS preview for one repeat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from proberca.propagation.ipw_rls_online import RLSConfig, evaluate_ipw_rls_debug, run_ipw_rls_preview


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A6 true IPW-masked RLS preview.")
    parser.add_argument("--raw-input", required=True)
    parser.add_argument("--candidate-input", required=True)
    parser.add_argument("--probe-policy-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--forgetting-factor", type=float, default=0.98)
    parser.add_argument("--ridge-init", type=float, default=100.0)
    parser.add_argument("--min-sampling-probability", type=float, default=0.05)
    parser.add_argument("--max-ipw-weight", type=float, default=20.0)
    parser.add_argument("--max-parents", type=int, default=32)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = RLSConfig(args.forgetting_factor, args.ridge_init, args.min_sampling_probability, args.max_ipw_weight, args.max_parents)
    result = run_ipw_rls_preview(args.raw_input, args.candidate_input, args.probe_policy_input, args.output, cfg)
    debug = None
    incidents = Path(args.raw_input) / "incidents.jsonl"
    if args.debug_evaluate_incidents and incidents.exists():
        debug = evaluate_ipw_rls_debug(args.output, str(incidents))
        _write_json(Path(args.output) / "ipw_rls_debug_evaluation.json", debug)
    metadata = result["metadata"]
    print("probeRCA A6 IPW-masked RLS preview 摘要")
    print(f"node_count：{metadata['node_count']}")
    print(f"total_updates：{metadata['total_updates']}")
    print(f"skipped_updates：{metadata['skipped_updates']}")
    print(f"average_abs_residual：{metadata['average_abs_residual']}")
    if debug:
        print(f"debug_root_metric_residual_rank_mean：{debug.get('root_metric_residual_rank_mean')}")
        print(f"debug_root_service_residual_rank_mean：{debug.get('root_service_residual_rank_mean')}")
    print("consumes_sampling_probability=true")
    print("consumes_observation_mask=true")
    print("update_mode=online_rls")
    print("batch_ridge_used=false")
    print("注意：当前是 A6 True IPW-masked RLS，只运行传播学习 preview，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run A2 blind P2 rerun over existing real raw metrics."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.blind_rerun import run_p2_blind_rerun


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A2 blind P2 rerun.")
    parser.add_argument("--output", default="data/p2_online_boutique/blind_rerun")
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()
    result = run_p2_blind_rerun(output_dir=args.output, top_k=args.top_k)
    summary = result["summary"]
    print("probeRCA A2 Blind P2 Rerun 摘要")
    for key in [
        "total_repeats",
        "total_successful_rca",
        "service_hit_at_1_overall",
        "metric_hit_at_3_overall",
        "root_type_accuracy_overall",
        "path_fidelity_overall",
        "auxiliary_metric_hit_at_1_overall",
        "auxiliary_metric_mrr_overall",
    ]:
        print(f"{key}：{summary.get(key)}")
    print("注意：当前是 A2 Blind P2 Rerun。使用已有真实 raw metrics 和 blind evidence，不重新注入故障。当前仍使用 incident start_ts/end_ts 作为 alert window，A3 才实现真正 Alert Gate。")
    return 0 if not result.get("failures") else 1


if __name__ == "__main__":
    raise SystemExit(main())

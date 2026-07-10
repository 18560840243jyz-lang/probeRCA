from __future__ import annotations

import argparse
import json

from proberca.adapters.online_boutique.integrated_replay import run_p2_integrated_replay


def main() -> int:
    parser = argparse.ArgumentParser(description="Run B2 integrated replay over existing raw metrics.")
    parser.add_argument("--output", default="data/p2_online_boutique/b2_integrated_replay")
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    result = run_p2_integrated_replay(args.output, debug_evaluate_incidents=args.debug_evaluate_incidents)
    summary = result["summary"]
    print("注意：当前是 B2 Integrated Replay Existing Raw Metrics。使用已有 raw metrics，不重新注入故障，不运行旧 P1 RCA，labels 只用于结果生成后的 evaluation。")
    print(json.dumps({
        "total_repeats": summary.get("total_repeats"),
        "repeats_completed": summary.get("repeats_completed"),
        "repeats_failed": summary.get("repeats_failed"),
        "service_hit_at_1_overall": summary.get("service_hit_at_1_overall"),
        "metric_hit_at_3_overall": summary.get("metric_hit_at_3_overall"),
        "root_type_accuracy_overall": summary.get("root_type_accuracy_overall"),
        "path_fidelity_overall": summary.get("path_fidelity_overall"),
        "uses_root_labels_for_inference": summary.get("uses_root_labels_for_inference"),
        "evaluation_uses_labels_posthoc": summary.get("evaluation_uses_labels_posthoc"),
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not result["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

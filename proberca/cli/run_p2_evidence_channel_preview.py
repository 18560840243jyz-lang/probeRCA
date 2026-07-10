"""Run A7 evidence channel preview over all existing P2 repeats."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.evidence_channel_preview import run_p2_evidence_channel_preview
from proberca.evidence.evidence_channel import EvidenceChannelConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P2 A7 evidence channel preview.")
    parser.add_argument("--output", default="data/p2_online_boutique/a7_evidence_channel_preview")
    parser.add_argument("--residual-clip-value", type=float, default=10.0)
    parser.add_argument("--max-evidence-effect", type=float, default=5.0)
    parser.add_argument("--debug-evaluate-incidents", action="store_true")
    args = parser.parse_args()
    cfg = EvidenceChannelConfig(residual_clip_value=args.residual_clip_value, max_evidence_effect=args.max_evidence_effect)
    summary = run_p2_evidence_channel_preview(args.output, cfg, args.debug_evaluate_incidents)
    print("probeRCA A7 P2 evidence channel preview 摘要")
    print(f"total_repeats：{summary['total_repeats']}")
    print(f"repeats_completed：{summary['repeats_completed']}")
    print(f"average_abs_raw_residual：{summary['average_abs_raw_residual']}")
    print(f"average_abs_calibrated_residual：{summary['average_abs_calibrated_residual']}")
    print(f"max_abs_raw_residual：{summary['max_abs_raw_residual']}")
    print(f"max_abs_calibrated_residual：{summary['max_abs_calibrated_residual']}")
    if "debug_root_metric_calibrated_residual_rank_mean" in summary:
        print(f"debug_root_metric_calibrated_residual_rank_mean：{summary['debug_root_metric_calibrated_residual_rank_mean']}")
        print(f"debug_root_service_calibrated_residual_rank_mean：{summary['debug_root_service_calibrated_residual_rank_mean']}")
    print("consumes_blind_evidence=true")
    print("produces_calibrated_residuals=true")
    print("raw_residual_directly_used_for_sparse_inversion=false")
    print("注意：当前是 A7 C h_t Evidence Channel preview，不运行 RCA pipeline，不重新注入故障。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

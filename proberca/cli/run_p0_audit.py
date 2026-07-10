"""CLI for probeRCA P0 sanity audit."""

from __future__ import annotations

import argparse
import sys

from proberca.eval.p0_audit import run_p0_audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P0 sanity audit.")
    parser.add_argument("--output", default="data/p0_single_vm/audit", help="Audit output directory.")
    parser.add_argument("--quick", action="store_true", help="Run a smaller audit for tests and fast checks.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_p0_audit(args.output, quick=args.quick)
    except (FileNotFoundError, ValueError) as exc:
        print(f"P0 审计失败：{exc}", file=sys.stderr)
        return 1

    summary = result["summary"]
    print("probeRCA P0 sanity audit 完成")
    print("注意：当前是 P0 sanity audit，不包含 P1 adaptive sampling、IPW 或 optional drift。")
    print(f"label_leakage_passed：{summary['label_leakage_passed']}")
    print(f"suspicious_files：{summary['suspicious_files']}")
    print(f"multi_seed_mean_service_hit_at_1：{summary['multi_seed_mean_service_hit_at_1']}")
    print(f"multi_seed_min_service_hit_at_1：{summary['multi_seed_min_service_hit_at_1']}")
    print(f"multi_seed_mean_metric_hit_at_1：{summary['multi_seed_mean_metric_hit_at_1']}")
    print(f"multi_seed_min_metric_hit_at_1：{summary['multi_seed_min_metric_hit_at_1']}")
    print("semantic ablation 对比：")
    print(f"- full：{summary['full_vs_no_semantic']['full']}")
    print(f"- no_semantic_evidence：{summary['full_vs_no_semantic']['no_semantic_evidence']}")
    print("noise sensitivity 摘要：")
    for row in summary["noise_sensitivity"]["runs"]:
        print(f"- noise_std={row['noise_std']}, metric_hit_at_1={row['metric_hit_at_1']}, service_hit_at_1={row['service_hit_at_1']}, root_type_accuracy={row['root_type_accuracy']}, path_fidelity={row['path_fidelity']}")
    print(f"audit_passed：{summary['audit_passed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

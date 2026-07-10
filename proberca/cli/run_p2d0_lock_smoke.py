"""CLI for P2D-0 real lock contention feasibility smoke."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2d0_lock_smoke import run_p2d0_lock_smoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2D-0 real lock contention feasibility smoke.")
    parser.add_argument("--config", default="configs/p2d0_online_boutique_lock_smoke.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_p2d0_lock_smoke(args.config)
    summary = result["summary"]
    print("probeRCA P2D-0 real lock contention feasibility smoke")
    for key in [
        "output_dir",
        "target_service",
        "sidecar_injected",
        "sidecar_removed",
        "lock_metrics_available",
        "lock_fault_feasible",
        "lock_wait_ms_sum_total",
        "lock_wait_ms_p95_max",
        "lock_contention_count_total",
        "frontend_p99_before_ms",
        "frontend_p99_during_ms",
        "frontend_p99_after_ms",
        "limitation",
    ]:
        value = result.get("output_dir") if key == "output_dir" else summary.get(key)
        print(f"{key}：{value}")
    print("注意：当前是 P2D-0 real lock contention feasibility smoke，不运行 RCA pipeline，不输出准确率。当前锁故障来自 cartservice Pod sidecar lock-stress，不是原始业务代码内部 bug。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

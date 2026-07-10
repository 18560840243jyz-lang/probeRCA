"""CLI for P2C-0 real IO fault feasibility smoke."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2c0_io_smoke import run_p2c0_io_smoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2C-0 real IO smoke.")
    parser.add_argument("--config", default="configs/p2c0_online_boutique_io_smoke.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_p2c0_io_smoke(args.config)
    summary = result["summary"]
    print("probeRCA P2C-0 real IO fault feasibility smoke")
    print(f"output_dir：{result['output_dir']}")
    for key in [
        "target_service",
        "pod_name",
        "io_stress_started",
        "io_stress_completed",
        "io_stress_cleaned",
        "io_fault_feasible",
        "write_bytes_delta_during",
        "write_ops_delta_during",
        "io_time_delta_ms_during",
        "frontend_p99_before_ms",
        "frontend_p99_during_ms",
    ]:
        print(f"{key}：{summary.get(key)}")
    print("注意：当前是 P2C-0 real IO fault feasibility smoke，不运行 RCA pipeline，不输出准确率。")
    return 0 if summary.get("io_fault_feasible") else 1


if __name__ == "__main__":
    raise SystemExit(main())

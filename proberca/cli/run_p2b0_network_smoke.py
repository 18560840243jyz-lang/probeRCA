"""CLI for P2B-0 Online Boutique network fault smoke."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2b0_network_smoke import run_p2b0_network_smoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2B-0 real network fault feasibility smoke.")
    parser.add_argument("--config", default="configs/p2b0_online_boutique_network_smoke.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_p2b0_network_smoke(args.config)
    s = result["summary"]
    print("probeRCA P2B-0 real network fault feasibility smoke")
    print(f"output_dir：{result['output_dir']}")
    print(f"target_service：{s.get('target_service')}")
    print(f"pod_name：{s.get('pod_name')}")
    print(f"netns_pid：{s.get('netns_pid')}")
    print(f"netem_applied：{s.get('netem_applied')}")
    print(f"netem_restored：{s.get('netem_restored')}")
    print(f"network_fault_feasible：{s.get('network_fault_feasible')}")
    print(f"retrans_delta_during：{s.get('retrans_delta_during')}")
    print(f"rtt_before_ms：{s.get('rtt_before_ms')}")
    print(f"rtt_during_ms：{s.get('rtt_during_ms')}")
    print(f"frontend_p99_before_ms：{s.get('frontend_p99_before_ms')}")
    print(f"frontend_p99_during_ms：{s.get('frontend_p99_during_ms')}")
    print("注意：当前是 P2B-0 real network fault feasibility smoke，不运行 RCA pipeline，不输出准确率。")
    return 0 if s.get("network_fault_feasible") else 1


if __name__ == "__main__":
    raise SystemExit(main())

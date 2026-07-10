"""CLI for P1F single-seed P1 RCA result and evaluation."""

from __future__ import annotations

import argparse
import sys

from proberca.eval.p1_experiment import run_p1f_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P1F single-seed RCA result pipeline.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--top-k", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_p1f_experiment(output_dir=args.output, seed=args.seed, top_k=args.top_k)
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1F single-seed 运行失败：{exc}", file=sys.stderr)
        return 1

    evaluation = result["evaluation"]
    print("probeRCA P1F single-seed P1 RCA result 完成")
    print(f"incidents_count：{evaluation['incidents_count']}")
    print(f"service_hit_at_1：{evaluation['service_hit_at_1']}")
    print(f"service_hit_at_3：{evaluation['service_hit_at_3']}")
    print(f"metric_hit_at_1：{evaluation['metric_hit_at_1']}")
    print(f"metric_hit_at_3：{evaluation['metric_hit_at_3']}")
    print(f"root_type_accuracy：{evaluation['root_type_accuracy']}")
    print(f"path_fidelity：{evaluation['path_fidelity']}")
    print(f"observed_ratio：{evaluation['observed_ratio']}")
    print("注意：当前是 P1F single-seed P1 RCA result，不是 P1 gate。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run the probeRCA P1A adaptive observation pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P1A adaptive observation simulation.")
    parser.add_argument("--output", default="data/p1_single_vm/demo")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output)
    try:
        generated = generate_dataset(
            SyntheticConfig(
                seed=args.seed,
                output_dir=str(output_dir),
                baseline_windows=args.baseline_windows,
                faulty_windows=args.faulty_windows,
                instances_per_service=args.instances_per_service,
            )
        )
        normalized = normalize_dataset(input_dir=output_dir, output_dir=output_dir)
        observed = simulate_adaptive_observation(
            input_dir=output_dir,
            output_dir=output_dir,
            config=ObservationPolicyConfig(seed=args.seed),
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P1A 自适应观测流水线失败：{exc}", file=sys.stderr)
        return 1

    generated_metadata = generated["metadata"]
    normalized_metadata = normalized["metadata"]
    observed_metadata = observed["metadata"]
    print("probeRCA P1A adaptive observation pipeline 完成")
    print(f"output_dir：{output_dir}")
    print(f"metrics_count：{generated_metadata['metrics_count']}")
    print(f"normalized_count：{normalized_metadata['normalized_count']}")
    print(f"observed_records：{observed_metadata['observed_records']}")
    print(f"observed_ratio：{observed_metadata['observed_ratio']:.6f}")
    print("注意：当前只运行 generate_dataset、normalize_dataset、simulate_adaptive_observation；不包含 stable propagation、sparse inversion、semantic evidence、path explanation 或 IPW-masked RLS。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

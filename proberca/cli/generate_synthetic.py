"""CLI for generating probeRCA P0 synthetic pseudo-distributed data."""

from __future__ import annotations

import argparse

from proberca.data.synthetic import SyntheticConfig, generate_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate probeRCA P0 synthetic pseudo-distributed data.")
    parser.add_argument("--output", default="data/p0_single_vm/demo", help="Output directory for generated files.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed.")
    parser.add_argument("--baseline-windows", type=int, default=30, help="Number of normal windows.")
    parser.add_argument("--faulty-windows", type=int, default=30, help="Number of faulty windows per incident.")
    parser.add_argument("--instances-per-service", type=int, default=2, help="Instances per service.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = generate_dataset(
        SyntheticConfig(
            seed=args.seed,
            output_dir=args.output,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
        )
    )
    metadata = result["metadata"]

    print("probeRCA P0 合成伪分布式数据生成完成")
    print(f"输出目录：{result['output_dir']}")
    print(f"metrics 数量：{metadata['metrics_count']}")
    print(f"evidence 数量：{metadata['evidence_count']}")
    print(f"incidents 数量：{metadata['incidents_count']}")
    print(f"graph edges 数量：{metadata['graph_edges_count']}")
    print("incident 摘要：")
    for incident in result["incidents"]:
        print(
            "- "
            f"incident_id={incident['incident_id']}, "
            f"root_service={incident['root_service']}, "
            f"root_metric={incident['root_metric']}, "
            f"root_type={incident['root_type']}, "
            f"symptom_service={incident['symptom_service']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

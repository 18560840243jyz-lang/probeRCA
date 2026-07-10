"""CLI for running the probeRCA P0 end-to-end experiment."""

from __future__ import annotations

import argparse
import sys

from proberca.eval.p0_experiment import run_p0_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run probeRCA P0 end-to-end experiment.")
    parser.add_argument("--output", default="data/p0_single_vm/demo", help="Output dataset directory.")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--baseline-windows", type=int, default=30)
    parser.add_argument("--faulty-windows", type=int, default=30)
    parser.add_argument("--instances-per-service", type=int, default=2)
    parser.add_argument("--top-k", type=int, default=5)
    return parser


def _top_metric(result: dict) -> str | None:
    if not result.get("top_metrics"):
        return None
    top = result["top_metrics"][0]
    return f"{top['service']}.{top['metric']}"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_p0_experiment(
            output_dir=args.output,
            seed=args.seed,
            baseline_windows=args.baseline_windows,
            faulty_windows=args.faulty_windows,
            instances_per_service=args.instances_per_service,
            top_k=args.top_k,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"P0 实验失败：{exc}", file=sys.stderr)
        return 1

    evaluation = result["evaluation"]
    results_by_incident = {item["incident_id"]: item for item in result["results"]}
    print("probeRCA P0 端到端实验完成")
    print("注意：当前是 P0 单机伪分布式实验，不包含真实 eBPF、Kubernetes 或分布式部署。")
    print(f"输出目录：{result['output_dir']}")
    print(f"seed：{args.seed}")
    print(f"incidents_count：{evaluation['incidents_count']}")
    print(f"service_hit_at_1：{evaluation['service_hit_at_1']}")
    print(f"service_hit_at_3：{evaluation['service_hit_at_3']}")
    print(f"service_mrr：{evaluation['service_mrr']}")
    print(f"metric_hit_at_1：{evaluation['metric_hit_at_1']}")
    print(f"metric_hit_at_3：{evaluation['metric_hit_at_3']}")
    print(f"metric_mrr：{evaluation['metric_mrr']}")
    print(f"root_type_accuracy：{evaluation['root_type_accuracy']}")
    print(f"path_fidelity：{evaluation['path_fidelity']}")
    print("incident prediction 摘要：")
    for item in evaluation["per_incident"]:
        rca = results_by_incident[item["incident_id"]]
        predicted_top1_service = rca["top_services"][0]["service"] if rca.get("top_services") else None
        print(
            f"- incident_id={item['incident_id']}, "
            f"true_root_service={item['root_service']}, "
            f"predicted_top1_service={predicted_top1_service}, "
            f"true_root_metric={item['root_metric']}, "
            f"predicted_top1_metric={_top_metric(rca)}, "
            f"true_root_type={item['root_type']}, "
            f"predicted_root_type={item['predicted_root_type']}, "
            f"path_intersects_injected_path={item['path_intersects_injected_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

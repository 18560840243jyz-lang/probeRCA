"""Run P2A-1 Online Boutique CPU fault experiment."""

from __future__ import annotations

import argparse
import json

from proberca.adapters.online_boutique.p2a1_cpu_experiment import run_p2a1_cpu_fault_experiment


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run P2A-1 real CPU fault injection and minimal metric collection.")
    parser.add_argument("--config", default="configs/p2a1_online_boutique_cpu_fault.yaml")
    args = parser.parse_args(argv)

    try:
        result = run_p2a1_cpu_fault_experiment(args.config)
    except Exception as exc:  # noqa: BLE001 - CLI must report clear experiment failure.
        print(f"P2A-1 CPU fault experiment failed：{exc}")
        return 1

    quality = result["data_quality_report"]
    print("P2A-1 Online Boutique CPU 故障注入与最小指标采集完成")
    print(f"output_dir：{result['output_dir']}")
    print(f"metrics_count：{quality['metrics_count']}")
    print(f"services_seen：{json.dumps(quality['services_seen'], ensure_ascii=False)}")
    print(f"metrics_seen：{json.dumps(quality['metrics_seen'], ensure_ascii=False)}")
    print(f"fault_injection_succeeded：{quality['fault_injection_succeeded']}")
    print(f"restore_succeeded：{quality['restore_succeeded']}")
    print(f"cadvisor_metrics_available：{quality.get('cadvisor_metrics_available')}")
    print(f"cgroup_cpu_stat_available：{quality['cgroup_cpu_stat_available']}")
    print(f"paymentservice_cpu_metric_present：{quality['paymentservice_cpu_metric_present']}")
    print(f"paymentservice_throttled_metric_present：{quality['paymentservice_throttled_metric_present']}")
    print(f"root_service_metric_coverage_passed：{quality.get('root_service_metric_coverage_passed')} ")
    print(f"frontend_latency_metric_present：{quality['frontend_latency_metric_present']}")
    print("注意：当前是 P2A-1 real CPU fault injection + metric collection，不运行 RCA pipeline，不输出准确率。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run P2E real multi-fault summary."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2e_multifault_summary import write_p2e_multifault_summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build P2E real multi-fault summary.")
    parser.add_argument("--output", default="data/p2_online_boutique/multifault_summary")
    args = parser.parse_args()
    result = write_p2e_multifault_summary(output_dir=args.output)
    overall = result["summary"]["overall"]
    acceptance = result["acceptance"]
    aux = acceptance["auxiliary_metrics"]
    print("probeRCA P2E real multi-fault summary")
    print(f"output_dir：{result['output_dir']}")
    print(f"total_repeats：{overall.get('total_repeats')}")
    print(f"total_successful_quality：{overall.get('total_successful_quality')}")
    print(f"total_successful_rca：{overall.get('total_successful_rca')}")
    print(f"service_hit_at_1_overall：{overall.get('service_hit_at_1_overall')}")
    print(f"metric_hit_at_3_overall：{overall.get('metric_hit_at_3_overall')}")
    print(f"root_type_accuracy_overall：{overall.get('root_type_accuracy_overall')}")
    print(f"path_fidelity_overall：{overall.get('path_fidelity_overall')}")
    print(f"auxiliary metric_hit_at_1_overall：{aux.get('metric_hit_at_1_overall_auxiliary')}")
    print(f"auxiliary metric_mrr_overall：{aux.get('metric_mrr_overall_auxiliary')}")
    print(f"p2e_passed：{acceptance.get('p2e_passed')}")
    print(f"decision：{acceptance.get('decision')}")
    if acceptance.get("failed_checks"):
        print(f"failed_checks：{acceptance.get('failed_checks')}")
    print("注意：当前是 P2E real multi-fault summary。metric Hit@1 是辅助指标，P2 主指标是 service Hit@1、metric Hit@3、root type accuracy 和 path fidelity。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

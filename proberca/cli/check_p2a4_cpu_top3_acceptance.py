"""CLI for P2A-4 CPU Top3 acceptance."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2a4_top3_acceptance import write_p2a4_cpu_top3_acceptance


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check P2A-4 CPU repeat Top3 acceptance.")
    parser.add_argument("--input", default="data/p2_online_boutique/cpu_paymentservice_repeated_controlled")
    parser.add_argument("--output", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = write_p2a4_cpu_top3_acceptance(args.input, args.output)
    result = payload["acceptance"]
    key = result["key_metrics"]
    aux = result["auxiliary_metrics"]
    print("probeRCA P2A-4 CPU repeated real injection Top3 acceptance")
    print(f"input_dir：{payload['input_dir']}")
    print(f"output_path：{payload['output_path']}")
    print(f"p2a4_passed：{result['p2a4_passed']}")
    print(f"decision：{result['decision']}")
    print(f"failed_checks：{result['failed_checks']}")
    print(f"service_hit_at_1_mean：{key['service_hit_at_1_mean']}")
    print(f"metric_hit_at_3_mean：{key['metric_hit_at_3_mean']}")
    print(f"root_type_accuracy_mean：{key['root_type_accuracy_mean']}")
    print(f"path_fidelity_mean：{key['path_fidelity_mean']}")
    print(f"auxiliary_metric_hit_at_1_mean：{aux['metric_hit_at_1_mean']}")
    print(f"auxiliary_metric_mrr_mean：{aux['metric_mrr_mean']}")
    if result["p2a4_passed"]:
        print("P2A-4 CPU repeated real injection Top3 acceptance passed.")
    return 0 if result["p2a4_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""CLI for P2C-1 repeated real I/O fault injection."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.p2c1_io_repeat import load_io_repeat_config, run_p2c1_io_repeated_experiment, write_yaml


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run P2C-1 repeated real I/O fault injection.")
    parser.add_argument("--config", default="configs/p2c1_online_boutique_io_repeated.yaml")
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--sleep-between-repeats-sec", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_path = args.config
    if args.repeats is not None or args.sleep_between_repeats_sec is not None:
        cfg = load_io_repeat_config(config_path)
        if args.repeats is not None:
            cfg.setdefault("repeat_experiment", {})["repeats"] = int(args.repeats)
        if args.sleep_between_repeats_sec is not None:
            cfg.setdefault("repeat_experiment", {})["sleep_between_repeats_sec"] = int(args.sleep_between_repeats_sec)
        config_path = "/tmp/proberca_p2c1_io_repeat_override.yaml"
        write_yaml(config_path, cfg)
    result = run_p2c1_io_repeated_experiment(config_path)
    summary = result["summary"]
    print("probeRCA P2C-1 repeated real IO fault injection")
    for key in [
        "repeats_requested",
        "repeats_completed",
        "repeats_successful_quality",
        "repeats_successful_rca",
        "service_hit_at_1_mean",
        "metric_hit_at_3_mean",
        "root_type_accuracy_mean",
        "path_fidelity_mean",
        "metric_hit_at_1_mean",
    ]:
        print(f"{key}：{summary.get(key)}")
    for row in summary.get("per_repeat", []):
        print(f"repeat_{int(row.get('repeat_index', 0)):02d} predicted_top1_metric：{row.get('predicted_top1_metric', '')}")
    print("注意：当前是 P2C-1 repeated real IO fault injection，只代表 IO 故障重复实验，不代表多故障总体准确率。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

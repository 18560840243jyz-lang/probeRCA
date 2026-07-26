"""Run the final collection-only raw primitive exporter."""

from __future__ import annotations

import argparse
from pathlib import Path

from proberca.dataplane.primitive_exporter import (
    FinalPrimitiveExporter,
    load_final_primitive_exporter_config,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export final-scheme raw counters, cumulative histogram buckets, "
            "and gauges. This process never runs alerting or RCA."
        ),
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--snapshot-once",
        action="store_true",
        help="collect one aligned snapshot and write it to stdout",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    exporter = FinalPrimitiveExporter(
        load_final_primitive_exporter_config(args.config)
    )
    if args.snapshot_once:
        timestamp_ns = (
            exporter.wall_clock_ns() // 1_000_000_000
        ) * 1_000_000_000
        print(exporter.collect_snapshot(timestamp_ns), end="")
        return 0
    exporter.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run only the final control-plane algorithm over a sealed collection archive."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from proberca.controlplane import FinalControlConfig, FinalControlPlane, save_control_run
from proberca.dataplane import CollectionArchive
from proberca.dataplane.contracts import canonical_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a sealed ProbeRCA collection with the final control plane.",
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    payload = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("control config must contain a mapping")
    config = FinalControlConfig.from_dict(payload)
    archive = CollectionArchive.load(args.archive)
    run = FinalControlPlane(config).run(archive)
    save_control_run(args.output, run)
    print(canonical_json({
        "dataset_id": run.dataset_id,
        "output": str(args.output.resolve()),
        "phase": "control_complete",
        "result_count": len(run.results),
        "run_fingerprint": run.run_fingerprint,
        "window_count": run.processed_window_count,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

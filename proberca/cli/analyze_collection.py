"""Run only the final control-plane algorithm over a sealed collection archive."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from proberca.controlplane import FinalControlConfig, FinalControlPlane, save_control_run
from proberca.dataplane import (
    BurstArchive,
    BurstCalibrationArtifact,
    BurstJoinedCollectionArchive,
    CollectionArchive,
)
from proberca.dataplane.contracts import canonical_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a sealed ProbeRCA collection with the final control plane.",
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--burst-archive", type=Path)
    parser.add_argument("--burst-calibration", type=Path)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    payload = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("control config must contain a mapping")
    config = FinalControlConfig.from_dict(payload)
    if (args.burst_archive is None) != (args.burst_calibration is None):
        raise ValueError(
            "--burst-archive and --burst-calibration must be supplied together"
        )
    archive = CollectionArchive.load(args.archive)
    raw_burst = None
    calibration = None
    if args.burst_archive is not None:
        raw_burst = BurstArchive.load(args.burst_archive)
        calibration = BurstCalibrationArtifact.load(args.burst_calibration)
        archive = BurstJoinedCollectionArchive.create(
            normal_archive=archive,
            burst_archive=raw_burst,
            calibration_artifact=calibration,
        )
    run = FinalControlPlane(config).run(archive)
    save_control_run(args.output, run)
    print(canonical_json({
        "dataset_id": run.dataset_id,
        "output": str(args.output.resolve()),
        "phase": "control_complete",
        "calibration_ready": run.calibration_readiness.get("ready", False),
        "calibration_readiness_report": str(
            (args.output / "calibration-readiness.json").resolve()
        ),
        "result_count": len(run.results),
        "run_fingerprint": run.run_fingerprint,
        "window_count": run.processed_window_count,
        "burst_archive_manifest_fingerprint": (
            raw_burst.manifest_fingerprint if raw_burst is not None else None
        ),
        "burst_calibration_fingerprint": (
            calibration.artifact_fingerprint
            if calibration is not None else None
        ),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

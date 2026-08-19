"""Create a frozen Healthy-only Burst calibration artifact."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from proberca.dataplane import BurstArchive
from proberca.dataplane.burst_replay import (
    BurstCalibrationPolicy,
    calibrate_healthy_burst,
)
from proberca.dataplane.contracts import canonical_json


def _mapping(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a mapping")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a label-free Burst calibration from a sealed Healthy "
            "raw-Burst archive."
        ),
    )
    parser.add_argument("--burst-archive", type=Path, required=True)
    parser.add_argument("--collection-contract", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    archive = BurstArchive.load(args.burst_archive)
    contract = _mapping(args.collection_contract)
    policy = BurstCalibrationPolicy.from_dict(_mapping(args.policy))
    artifact = calibrate_healthy_burst(
        archive=archive,
        collection_contract=contract,
        policy=policy,
    )
    artifact.save(args.output)
    print(canonical_json({
        "artifact_fingerprint": artifact.artifact_fingerprint,
        "calibration_count": len(artifact.calibrations),
        "output": str(args.output.resolve()),
        "phase": "burst_calibration_complete",
        "source_dataset_id": artifact.source_dataset_id,
        "source_burst_manifest_fingerprint": (
            artifact.source_burst_manifest_fingerprint
        ),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

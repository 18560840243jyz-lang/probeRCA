#!/usr/bin/env python3
"""Build real Pilot effectiveness and registry-freeze evidence reports."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.effectiveness import (
    build_real_pilot_report,
    evaluate_fault_effectiveness,
)
from proberca.campaign.injectors import load_injector_registry


def _write_new(path: Path, payload: dict) -> None:
    if path.exists():
        raise RuntimeError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--coordinate", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--injection-session", type=Path, required=True)
    parser.add_argument("--contamination", type=Path, required=True)
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument("--effectiveness-output", type=Path, required=True)
    parser.add_argument("--pilot-report-output", type=Path, required=True)
    arguments = parser.parse_args()
    registry = load_injector_registry(arguments.registry, require_frozen=False)
    profiles = [
        item for item in registry["profiles"]
        if item["profile_id"] == arguments.profile_id
    ]
    if len(profiles) != 1:
        raise SystemExit("Pilot profile does not resolve uniquely")
    profile = profiles[0]
    coordinate = json.loads(arguments.coordinate.read_text(encoding="utf-8"))
    session = json.loads(arguments.injection_session.read_text(encoding="utf-8"))
    contamination = json.loads(arguments.contamination.read_text(encoding="utf-8"))
    dataset = arguments.dataset.resolve()
    effectiveness = evaluate_fault_effectiveness(
        normal_root=dataset / "normal",
        primitive_roots=sorted((dataset / "primitives").iterdir()),
        coordinate=coordinate, profile=profile,
        injection_session=session, contamination=contamination,
        load_intent_root=(
            dataset / "load-intent" if (dataset / "load-intent").is_dir() else None
        ),
    )
    pilot = build_real_pilot_report(
        effectiveness_report=effectiveness, injection_session=session,
        profile=profile, dataset_sha256=arguments.dataset_sha256,
    )
    _write_new(arguments.effectiveness_output.resolve(), effectiveness)
    _write_new(arguments.pilot_report_output.resolve(), pilot)
    print(json.dumps(pilot, indent=2, sort_keys=True))
    return 0 if pilot["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

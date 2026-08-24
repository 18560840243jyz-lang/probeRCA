#!/usr/bin/env python3
"""Build one strict 300-window qualification observation from sealed data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.generator import load_campaign_config
from proberca.campaign.qualification_observation import (
    build_archive_qualification_observation,
)
from proberca.controlplane.config import FinalControlConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--normal", type=Path, required=True)
    parser.add_argument("--burst", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.output.exists():
        raise SystemExit("refusing to overwrite qualification observation")
    control = FinalControlConfig.from_dict(yaml.safe_load(
        arguments.control.read_text(encoding="utf-8")
    ))
    observation = build_archive_qualification_observation(
        normal_root=arguments.normal,
        burst_root=arguments.burst,
        campaign_config=load_campaign_config(arguments.campaign),
        control_config=control,
        profile_id=arguments.profile_id,
        telemetry=json.loads(arguments.telemetry.read_text(encoding="utf-8")),
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(observation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(observation, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

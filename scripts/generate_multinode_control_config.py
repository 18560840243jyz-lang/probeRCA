#!/usr/bin/env python3
"""Generate one 108-coordinate control config for a candidate/frozen load."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.control_config import (
    build_multinode_control_config,
    candidate_profile_fingerprint,
)
from proberca.campaign.generator import load_campaign_config
from proberca.campaign.load_profile import load_frozen_load_profile
from proberca.controlplane.config import FinalControlConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--base-control", type=Path, required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--candidate-profile-id")
    selection.add_argument("--frozen-load-profile", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    campaign = load_campaign_config(arguments.campaign)
    base = FinalControlConfig.from_dict(yaml.safe_load(
        arguments.base_control.read_text(encoding="utf-8")
    ))
    if arguments.frozen_load_profile:
        frozen = load_frozen_load_profile(arguments.frozen_load_profile)
        profile = frozen["selected_profile"]
        profile_id = profile["profile_id"]
        profile_fingerprint = frozen["load_profile_fingerprint"]
    else:
        matches = [
            item for item in campaign["load_qualification"]["profiles"]
            if item["profile_id"] == arguments.candidate_profile_id
        ]
        if len(matches) != 1:
            raise SystemExit("candidate load profile does not resolve uniquely")
        profile = matches[0]
        profile_id = profile["profile_id"]
        profile_fingerprint = candidate_profile_fingerprint(profile)
    config = build_multinode_control_config(
        campaign_config=campaign,
        base_control_config=base,
        load_profile_id=profile_id,
        load_profile_fingerprint=profile_fingerprint,
    )
    if arguments.output.exists():
        raise SystemExit("refusing to overwrite multi-node control config")
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        yaml.safe_dump(config.to_dict(), sort_keys=False), encoding="utf-8",
    )
    print(config.config_fingerprint)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

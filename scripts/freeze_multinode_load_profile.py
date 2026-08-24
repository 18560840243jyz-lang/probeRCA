#!/usr/bin/env python3
"""Freeze the highest of the three objectively qualified load profiles."""

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
from proberca.campaign.load_profile import freeze_load_profile


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.output.exists():
        raise SystemExit("refusing to overwrite a frozen load profile")
    reports = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in arguments.qualification_report
    ]
    artifact = freeze_load_profile(
        load_campaign_config(arguments.campaign), reports,
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        yaml.safe_dump(artifact, sort_keys=False), encoding="utf-8",
    )
    print(json.dumps(artifact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Freeze the injector registry only after all real Pilot reports pass."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.execution import freeze_injector_registry
from proberca.campaign.injectors import load_injector_registry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--pilot-report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.output.exists():
        raise SystemExit("refusing to overwrite frozen injector registry")
    candidate = load_injector_registry(arguments.candidate, require_frozen=False)
    reports = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in arguments.pilot_report
    ]
    frozen = freeze_injector_registry(candidate, reports)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = arguments.output.with_suffix(arguments.output.suffix + ".tmp")
    temporary.write_text(
        yaml.safe_dump(frozen, sort_keys=False), encoding="utf-8",
    )
    os.replace(temporary, arguments.output)
    print(json.dumps({
        "output": str(arguments.output.resolve()),
        "profile_count": len(frozen["profiles"]),
        "registry_fingerprint": frozen["registry_fingerprint"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

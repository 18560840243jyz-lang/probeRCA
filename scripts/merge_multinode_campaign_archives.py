#!/usr/bin/env python3
"""Merge three worker-local aligned archives into one formal dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.multinode_merge import merge_worker_archives


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--normal", type=Path, action="append", required=True)
    parser.add_argument("--burst", type=Path, action="append", required=True)
    parser.add_argument("--normal-output", type=Path, required=True)
    parser.add_argument("--burst-output", type=Path, required=True)
    arguments = parser.parse_args()
    report = merge_worker_archives(
        normal_roots=arguments.normal, burst_roots=arguments.burst,
        normal_output=arguments.normal_output,
        burst_output=arguments.burst_output,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

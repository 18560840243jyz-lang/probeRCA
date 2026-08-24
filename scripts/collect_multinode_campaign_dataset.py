#!/usr/bin/env python3
"""Label-agnostic CLI for one synchronized three-Worker dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.distributed import collect_distributed_dataset


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--windows", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--first-window-start-ns", type=int)
    arguments = parser.parse_args()
    report = collect_distributed_dataset(
        repository=arguments.repository, node_inventory=arguments.nodes,
        case_id=arguments.case_id, window_count=arguments.windows,
        output_root=arguments.output,
        first_window_start_ns=arguments.first_window_start_ns,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

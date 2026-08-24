#!/usr/bin/env python3
"""Seal the final merged dataset plus all three workers' raw evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.dataset_package import seal_dataset_directory


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    arguments = parser.parse_args()
    metadata = json.loads(arguments.metadata.read_text(encoding="utf-8"))
    result = seal_dataset_directory(arguments.dataset_root, metadata)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Upload one sealed dataset and independently hash every remote object."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.storage import RcloneArchiveStore, upload_and_verify_dataset


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--remote-prefix", required=True)
    parser.add_argument("--object-prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    report = upload_and_verify_dataset(
        arguments.dataset,
        RcloneArchiveStore(arguments.remote_prefix),
        object_prefix=arguments.object_prefix,
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

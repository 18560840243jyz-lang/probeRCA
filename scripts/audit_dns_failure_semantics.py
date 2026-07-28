#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from proberca.dataplane.dns_semantic_audit import run_semantic_audit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-ms", type=int, default=5000)
    parser.add_argument("--source-service", default="frontend")
    args = parser.parse_args()
    report = run_semantic_audit(
        capture_dir=args.capture,
        output_dir=args.output,
        timeout_ms=args.timeout_ms,
        source_service=args.source_service,
    )
    print(json.dumps(report["totals"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

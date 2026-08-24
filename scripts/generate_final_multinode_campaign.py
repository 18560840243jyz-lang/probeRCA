#!/usr/bin/env python3
"""Generate public and encrypted-private manifests for the final campaign."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.generator import build_campaign_plan, load_campaign_config
from proberca.campaign.injectors import load_injector_registry
from proberca.campaign.load_profile import load_frozen_load_profile
from proberca.campaign.sealing import seal_private_manifest


def _write_new_json(path: Path, value) -> None:
    if path.exists():
        raise RuntimeError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=Path,
        default=REPOSITORY / "configs/final_multinode_campaign.yaml",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--recipient-certificate", type=Path, required=True)
    parser.add_argument("--frozen-injectors", type=Path)
    parser.add_argument("--frozen-load-profile", type=Path)
    parser.add_argument(
        "--preview", action="store_true",
        help="generate a non-executable pre-Pilot matrix preview",
    )
    parser.add_argument(
        "--commitment-secret-env",
        default="PROBERCA_CAMPAIGN_COMMITMENT_SECRET",
    )
    parser.add_argument("--openssl", default="openssl")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    secret_text = os.environ.get(args.commitment_secret_env)
    if not secret_text or len(secret_text.encode("utf-8")) < 32:
        raise SystemExit(
            f"{args.commitment_secret_env} must contain at least 32 bytes"
        )
    output = args.output.resolve()
    if output.exists():
        raise SystemExit("campaign output directory already exists")
    config = load_campaign_config(args.config)
    formal = bool(args.frozen_injectors or args.frozen_load_profile)
    if args.preview == formal or bool(args.frozen_injectors) != bool(
        args.frozen_load_profile
    ):
        raise SystemExit(
            "choose preview, or both frozen injectors and frozen load profile"
        )
    injectors = None
    if args.frozen_injectors:
        injectors = load_injector_registry(
            args.frozen_injectors, require_frozen=True,
        )
    load_profile = (
        load_frozen_load_profile(args.frozen_load_profile)
        if args.frozen_load_profile else None
    )
    plan = build_campaign_plan(
        config, commitment_secret=secret_text.encode("utf-8"),
        frozen_injectors=injectors, frozen_load_profile=load_profile,
    )
    output.mkdir(parents=True)
    _write_new_json(output / "campaign-public-manifest.json", plan.public_manifest)
    _write_new_json(output / "campaign-summary.json", plan.summary)
    seal_private_manifest(
        plan.private_manifest,
        recipient_certificate=args.recipient_certificate,
        output_path=output / "campaign-private-manifest.json.cms",
        openssl_executable=args.openssl,
    )
    print(json.dumps(plan.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

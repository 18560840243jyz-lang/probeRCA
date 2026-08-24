#!/usr/bin/env python3
"""Install only the explicit multi-node open-loop campaign load source."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.load_profile import load_frozen_load_profile
from proberca.campaign.model import fingerprint


NAMESPACE = "proberca-system"


def _run(arguments, **kwargs):
    return subprocess.run(
        [str(item) for item in arguments], check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs,
    )


def _profile(path: Path, profile_id: str) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    profiles = payload["load_qualification"]["profiles"]
    matches = [item for item in profiles if item["profile_id"] == profile_id]
    if len(matches) != 1:
        raise ValueError("load profile does not resolve uniquely")
    return matches[0]


def install(
    *, config: Path, profile_id: str, kubeconfig: Path, context: str,
    load_profile_fingerprint: str | None = None,
) -> None:
    profile = _profile(config, profile_id)
    profile_fingerprint = load_profile_fingerprint or fingerprint(profile)
    prefix = ["kubectl", "--kubeconfig", str(kubeconfig), "--context", context]
    _run([*prefix, "get", "namespace", NAMESPACE])
    script = REPOSITORY / "scripts/multinode_open_loop_load.py"
    generated = _run([
        *prefix, "-n", NAMESPACE, "create", "configmap",
        "proberca-multinode-open-loop-load",
        f"--from-file=multinode_open_loop_load.py={script}",
        "--dry-run=client", "-o", "yaml",
    ])
    _run([*prefix, "apply", "-f", "-"], input=generated.stdout)
    _run([
        *prefix, "-n", NAMESPACE, "apply", "-f",
        str(REPOSITORY / "deploy/multinode-campaign/open-loop-load.yaml"),
    ])
    weights = json.dumps(
        profile["behavior_weights"], sort_keys=True, separators=(",", ":"),
    )
    _run([
        *prefix, "-n", NAMESPACE, "set", "env",
        "deployment/proberca-multinode-open-loop-load",
        f"TARGET_ARRIVAL_RATE_RPS={profile['target_arrival_rate_rps']}",
        f"WORKERS={profile['workers']}",
        f"BEHAVIOR_WEIGHTS_JSON={weights}",
        f"LOAD_PROFILE_ID={profile['profile_id']}",
        f"LOAD_PROFILE_FINGERPRINT={profile_fingerprint}",
    ])
    _run([
        *prefix, "-n", NAMESPACE, "scale",
        "deployment/proberca-multinode-open-loop-load", "--replicas=1",
    ])
    _run([
        *prefix, "-n", NAMESPACE, "rollout", "status",
        "deployment/proberca-multinode-open-loop-load", "--timeout=180s",
    ])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=Path,
        default=REPOSITORY / "configs/final_multinode_campaign.yaml",
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--profile-id")
    selection.add_argument("--frozen-load-profile", type=Path)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    return parser


def main(argv=None) -> int:
    arguments = build_parser().parse_args(argv)
    profile_id = arguments.profile_id
    profile_fingerprint = None
    if arguments.frozen_load_profile:
        frozen = load_frozen_load_profile(arguments.frozen_load_profile)
        profile_id = frozen["selected_profile"]["profile_id"]
        profile_fingerprint = frozen["load_profile_fingerprint"]
    install(
        config=arguments.config.resolve(), profile_id=profile_id,
        kubeconfig=arguments.kubeconfig.resolve(), context=arguments.context,
        load_profile_fingerprint=profile_fingerprint,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

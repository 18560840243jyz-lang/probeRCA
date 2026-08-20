#!/usr/bin/env python3
"""Qualify candidate Healthy load profiles without running fault injection."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import yaml

from proberca.controlplane import FinalControlConfig
from proberca.controlplane.load_qualification import (
    summarize_load_qualification,
)
from proberca.dataplane import BurstArchive, CollectionArchive
from proberca.dataplane.contracts import canonical_json, fingerprint
from proberca.load_profiles import (
    HealthyLoadProfiles,
    kubectl_profile_commands,
)


REPOSITORY = Path(__file__).resolve().parents[1]
KUBECONFIG = "/home/jyz/.kube/config"
KUBE_CONTEXT = "kind-proberca-ob"


def _run(arguments: tuple[str, ...] | list[str], **kwargs):
    return subprocess.run(
        list(arguments), check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        **kwargs,
    )


def _pod_restarts(namespace: str, formal_services: set[str]) -> dict[str, int]:
    result = _run((
        "kubectl", "--kubeconfig", KUBECONFIG,
        "--context", KUBE_CONTEXT, "-n", namespace,
        "get", "pods", "-o", "json",
    ))
    payload = json.loads(result.stdout)
    restarts: dict[str, int] = {}
    for pod in payload["items"]:
        service = (pod.get("metadata", {}).get("labels") or {}).get("app")
        if service not in formal_services:
            continue
        statuses = pod.get("status", {}).get("containerStatuses") or []
        restarts[pod["metadata"]["name"]] = sum(
            int(item.get("restartCount", 0)) for item in statuses
        )
    return restarts


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _assert_aligned(normal: CollectionArchive, burst: BurstArchive) -> None:
    if normal.dataset_id != burst.dataset_id \
            or normal.window_count != burst.window_count:
        raise RuntimeError("qualification Normal/Burst archives differ")
    for normal_window, burst_window in zip(
        normal.iter_windows(), burst.iter_windows(), strict=True,
    ):
        normal_boundary = (
            normal_window.sequence,
            normal_window.window_start_ns,
            normal_window.window_end_ns,
        )
        burst_boundary = (
            burst_window.sequence,
            burst_window.window_start_ns,
            burst_window.window_end_ns,
        )
        if normal_boundary != burst_boundary:
            raise RuntimeError(
                "qualification Normal/Burst window boundaries differ"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profiles",
        type=Path,
        default=REPOSITORY / "configs/final_healthy_load_profiles.yaml",
    )
    parser.add_argument(
        "--control-config",
        type=Path,
        default=REPOSITORY / "configs/final_control.yaml",
    )
    parser.add_argument(
        "--source-config",
        type=Path,
        default=REPOSITORY / "configs/final_live_collector.example.yaml",
    )
    parser.add_argument(
        "--collection-contract",
        type=Path,
        default=REPOSITORY / "configs/final_collection_contract.yaml",
    )
    parser.add_argument(
        "--burst-config",
        type=Path,
        default=REPOSITORY / "configs/final_live_burst.example.yaml",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--settle-seconds", type=int, default=30)
    parser.add_argument("--delete-raw", action="store_true")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    profiles = HealthyLoadProfiles.load(args.profiles)
    if profiles.status != "qualifying":
        raise SystemExit("qualification requires status=qualifying")
    control_payload = yaml.safe_load(
        args.control_config.read_text(encoding="utf-8")
    )
    control = FinalControlConfig.from_dict(control_payload)
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    formal_services = {
        entity_id.rsplit("::", 1)[1]
        for entity_id in control.formal_service_entity_ids
    }
    results = []
    for profile in profiles.profiles:
        for command in kubectl_profile_commands(
            profiles, profile,
            kubeconfig=KUBECONFIG,
            context=KUBE_CONTEXT,
        ):
            completed = _run(command)
            print(completed.stdout, end="", flush=True)
        time.sleep(args.settle_seconds)
        before = _pod_restarts(profiles.namespace, formal_services)
        profile_root = root / profile.profile_id
        normal_root = profile_root / "normal"
        burst_root = profile_root / "burst"
        profile_root.mkdir()
        command = [
            sys.executable, "-u", "-m", "proberca.cli.collect_final",
            "--source-config", str(args.source_config),
            "--collection-contract", str(args.collection_contract),
            "--burst-config", str(args.burst_config),
            "--output", str(normal_root),
            "--burst-output", str(burst_root),
            "--windows", str(profiles.qualification["duration_windows"]),
        ]
        completed = _run(command, cwd=REPOSITORY)
        (profile_root / "collection-command-output.jsonl").write_text(
            completed.stdout, encoding="utf-8"
        )
        after = _pod_restarts(profiles.namespace, formal_services)
        pod_restart_delta = sum(
            max(0, count - before.get(pod, 0))
            for pod, count in after.items()
        )
        normal = CollectionArchive.load(normal_root)
        burst = BurstArchive.load(burst_root)
        _assert_aligned(normal, burst)
        summary = summarize_load_qualification(
            normal,
            config=control,
            profile_id=profile.profile_id,
            profile_fingerprint=profile.profile_fingerprint,
            qualification=profiles.qualification,
            pod_restart_delta=pod_restart_delta,
        )
        summary.update({
            "collection_complete": True,
            "normal_burst_aligned": True,
            "burst_manifest_fingerprint": burst.manifest_fingerprint,
            "normal_windows_sha256": normal.windows_sha256,
            "burst_windows_sha256": burst.windows_sha256,
            "profile_parameters": profile.to_dict(),
            "summary_fingerprint": "",
        })
        summary["summary_fingerprint"] = fingerprint(summary)
        _atomic_json(profile_root / "qualification-summary.json", summary)
        results.append(summary)
        if args.delete_raw:
            shutil.rmtree(normal_root)
            shutil.rmtree(burst_root)
        print(canonical_json({
            "phase": "profile_qualified",
            "profile_id": profile.profile_id,
            "qualified": summary["qualified"],
            "failed_reasons": summary["failed_reasons"],
        }), flush=True)
    qualified = [item for item in results if item["qualified"]]
    selected = max(
        qualified,
        key=lambda item: item["profile_parameters"]["scale_percent"],
        default=None,
    )
    overall = {
        "schema_version": "probeRCA-load-qualification-run-v2",
        "profiles": [
            {
                "profile_id": item["profile_id"],
                "profile_fingerprint": item["profile_fingerprint"],
                "qualified": item["qualified"],
                "failed_reasons": item["failed_reasons"],
                "summary_fingerprint": item["summary_fingerprint"],
            }
            for item in results
        ],
        "selected_profile_id": (
            None if selected is None else selected["profile_id"]
        ),
        "selected_profile_fingerprint": (
            None if selected is None else selected["profile_fingerprint"]
        ),
    }
    _atomic_json(root / "qualification-results.json", overall)
    print(canonical_json(overall), flush=True)
    return 0 if selected is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())

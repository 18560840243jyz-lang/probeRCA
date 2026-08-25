#!/usr/bin/env python3
"""Collect one aligned dataset and optionally schedule one private fault."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import subprocess
from pathlib import Path
import sys
from typing import Any

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.distributed import collect_distributed_dataset
from proberca.campaign.generator import load_campaign_config
from proberca.campaign.injectors import load_injector_registry
from proberca.campaign.remote import RemoteNode, SSHAgentClient
from proberca.campaign.scheduled_injection import run_scheduled_injection
from proberca.campaign.target_resolver import resolve_case_target


def _kubectl_json(
    kubeconfig: Path, context: str, namespace: str, resource: str,
) -> list[dict[str, Any]]:
    completed = subprocess.run([
        "kubectl", "--kubeconfig", str(kubeconfig), "--context", context,
        "-n", namespace, "get", resource, "-o", "json",
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("Kubernetes identity read failed: " + completed.stderr.strip())
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise RuntimeError("Kubernetes identity response is malformed")
    return payload["items"]


def _agent(inventory: dict[str, Any]) -> SSHAgentClient:
    nodes = [
        RemoteNode(
            node_id=item["node_id"], host=item["host"], user=item["user"],
            port=int(item["port"]), identity_file=Path(item["identity_file"]),
        )
        for item in inventory["nodes"] if item["role"] == "worker"
    ]
    return SSHAgentClient(
        nodes, known_hosts_file=Path(inventory["known_hosts_file"]),
        timeout_seconds=120,
    )


def run_episode(
    *, repository: Path, nodes_path: Path, campaign_path: Path,
    registry_path: Path | None, case_id: str, window_count: int,
    output: Path, kubeconfig: Path, context: str,
    coordinate: dict[str, Any] | None = None,
    profile_id: str | None = None,
    allow_candidate_registry: bool = False,
    private_evidence_output: Path | None = None,
    load_intent_ledger: Path | None = Path(
        "/var/lib/proberca-campaign/load-intents/behavior-intents.jsonl"
    ),
) -> dict[str, Any]:
    """Run one case while keeping Test target semantics outside dataset_root."""

    if (coordinate is None) != (profile_id is None):
        raise ValueError("fault coordinate and injector profile must be supplied together")
    if coordinate is not None:
        if private_evidence_output is None:
            raise ValueError("fault episode requires an external private evidence path")
        dataset_root = output.resolve()
        private_root = private_evidence_output.resolve()
        if private_root == dataset_root or dataset_root in private_root.parents:
            raise ValueError("private fault evidence cannot be stored in dataset_root")
    campaign = load_campaign_config(campaign_path)
    inventory = yaml.safe_load(nodes_path.read_text(encoding="utf-8"))
    injection_executor = None
    injection_future = None
    target = None
    profile = None
    client = None
    if coordinate is not None:
        if registry_path is None:
            raise ValueError("fault episode requires an injector registry")
        registry = load_injector_registry(
            registry_path, require_frozen=not allow_candidate_registry,
        )
        matches = [
            item for item in registry["profiles"]
            if item["profile_id"] == profile_id
        ]
        if len(matches) != 1:
            raise ValueError("injector profile does not resolve uniquely")
        profile = matches[0]
        client = _agent(inventory)
        namespace = campaign["formal_scope"]["namespace"]
        target = resolve_case_target(
            coordinate=coordinate, campaign_config=campaign,
            node_inventory=inventory,
            pods=_kubectl_json(kubeconfig, context, namespace, "pods"),
            services=_kubectl_json(kubeconfig, context, namespace, "services"),
            client=client,
        )
        injection_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def started(first_window_start_ns: int, dataset_id: str) -> None:
        nonlocal injection_future
        if coordinate is None:
            return
        pre = int(campaign["timing"]["healthy_pre_seconds"])
        fault = int(campaign["timing"]["planned_fault_seconds"])
        injection_future = injection_executor.submit(
            run_scheduled_injection,
            profile=profile, target=target, client=client,
            dataset_id=dataset_id,
            apply_target_ns=first_window_start_ns + pre * 1_000_000_000,
            cleanup_target_ns=(
                first_window_start_ns + (pre + fault) * 1_000_000_000
            ),
        )

    try:
        collection = collect_distributed_dataset(
            repository=repository, node_inventory=nodes_path,
            case_id=case_id, window_count=window_count,
            output_root=output, on_capture_started=started,
            load_intent_ledger=load_intent_ledger,
        )
        injection = injection_future.result() if injection_future else None
    finally:
        if injection_executor is not None:
            injection_executor.shutdown(wait=True, cancel_futures=False)
    if injection is not None:
        private_evidence_output.parent.mkdir(parents=True, exist_ok=True)
        if private_evidence_output.exists():
            raise RuntimeError("refusing to overwrite private injection evidence")
        temporary = private_evidence_output.with_suffix(
            private_evidence_output.suffix + ".tmp"
        )
        temporary.write_text(
            json.dumps(injection, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, private_evidence_output)
    return {
        "case_id": case_id,
        "dataset_id": collection["dataset_id"],
        "window_count": collection["window_count"],
        "first_window_start_ns": collection["first_window_start_ns"],
        "fault_scheduled": injection is not None,
        "private_evidence_output": (
            str(private_evidence_output.resolve())
            if private_evidence_output is not None else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--windows", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--coordinate", type=Path)
    parser.add_argument("--profile-id")
    parser.add_argument("--allow-candidate-registry", action="store_true")
    parser.add_argument("--private-evidence-output", type=Path)
    parser.add_argument(
        "--load-intent-ledger", type=Path,
        default=Path("/var/lib/proberca-campaign/load-intents/behavior-intents.jsonl"),
    )
    arguments = parser.parse_args()
    coordinate = None
    if arguments.coordinate is not None:
        coordinate = json.loads(arguments.coordinate.read_text(encoding="utf-8"))
    report = run_episode(
        repository=arguments.repository.resolve(),
        nodes_path=arguments.nodes.resolve(),
        campaign_path=arguments.campaign.resolve(),
        registry_path=(
            arguments.registry.resolve() if arguments.registry else None
        ),
        case_id=arguments.case_id, window_count=arguments.windows,
        output=arguments.output.resolve(),
        kubeconfig=arguments.kubeconfig.resolve(), context=arguments.context,
        coordinate=coordinate, profile_id=arguments.profile_id,
        allow_candidate_registry=arguments.allow_candidate_registry,
        private_evidence_output=(
            arguments.private_evidence_output.resolve()
            if arguments.private_evidence_output else None
        ),
        load_intent_ledger=arguments.load_intent_ledger,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Probe all four rented nodes and emit the final physical Go/No-Go report."""

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

from proberca.campaign.injectors import load_injector_registry
from proberca.campaign.load_profile import load_frozen_load_profile
from proberca.campaign.generator import load_campaign_config
from proberca.campaign.model import fingerprint
from proberca.campaign.preflight import (
    CampaignPreflightObservation,
    evaluate_campaign_preflight,
)
from proberca.campaign.remote import RemoteNode, SSHAgentClient


def _load(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != \
            "probeRCA-multinode-node-inventory-v1":
        raise ValueError("unsupported multi-node inventory")
    nodes = payload.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != 4:
        raise ValueError("multi-node preflight requires exactly four nodes")
    if sorted(item.get("role") for item in nodes) != [
        "controller", "worker", "worker", "worker",
    ]:
        raise ValueError("node roles must contain one controller and three workers")
    return payload


def _pod_layout(
    kubeconfig: Path,
    context: str,
    campaign_config: Path,
    inventory: dict,
    image_lock_path: Path,
) -> tuple[bool, bool]:
    config = yaml.safe_load(campaign_config.read_text(encoding="utf-8"))
    image_lock = yaml.safe_load(image_lock_path.read_text(encoding="utf-8"))
    if image_lock.get("schema_version") != "probeRCA-multinode-image-lock-v1":
        raise ValueError("unsupported multi-node image lock")
    expected_images = image_lock.get("images", {})
    kubernetes_nodes = {
        item["node_id"]: item["kubernetes_node_name"]
        for item in inventory["nodes"] if item["role"] == "worker"
    }
    completed = subprocess.run([
        "kubectl", "--kubeconfig", str(kubeconfig), "--context", context,
        "-n", config["formal_scope"]["namespace"], "get", "pods", "-o", "json",
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError("cannot read formal Kubernetes Pod layout: " + completed.stderr)
    items = json.loads(completed.stdout).get("items", [])
    expected = {
        service: kubernetes_nodes[worker]
        for worker, services in config["placement"].items()
        for service in services
    }
    observed: dict[str, str] = {}
    images_pinned = True
    for pod in items:
        labels = pod.get("metadata", {}).get("labels", {})
        service = labels.get("app")
        if service not in expected:
            continue
        observed[service] = pod.get("spec", {}).get("nodeName")
        containers = pod.get("spec", {}).get("containers", [])
        images_pinned = images_pinned and len(containers) == 1 and (
            containers[0].get("image") == expected_images.get(service)
        )
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
        if not ready:
            return False, images_pinned
    return observed == expected, images_pinned


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--campaign-config", type=Path, required=True)
    parser.add_argument("--image-lock", type=Path, required=True)
    parser.add_argument("--injector-registry", type=Path, required=True)
    parser.add_argument("--load-profile", type=Path, required=True)
    parser.add_argument("--code-evidence", type=Path, required=True)
    parser.add_argument("--object-store-report", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    inventory = _load(arguments.nodes)
    nodes = [RemoteNode(
        item["node_id"], item["host"], item["user"], int(item["port"]),
        Path(item["identity_file"]),
    ) for item in inventory["nodes"]]
    client = SSHAgentClient(
        nodes, known_hosts_file=Path(inventory["known_hosts_file"]),
        agent_path=inventory["agent_path"], timeout_seconds=30,
    )
    node_reports = []
    for item in inventory["nodes"]:
        report = client.invoke(item["node_id"], "preflight", {
            "role": item["role"],
            "fault_interface": item.get("fault_interface"),
        })
        report["role"] = item["role"]
        report["required_free_bytes"] = int(item["required_free_bytes"])
        report["required_logical_cpu_count"] = int(
            item["required_logical_cpu_count"]
        )
        report["required_memory_bytes"] = int(item["required_memory_bytes"])
        node_reports.append(report)
    code = json.loads(arguments.code_evidence.read_text(encoding="utf-8"))
    object_store = json.loads(arguments.object_store_report.read_text(encoding="utf-8"))
    registry = load_injector_registry(arguments.injector_registry, require_frozen=True)
    load_profile = load_frozen_load_profile(arguments.load_profile)
    campaign_config = load_campaign_config(arguments.campaign_config)
    if load_profile["campaign_config_fingerprint"] != fingerprint(campaign_config):
        raise RuntimeError("frozen load profile belongs to another campaign")
    placement, images = _pod_layout(
        arguments.kubeconfig, arguments.context, arguments.campaign_config,
        inventory, arguments.image_lock,
    )
    controller = next(
        report for report, node in zip(node_reports, inventory["nodes"])
        if node["role"] == "controller"
    )
    observation = CampaignPreflightObservation(
        repository_tests_passed=code.get("repository_tests_passed") is True,
        deterministic_manifests_passed=code.get("deterministic_manifests_passed") is True,
        cms_roundtrip_passed=code.get("cms_roundtrip_passed") is True,
        interruption_resume_rehearsal_passed=
            code.get("interruption_resume_rehearsal_passed") is True,
        fake_agent_rehearsal_passed=code.get("fake_agent_rehearsal_passed") is True,
        filesystem_restore_rehearsal_passed=
            code.get("filesystem_restore_rehearsal_passed") is True,
        object_store_adapter_tested=code.get("object_store_adapter_tested") is True,
        node_reports=tuple(node_reports),
        object_store_readback_passed=
            object_store.get("independent_readback_passed") is True,
        frozen_image_digests=images,
        placement_verified=placement,
        injector_registry_frozen=registry["status"] == "frozen",
        load_profile_frozen=bool(load_profile.get("load_profile_fingerprint")),
        real_pilot_count=sum(
            bool(item.get("pilot_evidence")) for item in registry["profiles"]
        ),
        expected_pilot_count=len(registry["profiles"]),
        central_free_bytes=int(controller["free_bytes"]),
        required_central_free_bytes=int(
            next(item["required_free_bytes"] for item in inventory["nodes"]
                 if item["role"] == "controller")
        ),
    )
    report = evaluate_campaign_preflight(observation)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["formal_campaign_go"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

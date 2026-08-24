#!/usr/bin/env python3
"""Render the vendored Online Boutique release with pinned images/placement."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.generator import load_campaign_config
from proberca.campaign.model import fingerprint


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def render(
    repository: Path, campaign_path: Path, nodes_path: Path,
    image_lock_path: Path, output: Path,
) -> dict:
    campaign = load_campaign_config(campaign_path)
    nodes = _load_yaml(nodes_path)
    lock = _load_yaml(image_lock_path)
    if lock.get("schema_version") != "probeRCA-multinode-image-lock-v1":
        raise ValueError("unsupported image lock")
    source = repository / lock["online_boutique_source"]["manifest_path"]
    if _sha(source) != lock["online_boutique_source"]["manifest_sha256"]:
        raise ValueError("vendored Online Boutique manifest SHA-256 mismatch")
    workers = {
        item["node_id"]: item["kubernetes_node_name"]
        for item in nodes["nodes"] if item["role"] == "worker"
    }
    if set(workers) != set(campaign["placement"]):
        raise ValueError("workload render nodes differ from campaign placement")
    services = {
        service for values in campaign["placement"].values() for service in values
    }
    images = lock["images"]
    if set(images) - {"campaign_load", "beyla"} != services:
        raise ValueError("image lock does not cover exactly the 11 formal services")
    if any("@sha256:" not in value for value in images.values()):
        raise ValueError("all formal images must use immutable digests")
    cadence = _load_yaml(
        repository / "deploy/final-dataplane/healthy-probe-cadence.yaml"
    )
    documents = []
    deployments = {}
    service_objects = {}
    for document in yaml.safe_load_all(source.read_text(encoding="utf-8")):
        if not document:
            continue
        kind = document.get("kind")
        name = document.get("metadata", {}).get("name")
        if kind == "Deployment" and name == "loadgenerator":
            continue
        if kind == "Deployment" and name in services:
            specification = document["spec"]
            specification["replicas"] = 1
            template = specification["template"]
            template.setdefault("metadata", {}).setdefault("labels", {})[
                "proberca.io/formal-scope"
            ] = "included"
            worker = next(
                item for item, values in campaign["placement"].items()
                if name in values
            )
            pod = template["spec"]
            pod["nodeSelector"] = {"kubernetes.io/hostname": workers[worker]}
            containers = pod.get("containers", [])
            if len(containers) != 1:
                raise ValueError("formal workload contains sidecars or wrappers")
            container = containers[0]
            container["image"] = images[name]
            profile = cadence["deployments"].get(name, {})
            if "capacity_resources" in profile:
                container["resources"] = profile["capacity_resources"]
            if name == "emailservice":
                container.pop("startupProbe", None)
                container.pop("readinessProbe", None)
                container.pop("livenessProbe", None)
                container.update({
                    "startupProbe": {
                        "grpc": {"port": 8080}, "periodSeconds": 2,
                        "timeoutSeconds": 3, "failureThreshold": 30,
                    },
                    "readinessProbe": {
                        "grpc": {"port": 8080}, "periodSeconds": 5,
                        "timeoutSeconds": 3, "failureThreshold": 3,
                        "successThreshold": 1,
                    },
                    "livenessProbe": {
                        "grpc": {"port": 8080}, "periodSeconds": 10,
                        "timeoutSeconds": 3, "failureThreshold": 3,
                    },
                })
            deployments[name] = document
        if kind == "Service" and name in services:
            service_objects[name] = document
        documents.append(document)
    if set(deployments) != services or set(service_objects) != services:
        raise ValueError("vendored manifest lacks a formal Deployment or Service")
    email_ports = service_objects["emailservice"]["spec"]["ports"]
    if len(email_ports) != 1 or email_ports[0].get("port") != 5000 \
            or email_ports[0].get("targetPort") != 8080 \
            or email_ports[0].get("protocol", "TCP") != "TCP":
        raise ValueError("emailservice 5000 to 8080 Service contract changed")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise ValueError("refusing to overwrite rendered workloads")
    output.write_text(
        yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8",
    )
    report = {
        "schema_version": "probeRCA-multinode-workload-render-v1",
        "upstream_git_sha": lock["online_boutique_source"]["upstream_git_sha"],
        "source_manifest_sha256": _sha(source),
        "rendered_manifest_sha256": _sha(output),
        "formal_service_count": len(deployments),
        "excluded_loadgenerator": "loadgenerator" not in deployments,
        "images_pinned": all(
            "@sha256:" in item["spec"]["template"]["spec"]["containers"][0]["image"]
            for item in deployments.values()
        ),
        "placement": {
            name: item["spec"]["template"]["spec"]["nodeSelector"]
            for name, item in sorted(deployments.items())
        },
    }
    report["render_fingerprint"] = fingerprint(report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--image-lock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    arguments = parser.parse_args()
    report = render(
        arguments.repository.resolve(), arguments.campaign.resolve(),
        arguments.nodes.resolve(), arguments.image_lock.resolve(),
        arguments.output.resolve(),
    )
    arguments.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Install only the rendered formal workloads and pinned Beyla on four nodes."""

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

from proberca.campaign.generator import load_campaign_config


def _run(arguments: list[str], *, input_text: str | None = None) -> str:
    completed = subprocess.run(
        arguments, input=input_text, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("cluster workload install failed: " + completed.stderr.strip())
    return completed.stdout


def install(
    *, repository: Path, campaign_path: Path, nodes_path: Path,
    image_lock_path: Path, rendered_workloads: Path,
    kubeconfig: Path, context: str,
) -> dict:
    campaign = load_campaign_config(campaign_path)
    nodes = yaml.safe_load(nodes_path.read_text(encoding="utf-8"))
    lock = yaml.safe_load(image_lock_path.read_text(encoding="utf-8"))
    prefix = [
        "kubectl", "--kubeconfig", str(kubeconfig), "--context", context,
    ]
    for node in nodes["nodes"]:
        role = "control" if node["role"] == "controller" else "worker"
        _run([
            *prefix, "label", "node", node["kubernetes_node_name"],
            f"proberca.io/node-role={role}", "--overwrite",
        ])
    _run([*prefix, "apply", "-f", str(rendered_workloads)])
    _run([
        *prefix, "-n", campaign["formal_scope"]["namespace"], "delete",
        "deployment", "loadgenerator", "--ignore-not-found=true",
    ])
    _run([
        *prefix, "apply", "-f",
        str(repository / "deploy/final-dataplane/beyla.yaml"),
    ])
    _run([
        *prefix, "-n", "proberca-observe", "set", "image",
        "daemonset/proberca-beyla", f"beyla={lock['images']['beyla']}",
    ])
    patch = json.dumps({
        "spec": {"template": {"spec": {
            "nodeSelector": {"proberca.io/node-role": "worker"},
        }}},
    }, separators=(",", ":"))
    _run([
        *prefix, "-n", "proberca-observe", "patch",
        "daemonset/proberca-beyla", "--type=merge", "-p", patch,
    ])
    _run([
        *prefix, "-n", "proberca-observe", "rollout", "status",
        "daemonset/proberca-beyla", "--timeout=300s",
    ])
    for deployment in yaml.safe_load(
        (repository / "deploy/final-dataplane/healthy-probe-cadence.yaml").read_text(
            encoding="utf-8"
        )
    )["instrumentation_restart_order"]:
        _run([
            *prefix, "-n", campaign["formal_scope"]["namespace"],
            "rollout", "restart", f"deployment/{deployment}",
        ])
        _run([
            *prefix, "-n", campaign["formal_scope"]["namespace"],
            "rollout", "status", f"deployment/{deployment}", "--timeout=300s",
        ])
    return {
        "formal_service_count": campaign["formal_scope"]["service_count"],
        "beyla_worker_count": len(campaign["workers"]),
        "dns_experimental_enabled": False,
        "default_loadgenerator_removed": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--image-lock", type=Path, required=True)
    parser.add_argument("--rendered-workloads", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    arguments = parser.parse_args()
    report = install(
        repository=arguments.repository.resolve(),
        campaign_path=arguments.campaign.resolve(),
        nodes_path=arguments.nodes.resolve(),
        image_lock_path=arguments.image_lock.resolve(),
        rendered_workloads=arguments.rendered_workloads.resolve(),
        kubeconfig=arguments.kubeconfig.resolve(), context=arguments.context,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

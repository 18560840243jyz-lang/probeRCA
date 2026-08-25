#!/usr/bin/env python3
"""Render worker-local exporters and collector configs for the frozen layout."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.generator import load_campaign_config
from proberca.campaign.model import fingerprint


def _promql_worker(promql: str, worker: str) -> str:
    if "{" in promql:
        left, right = promql.split("{", 1)
        return f'{left}{{worker="{worker}",' + right
    return f'{promql}{{worker="{worker}"}}'


def render(
    repository: Path, campaign_path: Path, nodes_path: Path, output: Path,
) -> dict:
    campaign = load_campaign_config(campaign_path)
    nodes = yaml.safe_load(nodes_path.read_text(encoding="utf-8"))
    if nodes.get("schema_version") != "probeRCA-multinode-node-inventory-v1":
        raise ValueError("unsupported node inventory")
    worker_nodes = {
        item["node_id"]: item for item in nodes["nodes"] if item["role"] == "worker"
    }
    if set(worker_nodes) != set(campaign["workers"]):
        raise ValueError("node inventory workers differ from campaign workers")
    output.mkdir(parents=True, exist_ok=False)
    base_source = yaml.safe_load((
        repository / "configs/final_live_collector.example.yaml"
    ).read_text(encoding="utf-8"))
    all_services = [
        f"online-boutique/{service}"
        for worker in campaign["workers"] for service in campaign["placement"][worker]
    ]
    cluster_id = campaign["formal_scope"]["cluster_id"]
    formal_service_ids = [
        f"{cluster_id}::online-boutique::{service.split('/', 1)[1]}"
        for service in all_services
    ]
    edge_ids = [
        f"{cluster_id}::online-boutique::{edge['src']}->{edge['dst']}::tcp"
        for edge in campaign["formal_tcp_edges"]
    ]
    prometheus_targets = []
    rendered = {}
    for worker in campaign["workers"]:
        node = worker_nodes[worker]
        root = output / worker
        root.mkdir()
        local_service_names = set(campaign["placement"][worker])
        local_service_ids = [
            f"{cluster_id}::online-boutique::{service}"
            for service in campaign["placement"][worker]
        ]
        local_edge_ids = [
            entity_id for entity_id, edge in zip(
                edge_ids, campaign["formal_tcp_edges"], strict=True,
            )
            if edge["src"] in local_service_names
        ]
        exporter = {
            "schema_version": "probeRCA-final-primitive-exporter-v7",
            "cluster_id": cluster_id,
            "kubeconfig_path": nodes["readonly_kubeconfig_path"],
            "kubernetes_context": nodes["kubernetes_context"],
            "namespaces": ["online-boutique"],
            "include_services": all_services,
            "kind_node_container": "",
            "beyla_port": 9400,
            "node_exporter_url": "http://127.0.0.1:9100/metrics",
            "bpf_loader_path": "/usr/local/lib/proberca-final/proberca-final-ebpf-loader",
            "bpf_map_directory": "/sys/fs/bpf/proberca-final",
            "dns_aggregation_policy_path": "",
            "dns_timeout_ms": 5000,
            "listen_host": "0.0.0.0",
            "listen_port": 9477,
            "snapshot_period_sec": 1,
            "source_timeout_sec": 5.0,
            "inventory_max_staleness_sec": 30.0,
            "acquisition_max_pending": 6,
            "beyla_acquisition_workers": 6,
            "raw_acquisition_workers": 36,
            "publish_queue_max_pending": 4,
            "publish_visibility_sec": 0.75,
            "experimental_dns_enabled": False,
            "runtime_mode": "host",
            "monitored_node_name": node["kubernetes_node_name"],
            "local_services": [
                f"online-boutique/{service}"
                for service in campaign["placement"][worker]
            ],
            "formal_tcp_edge_entity_ids": local_edge_ids,
            "host_cgroup_root": "/sys/fs/cgroup",
        }
        source = copy.deepcopy(base_source)
        source["schema_version"] = "probeRCA-final-live-collector-v3"
        source["cluster_id"] = cluster_id
        source["formal_tcp_edge_entity_ids"] = local_edge_ids
        source["projection_mode"] = "worker-local"
        source["projection_owner"] = worker
        source["projection_node_name"] = node["kubernetes_node_name"]
        source["projection_service_entity_ids"] = local_service_ids
        source["formal_service_entity_ids"] = formal_service_ids
        source["topology_tcp_edge_entity_ids"] = edge_ids
        source["kubernetes"].update({
            "cluster_id": cluster_id,
            "kubeconfig_path": nodes["readonly_kubeconfig_path"],
            "context": nodes["kubernetes_context"],
            "namespaces": ["online-boutique"],
        })
        source["prometheus"]["base_url"] = nodes["prometheus_base_url"]
        for query in source["prometheus"]["queries"]:
            query["promql"] = _promql_worker(query["promql"], worker)
            optional = list(query.get("optional_labels", ()))
            if "worker" not in optional:
                optional.append("worker")
            query["optional_labels"] = optional
        burst = {
            "schema_version": "probeRCA-final-live-burst-v3",
            "cluster_id": cluster_id,
            "event_log_path": "/var/lib/proberca-final-burst/events.jsonl",
            "cgroup_root": "/sys/fs/cgroup",
            "network_class_path": "/sys/class/net",
            "maximum_event_lag_sec": 0.5,
            "expected_program_count": 31,
            "sampling_profile": "low",
            "max_buffered_event_records": 250000,
            "runtime_mode": "host",
            "monitored_node_name": node["kubernetes_node_name"],
        }
        (root / "primitive-exporter.yaml").write_text(
            yaml.safe_dump(exporter, sort_keys=False), encoding="utf-8",
        )
        (root / "live-collector.yaml").write_text(
            yaml.safe_dump(source, sort_keys=False), encoding="utf-8",
        )
        (root / "live-burst.yaml").write_text(
            yaml.safe_dump(burst, sort_keys=False), encoding="utf-8",
        )
        service = f"""[Unit]
Description=ProbeRCA multi-node local primitive exporter ({worker})
After=network-online.target proberca-final-ebpf.service
Requires=proberca-final-ebpf.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/proberca/current
Environment=PYTHONPATH=/opt/proberca/current
ExecStart=/usr/bin/python3 -m proberca.cli.export_final_primitives --config /etc/proberca/primitive-exporter.yaml
Restart=on-failure
RestartSec=2s
Nice=-5
CPUWeight=200

[Install]
WantedBy=multi-user.target
"""
        (root / "proberca-final-primitive-exporter.service").write_text(
            service, encoding="utf-8",
        )
        prometheus_targets.append({
            "targets": [f"{node['host']}:9477"], "labels": {"worker": worker},
        })
        rendered[worker] = {
            "node": node["host"],
            "config_fingerprint": fingerprint({
                "exporter": exporter, "source": source, "burst": burst,
            }),
            "local_services": exporter["local_services"],
            "local_tcp_edge_count": len(local_edge_ids),
        }
    prometheus = {
        "job_name": "proberca-final-primitives-multinode",
        "honor_timestamps": True,
        "scrape_interval": "250ms",
        "scrape_timeout": "200ms",
        "static_configs": prometheus_targets,
    }
    (output / "prometheus-scrape-job.yaml").write_text(
        yaml.safe_dump(prometheus, sort_keys=False), encoding="utf-8",
    )
    node_exporter = {
        "job_name": "proberca-worker-node-exporter",
        "honor_timestamps": True,
        "scrape_interval": "1s",
        "scrape_timeout": "800ms",
        "static_configs": [
            {
                "targets": [f"{worker_nodes[worker]['host']}:9100"],
                "labels": {"worker": worker},
            }
            for worker in campaign["workers"]
        ],
    }
    full_prometheus = {
        "global": {
            "scrape_interval": "1s",
            "evaluation_interval": "1s",
        },
        "scrape_configs": [prometheus, node_exporter],
    }
    (output / "prometheus.yaml").write_text(
        yaml.safe_dump(full_prometheus, sort_keys=False), encoding="utf-8",
    )
    summary = {
        "schema_version": "probeRCA-multinode-dataplane-render-v1",
        "cluster_id": cluster_id,
        "worker_count": len(rendered),
        "formal_service_count": len(all_services),
        "formal_tcp_edge_count": len(edge_ids),
        "expected_records_per_window": campaign["formal_scope"][
            "expected_records_per_window"
        ],
        "workers": rendered,
        "render_fingerprint": fingerprint({
            "campaign": campaign, "workers": rendered,
            "prometheus": full_prometheus,
        }),
    }
    (output / "render-report.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    result = render(
        arguments.repository.resolve(), arguments.campaign.resolve(),
        arguments.nodes.resolve(), arguments.output.resolve(),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

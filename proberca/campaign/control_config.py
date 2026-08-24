"""Build the formal multi-node control scope without hard-coded counts."""

from __future__ import annotations

import copy
from typing import Any

from proberca.controlplane.config import FinalControlConfig

from .model import fingerprint


SERVICE_ROOT_METRICS = (
    "cpu_throttle_ratio",
    "cpu_usage_rate",
    "futex_wait_time_rate",
    "io_psi",
    "local_socket_failure_rate",
    "memory_working_set_ratio",
)
HOST_ROOT_METRICS = (
    "cpu_psi", "io_psi", "memory_psi", "nic_drop_error_rate",
)
TCP_ROOT_METRICS = ("edge_failure_rate", "edge_latency_p95")


def build_multinode_control_config(
    *,
    campaign_config: dict[str, Any],
    base_control_config: FinalControlConfig,
    load_profile_id: str,
    load_profile_fingerprint: str,
) -> FinalControlConfig:
    """Project the frozen campaign topology into the existing control schema."""

    cluster_id = str(campaign_config["formal_scope"]["cluster_id"])
    namespace = str(campaign_config["formal_scope"]["namespace"])
    services = sorted({
        service
        for values in campaign_config["placement"].values()
        for service in values
    })
    workers = sorted(campaign_config["workers"])
    edges = sorted(
        (str(item["src"]), str(item["dst"]))
        for item in campaign_config["formal_tcp_edges"]
    )
    roots = {
        f"{cluster_id}::{namespace}::{service}::{metric}"
        for service in services for metric in SERVICE_ROOT_METRICS
    }
    roots.update(
        f"{cluster_id}::host::{worker}::{metric}"
        for worker in workers for metric in HOST_ROOT_METRICS
    )
    roots.update(
        f"{cluster_id}::{namespace}::{source}->{target}::tcp::{metric}"
        for source, target in edges for metric in TCP_ROOT_METRICS
    )
    expected_roots = int(
        campaign_config["formal_scope"]["theoretical_root_coordinates"]
    )
    if len(roots) != expected_roots:
        raise ValueError(
            "multi-node control root scope differs from campaign declaration"
        )
    payload = copy.deepcopy(base_control_config.to_dict())
    payload["load_profile_id"] = str(load_profile_id)
    payload["load_profile_fingerprint"] = str(load_profile_fingerprint)
    payload["calibration_required_root_coordinates"] = sorted(roots)
    result = FinalControlConfig.from_dict(payload)
    if len(result.formal_service_entity_ids) != len(services):
        raise ValueError("multi-node control service scope is incomplete")
    if len(result.formal_host_entity_ids) != len(workers):
        raise ValueError("multi-node control host scope is incomplete")
    if len(result.formal_tcp_edge_entity_ids) != len(edges):
        raise ValueError("multi-node control TCP scope is incomplete")
    return result


def candidate_profile_fingerprint(profile: dict[str, Any]) -> str:
    """Bind qualification models to exactly one candidate load profile."""

    return fingerprint(profile)

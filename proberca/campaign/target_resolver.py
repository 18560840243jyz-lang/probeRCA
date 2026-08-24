"""Resolve private campaign labels to immutable worker-local target bindings."""

from __future__ import annotations

from typing import Any

from .execution import AgentClient, TargetBinding


class TargetResolutionError(RuntimeError):
    pass


def _application_container(pod: dict[str, Any]) -> str:
    specifications = pod.get("spec", {}).get("containers", [])
    statuses = pod.get("status", {}).get("containerStatuses", [])
    if len(specifications) != 1 or len(statuses) != 1:
        raise TargetResolutionError(
            "formal workload must contain exactly one application container"
        )
    name = specifications[0].get("name")
    status = statuses[0]
    if status.get("name") != name or status.get("ready") is not True \
            or int(status.get("restartCount", -1)) < 0:
        raise TargetResolutionError("formal application container is not Ready")
    container_id = status.get("containerID")
    if not isinstance(container_id, str) or not container_id.startswith(
        "containerd://"
    ):
        raise TargetResolutionError("formal containerd identity is unavailable")
    return container_id


def _pod_for_service(
    service: str, namespace: str, expected_node: str,
    pods: list[dict[str, Any]],
) -> dict[str, Any]:
    candidates = [
        pod for pod in pods
        if pod.get("metadata", {}).get("labels", {}).get("app") == service
        and pod.get("metadata", {}).get("namespace") == namespace
        and pod.get("status", {}).get("phase") == "Running"
    ]
    if len(candidates) != 1:
        raise TargetResolutionError(
            f"formal service must have exactly one Running Pod: {service}"
        )
    pod = candidates[0]
    if pod.get("spec", {}).get("nodeName") != expected_node:
        raise TargetResolutionError("formal Pod placement differs from frozen scope")
    _application_container(pod)
    return pod


def _tcp_destination(
    destination: str, namespace: str, services: list[dict[str, Any]],
) -> tuple[str, int]:
    candidates = [
        item for item in services
        if item.get("metadata", {}).get("namespace") == namespace
        and item.get("metadata", {}).get("name") == destination
    ]
    if len(candidates) != 1:
        raise TargetResolutionError("TCP destination Service is not unique")
    specification = candidates[0].get("spec", {})
    cluster_ip = specification.get("clusterIP")
    ports = [
        item for item in specification.get("ports", [])
        if item.get("protocol", "TCP") == "TCP"
    ]
    if not isinstance(cluster_ip, str) or cluster_ip in {"", "None"} \
            or len(ports) != 1:
        raise TargetResolutionError(
            "TCP destination requires one ClusterIP TCP Service port"
        )
    return cluster_ip, int(ports[0]["port"])


def resolve_case_target(
    *,
    coordinate: dict[str, Any],
    campaign_config: dict[str, Any],
    node_inventory: dict[str, Any],
    pods: list[dict[str, Any]],
    services: list[dict[str, Any]],
    client: AgentClient,
) -> TargetBinding:
    """Resolve in label space, then ask the selected Worker to bind inodes."""

    kind = coordinate.get("entity_kind")
    entity_id = coordinate.get("entity_id")
    placement = campaign_config["placement"]
    namespace = campaign_config["formal_scope"]["namespace"]
    service_to_node = {
        service: node for node, values in placement.items() for service in values
    }
    node_rows = {
        item["node_id"]: item for item in node_inventory["nodes"]
        if item.get("role") == "worker"
    }
    if set(node_rows) != set(placement):
        raise TargetResolutionError("node inventory differs from frozen placement")
    payload: dict[str, Any] = {"entity_kind": kind, "entity_id": entity_id}
    if kind == "service":
        if entity_id not in service_to_node:
            raise TargetResolutionError("service target is outside formal scope")
        node_id = service_to_node[entity_id]
        pod = _pod_for_service(
            entity_id, namespace,
            node_rows[node_id]["kubernetes_node_name"], pods,
        )
        payload["container_id"] = _application_container(pod)
    elif kind == "tcp_edge":
        try:
            source, destination = str(entity_id).split("->", 1)
        except ValueError as error:
            raise TargetResolutionError("directed TCP target is invalid") from error
        formal_edges = {
            f"{item['src']}->{item['dst']}"
            for item in campaign_config["formal_tcp_edges"]
        }
        if entity_id not in formal_edges:
            raise TargetResolutionError("TCP target is outside formal scope")
        node_id = service_to_node[source]
        pod = _pod_for_service(
            source, namespace,
            node_rows[node_id]["kubernetes_node_name"], pods,
        )
        address, port = _tcp_destination(destination, namespace, services)
        payload.update({
            "container_id": _application_container(pod),
            "destination_ip": address,
            "destination_port": port,
            "interface": "eth0",
        })
    elif kind == "host":
        node_id = str(entity_id)
        if node_id not in node_rows:
            raise TargetResolutionError("host target is outside formal scope")
        interface = node_rows[node_id].get("fault_interface")
        if not isinstance(interface, str) or interface.startswith("CHANGE_ME"):
            raise TargetResolutionError(
                "host fault interface must be frozen before Pilot"
            )
        payload["interface"] = interface
    else:
        raise TargetResolutionError("unsupported formal target kind")
    resolved = client.invoke(node_id, "resolve", payload)
    if (
        resolved.get("node_id") != node_id
        or resolved.get("entity_kind") != kind
        or resolved.get("entity_id") != entity_id
    ):
        raise TargetResolutionError("worker resolved a different target")
    return TargetBinding(
        node_id=node_id,
        entity_kind=kind,
        entity_id=entity_id,
        runtime_identity_fingerprint=resolved["runtime_identity_fingerprint"],
        attributes=resolved["attributes"],
    )

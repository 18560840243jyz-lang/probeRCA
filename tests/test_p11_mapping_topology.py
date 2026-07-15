from __future__ import annotations

import pytest

from proberca.k8s.call_edges import ExplicitCallEdgeProvider
from proberca.k8s.endpoints import AmbiguousPodServiceMappingError
from proberca.k8s.inventory import KubernetesInventory
from proberca.k8s.ownership import OwnershipError, resolve_workload
from proberca.k8s.runtime_identity import runtime_identities
from proberca.k8s.topology_builder import LiveTopologyBuilder, TopologyBuildError

from test_p11_watch_inventory import obj


def pod(name, uid, rv="1", node="node-a", owner=None, ip=None, containers=None):
    metadata = {"name": name, "namespace": "observability", "uid": uid,
                "resourceVersion": rv}
    if owner:
        metadata["ownerReferences"] = [owner]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": metadata,
            "spec": {"nodeName": node, "containers": [{"name": "app"}]},
            "status": {"podIP": ip, "containerStatuses": containers or []}}


def service(name, uid, selector=None):
    return obj("Service", name, uid, "1", spec={"selector": selector or {}})


def endpoint_slice(name, uid, service_name, service_uid, pod_uid, address="10.0.0.1"):
    return {
        "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
        "metadata": {"name": name, "namespace": "observability", "uid": uid,
                     "resourceVersion": "1",
                     "labels": {"kubernetes.io/service-name": service_name},
                     "ownerReferences": [{"kind": "Service", "name": service_name,
                                          "uid": service_uid, "controller": True}]},
        "addressType": "IPv4", "ports": [{"port": 8080, "protocol": "TCP"}],
        "endpoints": [{"addresses": [address],
                       "conditions": {"ready": True, "serving": True,
                                      "terminating": False},
                       "targetRef": {"kind": "Pod", "uid": pod_uid},
                       "nodeName": "node-a"}],
    }


def inventory_with_backends(second_service=False):
    required = ("Pod", "Service", "EndpointSlice", "Node")
    inv = KubernetesInventory("cluster-a", required_kinds=required, stale_after_sec=30)
    resources = {
        "Pod": [pod("pod-a", "pod-1", ip="10.0.0.1", containers=[
            {"name": "app", "containerID": "containerd://full-id", "imageID": "sha256:x",
             "ready": True, "restartCount": 1, "started": True}])],
        "Service": [service("svc-a", "svc-1")],
        "EndpointSlice": [endpoint_slice("slice-a", "slice-1", "svc-a", "svc-1", "pod-1")],
        "Node": [obj("Node", "node-a", "node-1", "1", namespace=None)],
    }
    if second_service:
        resources["Service"].append(service("svc-b", "svc-2"))
        resources["EndpointSlice"].append(
            endpoint_slice("slice-b", "slice-2", "svc-b", "svc-2", "pod-1"))
    for kind, values in resources.items():
        inv.replace_kind(kind, values, f"rv-{kind}", 1)
    return inv


def test_owner_chain_uses_uid_and_never_name_heuristics():
    rs = obj("ReplicaSet", "web-abc", "rs-1", "1", apiVersion="apps/v1")
    rs["metadata"]["ownerReferences"] = [
        {"kind": "Deployment", "name": "web", "uid": "dep-1", "controller": True}]
    dep = obj("Deployment", "web", "dep-1", "1", apiVersion="apps/v1")
    p = pod("web-abc-123", "pod-1", owner={
        "kind": "ReplicaSet", "name": "web-abc", "uid": "rs-1", "controller": True})
    result = resolve_workload(p, {"rs-1": rs, "dep-1": dep})
    assert result.kind == "Deployment" and result.uid == "dep-1"
    with pytest.raises(OwnershipError):
        resolve_workload(p, {"dep-1": dep})


def test_runtime_identity_keeps_full_container_id_and_container_type():
    identities = runtime_identities(inventory_with_backends().freeze(2))
    assert identities[0].full_container_id == "containerd://full-id"
    assert identities[0].container_runtime == "containerd"
    assert identities[0].container_type == "app"


def test_multi_service_membership_fails_without_explicit_service_label():
    revision = inventory_with_backends(second_service=True).freeze(2)
    with pytest.raises(AmbiguousPodServiceMappingError):
        revision.resolve_service_for_pod("pod-1")
    assert revision.resolve_service_for_pod("pod-1", explicit_service="svc-a") == \
        "cluster-a::observability::svc-a"


def test_topology_uses_explicit_call_provider_and_preserves_host_relation():
    revision = inventory_with_backends(second_service=True).freeze(2)
    provider = ExplicitCallEdgeProvider.from_dicts("cluster-a", [{
        "source_service_id": "cluster-a::observability::svc-a",
        "destination_service_id": "cluster-a::observability::svc-b",
        "protocol": "http", "request_count": 4,
    }])
    snapshot = LiveTopologyBuilder("cluster-a").build(
        1_000_000_000, 2_000_000_000, revision,
        provider.collect_window(1_000_000_000, 2_000_000_000, revision))
    assert [(edge.src_service, edge.dst_service) for edge in snapshot.call_edges] == [
        ("svc-a", "svc-b")]
    assert snapshot.host_edges
    assert all(edge.relation_type != "impact" for edge in snapshot.call_edges)


def test_endpoint_membership_alone_never_creates_call_edge():
    revision = inventory_with_backends().freeze(2)
    snapshot = LiveTopologyBuilder("cluster-a").build(
        1, 2, revision, ())
    assert snapshot.call_edges == []
    stale = revision.with_stale(True)
    with pytest.raises(TopologyBuildError):
        LiveTopologyBuilder("cluster-a").build(1, 2, stale, ())


def test_endpoint_ready_policy_excludes_not_ready_and_deduplicates_slices():
    inv = KubernetesInventory(
        "cluster-a", required_kinds=("Pod", "Service", "EndpointSlice", "Node"),
        stale_after_sec=30, endpoint_ready_policy="ready_only")
    p = pod("pod-a", "pod-1", ip="10.0.0.1")
    svc = service("svc-a", "svc-1")
    ready = endpoint_slice("one", "slice-1", "svc-a", "svc-1", "pod-1")
    duplicate = endpoint_slice("two", "slice-2", "svc-a", "svc-1", "pod-1")
    not_ready = endpoint_slice("three", "slice-3", "svc-a", "svc-1", "pod-1")
    not_ready["endpoints"][0]["conditions"]["ready"] = False
    for kind, values in {
        "Pod": [p], "Service": [svc], "EndpointSlice": [ready, duplicate, not_ready],
        "Node": [obj("Node", "node-a", "node-1", "1", namespace=None)],
    }.items():
        inv.replace_kind(kind, values, f"rv-{kind}", 1)
    revision = inv.freeze(2)
    assert revision.pod_to_services["pod-1"] == (
        "cluster-a::observability::svc-a",)


def test_pvc_pv_csi_and_hostnetwork_resources_are_explicit_and_stable():
    inv = inventory_with_backends(second_service=True)
    pods = list(inv._objects["Pod"].values())
    for index, item in enumerate(pods):
        item["spec"]["hostNetwork"] = True
        item["spec"]["volumes"] = [{"persistentVolumeClaim": {"claimName": "shared"}}]
    pvc = obj("PersistentVolumeClaim", "shared", "pvc-uid", "1",
              spec={"volumeName": "pv-a"})
    pv = obj("PersistentVolume", "pv-a", "pv-uid", "1", namespace=None,
             spec={"csi": {"driver": "driver.example", "volumeHandle": "handle-a"}})
    inv.replace_kind("Pod", pods, "rv-Pod-2", 2)
    inv.replace_kind("PersistentVolumeClaim", [pvc], "rv-pvc", 2)
    inv.replace_kind("PersistentVolume", [pv], "rv-pv", 2)
    revision = inv.freeze(3)
    snapshot = LiveTopologyBuilder("cluster-a").build(1, 2, revision, ())
    resource_types = {item.resource_type for item in snapshot.service_resources}
    assert {"pvc", "pv", "csi", "host_network"} <= resource_types

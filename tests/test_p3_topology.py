from __future__ import annotations

from dataclasses import replace

import pytest

from proberca.config import ImpactDerivationRule
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    ServiceNodePlacement,
    ServiceResourceBinding,
    TopologyEdge,
    TopologySnapshot,
)
from proberca.topology import (
    ImpactRuleConflictError,
    TopologyNotFoundError,
    TopologyOverlapError,
    TopologyStore,
    build_topology_graph,
)


def edge(src, dst, relation="call", protocol="http", **changes):
    return TopologyEdge(src, dst, relation, src_namespace="ns", dst_namespace="ns",
                        protocol=protocol, **changes)


def snapshot(snapshot_id="s1", start=0, end=100, cluster="cluster-a", *, order=False):
    services = ["ns::api", "ns::worker", "ns::db", "ns::peer", "ns::other"]
    calls = [edge("api", "worker"), edge("worker", "db")]
    impacts = [edge("db", "worker", "impact", protocol=None)]
    nodes = [ServiceNodePlacement("ns", "worker", "node-a", "pod-worker"),
             ServiceNodePlacement("ns", "peer", "node-a", "pod-peer"),
             ServiceNodePlacement("ns", "other", "node-b", "pod-other")]
    resources = [ServiceResourceBinding("ns", "worker", "db", "orders"),
                 ServiceResourceBinding("ns", "peer", "db", "orders"),
                 ServiceResourceBinding("ns", "other", "db", "other")]
    if order:
        services.reverse(); calls.reverse(); nodes.reverse(); resources.reverse()
    return TopologySnapshot(
        schema_version=PROBERCA_SCHEMA_VERSION, snapshot_id=snapshot_id,
        valid_from_ns=start, valid_to_ns=end, cluster_id=cluster,
        services=services, call_edges=[*calls, *impacts], host_edges=[], resource_edges=[],
        service_nodes=nodes, service_resources=resources,
    )


def rule(direction="reverse", protocol="http", rule_id="r1"):
    return ImpactDerivationRule.from_dict({
        "rule_id": rule_id, "source_relation_type": "call", "protocol": protocol,
        "direction": direction, "enabled": True, "provenance_label": "configured-impact",
    })


def test_topology_store_half_open_boundaries_clusters_and_restore(tmp_path):
    store = TopologyStore([snapshot()])
    assert store.query("cluster-a", 0).snapshot_id == "s1"
    assert store.query("cluster-a", 99).snapshot_id == "s1"
    with pytest.raises(TopologyNotFoundError): store.query("cluster-a", 100)
    with pytest.raises(TopologyNotFoundError): store.query("cluster-b", 1)
    store.add(snapshot("other", 0, 100, "cluster-b"))
    assert store.query("cluster-b", 1).snapshot_id == "other"
    path = tmp_path / "topology.json"; store.save_json(path)
    restored = TopologyStore.load_json(path)
    assert restored.to_dict() == store.to_dict()
    assert restored.query("cluster-a", 1) == store.query("cluster-a", 1)


def test_topology_store_overlap_expiry_and_order_independence():
    with pytest.raises(TopologyOverlapError):
        TopologyStore([snapshot(), snapshot("overlap", 50, 150)])
    a = TopologyStore([snapshot(order=False)])
    b = TopologyStore([snapshot(order=True)])
    assert a.query("cluster-a", 1).to_dict() == b.query("cluster-a", 1).to_dict()
    a.remove_expired(100)
    with pytest.raises(TopologyNotFoundError): a.query("cluster-a", 1)


def test_call_explicit_impact_host_and_resource_semantics():
    graph = build_topology_graph(snapshot(), [], allow_cross_namespace=False)
    assert [(item.src_service_id, item.dst_service_id) for item in graph.call_edges] == [
        ("cluster-a::ns::api", "cluster-a::ns::worker"),
        ("cluster-a::ns::worker", "cluster-a::ns::db"),
    ]
    assert [(item.src_service_id, item.dst_service_id) for item in graph.impact_edges] == [
        ("cluster-a::ns::db", "cluster-a::ns::worker")
    ]
    assert not any(item.src_service_id.endswith("worker") and item.dst_service_id.endswith("api")
                   for item in graph.impact_edges)
    host = graph.host_relations[0]
    assert host.symmetric and host.detail["node_name"] == "node-a"
    resource = graph.resource_relations[0]
    assert resource.symmetric and resource.detail == {"resource_type": "db", "resource_id": "orders"}
    assert not any("other" in item.relation_id and "orders" in item.relation_id for item in graph.resource_relations)


@pytest.mark.parametrize("direction,expected", [
    ("reverse", {("worker", "api"), ("db", "worker")}),
    ("forward", {("api", "worker"), ("worker", "db")}),
    ("bidirectional", {("api", "worker"), ("worker", "api"), ("worker", "db"), ("db", "worker")}),
    ("none", set()),
])
def test_impact_derivation_directions_and_provenance(direction, expected):
    source = replace(snapshot(), call_edges=[edge("api", "worker"), edge("worker", "db")])
    graph = build_topology_graph(source, [rule(direction)], False)
    actual = {(item.src_service_id.split("::")[-1], item.dst_service_id.split("::")[-1])
              for item in graph.impact_edges}
    assert actual == expected
    for item in graph.impact_edges:
        assert item.detail["rule_id"] == "r1" and item.detail["source_relation_id"]


def test_impact_rules_are_exact_and_conflicts_fail():
    graph = build_topology_graph(replace(snapshot(), call_edges=[edge("api", "worker", protocol="grpc")]),
                                 [rule(protocol="http")], False)
    assert graph.impact_edges == []
    with pytest.raises(ImpactRuleConflictError):
        build_topology_graph(replace(snapshot(), call_edges=[edge("api", "worker")]),
                             [rule("forward", rule_id="a"), rule("reverse", rule_id="b")], False)


def test_namespace_and_cluster_are_not_implicitly_connected():
    cross = TopologyEdge("api", "remote", "call", src_namespace="ns", dst_namespace="other", protocol="http")
    source = replace(snapshot(), services=[*snapshot().services, "other::remote"], call_edges=[cross])
    graph = build_topology_graph(source, [], False)
    assert graph.call_edges == []
    allowed = build_topology_graph(source, [], True)
    assert allowed.call_edges[0].src_service_id == "cluster-a::ns::api"
    assert allowed.call_edges[0].dst_service_id == "cluster-a::other::remote"

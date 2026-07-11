from __future__ import annotations

from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
import test_p3_topology as topo
from proberca.candidates import (
    CandidateOverflowError,
    CandidateSubgraphBuilder,
    StaleAlertTopologyError,
)
from proberca.config import MetricSignalSpec, ProbeRCAConfig
from proberca.data.schema import AlertEvent, TopologyEdge
from proberca.topology import TopologyStore


def config(**candidate_changes):
    payload = p1.valid_config_dict()
    payload["candidate_graph"].update({
        "allow_cross_namespace": False,
        "allowed_namespaces": ["ns"],
        "max_candidate_services": 20,
        "max_candidate_node_metrics": 50,
        "max_candidate_physical_edges": 20,
        "fail_on_candidate_overflow": True,
        "include_trigger_edge_endpoints": True,
        "include_all_provenance_paths": True,
        "max_provenance_paths_per_object": 10,
    })
    payload["candidate_graph"].update(candidate_changes)
    payload["impact_derivation_rules"] = []
    payload["rca_metric_families"] = ["request", "cpu"]
    payload["shock_templates"]["edge.signal"] = {
        "source_metric_families": ["request"], "target_metric_families": ["request"]
    }
    return ProbeRCAConfig.from_dict(payload)


def service_id(name, namespace="ns"):
    return f"cluster-a::{namespace}::{name}"


def alert(state="soft", ts=10, services=None, edges=None):
    services = [service_id("worker")] if services is None else services
    edges = [] if edges is None else edges
    return AlertEvent(
        schema_version="1.0", alert_id=f"alert-{state}-{ts}", timestamp_ns=ts, state=state,
        trigger_services=services, trigger_edges=edges,
        service_scores={item: 3.0 for item in services}, edge_scores={item: 3.0 for item in edges},
        reason='{"code":"test"}', frozen_baseline=state == "hard",
        frozen_service_model=state in {"soft", "hard"}, frozen_metric_model=state == "hard",
    )


def source_snapshot(snapshot_id="s1", start=0, end=100):
    base = topo.snapshot(snapshot_id, start, end)
    services = [*base.services, "ns::root"]
    edges = [*base.call_edges, topo.edge("root", "db", "impact", protocol=None)]
    return replace(base, services=services, call_edges=edges)


def node_metric(service, family="cpu", metric=None):
    metric = metric or f"{family}.signal"
    return p1.make_node(timestamp_ns=10, window_sec=1, cluster_id="cluster-a", namespace="ns",
                        service_name=service, pod_uid=None, container_id=None, scope="service",
                        metric_family=family, metric_name=metric, metric_kind="gauge", unit="units")


def edge_metric(src="worker", dst="db", protocol="http", metric="edge.signal"):
    return p1.make_edge(timestamp_ns=10, window_sec=1, cluster_id="cluster-a", namespace="ns",
                        src_service=src, dst_service=dst, protocol=protocol, metric_name=metric,
                        metric_kind="gauge", scope="service_pair", unit="units")


def signal(record):
    return MetricSignalSpec.from_dict({
        "record_type": record.record_type,
        "metric_family": record.metric_family if record.record_type == "node_metric" else None,
        "metric_name": record.metric_name,
        "protocol": record.protocol if record.record_type == "edge_metric" else None,
        "transform": "identity", "polarity": "increase_bad", "rare_event_threshold": None,
        "direct_hard": False, "z_cap": 6.0, "aggregation_output_id": record.stable_id,
    })


def build_inputs():
    nodes = [node_metric(name) for name in ("worker", "db", "root", "peer")]
    edge = edge_metric()
    return nodes, [edge], [*(signal(item) for item in nodes), signal(edge)]


def test_candidate_service_formula_and_non_recursive_context():
    nodes, edges, specs = build_inputs()
    result = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(), TopologyStore([source_snapshot()]), nodes, edges
    )
    assert result.candidate_services == sorted([
        service_id("worker"), service_id("db"), service_id("root"), service_id("peer")
    ])
    assert service_id("other") not in result.candidate_services
    reasons = {(item.object_id, item.reason_code) for item in result.provenance}
    assert (service_id("worker"), "trigger_service") in reasons
    assert (service_id("db"), "impact_ancestor") in reasons
    assert (service_id("root"), "impact_ancestor") in reasons
    assert (service_id("db"), "call_descendant") in reasons
    assert (service_id("peer"), "cohost") in reasons
    assert (service_id("peer"), "shared_resource") in reasons


def test_hop_limits_trigger_edge_endpoints_and_cycles():
    nodes, edges, specs = build_inputs()
    limited = replace(config().candidate_graph, upstream_hops=1, downstream_hops=1)
    cfg = replace(config(), candidate_graph=limited)
    result = CandidateSubgraphBuilder(cfg, specs).prepare(alert(), TopologyStore([source_snapshot()]), nodes, edges)
    assert service_id("root") not in result.candidate_services
    physical = "cluster-a::ns::worker->db::http"
    edge_alert = alert(edges=[physical], services=[])
    endpoint_result = CandidateSubgraphBuilder(config(), specs).prepare(
        edge_alert, TopologyStore([source_snapshot()]), nodes, edges)
    assert service_id("worker") in endpoint_result.seed_services and service_id("db") in endpoint_result.seed_services
    cycle = replace(source_snapshot(), call_edges=[*source_snapshot().call_edges,
                    topo.edge("worker", "root", "impact", protocol=None)])
    assert CandidateSubgraphBuilder(config(), specs).prepare(alert(), TopologyStore([cycle]), nodes, edges)


def test_candidate_metrics_physical_edges_and_shocks_are_observed():
    nodes, edges, specs = build_inputs()
    result = CandidateSubgraphBuilder(config(), specs).prepare(alert(), TopologyStore([source_snapshot()]), nodes, edges)
    assert result.candidate_node_ids == sorted(item.stable_id for item in nodes)
    assert result.candidate_edge_metric_ids == [edges[0].stable_id]
    assert result.candidate_shock_ids == [
        "cluster-a::ns::worker->db::http::shock::edge.signal"
    ]
    assert [item["physical_edge_id"] for item in result.physical_edges] == [
        "cluster-a::ns::worker->db::http"
    ]
    no_edge = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(), TopologyStore([source_snapshot()]), nodes, [])
    assert no_edge.candidate_edge_metric_ids == [] and no_edge.candidate_shock_ids == []
    assert no_edge.missing_edge_metrics


def test_metric_family_and_signal_configuration_are_required():
    nodes, edges, specs = build_inputs()
    memory = node_metric("worker", "memory")
    unconfigured = node_metric("worker", "cpu", "cpu.unconfigured")
    result = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(), TopologyStore([source_snapshot()]), [*nodes, memory, unconfigured], edges)
    assert memory.stable_id not in result.candidate_node_ids
    assert unconfigured.stable_id not in result.candidate_node_ids
    missing_nodes = nodes[:-1]
    missing = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(), TopologyStore([source_snapshot()]), missing_nodes, edges)
    assert nodes[-1].stable_id in missing.missing_node_metrics


@pytest.mark.parametrize("state,eligible", [("soft", False), ("hard", True), ("edge_anomaly", False)])
def test_alert_state_eligibility(state, eligible):
    nodes, edges, specs = build_inputs()
    result = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(state), TopologyStore([source_snapshot()]), nodes, edges)
    assert result.rca_eligible is eligible and result.alert_state == state


@pytest.mark.parametrize("state", ["healthy", "recovery"])
def test_non_candidate_alert_states_are_rejected(state):
    nodes, edges, specs = build_inputs()
    with pytest.raises(ValueError):
        CandidateSubgraphBuilder(config(), specs).prepare(
            alert(state), TopologyStore([source_snapshot()]), nodes, edges)


def test_stale_alert_and_overflow_fail_fast():
    nodes, edges, specs = build_inputs()
    builder = CandidateSubgraphBuilder(config(), specs)
    with pytest.raises(StaleAlertTopologyError):
        builder.prepare(alert(services=[service_id("missing")]), TopologyStore([source_snapshot()]), nodes, edges)
    tiny = config(max_candidate_services=2)
    with pytest.raises(CandidateOverflowError):
        CandidateSubgraphBuilder(tiny, specs).prepare(alert(), TopologyStore([source_snapshot()]), nodes, edges)


def test_input_order_does_not_change_candidate_serialization():
    nodes, edges, specs = build_inputs()
    def fixed_clock():
        values = iter((0, 100))
        return values.__next__
    first = CandidateSubgraphBuilder(config(), specs, fixed_clock()).prepare(
        alert(), TopologyStore([source_snapshot()]), nodes, edges)
    reordered = replace(source_snapshot(), services=list(reversed(source_snapshot().services)),
                        call_edges=list(reversed(source_snapshot().call_edges)),
                        service_nodes=list(reversed(source_snapshot().service_nodes)),
                        service_resources=list(reversed(source_snapshot().service_resources)))
    second = CandidateSubgraphBuilder(config(), list(reversed(specs)), fixed_clock()).prepare(
        alert(), TopologyStore([reordered]), list(reversed(nodes)), list(reversed(edges)))
    assert first.to_dict() == second.to_dict()


def test_duplicate_pod_and_flow_records_do_not_duplicate_candidate_provenance():
    nodes, edges, specs = build_inputs()
    duplicate_node = replace(nodes[0], scope="pod", pod_uid="pod-x")
    duplicate_edge = replace(edges[0], scope="flow", src_pod_uid="pod-x", dst_pod_uid="pod-y")
    result = CandidateSubgraphBuilder(config(), specs).prepare(
        alert(), TopologyStore([source_snapshot()]), [*nodes, duplicate_node], [*edges, duplicate_edge])
    assert result.candidate_node_ids.count(nodes[0].stable_id) == 1
    assert result.candidate_edge_metric_ids.count(edges[0].stable_id) == 1

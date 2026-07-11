from __future__ import annotations

from dataclasses import replace

import pytest

from proberca.alerting import UpdateGate
from proberca.config import MetricParentRule, PropagationConfig
from proberca.data.schema import (
    CandidateProvenance,
    CandidateSubgraph,
    NodeAnomalyRecord,
    PROBERCA_SCHEMA_VERSION,
)
from proberca.propagation.metric_history import (
    MetricHealthyHistoryStore,
    MetricHistoryConflictError,
    MetricHistoryOrderError,
)
from proberca.propagation.metric_rules import MetricParentRuleRegistry


NS = 1_000_000_000


def rule(rule_id="self", relation="self_history", target_family="request",
         parent_family="request", lags=None, target_names=None, parent_names=None):
    return MetricParentRule.from_dict({
        "rule_id": rule_id,
        "enabled": True,
        "target_family": target_family,
        "target_metric_names": target_names,
        "relation_type": relation,
        "parent_family": parent_family,
        "parent_metric_names": parent_names,
        "lags": lags or [1],
        "require_signal_spec": True,
        "provenance_label": f"configured-{rule_id}",
    })


def config(rules=None, **changes):
    payload = {
        "service_lags": [1], "metric_lags": [1, 2],
        "rls_forgetting_factor": 0.99, "metric_ridge": 0.1,
        "metric_history_sec": 20, "metric_min_training_rows": 3,
        "metric_min_observation_quality": 0.8,
        "metric_max_condition_number": 1e12, "metric_max_gap_windows": 2,
        "metric_model_cache_size": 2, "metric_include_self_history": True,
        "metric_parent_rules": [item.to_dict() for item in (rules or [rule(lags=[1, 2])])],
    }
    payload.update(changes)
    return PropagationConfig.from_dict(payload)


def anomaly(service, metric, family, value, window, **changes):
    start = window * NS
    payload = dict(
        schema_version=PROBERCA_SCHEMA_VERSION,
        timestamp_ns=start + NS,
        window_start_ns=start,
        window_end_ns=start + NS,
        cluster_id="cluster-a", namespace="ns", service_name=service,
        service_id=f"cluster-a::ns::{service}",
        node_id=f"cluster-a::ns::{service}::{metric}",
        metric_family=family, metric_name=metric,
        signed_z=float(value), anomaly_score=max(float(value), 0.0),
        baseline_ready=True, observation_quality=1.0,
        source_alert_state="healthy",
        source_metric_record_id=f"source::{service}::{metric}",
        baseline_config_fingerprint="b" * 64,
        signal_spec_id=f"signal::{service}::{metric}",
    )
    payload.update(changes)
    return NodeAnomalyRecord(**payload)


def window(window, **changes):
    return [
        anomaly("api", "request.lat", "request", 1 + window, window, **changes),
        anomaly("api", "cpu.use", "cpu", 2 + window, window, **changes),
        anomaly("db", "request.lat", "request", 3 + window, window, **changes),
        anomaly("db", "cpu.use", "cpu", 4 + window, window, **changes),
    ]


def gate(update=True, baseline=True):
    return UpdateGate(True, True, [], [], update, not update, not update,
                      False, False, baseline)


def candidate(state="soft", alert_id="alert-soft", candidate_id="candidate-1"):
    services = ["cluster-a::ns::api", "cluster-a::ns::db"]
    nodes = [
        "cluster-a::ns::api::cpu.use", "cluster-a::ns::api::request.lat",
        "cluster-a::ns::db::cpu.use", "cluster-a::ns::db::request.lat",
    ]
    provenance = [
        CandidateProvenance(item, "service", "trigger_service", item, 0, [], [],
                            "top-1", alert_id, {}) for item in services
    ] + [
        CandidateProvenance(item, "node_metric", "configured_metric", item, 0, [], [],
                            "top-1", alert_id, {}) for item in nodes
    ]
    return CandidateSubgraph(
        schema_version="1.0", candidate_id=candidate_id, cluster_id="cluster-a",
        namespace_scope=["ns"], alert_id=alert_id, alert_state=state,
        alert_timestamp_ns=10 * NS, topology_snapshot_id="top-1",
        topology_valid_from_ns=0, topology_valid_to_ns=100 * NS,
        seed_services=services, trigger_edges=[], candidate_services=services,
        candidate_node_ids=nodes, candidate_edge_metric_ids=[], candidate_shock_ids=[],
        call_edges=[{"relation_id": "call-1", "src_service_id": services[0],
                     "dst_service_id": services[1]}],
        impact_edges=[{"relation_id": "impact-1", "src_service_id": services[1],
                       "dst_service_id": services[0]}],
        host_relations=[{"relation_id": "host-1", "src_service_id": services[0],
                         "dst_service_id": services[1]}],
        resource_relations=[{"relation_id": "resource-1", "src_service_id": services[0],
                             "dst_service_id": services[1]}],
        physical_edges=[], provenance=provenance, missing_node_metrics=[],
        missing_edge_metrics=[], rca_eligible=state == "hard", quality_issues=[],
        config_fingerprint="a" * 64, service_count=2, node_metric_count=4,
        physical_edge_count=0, shock_count=0, build_latency_ms=0.1,
    )


def test_node_anomaly_record_round_trip_and_identity():
    item = anomaly("api", "request.lat", "request", 2, 0)
    assert item.record_type == "node_anomaly"
    assert NodeAnomalyRecord.from_dict(item.to_dict()) == item
    with pytest.raises(ValueError):
        replace(item, node_id="cluster-a::ns::db::request.lat")
    with pytest.raises(ValueError):
        replace(item, service_id="cluster-b::ns::api")


@pytest.mark.parametrize("changes,error", [
    ({"signed_z": float("nan")}, ValueError),
    ({"signed_z": float("inf")}, ValueError),
    ({"anomaly_score": -1.0}, ValueError),
    ({"observation_quality": -0.1}, ValueError),
    ({"observation_quality": 1.1}, ValueError),
    ({"timestamp_ns": 0}, ValueError),
    ({"metric_family": ""}, ValueError),
    ({"signal_spec_id": ""}, ValueError),
])
def test_node_anomaly_strict_boundaries(changes, error):
    with pytest.raises(error):
        replace(anomaly("api", "request.lat", "request", 1, 0), **changes)


def test_healthy_history_gate_quality_and_baseline_rules():
    store = MetricHealthyHistoryStore(config(), window_sec=1)
    assert store.ingest_healthy_window(window(0), gate()).inserted_count == 4
    assert store.ingest_healthy_window(window(1, baseline_ready=False), gate()).inserted_count == 0
    assert store.ingest_healthy_window(window(2, observation_quality=0.1), gate()).inserted_count == 0
    for index, alert_state in enumerate(("soft", "hard", "recovery"), start=3):
        result = store.ingest_healthy_window(window(index, source_alert_state=alert_state), gate(update=False))
        assert result.inserted_count == 0
    assert store.node_ids() == sorted(item.node_id for item in window(0))


def test_edge_anomaly_behavior_follows_gate_only():
    store = MetricHealthyHistoryStore(config(), 1)
    result = store.ingest_healthy_window(window(0, source_alert_state="edge_anomaly"), gate(update=True))
    assert result.inserted_count == 4


def test_frozen_node_ids_are_not_added_to_healthy_history():
    store = MetricHealthyHistoryStore(config(), 1)
    records = window(0)
    frozen = records[0].node_id
    update_gate = replace(gate(), frozen_node_ids=[frozen])
    result = store.ingest_healthy_window(records, update_gate)
    assert result.inserted_count == 3
    assert store.get(frozen, records[0].timestamp_ns) is None


def test_history_duplicate_conflict_order_and_no_fill():
    store = MetricHealthyHistoryStore(config(), 1)
    records = window(0)
    store.ingest_healthy_window(records, gate())
    assert store.ingest_healthy_window(records, gate()).inserted_count == 0
    conflict = [replace(records[0], signed_z=99.0), *records[1:]]
    with pytest.raises(MetricHistoryConflictError):
        store.ingest_healthy_window(conflict, gate())
    store.ingest_healthy_window(window(2), gate())
    assert store.get(records[0].node_id, 2 * NS) is None
    with pytest.raises(MetricHistoryOrderError):
        store.ingest_healthy_window(window(1), gate())


def test_history_replay_reorder_and_snapshot(tmp_path):
    store = MetricHealthyHistoryStore(config(), 1)
    results = store.ingest_replay([(window(1), gate()), (window(0), gate())])
    assert all(item.reordered for item in results)
    path = tmp_path / "history"
    store.snapshot(path)
    restored = MetricHealthyHistoryStore.restore(path, config(), 1)
    assert restored.to_dict() == store.to_dict()


@pytest.mark.parametrize("relation,expected_parent", [
    ("self_history", "cluster-a::ns::api::request.lat"),
    ("same_service", "cluster-a::ns::api::cpu.use"),
    ("impact", "cluster-a::ns::db::cpu.use"),
    ("host", "cluster-a::ns::db::cpu.use"),
    ("resource", "cluster-a::ns::db::cpu.use"),
])
def test_parent_relation_semantics(relation, expected_parent):
    target_family = "request"
    parent_family = "request" if relation == "self_history" else "cpu"
    registry = MetricParentRuleRegistry(config([
        rule(relation=relation, target_family=target_family, parent_family=parent_family)
    ]))
    features = registry.build(candidate(), {item.node_id: item for item in window(0)})
    target = "cluster-a::ns::api::request.lat"
    assert expected_parent in [item.parent_node_id for item in features[target]]


def test_call_only_is_not_a_supported_rule():
    with pytest.raises(ValueError):
        rule(relation="call")


@pytest.mark.parametrize("lags", [[0], [3], [1, 1]])
def test_rule_lag_validation(lags):
    with pytest.raises(ValueError):
        config([rule(lags=lags)])


def test_exact_metric_names_no_substring_and_candidate_boundary():
    exact = rule(target_names=["request.lat"], parent_names=["cpu.use"],
                 relation="same_service", parent_family="cpu")
    registry = MetricParentRuleRegistry(config([exact]))
    index = {item.node_id: item for item in window(0)}
    features = registry.build(candidate(), index)
    assert features["cluster-a::ns::api::request.lat"][0].parent_node_id.endswith("::cpu.use")
    wrong = replace(exact, target_metric_names=["lat"])
    assert MetricParentRuleRegistry(config([wrong])).build(candidate(), index) == {}
    outside = anomaly("outside", "cpu.use", "cpu", 1, 0)
    index[outside.node_id] = outside
    assert all(outside.node_id != feature.parent_node_id
               for values in registry.build(candidate(), index).values() for feature in values)


def test_duplicate_feature_preserves_all_rule_provenance_and_order():
    rules = [rule("a", "same_service", parent_family="cpu"),
             rule("b", "same_service", parent_family="cpu")]
    features = MetricParentRuleRegistry(config(rules)).build(
        candidate(), {item.node_id: item for item in reversed(window(0))})
    target = "cluster-a::ns::api::request.lat"
    assert len(features[target]) == 1
    assert features[target][0].rule_ids == ["a", "b"]
    assert features[target] == sorted(features[target], key=lambda item: (item.parent_node_id, item.lag))


@pytest.mark.parametrize("field,value", [
    ("metric_history_sec", 0), ("metric_min_training_rows", 2),
    ("metric_min_observation_quality", 1.1), ("metric_max_condition_number", 1.0),
    ("metric_max_gap_windows", 0), ("metric_model_cache_size", 0),
    ("metric_include_self_history", False),
])
def test_metric_propagation_config_boundaries(field, value):
    payload = config().__dict__.copy()
    payload["metric_parent_rules"] = [item.to_dict() for item in payload["metric_parent_rules"]]
    payload[field] = value
    with pytest.raises((TypeError, ValueError)):
        PropagationConfig.from_dict(payload)

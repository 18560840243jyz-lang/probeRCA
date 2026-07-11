from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from proberca.alerting import UpdateGate
from proberca.config import ImpactDerivationRule, PropagationConfig
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    ServiceNodePlacement,
    ServiceResourceBinding,
    ServiceStateRecord,
    TopologyEdge,
    TopologySnapshot,
)
from proberca.propagation.service_rls import (
    NumericalPropagationError,
    ServicePropagationLearner,
)


NS = 1_000_000_000


def propagation_config(**changes):
    payload = {
        "service_lags": [1],
        "metric_lags": [1],
        "rls_forgetting_factor": 1.0,
        "metric_ridge": 0.1,
        "rls_initial_covariance": 100.0,
        "service_min_updates": 2,
        "service_min_observation_quality": 0.5,
        "service_max_gap_windows": 2,
        "topology_reconfigure_min_updates": 1,
        "include_self_history": True,
        "include_impact_parents": True,
        "include_host_parents": True,
        "include_resource_parents": True,
    }
    payload.update(changes)
    return PropagationConfig.from_dict(payload)


def impact_rule():
    return ImpactDerivationRule.from_dict({
        "rule_id": "reverse-http",
        "source_relation_type": "call",
        "protocol": "http",
        "direction": "reverse",
        "enabled": True,
        "provenance_label": "dependency-impact",
    })


def topology(snapshot_id="top-1", *, with_impact=True, host=True, resource=True):
    call = TopologyEdge("api", "db", "call", "ns", "ns", "http")
    explicit = TopologyEdge("db", "api", "impact", "ns", "ns", None)
    return TopologySnapshot(
        schema_version=PROBERCA_SCHEMA_VERSION,
        snapshot_id=snapshot_id,
        valid_from_ns=0,
        valid_to_ns=10_000 * NS,
        cluster_id="cluster-a",
        services=["ns::api", "ns::db", "ns::peer", "ns::unrelated"],
        call_edges=[call, explicit] if with_impact else [call],
        host_edges=[],
        resource_edges=[],
        service_nodes=(
            [ServiceNodePlacement("ns", "api", "node-a", "pod-api"),
             ServiceNodePlacement("ns", "peer", "node-a", "pod-peer")]
            if host else []
        ),
        service_resources=(
            [ServiceResourceBinding("ns", "api", "db", "shared"),
             ServiceResourceBinding("ns", "peer", "db", "shared")]
            if resource else []
        ),
    )


def state(service, value, window, **changes):
    start = window * NS
    payload = dict(
        schema_version=PROBERCA_SCHEMA_VERSION,
        timestamp_ns=start + NS,
        window_start_ns=start,
        window_end_ns=start + NS,
        cluster_id="cluster-a",
        namespace="ns",
        service_name=service,
        service_id=f"cluster-a::ns::{service}",
        value=float(value),
        baseline_ready=True,
        family_coverage={"request": True},
        missing_families=[],
        observation_quality=1.0,
        source_alert_state="healthy",
        config_fingerprint="a" * 64,
    )
    payload.update(changes)
    return ServiceStateRecord(**payload)


def gate(update=True, baseline=True):
    return UpdateGate(True, True, [], [], update, False, not update,
                      False, False, baseline)


def learner(**config_changes):
    return ServicePropagationLearner(
        propagation_config(**config_changes),
        window_sec=1,
        impact_derivation_rules=[],
        allow_cross_namespace=False,
    )


def test_service_state_record_is_strict_and_cluster_aware():
    record = state("api", 1.5, 0)
    assert record.record_type == "service_state"
    assert ServiceStateRecord.from_dict(record.to_dict()) == record
    with pytest.raises(ValueError):
        replace(record, service_id="cluster-a::ns::other")
    with pytest.raises(ValueError):
        replace(record, observation_quality=1.1)
    with pytest.raises(ValueError):
        replace(record, value=float("nan"))


def test_parent_features_use_self_impact_host_resource_and_exclude_call_only():
    model = learner()
    result = model.process_window(
        [state(name, 1.0, 0) for name in ("api", "db", "peer", "unrelated")],
        gate(), topology(),
    )
    keys = model.feature_keys("cluster-a::ns::api")
    parents = [key.parent_service_id for key in keys]
    assert parents == sorted(parents)
    assert parents == ["cluster-a::ns::api", "cluster-a::ns::db", "cluster-a::ns::peer"]
    assert "cluster-a::ns::unrelated" not in parents
    assert result.topology_reconfigured


def test_call_only_edge_is_not_a_parent_without_impact_rule():
    model = learner()
    model.process_window([state(name, 1, 0) for name in ("api", "db", "peer", "unrelated")],
                         gate(), topology(with_impact=False, host=False, resource=False))
    assert [key.parent_service_id for key in model.feature_keys("cluster-a::ns::api")] == [
        "cluster-a::ns::api"
    ]


def test_derived_impact_parent_uses_configured_rule_only():
    model = ServicePropagationLearner(propagation_config(), 1, [impact_rule()], False)
    model.process_window([state(name, 1, 0) for name in ("api", "db", "peer", "unrelated")],
                         gate(), topology(with_impact=False, host=False, resource=False))
    parent_ids = [key.parent_service_id for key in model.feature_keys("cluster-a::ns::api")]
    assert "cluster-a::ns::db" in parent_ids


def test_prediction_precedes_update_and_current_target_does_not_leak():
    first = learner(service_min_updates=1)
    second = learner(service_min_updates=1)
    for model in (first, second):
        model.process_window([state("api", 2, 0), state("db", 1, 0),
                              state("peer", 0, 0), state("unrelated", 0, 0)], gate(), topology())
    a = first.process_window([state("api", 4, 1), state("db", 1, 1),
                              state("peer", 0, 1), state("unrelated", 0, 1)], gate(), topology())
    b = second.process_window([state("api", 400, 1), state("db", 1, 1),
                               state("peer", 0, 1), state("unrelated", 0, 1)], gate(), topology())
    pa = next(item for item in a.predictions if item.target_service_id.endswith("::api"))
    pb = next(item for item in b.predictions if item.target_service_id.endswith("::api"))
    assert pa.predicted_value == pb.predicted_value == 0.0
    assert pa.actual_value == 4.0 and pb.actual_value == 400.0


def test_rls_exact_update_and_signed_coefficients():
    model = learner(service_min_updates=1, rls_initial_covariance=2.0)
    model.process_window([state("api", 2, 0), state("db", 0, 0),
                          state("peer", 0, 0), state("unrelated", 0, 0)], gate(), topology(host=False, resource=False))
    result = model.process_window([state("api", -3, 1), state("db", 0, 1),
                                   state("peer", 0, 1), state("unrelated", 0, 1)], gate(), topology(host=False, resource=False))
    prediction = next(item for item in result.predictions if item.target_service_id.endswith("::api"))
    coefficient = next(item for item in model.export_sparse_coefficients()
                       if item.target_service_id.endswith("::api") and item.parent_service_id.endswith("::api"))
    assert prediction.predicted_value == 0.0
    assert coefficient.coefficient == pytest.approx(-12.0 / 9.0)
    assert coefficient.positive_support == 0.0
    assert np.allclose(model.model_covariance("cluster-a::ns::api"),
                       model.model_covariance("cluster-a::ns::api").T)


def test_freeze_preserves_parameters_but_still_predicts():
    model = learner(service_min_updates=1)
    model.process_window([state(name, 1, 0) for name in ("api", "db", "peer", "unrelated")],
                         gate(), topology())
    model.process_window([state(name, 2, 1) for name in ("api", "db", "peer", "unrelated")],
                         gate(), topology())
    before = model.model_state("cluster-a::ns::api")
    frozen = model.process_window(
        [state(name, 3, 2, source_alert_state="hard") for name in ("api", "db", "peer", "unrelated")],
        gate(update=False), topology(),
    )
    after = model.model_state("cluster-a::ns::api")
    assert np.array_equal(before.theta, after.theta)
    assert np.array_equal(before.covariance, after.covariance)
    assert before.update_count == after.update_count
    prediction = next(item for item in frozen.predictions if item.target_service_id.endswith("::api"))
    assert prediction.frozen and not prediction.updated
    assert prediction.skipped_reason == "update_gate_closed"


def test_dense_and_sparse_exports_agree():
    model = learner(service_min_updates=1)
    for window, value in enumerate((1.0, 2.0, 3.0)):
        model.process_window([state(name, value, window) for name in ("api", "db", "peer", "unrelated")],
                             gate(), topology())
    dense, service_ids, lags = model.export_dense_matrices()
    assert dense.shape == (1, 4, 4) and lags == [1]
    index = {service_id: i for i, service_id in enumerate(service_ids)}
    for item in model.export_sparse_coefficients():
        assert dense[0, index[item.target_service_id], index[item.parent_service_id]] == item.coefficient


def test_numerical_failure_is_explicit():
    model = learner(rls_forgetting_factor=1e-300)
    model.process_window([state(name, 0, 0) for name in ("api", "db", "peer", "unrelated")],
                         gate(), topology())
    with pytest.raises(NumericalPropagationError):
        model.process_window([state(name, 0, 1) for name in ("api", "db", "peer", "unrelated")],
                             gate(), topology())

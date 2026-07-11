from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from proberca.alerting import UpdateGate
from proberca.config import PropagationConfig
from proberca.data.schema import ServiceStateRecord
from proberca.propagation.service_rls import (
    NumericalPropagationError,
    PropagationTimeError,
    ServicePropagationLearner,
)
from proberca.propagation.service_model import (
    ServiceFeatureKey,
    ServicePropagationContribution,
    ServicePropagationPrediction,
)

from test_p4_dynamic_snapshot import window_states
from test_p4_service_propagation import gate, learner, propagation_config, state, topology


@pytest.mark.parametrize("field,value,error", [
    ("service_lags", [], ValueError),
    ("service_lags", [0], ValueError),
    ("service_lags", [1, 1], ValueError),
    ("rls_forgetting_factor", 0.0, ValueError),
    ("rls_forgetting_factor", 1.1, ValueError),
    ("rls_initial_covariance", 0.0, ValueError),
    ("rls_initial_covariance", float("inf"), ValueError),
    ("service_min_updates", 0, ValueError),
    ("service_min_observation_quality", -0.1, ValueError),
    ("service_min_observation_quality", 1.1, ValueError),
    ("service_max_gap_windows", 0, ValueError),
    ("topology_reconfigure_min_updates", 0, ValueError),
    ("include_self_history", False, ValueError),
    ("include_impact_parents", "yes", TypeError),
    ("include_host_parents", 1, TypeError),
    ("include_resource_parents", None, TypeError),
])
def test_propagation_config_strict_boundaries(field, value, error):
    payload = propagation_config().__dict__.copy()
    payload[field] = value
    with pytest.raises(error):
        PropagationConfig.from_dict(payload)


@pytest.mark.parametrize("changes,error", [
    ({"timestamp_ns": 0}, ValueError),
    ({"window_start_ns": 2_000_000_000}, ValueError),
    ({"window_end_ns": 0}, ValueError),
    ({"service_id": "cluster-a::ns::wrong"}, ValueError),
    ({"cluster_id": ""}, ValueError),
    ({"namespace": ""}, ValueError),
    ({"service_name": ""}, ValueError),
    ({"value": float("inf")}, ValueError),
    ({"value": float("nan")}, ValueError),
    ({"observation_quality": -0.1}, ValueError),
    ({"observation_quality": 1.1}, ValueError),
    ({"missing_families": ["cpu", "cpu"]}, ValueError),
    ({"baseline_ready": 1}, TypeError),
    ({"source_alert_state": "incident"}, ValueError),
])
def test_service_state_strict_boundaries(changes, error):
    with pytest.raises(error):
        replace(state("api", 1, 0), **changes)


@pytest.mark.parametrize("state_name", ["soft", "hard", "recovery"])
def test_closed_alert_gates_freeze_without_updating(state_name):
    model = learner(service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    before = model.model_state("cluster-a::ns::api")
    records = window_states(1, source_alert_state=state_name)
    result = model.process_window(records, gate(update=False), topology())
    after = model.model_state("cluster-a::ns::api")
    assert np.array_equal(before.theta, after.theta)
    assert np.array_equal(before.covariance, after.covariance)
    assert all(item.frozen and not item.updated and item.gate_state == state_name
               for item in result.predictions)


def test_isolated_edge_gate_behavior_is_consumed_without_reinterpretation():
    model = learner(service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    edge_gate = UpdateGate(True, True, [], ["edge-id"], True, False, False,
                           False, False, True)
    result = model.process_window(window_states(1, source_alert_state="edge_anomaly"),
                                  edge_gate, topology())
    assert result.predictions and all(item.updated for item in result.predictions)


@pytest.mark.parametrize("disabled,parent_suffix", [
    ("include_impact_parents", "::db"),
    ("include_host_parents", "::peer"),
    ("include_resource_parents", "::peer"),
])
def test_parent_type_configuration_is_respected(disabled, parent_suffix):
    changes = {disabled: False}
    source = topology(host=disabled != "include_resource_parents",
                      resource=disabled != "include_host_parents")
    model = learner(**changes)
    model.process_window(window_states(0), gate(), source)
    parents = [item.parent_service_id for item in model.feature_keys("cluster-a::ns::api")]
    assert not any(item.endswith(parent_suffix) for item in parents)


def test_records_outside_topology_cluster_fail_fast():
    model = learner()
    invalid = replace(state("api", 1, 0), cluster_id="cluster-b",
                      service_id="cluster-b::ns::api")
    with pytest.raises(ValueError, match="topology"):
        model.process_window([invalid], gate(), topology())


def test_conflicting_duplicate_service_state_fails():
    model = learner()
    first = state("api", 1, 0)
    with pytest.raises(PropagationTimeError):
        model.process_window([first, replace(first, value=2.0)], gate(), topology())


def test_current_low_quality_target_predicts_but_does_not_update():
    model = learner(service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    records = [replace(item, observation_quality=0.1) if item.service_name == "api" else item
               for item in window_states(1)]
    result = model.process_window(records, gate(), topology())
    prediction = next(item for item in result.predictions if item.target_service_id.endswith("::api"))
    assert not prediction.updated and prediction.skipped_reason == "low_observation_quality"


def test_contributions_sum_exactly_and_have_no_hidden_term():
    model = learner(service_min_updates=1)
    model.process_window(window_states(0), gate(), topology())
    result = model.process_window(window_states(1), gate(), topology())
    for prediction in result.predictions:
        assert prediction.predicted_value == pytest.approx(
            sum(item.contribution_value for item in prediction.contributions)
        )
        for contribution in prediction.contributions:
            assert contribution.contribution_value == pytest.approx(
                contribution.coefficient * contribution.parent_value
            )
            assert contribution.positive_support == max(contribution.coefficient, 0.0)


def test_two_service_lag_one_coefficient_recovery():
    model = learner(service_min_updates=20, rls_initial_covariance=1_000_000.0,
                    include_host_parents=False, include_resource_parents=False)
    source = topology(host=False, resource=False)
    db_values = [np.sin(index * 0.37) + 0.3 * np.cos(index * 0.11) for index in range(180)]
    api_values = [0.0] + [0.65 * db_values[index - 1] for index in range(1, 180)]
    for window, (api, db) in enumerate(zip(api_values, db_values)):
        model.process_window(window_states(window, {
            "api": api, "db": db, "peer": 0.0, "unrelated": 0.0,
        }), gate(), source)
    coefficient = next(item for item in model.export_sparse_coefficients()
                       if item.target_service_id.endswith("::api")
                       and item.parent_service_id.endswith("::db") and item.lag == 1)
    assert coefficient.coefficient == pytest.approx(0.65, abs=2e-3)


def test_multi_lag_signed_coefficient_recovery():
    model = learner(service_lags=[1, 2], service_min_updates=20,
                    rls_initial_covariance=1_000_000.0,
                    include_impact_parents=False, include_host_parents=False,
                    include_resource_parents=False)
    source = topology(with_impact=False, host=False, resource=False)
    values = [1.0, -0.4]
    for _ in range(200):
        values.append(0.55 * values[-1] - 0.2 * values[-2])
    for window, value in enumerate(values):
        model.process_window(window_states(window, {
            "api": value, "db": 0.0, "peer": 0.0, "unrelated": 0.0,
        }), gate(), source)
    coefficients = {item.lag: item.coefficient for item in model.export_sparse_coefficients()
                    if item.target_service_id.endswith("::api")
                    and item.parent_service_id.endswith("::api")}
    assert coefficients[1] == pytest.approx(0.55, abs=2e-3)
    assert coefficients[2] == pytest.approx(-0.2, abs=2e-3)


def test_forgetting_factor_tracks_a_changed_relation():
    model = learner(rls_forgetting_factor=0.9, rls_initial_covariance=1000.0,
                    include_impact_parents=False, include_host_parents=False,
                    include_resource_parents=False, service_min_updates=2)
    source = topology(with_impact=False, host=False, resource=False)
    value = 1.0
    for window in range(30):
        model.process_window(window_states(window, {"api": value, "db": 0, "peer": 0, "unrelated": 0}),
                             gate(), source)
        value *= 0.4
    before = next(item.coefficient for item in model.export_sparse_coefficients()
                  if item.target_service_id.endswith("::api"))
    value = 1.0
    for window in range(30, 70):
        model.process_window(window_states(window, {"api": value, "db": 0, "peer": 0, "unrelated": 0}),
                             gate(), source)
        value *= 0.9
    after = next(item.coefficient for item in model.export_sparse_coefficients()
                 if item.target_service_id.endswith("::api"))
    assert after > before


def test_corrupted_nonfinite_model_fails_instead_of_filling_zero():
    model = learner()
    model.process_window(window_states(0), gate(), topology())
    model._models["cluster-a::ns::api"].theta[0] = float("nan")
    with pytest.raises(NumericalPropagationError):
        model.process_window(window_states(1), gate(), topology())


@pytest.mark.parametrize("change", [
    {"coefficient": float("nan")},
    {"contribution_value": 99.0},
    {"positive_support": -1.0},
    {"relation_ids": []},
    {"relation_types": ["call"]},
])
def test_contribution_contract_rejects_inconsistent_fields(change):
    payload = dict(
        parent_service_id="cluster-a::ns::db",
        target_service_id="cluster-a::ns::api",
        lag=1,
        coefficient=0.5,
        parent_value=2.0,
        contribution_value=1.0,
        positive_support=0.5,
        relation_ids=["relation-id"],
        relation_types=["impact"],
    )
    payload.update(change)
    with pytest.raises((TypeError, ValueError)):
        ServicePropagationContribution(**payload)


def test_prediction_contract_rejects_hidden_term_and_target_mismatch():
    contribution = ServicePropagationContribution(
        "cluster-a::ns::db", "cluster-a::ns::api", 1, 0.5, 2.0, 1.0, 0.5,
        ["relation-id"], ["impact"],
    )
    base = dict(
        schema_version="1.0", record_type="service_propagation_prediction",
        timestamp_ns=2_000_000_000, cluster_id="cluster-a", namespace_scope=["ns"],
        topology_snapshot_id="top", model_snapshot_id="model",
        target_service_id="cluster-a::ns::api", predicted_value=1.0,
        actual_value=1.5, prediction_error=0.5, ready=True, frozen=False,
        updated=True, skipped_reason=None, gate_state="healthy", observation_quality=1.0,
        contributions=[contribution], config_fingerprint="a" * 64,
    )
    with pytest.raises(ValueError, match="sum"):
        ServicePropagationPrediction(**{**base, "predicted_value": 2.0})
    wrong = replace(contribution, target_service_id="cluster-a::ns::other")
    with pytest.raises(ValueError, match="target"):
        ServicePropagationPrediction(**{**base, "contributions": [wrong]})


def test_service_feature_key_is_strict():
    with pytest.raises(ValueError):
        ServiceFeatureKey("", 1)
    with pytest.raises(ValueError):
        ServiceFeatureKey("cluster-a::ns::api", 0)


@pytest.mark.parametrize("relative_path", [
    "proberca/propagation/service_rls.py",
    "proberca/propagation/service_model.py",
    "proberca/propagation/__init__.py",
])
def test_p4_production_has_no_empty_implementation(relative_path):
    tree = ast.parse(Path(relative_path).read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))


@pytest.mark.parametrize("forbidden", [
    "paymentservice",
    "checkoutservice",
    "frontend",
    "online boutique",
    "incidentlabel",
    "pearson",
    "spearman",
    "granger",
    "metric_name.startswith",
    "protocol.startswith",
])
def test_p4_production_has_no_hardcoding_or_label_leakage(forbidden):
    source = "\n".join(
        Path(path).read_text(encoding="utf-8").lower()
        for path in ("proberca/propagation/service_rls.py", "proberca/propagation/service_model.py")
    )
    assert forbidden not in source


@pytest.mark.parametrize("forbidden_path", [
    "proberca/aggregation/",
    "proberca/baseline/",
    "proberca/alerting/",
    "proberca/topology/",
    "proberca/candidates/",
    "proberca/inference/",
    "proberca/evidence/",
])
def test_git_diff_does_not_modify_forbidden_modules(forbidden_path):
    import subprocess
    names = subprocess.run(["git", "diff", "--name-only"], check=True,
                           capture_output=True, text=True).stdout.splitlines()
    assert not any(name.startswith(forbidden_path) for name in names)

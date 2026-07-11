from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from proberca.data.schema import AlertEvent
from proberca.propagation.metric_ridge import (
    MetricPropagationLearner,
    MetricPropagationNumericalError,
)

from test_p5_history_rules import NS, anomaly, candidate, config, gate, rule


def alert(state="soft", timestamp=10 * NS, alert_id=None):
    return AlertEvent(
        schema_version="1.0", alert_id=alert_id or f"alert-{state}",
        timestamp_ns=timestamp, state=state,
        trigger_services=["cluster-a::ns::api"], trigger_edges=[],
        service_scores={"cluster-a::ns::api": 3.0}, edge_scores={},
        reason='{"code":"test"}', frozen_baseline=state in {"hard", "recovery"},
        frozen_service_model=state in {"soft", "hard", "recovery"},
        frozen_metric_model=state in {"hard", "recovery"},
    )


def all_self_rules(lags=None):
    return [
        rule("request-self", "self_history", "request", "request", lags or [1]),
        rule("cpu-self", "self_history", "cpu", "cpu", lags or [1]),
    ]


def values_window(window, values, state="healthy"):
    return [
        anomaly("api", "request.lat", "request", values[0], window, source_alert_state=state),
        anomaly("api", "cpu.use", "cpu", values[1], window, source_alert_state=state),
        anomaly("db", "request.lat", "request", values[2], window, source_alert_state=state),
        anomaly("db", "cpu.use", "cpu", values[3], window, source_alert_state=state),
    ]


def train_learner(sequences, rules=None, **changes):
    learner = MetricPropagationLearner(config(rules or all_self_rules(), **changes), 1)
    for window, values in enumerate(zip(*sequences)):
        learner.ingest_healthy_window(values_window(window, values), gate())
    return learner


def geometric(coefficient, count=10, start=1.0):
    values = [start]
    for _ in range(count - 1):
        values.append(coefficient * values[-1])
    return values


def test_soft_prepares_masked_ridge_and_exact_ar_coefficients():
    sequences = [geometric(value, 10, start) for value, start in
                 ((0.7, 1.0), (0.5, 2.0), (-0.4, 1.5), (0.8, 0.7))]
    learner = train_learner(sequences, metric_ridge=1e-8, metric_min_training_rows=3)
    result = learner.prepare_for_alert(alert(), candidate())
    assert result.info.lifecycle_state == "PREPARED"
    assert result.info.global_ready and result.info.ready_target_count == 4
    coefficients = {item.target_node_id: item.coefficient for item in learner.export_sparse_coefficients()}
    assert coefficients["cluster-a::ns::api::request.lat"] == pytest.approx(0.7, abs=1e-5)
    assert coefficients["cluster-a::ns::db::request.lat"] == pytest.approx(-0.4, abs=1e-5)


def test_training_rows_are_before_alert_and_metadata_is_explicit():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    matrix = learner.training_matrix_info("cluster-a::ns::api::request.lat")
    assert matrix.row_timestamps and max(matrix.row_timestamps) < 10 * NS
    assert matrix.effective_training_rows == len(matrix.row_timestamps)
    assert matrix.training_start_ns == min(matrix.row_timestamps)
    assert matrix.training_end_ns == max(matrix.row_timestamps)
    assert matrix.excluded_row_counts["missing_parent_lag"] == 1


def test_missing_parent_rows_are_excluded_not_zero_filled():
    learner = MetricPropagationLearner(config(all_self_rules(), metric_min_training_rows=3), 1)
    for index in range(8):
        records = values_window(index, (1, 1, 1, 1))
        if index == 3:
            records = [item for item in records if item.node_id != "cluster-a::ns::api::request.lat"]
        learner.ingest_healthy_window(records, gate())
    learner.prepare_for_alert(alert(), candidate())
    info = learner.training_matrix_info("cluster-a::ns::api::request.lat")
    assert info.excluded_row_counts["missing_parent_lag"] >= 1
    assert 4 * NS not in info.row_timestamps


def test_same_service_and_impact_cross_metric_recovery():
    rules = [
        rule("api-cpu-to-request", "same_service", "request", "cpu", [1],
             target_names=["request.lat"], parent_names=["cpu.use"]),
        rule("db-cpu-impact", "impact", "request", "cpu", [1],
             target_names=["request.lat"], parent_names=["cpu.use"]),
        rule("cpu-self", "self_history", "cpu", "cpu", [1]),
    ]
    api_cpu = [np.sin(index * 0.43) for index in range(12)]
    db_cpu = [np.cos(index * 0.31) for index in range(12)]
    api_request = [0.0] + [0.6 * api_cpu[i - 1] + 0.25 * db_cpu[i - 1] for i in range(1, 12)]
    db_request = [0.0] + [0.4 * db_cpu[i - 1] for i in range(1, 12)]
    learner = train_learner([api_request, api_cpu, db_request, db_cpu], rules,
                            metric_ridge=1e-8, metric_min_training_rows=4)
    learner.prepare_for_alert(alert(), candidate())
    coefficients = {(item.target_node_id, item.parent_node_id): item.coefficient
                    for item in learner.export_sparse_coefficients()}
    target = "cluster-a::ns::api::request.lat"
    assert coefficients[(target, "cluster-a::ns::api::cpu.use")] == pytest.approx(0.6, abs=1e-4)
    assert coefficients[(target, "cluster-a::ns::db::cpu.use")] == pytest.approx(0.25, abs=1e-4)


def test_multi_lag_signed_recovery_and_no_intercept():
    values = [1.0, -0.5]
    for _ in range(20):
        values.append(0.55 * values[-1] - 0.2 * values[-2])
    learner = train_learner([values] * 4, all_self_rules([1, 2]),
                            metric_lags=[1, 2], metric_ridge=1e-9,
                            metric_min_training_rows=5)
    learner.prepare_for_alert(alert(timestamp=22 * NS),
                              replace(candidate(), alert_timestamp_ns=22 * NS,
                                      topology_valid_to_ns=30 * NS))
    coefficients = {item.lag: item.coefficient for item in learner.export_sparse_coefficients()
                    if item.target_node_id == "cluster-a::ns::api::request.lat"}
    assert coefficients[1] == pytest.approx(0.55, abs=1e-4)
    assert coefficients[2] == pytest.approx(-0.2, abs=1e-4)


def test_condition_limit_marks_target_and_global_not_ready():
    learner = train_learner([[1.0] * 10] * 4, all_self_rules([1, 2]),
                            metric_lags=[1, 2], metric_ridge=1e-12,
                            metric_max_condition_number=1.01,
                            metric_min_training_rows=3)
    result = learner.prepare_for_alert(alert(), candidate())
    assert not result.info.global_ready
    assert result.info.lifecycle_state == "NOT_READY"
    assert result.info.unready_targets == sorted(candidate().candidate_node_ids)
    assert all(not item.ready for item in learner.export_sparse_coefficients())


def test_min_rows_does_not_drop_unready_target():
    learner = train_learner([[1, 2, 3]] * 4, metric_min_training_rows=5)
    result = learner.prepare_for_alert(alert(timestamp=4 * NS),
                                       replace(candidate(), alert_timestamp_ns=4 * NS))
    assert not result.info.global_ready and result.info.target_count == 4
    assert len(result.info.unready_targets) == 4


def test_one_missing_candidate_history_does_not_discard_ready_targets():
    learner = MetricPropagationLearner(config(all_self_rules(), metric_min_training_rows=3), 1)
    for index in range(8):
        records = [item for item in values_window(index, (1, 1, 1, 1))
                   if item.node_id != "cluster-a::ns::db::cpu.use"]
        learner.ingest_healthy_window(records, gate())
    result = learner.prepare_for_alert(alert(), candidate())
    assert not result.info.global_ready
    assert result.info.ready_target_count == 3
    assert result.info.unready_targets == ["cluster-a::ns::db::cpu.use"]


def test_dense_sparse_mask_and_readiness_are_consistent():
    learner = train_learner([geometric(0.6)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    dense, structural_mask, readiness, node_ids = learner.export_dense_matrices()
    assert dense.shape == structural_mask.shape == (2, 4, 4)
    assert readiness.shape == (4,) and readiness.all()
    index = {node_id: i for i, node_id in enumerate(node_ids)}
    for item in learner.export_sparse_coefficients():
        position = (item.lag - 1, index[item.target_node_id], index[item.parent_node_id])
        assert structural_mask[position]
        assert dense[position] == item.coefficient
    assert np.all(dense[~structural_mask] == 0.0)


def test_prediction_uses_only_lagged_history_and_contributions_sum():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    current_a = values_window(10, (100, 100, 100, 100), state="soft")
    current_b = values_window(10, (999, 999, 999, 999), state="soft")
    left = learner.predict_window(11 * NS, current_a)
    right = learner.predict_window(11 * NS, current_b)
    assert [item.predicted_value for item in left] == [item.predicted_value for item in right]
    for prediction in left:
        assert prediction.predicted_value == pytest.approx(
            sum(item.contribution_value for item in prediction.contributions))
        assert prediction.actual_value == 100


def test_missing_prediction_lag_is_unavailable_without_zero():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    predictions = learner.predict_window(20 * NS, None)
    assert all(not item.available and item.unavailable_reason == "missing_prediction_feature"
               and item.predicted_value is None for item in predictions)


def test_hard_same_candidate_freezes_without_coefficient_change():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    soft_candidate = candidate()
    learner.prepare_for_alert(alert(), soft_candidate)
    before = learner.export_sparse_coefficients()
    hard_candidate = replace(soft_candidate, alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard")
    result = learner.freeze_for_hard(alert("hard"), hard_candidate)
    assert result.info.lifecycle_state == "FROZEN" and result.info.frozen
    assert learner.export_sparse_coefficients() == before
    with pytest.raises(RuntimeError):
        learner.prepare_for_alert(alert(), soft_candidate)


def test_direct_hard_fits_prior_history_and_freezes():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    hard_candidate = replace(candidate(), alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard")
    result = learner.freeze_for_hard(alert("hard"), hard_candidate)
    assert result.info.lifecycle_state == "FROZEN"
    assert result.info.training_end_ns < alert("hard").timestamp_ns


def test_direct_hard_with_insufficient_history_is_not_ready_but_frozen():
    learner = train_learner([[1, 2, 3]] * 4, metric_min_training_rows=5)
    hard_candidate = replace(candidate(), alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard", alert_timestamp_ns=4 * NS)
    result = learner.freeze_for_hard(alert("hard", timestamp=4 * NS), hard_candidate)
    assert result.info.lifecycle_state == "NOT_READY"
    assert result.info.frozen and not result.info.global_ready


def test_target_without_legal_features_has_unavailable_prediction():
    request_only = [rule("request-self", "self_history", "request", "request", [1])]
    learner = train_learner([geometric(0.5)] * 4, request_only,
                            metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    predictions = learner.predict_window(11 * NS, None)
    cpu_predictions = [item for item in predictions if item.target_node_id.endswith("::cpu.use")]
    assert cpu_predictions and all(not item.available and item.predicted_value is None
                                   for item in cpu_predictions)


def test_hard_candidate_change_rebuilds_and_reports_issue():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    changed = replace(candidate("hard", "alert-hard", "candidate-2"), rca_eligible=True)
    result = learner.freeze_for_hard(alert("hard"), changed)
    assert "model_rebuilt_at_hard" in [item.reason_code for item in result.issues]
    assert result.info.lifecycle_state == "FROZEN"


def test_soft_recovery_archives_and_recovery_keeps_frozen():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    learner.prepare_for_alert(alert(), candidate())
    assert learner.archive_soft_model().lifecycle_state == "ARCHIVED"
    other = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    hard_candidate = replace(candidate(), alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard")
    other.freeze_for_hard(alert("hard"), hard_candidate)
    before = other.export_sparse_coefficients()
    assert other.handle_recovery().lifecycle_state == "FROZEN"
    assert other.export_sparse_coefficients() == before


def test_nonfinite_solver_input_raises_specialized_error():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    first = learner.history.series("cluster-a::ns::api::request.lat")[0]
    object.__setattr__(first, "signed_z", float("nan"))
    with pytest.raises(MetricPropagationNumericalError):
        learner.prepare_for_alert(alert(), candidate())

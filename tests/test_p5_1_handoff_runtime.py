from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import asdict
from dataclasses import replace
from pathlib import Path

import pytest

import test_p1_data_contracts as p1
from proberca.alerting import UpdateGate
from proberca.baseline import RobustBaselineStore
from proberca.config import BaselineConfig, MetricSignalSpec
from proberca.data.schema import EdgeMetricRecord, NodeAnomalyRecord
from proberca.propagation.metric_history import (
    MetricRuntimeHistoryStore,
    MetricTrainingHistoryStore,
    node_anomaly_from_p2,
)
from proberca.propagation.metric_ridge import MetricPropagationLearner

from test_p5_history_rules import NS, candidate, config, gate
from test_p5_metric_ridge import alert, all_self_rules, geometric, train_learner, values_window


def baseline_config():
    return BaselineConfig.from_dict({
        "healthy_history_sec": 20, "min_healthy_windows": 3,
        "min_scale": 0.1, "z_cap": 6.0,
    })


def source_metric(window, value, metric="request.lat"):
    return p1.make_node(
        timestamp_ns=(window + 1) * NS, window_sec=1,
        cluster_id="cluster-a", namespace="ns", service_name="api",
        pod_uid=None, container_id=None, scope="service",
        metric_family="request", metric_name=metric, metric_kind="gauge",
        unit="ms", value=float(value),
    )


def signal_spec(record):
    return MetricSignalSpec.from_dict({
        "record_type": "node_metric", "metric_family": record.metric_family,
        "metric_name": record.metric_name, "protocol": None,
        "transform": "identity", "polarity": "increase_bad",
        "rare_event_threshold": None, "direct_hard": False, "z_cap": 6.0,
        "aggregation_output_id": record.stable_id,
    })


def real_p2_score(window=3, value=2.0):
    baseline = baseline_config()
    store = RobustBaselineStore(baseline, 1)
    for index, healthy in enumerate((1.0, 1.1, 0.9)):
        record = source_metric(index, healthy)
        store.update(record, signal_spec(record), state="healthy")
    record = source_metric(window, value)
    result = store.score(record, signal_spec(record), record.timestamp_ns - NS,
                         record.timestamp_ns)
    assert result.score is not None
    spec = signal_spec(record)
    return record, result.score, spec, baseline


def adapted(window=3, value=2.0, state="healthy", update_gate=None):
    record, score, spec, baseline = real_p2_score(window, value)
    return node_anomaly_from_p2(record, score, update_gate or gate(), spec, state, baseline, 1)


def test_real_p2_output_adapts_without_changing_scores():
    record, score, spec, baseline = real_p2_score()
    converted = node_anomaly_from_p2(record, score, gate(), spec, "healthy", baseline, 1)
    assert converted.signed_z == score.signed_z
    assert converted.anomaly_score == score.anomaly
    assert converted.baseline_ready is True
    assert converted.observation_quality == score.coverage * (1.0 - score.event_loss_rate)
    assert converted.source_metric_record_id == record.stable_id
    assert converted.signal_spec_id == hashlib.sha256(
        json.dumps(asdict(spec), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert converted.baseline_config_fingerprint == hashlib.sha256(
        json.dumps({"config": asdict(baseline), "window_sec": 1},
                   sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def test_adapter_preserves_source_window_and_identity():
    record, score, spec, baseline = real_p2_score()
    converted = node_anomaly_from_p2(record, score, gate(), spec, "soft", baseline, 1)
    assert converted.timestamp_ns == record.timestamp_ns
    assert converted.window_start_ns == record.timestamp_ns - record.window_sec * NS
    assert converted.window_end_ns == record.timestamp_ns
    assert converted.service_id == "cluster-a::ns::api"
    assert converted.node_id == record.stable_id
    assert converted.source_alert_state == "soft"


@pytest.mark.parametrize("missing", ["baseline_config", "coverage", "event_loss_rate"])
def test_adapter_missing_p2_metadata_fails_fast(missing):
    record, score, spec, baseline = real_p2_score()
    if missing == "baseline_config":
        with pytest.raises(TypeError, match="metadata"):
            node_anomaly_from_p2(record, score, gate(), spec, "healthy", None, 1)
    else:
        score = replace(score, **{missing: None})
        with pytest.raises(ValueError, match="metadata"):
            node_anomaly_from_p2(record, score, gate(), spec, "healthy", baseline, 1)


def test_adapter_rejects_edge_metric_and_does_not_guess_metric_semantics():
    record, score, spec, baseline = real_p2_score()
    edge = p1.make_edge(metric_name=record.metric_name)
    with pytest.raises(TypeError):
        node_anomaly_from_p2(edge, score, gate(), spec, "healthy", baseline, 1)
    renamed = replace(record, metric_name="misleading.cpu.p99.rate")
    with pytest.raises(ValueError):
        node_anomaly_from_p2(renamed, score, gate(), spec, "healthy", baseline, 1)


def test_adapter_does_not_recompute_after_baseline_changes():
    record, score, spec, baseline = real_p2_score()
    converted = node_anomaly_from_p2(record, score, gate(), spec, "healthy", baseline, 1)
    assert converted.signed_z == score.signed_z and converted.anomaly_score == score.anomaly


@pytest.mark.parametrize("state_name,training_count,runtime_count", [
    ("healthy", 1, 1), ("soft", 0, 1), ("hard", 0, 1),
    ("recovery", 0, 1), ("edge_anomaly", 1, 1),
])
def test_training_and_runtime_store_write_rules(state_name, training_count, runtime_count):
    cfg = config()
    training = MetricTrainingHistoryStore(cfg, 1)
    runtime = MetricRuntimeHistoryStore(cfg, 1)
    item = adapted(state=state_name)
    update = state_name in {"healthy", "edge_anomaly"}
    update_gate = gate(update=update)
    assert training.ingest_healthy_window([item], update_gate).inserted_count == training_count
    assert runtime.ingest_runtime_window([item]).inserted_count == runtime_count


def test_training_and_runtime_buffers_are_independent():
    cfg = config()
    training = MetricTrainingHistoryStore(cfg, 1)
    runtime = MetricRuntimeHistoryStore(cfg, 1)
    item = adapted()
    training.ingest_healthy_window([item], gate())
    runtime.ingest_runtime_window([item])
    assert training._records is not runtime._records
    runtime._records[item.node_id].clear()
    assert training.get(item.node_id, item.timestamp_ns) == item


def prepared_learner(**changes):
    learner = train_learner([geometric(0.5)] * 4, all_self_rules(),
                            metric_min_training_rows=3, **changes)
    learner.prepare_for_alert(alert(), candidate())
    return learner


def hard_candidate(timestamp=11 * NS):
    return replace(candidate(), alert_state="hard", rca_eligible=True,
                   alert_id="alert-hard", alert_timestamp_ns=timestamp,
                   topology_valid_to_ns=30 * NS)


def closed_gate():
    return UpdateGate(False, False, [], [], False, False, True, True, True, True)


def test_process_window_predicts_before_current_runtime_ingest():
    left = prepared_learner()
    right = prepared_learner()
    low = values_window(10, (10, 10, 10, 10), "hard")
    high = values_window(10, (999, 999, 999, 999), "hard")
    left_result = left.process_window(low, closed_gate(), alert("hard", 11 * NS), hard_candidate())
    right_result = right.process_window(high, closed_gate(), alert("hard", 11 * NS), hard_candidate())
    assert [item.predicted_value for item in left_result.predictions] == [
        item.predicted_value for item in right_result.predictions
    ]
    assert all(left.runtime_history.get(item.node_id, 11 * NS) == item for item in low)


def test_hard_t_value_affects_t_plus_one_but_not_coefficients_or_training():
    left = prepared_learner()
    right = prepared_learner()
    before_coefficients = left.export_sparse_coefficients()
    before_rows = {node: left.training_matrix_info(node).effective_training_rows
                   for node in candidate().candidate_node_ids}
    before_cutoff = left.training_history.cutoff_timestamp_ns
    left.process_window(values_window(10, (10, 10, 10, 10), "hard"), closed_gate(),
                        alert("hard", 11 * NS), hard_candidate())
    right.process_window(values_window(10, (20, 20, 20, 20), "hard"), closed_gate(),
                         alert("hard", 11 * NS), hard_candidate())
    left_next = left.process_window(values_window(11, (1, 1, 1, 1), "recovery"), closed_gate(),
                                    None, None)
    right_next = right.process_window(values_window(11, (1, 1, 1, 1), "recovery"), closed_gate(),
                                      None, None)
    assert [item.predicted_value for item in left_next.predictions] != [
        item.predicted_value for item in right_next.predictions
    ]
    assert left.export_sparse_coefficients() == before_coefficients
    assert {node: left.training_matrix_info(node).effective_training_rows
            for node in candidate().candidate_node_ids} == before_rows
    assert left.training_history.cutoff_timestamp_ns == before_cutoff


def test_direct_hard_without_soft_supports_next_window_runtime_prediction():
    learner = train_learner([geometric(0.5)] * 4, all_self_rules(),
                            metric_min_training_rows=3)
    hard_result = learner.process_window(
        values_window(10, (8, 8, 8, 8), "hard"), closed_gate(),
        alert("hard", 11 * NS), hard_candidate(),
    )
    assert hard_result.lifecycle_result.info.frozen
    next_result = learner.process_window(
        values_window(11, (1, 1, 1, 1), "recovery"), closed_gate(), None, None
    )
    assert all(item.available for item in next_result.predictions)
    assert all(item.contributions[0].parent_value == 8 for item in next_result.predictions)


@pytest.mark.parametrize("state_name", ["soft", "hard", "recovery"])
def test_incident_windows_only_change_runtime_history(state_name):
    learner = prepared_learner()
    training_before = learner.training_history.to_dict()
    timestamp = 11 * NS
    optional_alert = alert("hard", timestamp) if state_name == "hard" else None
    optional_candidate = hard_candidate(timestamp) if state_name == "hard" else None
    learner.process_window(values_window(10, (5, 5, 5, 5), state_name), closed_gate(),
                           optional_alert, optional_candidate)
    assert learner.training_history.to_dict() == training_before
    assert learner.runtime_history.cutoff_timestamp_ns == timestamp


def test_runtime_gap_never_falls_back_to_training_history():
    learner = prepared_learner(metric_max_gap_windows=1)
    learner.process_window(values_window(10, (5, 5, 5, 5), "hard"), closed_gate(),
                           alert("hard", 11 * NS), hard_candidate())
    result = learner.process_window(values_window(14, (5, 5, 5, 5), "recovery"),
                                    closed_gate(), None, None)
    assert all(not item.available and item.unavailable_reason == "missing_prediction_feature"
               for item in result.predictions)
    assert any(issue.reason_code == "runtime_gap" for issue in result.runtime_result.issues)


def test_online_and_replay_use_same_process_order():
    online = prepared_learner()
    replay = prepared_learner()
    batches = [
        (values_window(10, (5, 5, 5, 5), "hard"), closed_gate(),
         alert("hard", 11 * NS), hard_candidate()),
        (values_window(11, (2, 2, 2, 2), "recovery"), closed_gate(), None, None),
    ]
    online_results = [online.process_window(*batch) for batch in batches]
    replay_results = replay.process_replay(list(reversed(batches)))
    assert [[item.predicted_value for item in result.predictions] for result in online_results] == [
        [item.predicted_value for item in result.predictions] for result in replay_results
    ]


def test_dual_history_snapshot_restores_frozen_continuation(tmp_path):
    uninterrupted = prepared_learner()
    hard_batch = (values_window(10, (5, 5, 5, 5), "hard"), closed_gate(),
                  alert("hard", 11 * NS), hard_candidate())
    uninterrupted.process_window(*hard_batch)
    path = tmp_path / "p51-model"
    uninterrupted.snapshot(path)
    restored = MetricPropagationLearner.restore(path, uninterrupted.config, 1,
                                                expected_candidate=hard_candidate())
    assert restored.training_history.to_dict() == uninterrupted.training_history.to_dict()
    assert restored.runtime_history.to_dict() == uninterrupted.runtime_history.to_dict()
    assert restored.training_history._records is not restored.runtime_history._records
    next_batch = (values_window(11, (2, 2, 2, 2), "recovery"), closed_gate(), None, None)
    assert restored.process_window(*next_batch).predictions == uninterrupted.process_window(*next_batch).predictions
    with pytest.raises(RuntimeError):
        restored.prepare_for_alert(alert(), candidate())


@pytest.mark.parametrize("relative_path", [
    "proberca/baseline/core.py", "proberca/propagation/metric_history.py",
    "proberca/propagation/metric_ridge.py",
])
def test_p51_production_has_no_empty_implementation(relative_path):
    tree = ast.parse(Path(relative_path).read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))


@pytest.mark.parametrize("forbidden", [
    "paymentservice", "checkoutservice", "online boutique", "incidentlabel",
    "metric_name.startswith", "median(", "mad =", "graph_sparse_inversion",
])
def test_p51_handoff_runtime_has_no_guessing_recompute_or_label_leakage(forbidden):
    source = "\n".join(Path(path).read_text(encoding="utf-8").lower() for path in (
        "proberca/propagation/metric_history.py", "proberca/propagation/metric_ridge.py",
    ))
    assert forbidden not in source

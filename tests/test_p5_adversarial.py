from __future__ import annotations

import ast
import re
import subprocess
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

import test_p1_data_contracts as p1
from proberca.config import MetricParentRule, PropagationConfig
from proberca.data.io import (
    read_record_json,
    read_records_jsonl,
    read_records_parquet,
    write_record_json,
    write_records_jsonl,
    write_records_parquet,
)
from proberca.data.schema import NodeAnomalyRecord
from proberca.propagation.metric_model import (
    MetricPropagationContribution,
    MetricPropagationPrediction,
)
from proberca.propagation.metric_ridge import MetricPropagationLearner

from test_p5_history_rules import NS, anomaly, candidate, config, gate, rule, window
from test_p5_metric_ridge import alert, all_self_rules, geometric, train_learner, values_window


@pytest.mark.parametrize("field,value,error", [
    ("metric_lags", [], ValueError),
    ("metric_lags", [0], ValueError),
    ("metric_lags", [1, 1], ValueError),
    ("metric_ridge", 0.0, ValueError),
    ("metric_history_sec", 0, ValueError),
    ("metric_min_training_rows", 1, ValueError),
    ("metric_min_observation_quality", -0.1, ValueError),
    ("metric_min_observation_quality", 1.1, ValueError),
    ("metric_max_condition_number", 1.0, ValueError),
    ("metric_max_gap_windows", 0, ValueError),
    ("metric_model_cache_size", 0, ValueError),
    ("metric_include_self_history", False, ValueError),
])
def test_config_adversarial_boundaries(field, value, error):
    payload = config().__dict__.copy()
    payload["metric_parent_rules"] = [item.to_dict() for item in payload["metric_parent_rules"]]
    payload[field] = value
    with pytest.raises(error):
        PropagationConfig.from_dict(payload)


@pytest.mark.parametrize("field,value", [
    ("rule_id", ""), ("target_family", ""), ("parent_family", ""),
    ("provenance_label", ""), ("enabled", 1), ("require_signal_spec", 0),
    ("target_metric_names", []), ("parent_metric_names", ["x", "x"]),
    ("lags", []), ("lags", [0]), ("lags", [1, 1]),
])
def test_parent_rule_strict_boundaries(field, value):
    payload = rule().__dict__.copy()
    payload[field] = value
    with pytest.raises((TypeError, ValueError)):
        MetricParentRule.from_dict(payload)


def test_parent_rule_rejects_unknown_metric_family():
    with pytest.raises(ValueError, match="family"):
        MetricParentRule.from_dict({**rule().__dict__, "target_family": "unknown-family"})


def test_history_duration_must_cover_lags_at_runtime_window_size():
    short = config(metric_history_sec=2, metric_lags=[1, 2])
    with pytest.raises(ValueError, match="history"):
        MetricPropagationLearner(short, window_sec=2)


def test_node_anomaly_json_jsonl_parquet_round_trip(tmp_path):
    records = window(0)
    json_path = tmp_path / "one.json"
    jsonl_path = tmp_path / "many.jsonl"
    parquet_path = tmp_path / "many.parquet"
    write_record_json(json_path, records[0])
    write_records_jsonl(jsonl_path, records)
    write_records_parquet(parquet_path, records)
    assert read_record_json(json_path) == records[0]
    assert read_records_jsonl(jsonl_path) == records
    assert read_records_parquet(parquet_path) == records


def test_edge_metric_payload_cannot_parse_as_node_anomaly():
    edge = p1.make_edge()
    with pytest.raises((TypeError, ValueError)):
        NodeAnomalyRecord.from_dict(edge.to_dict())


def test_history_cluster_namespace_and_node_series_are_isolated():
    learner = MetricPropagationLearner(config(), 1)
    first = anomaly("api", "request.lat", "request", 1, 0)
    second = replace(first, cluster_id="cluster-b", service_id="cluster-b::ns::api",
                     node_id="cluster-b::ns::api::request.lat")
    third = replace(first, namespace="other", service_id="cluster-a::other::api",
                    node_id="cluster-a::other::api::request.lat")
    learner.ingest_healthy_window([first, second, third], gate())
    assert learner.history.node_ids() == sorted([first.node_id, second.node_id, third.node_id])


@pytest.mark.parametrize("state_name,update", [
    ("healthy", True), ("edge_anomaly", True), ("soft", False),
    ("hard", False), ("recovery", False),
])
def test_history_behavior_is_exactly_gate_driven(state_name, update):
    learner = MetricPropagationLearner(config(), 1)
    result = learner.ingest_healthy_window(window(0, source_alert_state=state_name), gate(update=update))
    assert (result.inserted_count == 4) is update


def test_training_matrix_reports_missing_target_parent_and_gap():
    learner = MetricPropagationLearner(config(all_self_rules(), metric_min_training_rows=3,
                                              metric_max_gap_windows=1), 1)
    for index in (0, 1, 5, 6, 7, 8):
        records = values_window(index, (1, 1, 1, 1))
        if index == 6:
            records = [item for item in records if not item.node_id.endswith("::api::request.lat")]
        learner.ingest_healthy_window(records, gate())
    learner.prepare_for_alert(alert(), candidate())
    info = learner.training_matrix_info("cluster-a::ns::api::request.lat")
    assert info.excluded_row_counts["missing_target"] >= 1
    assert info.excluded_row_counts["missing_parent_lag"] >= 1
    assert info.excluded_row_counts["history_gap"] >= 1


def test_low_quality_and_nonhealthy_rows_are_explicitly_excluded_if_present():
    learner = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    node_id = "cluster-a::ns::api::request.lat"
    low = replace(anomaly("api", "request.lat", "request", 1, 8), observation_quality=0.1)
    soft = replace(anomaly("api", "request.lat", "request", 1, 9), source_alert_state="soft")
    learner.history._records[node_id][low.timestamp_ns] = low
    learner.history._records[node_id][soft.timestamp_ns] = soft
    later_candidate = replace(candidate(), alert_timestamp_ns=11 * NS,
                              topology_valid_to_ns=20 * NS)
    learner.prepare_for_alert(alert(timestamp=11 * NS), later_candidate)
    info = learner.training_matrix_info(node_id)
    assert info.excluded_row_counts["low_quality"] >= 1
    assert info.excluded_row_counts["non_healthy_window"] >= 1


def test_hard_window_values_do_not_change_fitted_coefficients():
    left = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    right = train_learner([geometric(0.5)] * 4, metric_min_training_rows=3)
    left.ingest_healthy_window(values_window(10, (100, 100, 100, 100), "hard"), gate(update=False))
    right.ingest_healthy_window(values_window(10, (999, 999, 999, 999), "hard"), gate(update=False))
    hard_candidate = replace(candidate(), alert_state="hard", rca_eligible=True,
                             alert_id="alert-hard")
    left.freeze_for_hard(alert("hard"), hard_candidate)
    right.freeze_for_hard(alert("hard"), hard_candidate)
    assert left.export_sparse_coefficients() == right.export_sparse_coefficients()


def test_online_and_replay_fit_are_identical():
    cfg = config(all_self_rules(), metric_min_training_rows=3)
    online = MetricPropagationLearner(cfg, 1)
    replay = MetricPropagationLearner(cfg, 1)
    batches = [(values_window(index, (index + 1,) * 4), gate()) for index in range(8)]
    for records, update_gate in batches:
        online.ingest_healthy_window(records, update_gate)
    replay.ingest_replay(list(reversed(batches)))
    online.prepare_for_alert(alert(), candidate())
    replay.prepare_for_alert(alert(), candidate())
    assert online.export_sparse_coefficients() == replay.export_sparse_coefficients()


def test_ridge_penalty_comes_from_config_and_changes_solution():
    sequences = [geometric(0.6)] * 4
    low = train_learner(sequences, metric_ridge=1e-8, metric_min_training_rows=3)
    high = train_learner(sequences, metric_ridge=100.0, metric_min_training_rows=3)
    low.prepare_for_alert(alert(), candidate())
    high.prepare_for_alert(alert(), candidate())
    assert abs(low.export_sparse_coefficients()[0].coefficient) > abs(
        high.export_sparse_coefficients()[0].coefficient
    )


def valid_contribution(**changes):
    payload = dict(
        target_node_id="cluster-a::ns::api::request.lat",
        parent_node_id="cluster-a::ns::api::request.lat", lag=1,
        coefficient=-0.5, parent_value=2.0, contribution_value=-1.0,
        positive_support=0.0, relation_type="self_history", relation_types=["self_history"],
        relation_ids=["self"], rule_ids=["rule"],
    )
    payload.update(changes)
    return MetricPropagationContribution(**payload)


def test_contribution_exposes_deterministic_primary_relation_type():
    assert valid_contribution().relation_type == "self_history"


@pytest.mark.parametrize("changes", [
    {"coefficient": float("nan")}, {"parent_value": float("inf")},
    {"contribution_value": 0.0}, {"positive_support": 0.5},
])
def test_contribution_contract_rejects_invalid_math(changes):
    with pytest.raises(ValueError):
        valid_contribution(**changes)


def test_prediction_contract_rejects_hidden_term():
    contribution = valid_contribution()
    base = dict(
        schema_version="1.0", record_type="metric_propagation_prediction",
        timestamp_ns=2 * NS, alert_id="a", candidate_id="c", topology_snapshot_id="t",
        model_snapshot_id="m", target_node_id=contribution.target_node_id,
        predicted_value=-1.0, actual_value=None, ready=True, frozen=False,
        provisional=False, available=True, unavailable_reason=None,
        observation_quality=None, contributions=[contribution], config_fingerprint="a" * 64,
    )
    with pytest.raises(ValueError, match="sum"):
        MetricPropagationPrediction(**{**base, "predicted_value": 0.0})


@pytest.mark.parametrize("relative_path", [
    "proberca/propagation/metric_history.py",
    "proberca/propagation/metric_rules.py",
    "proberca/propagation/metric_model.py",
    "proberca/propagation/metric_ridge.py",
])
def test_p5_production_has_no_empty_implementation(relative_path):
    tree = ast.parse(Path(relative_path).read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))


@pytest.mark.parametrize("forbidden", [
    "paymentservice", "checkoutservice", "frontend", "online boutique",
    "incidentlabel", "pearson", "spearman", "granger", "linalg.pinv",
    "linalg.lstsq", "linalg.inv", "metric_name.startswith", "protocol.startswith",
    "graph_sparse_inversion", "x_shock", "edge residual", "node residual",
])
def test_p5_production_has_no_hardcoding_leakage_or_fallback(forbidden):
    source = "\n".join(Path(path).read_text(encoding="utf-8").lower() for path in (
        "proberca/propagation/metric_history.py", "proberca/propagation/metric_rules.py",
        "proberca/propagation/metric_model.py", "proberca/propagation/metric_ridge.py",
    ))
    assert forbidden not in source


def test_p5_uses_stable_sha256_not_python_hash():
    source = Path("proberca/propagation/metric_ridge.py").read_text(encoding="utf-8")
    assert "hashlib.sha256" in source
    assert re.search(r"(?<!_)\bhash\(", source) is None


@pytest.mark.parametrize("forbidden_path", [
    "proberca/aggregation/", "proberca/baseline/", "proberca/alerting/",
    "proberca/topology/", "proberca/candidates/", "proberca/inference/",
    "proberca/evidence/", "proberca/propagation/service_rls.py",
])
def test_git_diff_does_not_modify_forbidden_modules(forbidden_path):
    names = subprocess.run(["git", "diff", "--name-only"], check=True,
                           capture_output=True, text=True).stdout.splitlines()
    assert forbidden_path not in names and not any(name.startswith(forbidden_path)
                                                    for name in names if forbidden_path.endswith("/"))

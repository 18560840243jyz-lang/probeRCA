from __future__ import annotations

import ast
import inspect
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

import test_p1_data_contracts as p1
import test_p2_aggregation as agg
import test_p2_alerting as alert
import test_p2_baseline as base
from proberca.aggregation import AggregationPlan, CounterDeltaTracker, WindowAggregator
from proberca.alerting import AlertStateMachine
from proberca.baseline import MetricSignalRegistry, RobustBaselineStore
from proberca.config import (
    AlertStateConfig,
    BaselineConfig,
    CompositeAlertRule,
    MetricSignalSpec,
    MonotonicCounterPolicy,
    ScoreConfig,
    WindowConfig,
)


@pytest.mark.parametrize("payload", [
    {"window_sec": 0, "allowed_lateness_sec": 0},
    {"window_sec": -1, "allowed_lateness_sec": 0},
    {"window_sec": True, "allowed_lateness_sec": 0},
    {"window_sec": 1, "allowed_lateness_sec": -1},
])
def test_window_config_rejects_invalid_values(payload):
    with pytest.raises((ValueError, TypeError)):
        WindowConfig.from_dict(payload)


@pytest.mark.parametrize("payload", [
    {"healthy_history_sec": 0, "min_healthy_windows": 1, "min_scale": 1, "z_cap": 1},
    {"healthy_history_sec": 2, "min_healthy_windows": 3, "min_scale": 1, "z_cap": 1},
    {"healthy_history_sec": 3, "min_healthy_windows": 1, "min_scale": 0, "z_cap": 1},
    {"healthy_history_sec": 3, "min_healthy_windows": 1, "min_scale": 1, "z_cap": 0},
    {"healthy_history_sec": 3, "min_healthy_windows": 1, "min_scale": float("nan"), "z_cap": 1},
])
def test_baseline_config_rejects_invalid_values(payload):
    with pytest.raises((ValueError, TypeError)):
        BaselineConfig.from_dict(payload)


@pytest.mark.parametrize("changes", [
    {"recovery_threshold": 0.6},
    {"healthy_threshold": 2.1},
    {"soft_threshold": 4.1},
    {"hard_threshold": float("inf")},
    {"soft_consecutive_windows": 0},
    {"hard_consecutive_windows": -1},
    {"recovery_cooldown_sec": -1},
])
def test_alert_config_rejects_invalid_values(changes):
    payload = {
        "healthy_threshold": 0.5, "soft_threshold": 2.0, "soft_consecutive_windows": 2,
        "hard_threshold": 4.0, "hard_consecutive_windows": 2, "recovery_threshold": 0.25,
        "recovery_windows": 2, "recovery_cooldown_sec": 2, "edge_business_impact_threshold": 1.5,
    }
    payload.update(changes)
    with pytest.raises((ValueError, TypeError)):
        AlertStateConfig.from_dict(payload)


@pytest.mark.parametrize("record,spec_changes,matched", [
    (base.node(1, 1), {}, True),
    (base.node(1, 1, metric="other"), {}, False),
    (base.edge(1, 1), {"record_type": "edge_metric", "metric_family": None,
                       "metric_name": "opaque.edge", "protocol": "tcp",
                       "aggregation_output_id": base.edge(1, 1).stable_id}, True),
    (base.edge(1, 1), {"record_type": "edge_metric", "metric_family": None,
                       "metric_name": "opaque.edge", "protocol": "udp",
                       "aggregation_output_id": base.edge(1, 1).stable_id}, False),
])
def test_signal_registry_uses_exact_fields(record, spec_changes, matched):
    registry = MetricSignalRegistry([base.signal(**spec_changes)])
    assert (registry.resolve(record) is not None) is matched


@pytest.mark.parametrize("record_change", [
    {"metric_kind": "gauge"},
    {"scope": "service"},
    {"unit": "seconds"},
    {"metric_name": "unconfigured"},
])
def test_aggregation_rejects_contract_conflicts(record_change):
    output = "cluster-a::observability::service-a::request.count"
    record = agg.node(1, value=1)
    record = replace(record, **record_change)
    plan = AggregationPlan([(output, agg.spec())])
    with pytest.raises(ValueError):
        plan.execute([record], 0, 1_000_000_000)


@pytest.mark.parametrize("policy_name", ["use_current_value", "mark_missing", "reject_window"])
def test_counter_policy_is_explicit_and_snapshotted(tmp_path, policy_name):
    policy = agg.counter_policy(policy_name)
    tracker = CounterDeltaTracker(policy)
    tracker.process(agg.node(1, value=2, kind="monotonic_counter"))
    path = tmp_path / f"counter-{policy_name}.json"
    tracker.save_json(path)
    assert CounterDeltaTracker.load_json(path, policy).to_dict() == tracker.to_dict()
    other = agg.counter_policy("mark_missing" if policy_name != "mark_missing" else "use_current_value")
    with pytest.raises(ValueError, match="policy mismatch"):
        CounterDeltaTracker.load_json(path, other)


def test_window_snapshot_rejects_plan_change(tmp_path):
    output = "cluster-a::observability::service-a::request.count"
    plan = AggregationPlan([(output, agg.spec())])
    window = WindowAggregator(1, 0, plan)
    window.add(agg.node(1))
    path = tmp_path / "window.json"; window.save_json(path)
    changed = AggregationPlan([(output, agg.spec(output_metric_name="different"))])
    with pytest.raises(ValueError, match="plan mismatch"):
        WindowAggregator.load_json(path, changed)


def test_baseline_snapshot_rejects_config_change(tmp_path):
    store, spec = base.warm_store()
    path = tmp_path / "baseline.json"; store.save_json(path)
    with pytest.raises(ValueError, match="configuration mismatch"):
        RobustBaselineStore.load_json(path, base.baseline_config(min_scale=0.25), 1)


def test_alert_snapshot_rejects_config_change(tmp_path):
    machine = AlertStateMachine(alert.config(), 1)
    path = tmp_path / "alert.json"; machine.save_json(path)
    with pytest.raises(ValueError, match="configuration mismatch"):
        AlertStateMachine.load_json(path, alert.config(soft_threshold=2.1), 1)


@pytest.mark.parametrize("service,edge,expected", [
    (0.0, 0.0, "healthy"),
    (3.0, 0.0, "soft"),
    (5.0, 0.0, "hard"),
    (0.0, 5.0, "healthy"),
])
def test_single_window_thresholds_follow_consecutive_configuration(service, edge, expected):
    machine = AlertStateMachine(alert.config(soft_consecutive_windows=1, hard_consecutive_windows=1), 1)
    result = machine.step(1, alert.states(service=service, edge=edge, request=0.0), [], True)
    assert result.state == expected


@pytest.mark.parametrize("target", ["same_service", "same_edge"])
def test_composite_any_of_supports_both_target_types(target):
    rule = CompositeAlertRule.from_dict({
        "rule_id": f"any-{target}", "target": target, "all_of": [], "any_of": ["metric-a", "metric-b"],
        "threshold": 2.0, "consecutive_windows": 1, "resulting_level": "hard",
    })
    service_id = "cluster-a::ns::svc-a"
    edge_id = "cluster-a::ns::svc-a->svc-b::tcp"
    score = alert.metric_score("metric-a", service_id if target == "same_service" else None,
                               "opaque", 3.0, edge_id=edge_id if target == "same_edge" else None)
    result = AlertStateMachine(alert.config(), 1, [rule]).step(1, alert.states(service=0.1), [score], True)
    assert result.state == "hard"


@pytest.mark.parametrize("coverage,loss", [
    (0.0, 0.0), (1.0, 1.0), (0.25, 0.75), (0.9, 0.1),
])
def test_quality_boundaries_survive_aggregation(coverage, loss):
    output = "cluster-a::observability::service-a::request.count"
    record = agg.node(1, coverage=coverage, event_loss_rate=loss)
    result = AggregationPlan([(output, agg.spec())]).execute([record], 0, 1_000_000_000).node_records[0]
    assert result.coverage == coverage and result.event_loss_rate == loss


@pytest.mark.parametrize("changes", [
    {"coverage": -0.1}, {"coverage": 1.1}, {"event_loss_rate": -0.1},
    {"event_loss_rate": 1.1}, {"value": float("nan")}, {"value": float("inf")},
])
def test_p1_metric_contract_still_rejects_invalid_numbers(changes):
    with pytest.raises(ValueError):
        p1.make_node(**changes)


P2_PRODUCTION_FILES = [
    Path("proberca/aggregation/core.py"),
    Path("proberca/baseline/core.py"),
    Path("proberca/alerting/state_machine.py"),
    Path("proberca/config.py"),
    Path("proberca/data/schema.py"),
]


@pytest.mark.parametrize("path", P2_PRODUCTION_FILES)
def test_production_has_no_fixed_services_or_metric_name_heuristics(path):
    text = path.read_text(encoding="utf-8").lower()
    for forbidden in ("paymentservice", "checkoutservice", "online boutique", '"p99"', ".endswith(", ".startswith("):
        assert forbidden not in text


def test_p2_online_modules_do_not_import_incident_labels():
    for path in P2_PRODUCTION_FILES[:3]:
        assert "incidentlabel" not in path.read_text(encoding="utf-8").lower()


def test_p2_does_not_modify_forbidden_modules():
    changed = subprocess.run(["git", "diff", "--name-only", "HEAD"], check=True, text=True,
                             capture_output=True).stdout.splitlines()
    assert not any(path.startswith(("proberca/propagation/", "proberca/inference/", "proberca/evidence/",
                                    "experiments/", "bpf/")) for path in changed)


@pytest.mark.parametrize("path", P2_PRODUCTION_FILES[:3])
def test_p2_production_contains_no_todo_or_pass_statements(path):
    source = path.read_text(encoding="utf-8")
    assert "TODO" not in source and "FIXME" not in source
    tree = ast.parse(source)
    assert not any(isinstance(node, ast.Pass) for node in ast.walk(tree))


def test_p2_tests_have_no_skip_or_xfail_markers():
    forbidden = ["pytest." + "skip", "pytest.mark." + "skip", "pytest.mark." + "xfail"]
    for path in Path("tests").glob("test_p2_*.py"):
        source = path.read_text(encoding="utf-8")
        assert not any(marker in source for marker in forbidden)

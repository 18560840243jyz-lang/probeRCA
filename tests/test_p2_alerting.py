from __future__ import annotations

import json
from dataclasses import replace

import pytest

from proberca.alerting import AlertStateMachine
from proberca.baseline import AnomalyScore, EdgeState, ServiceState, StateScores
from proberca.config import AlertStateConfig, CompositeAlertRule


def config(**changes):
    payload = {
        "healthy_threshold": 0.5,
        "soft_threshold": 2.0,
        "soft_consecutive_windows": 2,
        "hard_threshold": 4.0,
        "hard_consecutive_windows": 2,
        "recovery_threshold": 0.25,
        "recovery_windows": 2,
        "recovery_cooldown_sec": 2,
        "edge_business_impact_threshold": 1.5,
    }
    payload.update(changes)
    return AlertStateConfig.from_dict(payload)


def states(service=0.0, edge=0.0, request=0.0):
    service_id = "cluster-a::ns::svc-a"
    edge_id = "cluster-a::ns::svc-a->svc-b::tcp"
    services = {service_id: ServiceState(service_id, service, {"request": request},
                                          {"request": True}, ["cpu"])} if service is not None else {}
    edges = {edge_id: EdgeState(edge_id, edge)} if edge is not None else {}
    return StateScores(services, edges, max(service or 0, 0.5 * (edge or 0)), [])


def metric_score(stable_id, service_id, metric, anomaly, direct=False, edge_id=None):
    return AnomalyScore(stable_id, "edge_metric" if edge_id else "node_metric",
                        None if edge_id else service_id, edge_id, None if edge_id else "request",
                        metric, anomaly, anomaly, anomaly, direct, 1.0, 0.0)


def test_healthy_warmup_and_no_duplicate_events():
    machine = AlertStateMachine(config(), window_sec=1)
    result = machine.step(1, states(), [], baseline_ready=False)
    assert result.state == "healthy" and result.events == []
    assert result.gate.update_node_baselines and not result.gate.baseline_ready
    ready = machine.step(2, states(), [], baseline_ready=True)
    assert ready.state == "healthy" and ready.events == []
    assert ready.gate.update_service_model
    assert machine.step(3, states(), [], baseline_ready=True).events == []
    gray = machine.step(4, states(service=1.0), [], baseline_ready=True)
    assert gray.state == "healthy"
    assert not gray.gate.update_node_baselines and not gray.gate.update_service_model
    warming = AlertStateMachine(config(soft_consecutive_windows=1, hard_consecutive_windows=1), 1)
    assert warming.step(1, states(service=5, edge=5), [], False).events == []
    assert warming.step(2, states(service=0), [], True).state == "healthy"


def test_soft_consecutive_spike_and_recovery():
    machine = AlertStateMachine(config(), 1)
    assert machine.step(1, states(service=3), [], baseline_ready=True).state == "healthy"
    soft = machine.step(2, states(service=3), [], baseline_ready=True)
    assert soft.state == "soft" and soft.events[0].state == "soft"
    assert not soft.gate.update_service_model and soft.gate.prepare_metric_model
    assert machine.step(3, states(service=0.1), [], baseline_ready=True).state == "soft"
    healthy = machine.step(4, states(service=0.1), [], baseline_ready=True)
    assert healthy.state == "healthy" and healthy.events[0].state == "healthy"
    one_spike = AlertStateMachine(config(), 1)
    one_spike.step(1, states(service=3), [], baseline_ready=True)
    assert one_spike.step(2, states(service=0), [], baseline_ready=True).state == "healthy"


def test_hard_freeze_recovery_cooldown_and_realert():
    machine = AlertStateMachine(config(), 1)
    machine.step(1, states(service=5), [], baseline_ready=True)
    hard = machine.step(2, states(service=5), [], baseline_ready=True)
    assert hard.state == "hard"
    assert not hard.gate.update_node_baselines and not hard.gate.update_edge_baselines
    assert hard.gate.request_burst and hard.gate.request_rca and hard.gate.freeze_metric_model
    assert hard.events[0].frozen_baseline
    machine.step(3, states(service=0.1), [], baseline_ready=True)
    recovery = machine.step(4, states(service=0.1), [], baseline_ready=True)
    assert recovery.state == "recovery" and recovery.events[0].state == "recovery"
    assert machine.step(5, states(service=0.1), [], baseline_ready=True).state == "recovery"
    healthy = machine.step(6, states(service=0.1), [], baseline_ready=True)
    assert healthy.state == "healthy"
    machine = AlertStateMachine(config(), 1)
    machine.step(1, states(service=5), [], True); machine.step(2, states(service=5), [], True)
    machine.step(3, states(service=0.1), [], True); machine.step(4, states(service=0.1), [], True)
    assert machine.step(5, states(service=5), [], True).state == "hard"


def test_isolated_edge_anomaly_freezes_only_edge():
    machine = AlertStateMachine(config(), 1)
    machine.step(1, states(service=0.1, edge=5, request=0.1), [], True)
    result = machine.step(2, states(service=0.1, edge=5, request=0.1), [], True)
    edge_id = next(iter(result.scores.edges))
    assert result.state != "hard"
    assert result.events[0].state == "edge_anomaly"
    assert result.gate.update_node_baselines and result.gate.update_edge_baselines
    assert result.gate.frozen_edge_ids == [edge_id]
    assert not result.gate.request_rca
    assert machine.step(3, states(service=0.1, edge=5, request=0.1), [], True).events == []


def test_edge_with_request_impact_and_direct_edge_enters_hard():
    for direct in (False, True):
        machine = AlertStateMachine(config(), 1)
        impact = 2.0 if not direct else 0.0
        edge_id = "cluster-a::ns::svc-a->svc-b::tcp"
        signals = [metric_score("edge-signal", None, "edge.metric", 5, direct=direct, edge_id=edge_id)] if direct else []
        machine.step(1, states(service=0.1, edge=5, request=impact), signals, True)
        result = machine.step(2, states(service=0.1, edge=5, request=impact), signals, True)
        assert result.state == "hard" and edge_id in result.events[-1].trigger_edges


def composite_rule(**changes):
    payload = {
        "rule_id": "request-pair",
        "target": "same_service",
        "all_of": ["latency-id", "error-id"],
        "any_of": [],
        "threshold": 3.0,
        "consecutive_windows": 1,
        "resulting_level": "hard",
    }
    payload.update(changes)
    return CompositeAlertRule.from_dict(payload)


def test_composite_rule_is_config_driven_and_strict():
    service_id = "cluster-a::ns::svc-a"
    metrics = [metric_score("latency-id", service_id, "opaque-a", 3.5),
               metric_score("error-id", service_id, "opaque-b", 4.0)]
    machine = AlertStateMachine(config(), 1, [composite_rule()])
    result = machine.step(1, states(service=0.1), metrics, True)
    assert result.state == "hard"
    assert json.loads(result.events[0].reason)["code"] == "composite_hard"
    with pytest.raises(ValueError):
        composite_rule(all_of=[], any_of=[])
    with pytest.raises(ValueError):
        composite_rule(all_of=["a"], any_of=["b"])


def test_composite_soft_and_same_target_consecutive_semantics():
    rule = composite_rule(resulting_level="soft", consecutive_windows=2)
    machine = AlertStateMachine(config(), 1, [rule])
    first_id = "cluster-a::ns::svc-a"
    second_id = "cluster-a::ns::svc-b"
    first = [metric_score("latency-id", first_id, "a", 4), metric_score("error-id", first_id, "b", 4)]
    second = [metric_score("latency-id", second_id, "a", 4), metric_score("error-id", second_id, "b", 4)]
    assert machine.step(1, states(service=0.1), first, True).state == "healthy"
    assert machine.step(2, states(service=0.1), second, True).state == "healthy"
    result = machine.step(3, states(service=0.1), second, True)
    assert result.state == "soft" and result.events[0].trigger_services == [second_id]


def test_direct_hard_node_and_event_fields():
    service_id = "cluster-a::ns::svc-a"
    direct = metric_score("rare-id", service_id, "opaque-rare", 6, direct=True)
    result = AlertStateMachine(config(), 1).step(1, states(service=0.1), [direct], True)
    assert result.state == "hard"
    event = result.events[0]
    assert event.trigger_services == [service_id]
    assert event.service_scores[service_id] == 0.1
    assert event.frozen_baseline and event.frozen_service_model and event.frozen_metric_model
    assert event.alert_id and json.loads(event.reason)["code"] == "direct_hard"


def test_state_snapshot_restore_is_identical(tmp_path):
    machine = AlertStateMachine(config(), 1)
    machine.step(1, states(service=3), [], True)
    path = tmp_path / "state.json"
    machine.save_json(path)
    restored = AlertStateMachine.load_json(path, config(), 1)
    expected = machine.step(2, states(service=3), [], True)
    actual = restored.step(2, states(service=3), [], True)
    assert actual == expected
    payload = json.loads(path.read_text())
    payload["format_version"] = "bad"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        AlertStateMachine.load_json(path, config(), 1)


def test_soft_trigger_change_emits_event_without_root_cause_objects():
    machine = AlertStateMachine(config(soft_consecutive_windows=1), 1)
    first = machine.step(1, states(service=3), [], True)
    assert first.events and not hasattr(first, "primary_root")
    second_scores = states(service=3)
    other_id = "cluster-a::ns::svc-b"
    second_scores.services[other_id] = ServiceState(other_id, 3.0, {"request": 3.0}, {"request": True}, [])
    changed = machine.step(2, second_scores, [], True)
    assert changed.events and other_id in changed.events[0].trigger_services

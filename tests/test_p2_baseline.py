from __future__ import annotations

from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
from proberca.baseline import (
    AmbiguousSignalSpecError,
    MetricSignalRegistry,
    RobustBaselineStore,
    ScoreAggregator,
)
from proberca.config import BaselineConfig, MetricSignalSpec, ScoreConfig


def signal(**changes):
    payload = {
        "record_type": "node_metric",
        "metric_family": "cpu",
        "metric_name": "opaque.metric",
        "protocol": None,
        "transform": "identity",
        "polarity": "increase_bad",
        "rare_event_threshold": None,
        "direct_hard": False,
        "z_cap": 6.0,
        "aggregation_output_id": "cluster-a::observability::service-a::opaque.metric",
    }
    payload.update(changes)
    return MetricSignalSpec.from_dict(payload)


def baseline_config(**changes):
    payload = {"healthy_history_sec": 10, "min_healthy_windows": 3, "min_scale": 0.5, "z_cap": 6.0}
    payload.update(changes)
    return BaselineConfig.from_dict(payload)


def node(ts, value, family="cpu", metric="opaque.metric"):
    return p1.make_node(timestamp_ns=ts, window_sec=1, metric_family=family, metric_name=metric,
                        value=value, metric_kind="gauge", scope="service", pod_uid=None,
                        container_id=None, unit="units")


def edge(ts, value, metric="opaque.edge"):
    return p1.make_edge(timestamp_ns=ts, window_sec=1, metric_name=metric, value=value,
                        metric_kind="gauge", scope="service_pair", unit="units")


def test_signal_registry_exact_unconfigured_and_ambiguous():
    record = node(1, 1)
    registry = MetricSignalRegistry([signal()])
    assert registry.resolve(record) == signal()
    missing, issues = MetricSignalRegistry([]).resolve_with_issues(record, 0, 1_000_000_000)
    assert missing is None and issues[0].reason_code == "unconfigured_metric"
    with pytest.raises(AmbiguousSignalSpecError):
        MetricSignalRegistry([signal(), signal(z_cap=5.0)]).resolve(record)
    assert MetricSignalRegistry([signal(metric_name="not-this")]).resolve_with_issues(record, 0, 1)[0] is None


@pytest.mark.parametrize("field,value", [
    ("transform", "sqrt"), ("polarity", "both"), ("record_type", "incident_label"),
    ("z_cap", 0), ("direct_hard", 1),
])
def test_signal_spec_strict_validation(field, value):
    with pytest.raises((ValueError, TypeError)):
        signal(**{field: value})


def warm_store(spec=None):
    spec = spec or signal()
    store = RobustBaselineStore(baseline_config(), window_sec=1)
    for ts, value in enumerate((1.0, 2.0, 3.0), 1):
        assert store.update(node(ts, value), spec, state="healthy")
    return store, spec


def test_warmup_median_mad_and_direction():
    store = RobustBaselineStore(baseline_config(), window_sec=1)
    spec = signal()
    record = node(1, 1)
    result = store.score(record, spec, 0, 1_000_000_000)
    assert result.score is None and result.issues[0].reason_code == "baseline_not_ready"
    store, spec = warm_store()
    stats = store.stats(record.stable_id)
    assert stats.center == 2.0 and stats.mad == 1.0 and stats.scale == pytest.approx(1.4826)
    high = store.score(node(4, 5), spec, 3_000_000_000, 4_000_000_000).score
    assert high.signed_z == pytest.approx((5 - 2) / 1.4826)
    assert high.anomaly == high.signed_z
    decrease = signal(polarity="decrease_bad")
    low = store.score(node(4, -1), decrease, 3_000_000_000, 4_000_000_000).score
    assert low.signed_z == pytest.approx((2 - (-1)) / 1.4826)


def test_zero_mad_transform_invalid_and_rare_event():
    store = RobustBaselineStore(baseline_config(min_scale=0.25), 1)
    identity = signal()
    for ts in range(3):
        store.update(node(ts, 2), identity, state="healthy")
    assert store.stats(node(1, 2).stable_id).scale == 0.25
    log_spec = signal(transform="log1p")
    log_store = RobustBaselineStore(baseline_config(), 1)
    for ts, value in enumerate((0.0, 1.0, 3.0)):
        log_store.update(node(ts, value), log_spec, state="healthy")
    assert log_store.score(node(4, 7), log_spec, 3, 4).score.transformed_value == pytest.approx(2.0794415416798357)
    with pytest.raises(ValueError):
        log_store.score(node(5, -1), log_spec, 4, 5)
    rare = signal(rare_event_threshold=5.0, direct_hard=True)
    rare_score = store.score(node(4, 5), rare, 3, 4).score
    assert rare_score.anomaly == 6.0 and rare_score.direct_hard is True
    decrease_rare = signal(polarity="decrease_bad", rare_event_threshold=0.0, direct_hard=True)
    assert store.score(node(5, -1), decrease_rare, 4, 5).score.direct_hard
    with pytest.raises(ValueError):
        store.score(node(5, 1, metric="different"), identity, 4, 5)


def test_baseline_health_gate_ring_buffer_and_snapshot(tmp_path):
    store, spec = warm_store()
    record = node(20, 9)
    original_count = store.stats(record.stable_id).count
    for state in ("soft", "hard", "recovery"):
        assert not store.update(record, spec, state=state)
    assert store.stats(record.stable_id).count == original_count
    assert not store.update(record, spec, state="healthy", frozen_ids={record.stable_id})
    assert store.update(record, spec, state="healthy")
    assert store.stats(record.stable_id).count <= baseline_config().healthy_history_sec
    path = tmp_path / "baseline.json"
    store.save_json(path)
    restored = RobustBaselineStore.load_json(path, baseline_config(), 1)
    assert restored.to_dict() == store.to_dict()
    assert restored.score(node(21, 10), spec, 20, 21) == store.score(node(21, 10), spec, 20, 21)


def score_config(**changes):
    payload = {
        "family_weights": {"request": 0.4, "cpu": 0.12, "memory": 0.12, "io": 0.12,
                           "net_local": 0.12, "lock": 0.12},
        "allow_partial_families": True,
        "edge_weight": 0.5,
        "edge_business_impact_threshold": 2.0,
    }
    payload.update(changes)
    return ScoreConfig.from_dict(payload)


def anomaly(record, value):
    spec = signal(metric_family=record.metric_family if record.record_type == "node_metric" else None,
                  metric_name=record.metric_name, record_type=record.record_type,
                  protocol=record.protocol if record.record_type == "edge_metric" else None,
                  aggregation_output_id=record.stable_id)
    store = RobustBaselineStore(baseline_config(), 1)
    for ts, baseline_value in enumerate((1.0, 2.0, 3.0), 1):
        store.update(replace(record, timestamp_ns=ts, value=baseline_value), spec, state="healthy")
    return store.score(replace(record, value=value), spec, 0, 1).score


def test_family_service_edge_and_global_scores():
    config = score_config()
    scorer = ScoreAggregator(config)
    cpu1 = anomaly(node(4, 5, "cpu", "opaque.metric"), 5)
    cpu2 = replace(cpu1, anomaly=1.0)
    request = replace(cpu1, metric_family="request", metric_name="request.metric", anomaly=3.0)
    edge_record = edge(4, 5)
    edge_score = anomaly(edge_record, 5)
    result = scorer.aggregate([cpu1, cpu2, request, edge_score])
    service = next(iter(result.services.values()))
    assert service.family_scores["cpu"] == cpu1.anomaly
    assert service.family_scores["request"] == 3.0
    assert service.score == pytest.approx(0.12 * cpu1.anomaly + 0.4 * 3.0)
    assert set(service.missing_families) == {"memory", "io", "net_local", "lock"}
    assert next(iter(result.edges.values())).score == edge_score.anomaly
    assert result.global_anomaly == max(service.score, 0.5 * edge_score.anomaly)


def test_family_weights_and_partial_policy():
    with pytest.raises(ValueError):
        score_config(family_weights={"cpu": 0.9})
    with pytest.raises(ValueError):
        score_config(family_weights={"cpu": -0.1, "request": 1.1})
    scorer = ScoreAggregator(score_config(allow_partial_families=False))
    cpu = anomaly(node(4, 5), 5)
    result = scorer.aggregate([cpu])
    service = next(iter(result.services.values()))
    assert service.score is None
    assert result.issues[0].reason_code == "missing_family"

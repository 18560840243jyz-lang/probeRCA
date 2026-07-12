from __future__ import annotations

from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
import test_p2_aggregation as p2agg
import test_p5_history_rules as p5
from proberca.aggregation import AggregationPlan
from proberca.config import (
    AlertStateConfig, BaselineConfig, MetricSignalSpec, ProbeRCAConfig, ScoreConfig,
)
from proberca.orchestration.engine import ProbeRCAEngine
from proberca.orchestration.checkpoint import (
    ReplayCheckpointError, restore_engine_checkpoint, save_engine_checkpoint,
)
from proberca.orchestration.state import EngineWindowInput


NS = 1_000_000_000
NODE_ID = "cluster-a::observability::service-a::cpu.throttled_usec"


def core_config(*, min_rows=3, analysis_delay=0):
    payload = p1.valid_config_dict()
    payload["window_sec"] = 1
    payload["propagation"].update({
        "service_lags": [1], "metric_lags": [1], "rls_initial_covariance": 10.0,
        "service_min_updates": 2, "service_min_observation_quality": 0.0,
        "service_max_gap_windows": 3, "topology_reconfigure_min_updates": 1,
        "metric_history_sec": 30, "metric_min_training_rows": min_rows,
        "metric_min_observation_quality": 0.0, "metric_max_condition_number": 1e12,
        "metric_max_gap_windows": 3, "metric_model_cache_size": 4,
        "metric_include_self_history": True,
        "metric_parent_rules": [p5.rule(
            target_family="cpu", parent_family="cpu", target_names=["cpu.throttled_usec"],
            parent_names=["cpu.throttled_usec"], lags=[1],
        ).to_dict()],
    })
    payload["candidate_graph"].update({
        "allowed_namespaces": ["observability"], "include_cohost": False,
        "include_shared_resource": False,
    })
    payload["rca_metric_families"] = ["cpu"]
    payload["diagnosis"] = {
        **ProbeRCAConfig.from_dict(payload).diagnosis.to_dict(),
        "minimum_relative_counterfactual_delta": 0.0,
        "minimum_margin_for_root": 0.0,
        "minimum_identifiability_threshold": 0.0,
        "strong_identifiability_threshold": 0.0,
    }
    payload["confidence"] = {"strong": 0.01, "weak": 0.0}
    payload["orchestration"] = {
        "analysis_delay_windows": analysis_delay,
        "evidence_window_windows": analysis_delay,
        "allow_single_active_incident_only": True, "fail_on_concurrent_incident": True,
        "retain_intermediates": False, "checkpoint_every_windows": 0,
        "strict_stage_identity": True, "continue_after_incident_failure": True,
    }
    return ProbeRCAConfig.from_dict(payload)


def raw_node(timestamp, value):
    return p1.make_node(
        timestamp_ns=timestamp, window_sec=1, metric_kind="gauge", scope="pod",
        metric_family="cpu", metric_name="cpu.throttled_usec", value=value,
        unit="us", sample_count=1, coverage=1.0, event_loss_rate=0.0,
    )


def aggregation_plan():
    spec = p2agg.spec(
        method="median_max", kind="gauge", source="pod", target="service",
        input_metric_ids=[NODE_ID], output_metric_name="cpu.throttled_usec",
        output_metric_kind="gauge", output_unit="us", median_weight=1.0,
    )
    return AggregationPlan([(NODE_ID, spec)])


def signal_specs():
    return [MetricSignalSpec.from_dict({
        "record_type": "node_metric", "metric_family": "cpu",
        "metric_name": "cpu.throttled_usec", "protocol": None,
        "transform": "identity", "polarity": "increase_bad",
        "rare_event_threshold": None, "direct_hard": False, "z_cap": 6.0,
        "aggregation_output_id": NODE_ID,
    })]


def topology():
    return replace(p1.make_topology(), valid_from_ns=0, valid_to_ns=100 * NS,
                   host_edges=[], resource_edges=[])


def engine(*, min_rows=3, analysis_delay=0):
    return ProbeRCAEngine(
        core_config(min_rows=min_rows, analysis_delay=analysis_delay),
        aggregation_plan=aggregation_plan(), signal_specs=signal_specs(),
        baseline_config=BaselineConfig.from_dict({
            "healthy_history_sec": 30, "min_healthy_windows": 3,
            "min_scale": 0.1, "z_cap": 6.0,
        }),
        score_config=ScoreConfig.from_dict({
            "family_weights": {"request": 0.0, "cpu": 1.0, "memory": 0.0,
                               "io": 0.0, "net_local": 0.0, "lock": 0.0},
            "allow_partial_families": True, "edge_weight": 1.0,
            "edge_business_impact_threshold": 1.0,
        }),
        alert_state_config=AlertStateConfig.from_dict({
            "healthy_threshold": 0.5, "soft_threshold": 1.0,
            "soft_consecutive_windows": 1, "hard_threshold": 2.0,
            "hard_consecutive_windows": 1, "recovery_threshold": 0.25,
            "recovery_windows": 1, "recovery_cooldown_sec": 1,
            "edge_business_impact_threshold": 1.0,
        }),
    )


def window(index, value, *, include_topology=False):
    end = index * NS
    return EngineWindowInput(
        end, end - NS, end, [raw_node(end - 1, value)], [],
        [topology()] if include_topology else [], [],
        [f"node:{index}"], index, [],
    )


def healthy_then_incident(target_engine, *, include_hard=True):
    values = [1.0, 1.0, 1.0, 1.0, 1.01, 0.99, 1.0, 1.2]
    results = [target_engine.process_window(window(i, value, include_topology=i == 1))
               for i, value in enumerate(values, start=1)]
    if include_hard:
        results.append(target_engine.process_window(window(9, 1.4)))
    return results


def test_engine_uses_one_canonical_stage_order_and_soft_does_not_diagnose():
    results = healthy_then_incident(engine(), include_hard=False)
    soft = results[-1]
    assert soft.state == "soft" and soft.alerts[-1].state == "soft"
    assert soft.candidate_subgraph is not None
    assert soft.reports == [] and soft.failures == []
    assert soft.stage_trace == ["topology", "p2", "p4", "p5", "p3_soft"]


def test_hard_anchor_runs_real_p6_to_p9_and_returns_unified_report():
    target = engine()
    hard = healthy_then_incident(target)[-1]
    assert hard.state == "hard"
    assert hard.pending_incident is not None
    assert hard.pending_incident.hard_anchor_ns == 9 * NS
    assert hard.pending_incident.analysis_cutoff_ns == 9 * NS
    assert hard.reports and hard.failures == []
    assert hard.reports[0].record_type == "rca_report"
    assert hard.reports[0].quality["normalized_evidence"] == "no_normalized_evidence"
    assert hard.stage_trace[-6:] == ["p3_hard", "p6", "p7", "p8", "p9", "report"]


def test_p5_not_ready_is_failure_without_service_only_or_fake_report():
    target = engine(min_rows=20)
    hard = healthy_then_incident(target)[-1]
    assert hard.reports == [] and len(hard.failures) == 1
    assert hard.failures[0].stage == "p5"
    assert hard.failures[0].reason_code == "metric_model_not_ready"


def test_analysis_delay_keeps_hard_anchor_and_waits_for_cutoff():
    target = engine(analysis_delay=1)
    hard = healthy_then_incident(target)[-1]
    assert hard.pending_incident.hard_anchor_ns == 9 * NS
    assert hard.pending_incident.analysis_cutoff_ns == 10 * NS
    assert hard.reports == []
    after = target.process_window(window(10, 1.4))
    assert after.reports
    assert after.pending_incident.hard_anchor_ns == 9 * NS
    assert after.pending_incident.hard_node_anomalies == hard.pending_incident.hard_node_anomalies


def test_same_hard_state_does_not_start_duplicate_incident():
    target = engine(analysis_delay=2)
    hard = healthy_then_incident(target)[-1]
    continued = target.process_window(window(10, 1.4))
    assert continued.pending_incident.pending_incident_id == hard.pending_incident.pending_incident_id


def test_checkpoint_restores_frozen_models_and_continues_diagnosis(tmp_path):
    first = engine(analysis_delay=1)
    hard = healthy_then_incident(first)[-1]
    checkpoint = tmp_path / "checkpoint"
    save_engine_checkpoint(first, checkpoint, manifest_hash="manifest-a", replay_sequence=9)
    resumed = engine(analysis_delay=1)
    assert restore_engine_checkpoint(resumed, checkpoint, manifest_hash="manifest-a") == 9
    next_result = resumed.process_window(window(10, 1.4))
    assert len(next_result.reports) == 1
    assert next_result.pending_incident.lifecycle == "diagnosed"
    with pytest.raises(ReplayCheckpointError, match="dataset"):
        restore_engine_checkpoint(engine(analysis_delay=1), checkpoint, manifest_hash="other")


def test_healthy_checkpoint_restores_training_and_runtime_histories(tmp_path):
    uninterrupted = engine()
    values = [1.0, 1.0, 1.0, 1.0, 1.01, 0.99, 1.0]
    for index, value in enumerate(values, 1):
        uninterrupted.process_window(window(index, value, include_topology=index == 1))
    checkpoint = tmp_path / "healthy-checkpoint"
    save_engine_checkpoint(
        uninterrupted, checkpoint, manifest_hash="manifest-a", replay_sequence=7)
    expected = uninterrupted.process_window(window(8, 1.2))
    resumed = engine()
    restore_engine_checkpoint(resumed, checkpoint, manifest_hash="manifest-a")
    actual = resumed.process_window(window(8, 1.2))
    assert actual.state == expected.state == "soft"
    assert [item.alert_id for item in actual.alerts] == [item.alert_id for item in expected.alerts]
    assert actual.candidate_subgraph.candidate_id == expected.candidate_subgraph.candidate_id
    assert resumed.metric_learner.training_history.node_ids() == \
        uninterrupted.metric_learner.training_history.node_ids()

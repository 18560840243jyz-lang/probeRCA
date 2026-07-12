from __future__ import annotations

from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
from proberca.config import OrchestrationConfig, ProbeRCAConfig, ReplayConfig
from proberca.orchestration.adapters import service_state_records_from_p2
from proberca.orchestration.state import ReplayIncidentFailure
from proberca.baseline import AnomalyScore, ServiceState, StateScores


def test_p10_config_defaults_are_strict_and_backward_compatible():
    config = ProbeRCAConfig.from_dict(p1.valid_config_dict())
    assert config.orchestration == OrchestrationConfig()
    assert config.replay == ReplayConfig()
    assert not config.orchestration.retain_intermediates
    assert config.replay.strict_order and not config.replay.allow_explicit_reorder


@pytest.mark.parametrize("changes", [
    {"analysis_delay_windows": -1},
    {"evidence_window_windows": -1},
    {"analysis_delay_windows": 1, "evidence_window_windows": 2},
    {"allow_single_active_incident_only": False},
    {"fail_on_concurrent_incident": False},
    {"strict_stage_identity": False},
])
def test_orchestration_config_rejects_degraded_or_invalid_modes(changes):
    payload = OrchestrationConfig().to_dict(); payload.update(changes)
    with pytest.raises((TypeError, ValueError)):
        OrchestrationConfig.from_dict(payload)


@pytest.mark.parametrize("changes", [
    {"parquet_batch_size": 0},
    {"strict_order": False, "allow_explicit_reorder": False},
    {"strict_order": True, "allow_explicit_reorder": True},
])
def test_replay_config_rejects_unsafe_or_ambiguous_modes(changes):
    payload = ReplayConfig().to_dict(); payload.update(changes)
    with pytest.raises((TypeError, ValueError)):
        ReplayConfig.from_dict(payload)


def test_service_state_adapter_uses_real_p2_score_and_quality():
    service_id = "cluster-a::ns::svc-a"
    state = ServiceState(service_id, 2.5, {"request": 2.5}, {"request": True}, ["cpu"])
    scores = StateScores({service_id: state}, {}, 2.5, [])
    metric_scores = [AnomalyScore(
        "cluster-a::ns::svc-a::request.lat", "node_metric", service_id, None,
        "request", "request.lat", 2.5, 2.5, 2.5, False, 0.8, 0.25,
    )]
    records = service_state_records_from_p2(
        scores, metric_scores, timestamp_ns=2_000_000_000, window_sec=1,
        baseline_ready=True, alert_state="soft", config_fingerprint="f" * 64,
    )
    assert records[0].value == 2.5
    assert records[0].observation_quality == pytest.approx(0.6)
    assert records[0].missing_families == ["cpu"]


def test_service_state_adapter_does_not_fill_missing_service_score_with_zero():
    service_id = "cluster-a::ns::svc-a"
    scores = StateScores({service_id: ServiceState(
        service_id, None, {}, {}, ["request"]
    )}, {}, 0.0, [])
    assert service_state_records_from_p2(
        scores, [], timestamp_ns=1_000_000_000, window_sec=1,
        baseline_ready=True, alert_state="healthy", config_fingerprint="f" * 64,
    ) == []


def test_incident_failure_has_deterministic_fingerprint_and_no_report_fields():
    first = ReplayIncidentFailure.create(
        pending_incident_id="pending-a", stage="p5", timestamp_ns=3,
        error=ValueError("model not ready"), reason_code="model_not_ready",
        context_ids=["candidate-a"], retryable=False, config_fingerprint="f" * 64,
    )
    second = ReplayIncidentFailure.create(
        pending_incident_id="pending-a", stage="p5", timestamp_ns=3,
        error=ValueError("model not ready"), reason_code="model_not_ready",
        context_ids=["candidate-a"], retryable=False, config_fingerprint="f" * 64,
    )
    assert first.failure_fingerprint == second.failure_fingerprint
    assert "report" not in first.to_dict()

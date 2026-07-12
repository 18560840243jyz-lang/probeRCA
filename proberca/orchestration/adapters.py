"""Lossless adapters between canonical P2 outputs and later-stage contracts."""

from __future__ import annotations

from proberca.baseline import AnomalyScore, StateScores
from proberca.data.schema import PROBERCA_SCHEMA_VERSION, ServiceStateRecord


def service_state_records_from_p2(
    state_scores: StateScores,
    metric_scores: list[AnomalyScore],
    *,
    timestamp_ns: int,
    window_sec: int,
    baseline_ready: bool,
    alert_state: str,
    config_fingerprint: str,
) -> list[ServiceStateRecord]:
    """Convert P2 service scores without recomputing or filling missing values."""
    if not isinstance(state_scores, StateScores) or any(
        not isinstance(item, AnomalyScore) for item in metric_scores
    ):
        raise TypeError("service-state adapter requires canonical P2 outputs")
    qualities: dict[str, list[float]] = {}
    for item in metric_scores:
        if item.record_type == "node_metric" and item.service_id is not None:
            qualities.setdefault(item.service_id, []).append(
                item.coverage * (1.0 - item.event_loss_rate)
            )
    output = []
    for service_id, state in sorted(state_scores.services.items()):
        if state.score is None:
            continue
        parts = service_id.split("::")
        if len(parts) != 3 or not qualities.get(service_id):
            continue
        output.append(ServiceStateRecord(
            PROBERCA_SCHEMA_VERSION, timestamp_ns,
            timestamp_ns - window_sec * 1_000_000_000, timestamp_ns,
            parts[0], parts[1], parts[2], service_id, state.score, baseline_ready,
            dict(state.family_coverage), list(state.missing_families),
            min(qualities[service_id]), alert_state, config_fingerprint,
        ))
    return output

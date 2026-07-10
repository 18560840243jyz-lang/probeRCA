"""Adaptive observation simulator for probeRCA P1A."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl

DEFAULT_ALWAYS_ON_METRICS = [
    "request.rps",
    "request.error_rate",
    "request.p50_latency_ms",
    "request.p95_latency_ms",
    "request.p99_latency_ms",
    "request.in_flight",
    "cpu.usage",
    "memory.usage",
]

DEFAULT_FINE_METRICS = [
    "cpu.throttled_usec",
    "cpu.pressure",
    "memory.pressure",
    "net.rtt_ms",
    "net.retrans",
    "io.bio_latency_ms",
    "io.queue_depth",
    "lock.futex_wait_ms",
]

ALERT_SIGNAL_METRICS = {"request.p99_latency_ms", "request.error_rate"}
OBSERVATION_MODES = {
    "always_on",
    "normal_sampled",
    "soft_alert_burst",
    "hard_alert_burst",
    "not_observed",
}


@dataclass
class ObservationPolicyConfig:
    """Policy knobs for adaptive observation simulation."""

    always_on_metrics: list[str] = field(default_factory=lambda: list(DEFAULT_ALWAYS_ON_METRICS))
    fine_metrics: list[str] = field(default_factory=lambda: list(DEFAULT_FINE_METRICS))
    normal_sampling_rate: float = 0.10
    soft_alert_sampling_rate: float = 0.40
    hard_alert_sampling_rate: float = 1.00
    min_sampling_probability: float = 0.05
    soft_alert_z_threshold: float = 3.0
    hard_alert_z_threshold: float = 6.0
    hard_alert_consecutive_windows: int = 2
    seed: int = 7


@dataclass
class SamplingLogRecord:
    """Sampling probability and observation decision for one service-metric window."""

    incident_id: str
    timestamp: float
    service: str
    metric: str
    sampling_probability: float
    observed: bool
    observation_mode: str
    reason: str
    source: str = "adaptive_observation_simulator"


@dataclass
class ObservationMaskRecord:
    """Observation mask row for one service-metric window."""

    incident_id: str
    timestamp: float
    service: str
    metric: str
    observed: bool
    sampling_probability: float
    source: str = "adaptive_observation_simulator"


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict]]:
    """Load normalized metrics and incidents required by P1A."""

    input_path = Path(input_dir)
    normalized_path = input_path / "normalized_metrics.jsonl"
    incidents_path = input_path / "incidents.jsonl"
    if not normalized_path.exists():
        raise FileNotFoundError(f"missing required input file: {normalized_path}")
    if not incidents_path.exists():
        raise FileNotFoundError(f"missing required input file: {incidents_path}")
    return read_jsonl(normalized_path), read_jsonl(incidents_path)


def _clamp_probability(value: float, config: ObservationPolicyConfig, *, always_on: bool = False) -> float:
    if always_on:
        return 1.0
    return float(min(1.0, max(config.min_sampling_probability, value)))


def _service_alert_state(recent_service_state: dict[str, Any], service: str) -> dict[str, Any]:
    state = recent_service_state.get(service)
    if not isinstance(state, dict):
        return {"current_alert_z": 0.0, "hard_streak": 0}
    return state


def classify_observation_mode(
    record: dict,
    recent_service_state: dict[str, Any],
    config: ObservationPolicyConfig,
) -> tuple[str, float, str]:
    """Classify observation mode and sampling probability for a normalized metric record."""

    metric = str(record["metric"])
    service = str(record["service"])
    always_on_metrics = set(config.always_on_metrics)
    fine_metrics = set(config.fine_metrics)

    if metric in always_on_metrics:
        return "always_on", 1.0, "always_on_metric"

    service_state = _service_alert_state(recent_service_state, service)
    current_alert_z = float(service_state.get("current_alert_z", 0.0))
    hard_streak = int(service_state.get("hard_streak", 0))

    if metric in fine_metrics:
        if hard_streak >= config.hard_alert_consecutive_windows:
            return (
                "hard_alert_burst",
                _clamp_probability(config.hard_alert_sampling_rate, config),
                "hard_alert",
            )
        if current_alert_z >= config.soft_alert_z_threshold:
            return (
                "soft_alert_burst",
                _clamp_probability(config.soft_alert_sampling_rate, config),
                "soft_alert",
            )
        return (
            "normal_sampled",
            _clamp_probability(config.normal_sampling_rate, config),
            "normal_sampling",
        )

    return (
        "normal_sampled",
        _clamp_probability(config.min_sampling_probability, config),
        "minimum_probability",
    )


def _incident_id(record: dict) -> str:
    value = record.get("incident_id")
    return "" if value is None else str(value)


def _prepare_alert_state(records: list[dict]) -> dict[tuple[str, float, str], dict[str, Any]]:
    grouped: dict[tuple[str, float, str], list[float]] = {}
    for record in records:
        metric = str(record["metric"])
        if metric not in ALERT_SIGNAL_METRICS:
            continue
        key = (_incident_id(record), float(record["timestamp"]), str(record["service"]))
        grouped.setdefault(key, []).append(abs(float(record.get("z_value", 0.0))))

    state_by_key: dict[tuple[str, float, str], dict[str, Any]] = {}
    streaks: dict[tuple[str, str], int] = {}
    for incident_id, timestamp, service in sorted(grouped, key=lambda item: (item[0], item[2], item[1])):
        current_alert_z = max(grouped[(incident_id, timestamp, service)])
        streak_key = (incident_id, service)
        if current_alert_z >= 0.0:  # State update is thresholded below and kept deterministic.
            pass
        state_by_key[(incident_id, timestamp, service)] = {"current_alert_z": current_alert_z}

    for incident_id in sorted({_incident_id(record) for record in records}):
        services = sorted({str(record["service"]) for record in records if _incident_id(record) == incident_id})
        timestamps = sorted({float(record["timestamp"]) for record in records if _incident_id(record) == incident_id})
        for service in services:
            streak = 0
            for timestamp in timestamps:
                key = (incident_id, timestamp, service)
                current_alert_z = float(state_by_key.get(key, {}).get("current_alert_z", 0.0))
                if current_alert_z >= 6.0:
                    streak += 1
                else:
                    streak = 0
                state_by_key[key] = {"current_alert_z": current_alert_z, "hard_streak": streak}
                streaks[(incident_id, service)] = streak
    return state_by_key


def _prepare_alert_state_with_config(records: list[dict], config: ObservationPolicyConfig) -> dict[tuple[str, float, str], dict[str, Any]]:
    grouped: dict[tuple[str, float, str], list[float]] = {}
    for record in records:
        if str(record["metric"]) not in ALERT_SIGNAL_METRICS:
            continue
        key = (_incident_id(record), float(record["timestamp"]), str(record["service"]))
        grouped.setdefault(key, []).append(abs(float(record.get("z_value", 0.0))))

    state_by_key: dict[tuple[str, float, str], dict[str, Any]] = {}
    incident_ids = sorted({_incident_id(record) for record in records})
    for incident_id in incident_ids:
        services = sorted({str(record["service"]) for record in records if _incident_id(record) == incident_id})
        timestamps = sorted({float(record["timestamp"]) for record in records if _incident_id(record) == incident_id})
        for service in services:
            hard_streak = 0
            for timestamp in timestamps:
                current_alert_z = max(grouped.get((incident_id, timestamp, service), [0.0]))
                if current_alert_z >= config.hard_alert_z_threshold:
                    hard_streak += 1
                else:
                    hard_streak = 0
                state_by_key[(incident_id, timestamp, service)] = {
                    "current_alert_z": current_alert_z,
                    "hard_streak": hard_streak,
                }
    return state_by_key


def _observed_metric_record(record: dict, probability: float, mode: str, reason: str) -> dict:
    observed_record = dict(record)
    observed_record["observed"] = True
    observed_record["sampling_probability"] = float(probability)
    observed_record["observation_mode"] = mode
    observed_record["reason"] = reason
    return observed_record


def simulate_adaptive_observation(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: ObservationPolicyConfig | None = None,
) -> dict:
    """Simulate adaptive observation masks and observed metric records."""

    policy = config or ObservationPolicyConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    normalized_records, _incidents = load_required_dataset(input_path)
    rng = np.random.default_rng(policy.seed)
    alert_state = _prepare_alert_state_with_config(normalized_records, policy)

    sampling_logs: list[dict] = []
    observation_masks: list[dict] = []
    observed_metrics: list[dict] = []
    mode_counts = {mode: 0 for mode in OBSERVATION_MODES}
    fine_metric_count = 0

    for record in sorted(
        normalized_records,
        key=lambda item: (_incident_id(item), float(item["timestamp"]), str(item["service"]), str(item["instance"]), str(item["metric"])),
    ):
        metric = str(record["metric"])
        service = str(record["service"])
        incident_id = _incident_id(record)
        timestamp = float(record["timestamp"])
        recent_service_state = {service: alert_state.get((incident_id, timestamp, service), {})}
        mode, probability, reason = classify_observation_mode(record, recent_service_state, policy)
        observed = True if mode == "always_on" else bool(rng.random() < probability)
        log_mode = mode if observed else "not_observed"
        if metric in set(policy.fine_metrics):
            fine_metric_count += 1
        mode_counts[log_mode] = mode_counts.get(log_mode, 0) + 1

        sampling_record = SamplingLogRecord(
            incident_id=incident_id,
            timestamp=timestamp,
            service=service,
            metric=metric,
            sampling_probability=float(probability),
            observed=observed,
            observation_mode=log_mode,
            reason=reason,
        )
        mask_record = ObservationMaskRecord(
            incident_id=incident_id,
            timestamp=timestamp,
            service=service,
            metric=metric,
            observed=observed,
            sampling_probability=float(probability),
        )
        sampling_logs.append(asdict(sampling_record))
        observation_masks.append(asdict(mask_record))
        if observed:
            observed_metrics.append(_observed_metric_record(record, probability, mode, reason))

    output_path.mkdir(parents=True, exist_ok=True)
    observed_path = output_path / "observed_metrics.jsonl"
    sampling_log_path = output_path / "sampling_log.jsonl"
    mask_path = output_path / "observation_mask.jsonl"
    metadata_path = output_path / "adaptive_observation_metadata.json"

    write_jsonl(observed_path, observed_metrics)
    write_jsonl(sampling_log_path, sampling_logs)
    write_jsonl(mask_path, observation_masks)

    total_records = len(normalized_records)
    observed_records = len(observed_metrics)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "total_records": total_records,
        "observed_records": observed_records,
        "observed_ratio": float(observed_records / total_records) if total_records else 0.0,
        "always_on_count": int(mode_counts.get("always_on", 0)),
        "fine_metric_count": int(fine_metric_count),
        "normal_sampled_count": int(mode_counts.get("normal_sampled", 0)),
        "soft_alert_burst_count": int(mode_counts.get("soft_alert_burst", 0)),
        "hard_alert_burst_count": int(mode_counts.get("hard_alert_burst", 0)),
        "not_observed_count": int(mode_counts.get("not_observed", 0)),
        "min_sampling_probability": float(policy.min_sampling_probability),
        "seed": int(policy.seed),
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "observed_metrics_path": str(observed_path),
        "sampling_log_path": str(sampling_log_path),
        "observation_mask_path": str(mask_path),
        "adaptive_observation_metadata_path": str(metadata_path),
        "metadata": metadata,
    }

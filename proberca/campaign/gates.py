"""Objective acquisition gates; control-plane predictions are deliberately absent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .model import fingerprint


@dataclass(frozen=True)
class QualificationObservation:
    profile_id: str
    target_arrival_rate_rps: float
    measured_rps: float
    normal_burst_aligned: bool
    source_gap_count: int
    pod_restart_delta: int
    topology_change_count: int
    runtime_identity_change_count: int
    business_error_rate: float
    worker_cpu_p95: float
    worker_memory_p95: float
    observed_tcp_edges: tuple[str, ...]
    required_tcp_edges: tuple[str, ...]
    projected_baseline_rows: dict[str, int] = field(default_factory=dict)
    projected_av_rows: dict[str, int] = field(default_factory=dict)
    required_baseline_rows: dict[str, int] = field(default_factory=dict)
    required_av_rows: dict[str, int] = field(default_factory=dict)


def evaluate_qualification(
    observation: QualificationObservation,
    objective_gates: dict[str, Any],
) -> dict[str, Any]:
    reasons = []
    if not observation.normal_burst_aligned:
        reasons.append("normal_burst_misaligned")
    if observation.source_gap_count:
        reasons.append("source_gap")
    if observation.pod_restart_delta:
        reasons.append("pod_restart")
    if observation.topology_change_count:
        reasons.append("topology_changed")
    if observation.runtime_identity_change_count:
        reasons.append("runtime_identity_changed")
    if observation.business_error_rate > float(
        objective_gates["maximum_business_error_rate"]
    ):
        reasons.append("business_error_rate")
    if observation.worker_cpu_p95 >= float(objective_gates["maximum_worker_cpu_p95"]):
        reasons.append("worker_cpu_pressure")
    if observation.worker_memory_p95 >= float(
        objective_gates["maximum_worker_memory_p95"]
    ):
        reasons.append("worker_memory_pressure")
    minimum_rps_ratio = float(objective_gates.get("minimum_achieved_rps_ratio", 0.90))
    if observation.target_arrival_rate_rps <= 0 or (
        observation.measured_rps / observation.target_arrival_rate_rps
    ) < minimum_rps_ratio:
        reasons.append("arrival_rate_not_achieved")
    if set(observation.required_tcp_edges) - set(observation.observed_tcp_edges):
        reasons.append("tcp_edge_count_missing")
    if any(
        observation.projected_baseline_rows.get(key, 0) < required
        for key, required in observation.required_baseline_rows.items()
    ):
        reasons.append("projected_baseline_rows_insufficient")
    if any(
        observation.projected_av_rows.get(key, 0) < required
        for key, required in observation.required_av_rows.items()
    ):
        reasons.append("projected_av_rows_insufficient")
    result = {
        "profile_id": observation.profile_id,
        "qualified": not reasons,
        "reasons": reasons,
        "measured_rps": observation.measured_rps,
        "target_arrival_rate_rps": observation.target_arrival_rate_rps,
    }
    result["report_fingerprint"] = fingerprint(result)
    return result


@dataclass(frozen=True)
class EpisodeIntegrityObservation:
    normal_burst_aligned: bool
    window_count: int
    expected_window_count: int
    dataset_id_matches: bool
    sha256_verified: bool
    source_timestamps_valid: bool
    pod_restart_delta: int
    topology_change_count: int
    runtime_identity_change_count: int
    injector_effective: bool
    non_target_contamination: bool
    cleanup_confirmed: bool
    collection_services_active: bool


def evaluate_episode_integrity(
    observation: EpisodeIntegrityObservation,
) -> dict[str, Any]:
    checks = {
        "normal_burst_aligned": observation.normal_burst_aligned,
        "window_count": observation.window_count == observation.expected_window_count,
        "dataset_id": observation.dataset_id_matches,
        "sha256": observation.sha256_verified,
        "source_timestamps": observation.source_timestamps_valid,
        "pod_restart": observation.pod_restart_delta == 0,
        "topology": observation.topology_change_count == 0,
        "runtime_identity": observation.runtime_identity_change_count == 0,
        "injector_effective": observation.injector_effective,
        "non_target_contamination": not observation.non_target_contamination,
        "cleanup_confirmed": observation.cleanup_confirmed,
        "collection_services_active": observation.collection_services_active,
    }
    return {
        "accepted": all(checks.values()),
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "checks": checks,
    }

"""Circular-safe channel aggregation for node and edge-shock evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from proberca.config import ProbeRCAConfig
from proberca.inversion.contracts import JointInversionSystem

from .observations import (
    EvidenceAlignmentError,
    EvidenceConflictError,
    EvidenceQualityError,
    EvidenceTimeWindowError,
    canonicalize_evidence,
)


@dataclass(frozen=True)
class EvidenceAggregationResult:
    node_h: np.ndarray
    shock_h: np.ndarray
    evidence_provenance: list[dict]
    excluded_evidence: list[dict]


def _noisy_or(values) -> float:
    result = 1.0
    for value in values:
        result *= 1.0 - value
    return float(1.0 - result)


def _independent_strengths(items):
    parents = list(range(len(items)))

    def root(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left, right):
        left_root, right_root = root(left), root(right)
        if left_root != right_root:
            parents[max(left_root, right_root)] = min(left_root, right_root)

    for left in range(len(items)):
        for right in range(left + 1, len(items)):
            same_channel = items[left][0].channel_id == items[right][0].channel_id
            same_source = bool(
                set(items[left][0].source_object_ids) & set(items[right][0].source_object_ids)
            )
            if same_channel or same_source:
                union(left, right)
    strengths = {}
    for index, (_, contribution) in enumerate(items):
        component = root(index)
        strengths[component] = max(strengths.get(component, 0.0), contribution)
    return [strengths[index] for index in sorted(strengths)]


def aggregate_evidence(joint_system, evidence_observations, config, analysis_cutoff_ns):
    if not isinstance(joint_system, JointInversionSystem) or not joint_system.solver_eligible:
        raise EvidenceAlignmentError("evidence requires a complete solver-eligible P6 system")
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    if analysis_cutoff_ns < joint_system.timestamp_ns:
        raise EvidenceTimeWindowError("analysis cutoff must not precede the P6 timestamp")
    records = canonicalize_evidence(evidence_observations)
    node_index = {item.node_id: item.column_index for item in joint_system.node_variable_refs}
    shock_index = {item.shock_id: item.column_index for item in joint_system.shock_variable_refs}
    residual_record_ids = {item.source_record_id for item in [
        *joint_system.node_row_refs, *joint_system.edge_row_refs,
    ]}
    residual_object_ids = {item.object_id for item in [
        *joint_system.node_row_refs, *joint_system.edge_row_refs,
    ]}
    channels: dict[tuple[str, str], list[tuple[object, float]]] = {}
    included, excluded = [], []
    oldest = joint_system.timestamp_ns - config.evidence.max_age_windows * config.window_sec * 1_000_000_000
    for item in records:
        target_map = node_index if item.target_type == "node" else shock_index
        if item.target_id not in target_map:
            raise EvidenceAlignmentError(
                f"evidence={item.evidence_id} target={item.target_id} is absent from P6"
            )
        if item.cluster_id != item.target_id.split("::", 1)[0] \
                or item.namespace != item.target_id.split("::")[1]:
            raise EvidenceAlignmentError(f"evidence={item.evidence_id} target identity mismatch")
        reason = None
        if item.analysis_cutoff_ns != analysis_cutoff_ns or item.timestamp_ns > analysis_cutoff_ns:
            raise EvidenceTimeWindowError(f"evidence={item.evidence_id} cutoff mismatch")
        if item.timestamp_ns < oldest:
            reason = "stale_evidence"
        elif item.observation_quality < config.evidence.min_observation_quality:
            reason = "low_quality_evidence"
        elif not item.independent_from_residual:
            reason = "non_independent_evidence_excluded"
        elif set(item.source_record_ids) & residual_record_ids \
                or set(item.source_object_ids) & residual_object_ids:
            reason = "circular_evidence_excluded"
        if reason is not None:
            excluded.append({
                "evidence_id": item.evidence_id, "target_id": item.target_id,
                "reason_code": reason, "record": item.to_dict(),
            })
            continue
        contribution = item.normalized_strength * item.observation_quality * item.reliability_weight
        if not np.isfinite(contribution) or not 0.0 <= contribution <= 1.0:
            raise EvidenceQualityError(f"evidence={item.evidence_id} contribution is invalid")
        key = (item.target_type, item.target_id)
        channels.setdefault(key, []).append((item, contribution))
        included.append(item.to_dict())
    node_h = np.zeros(len(node_index), dtype=float)
    shock_h = np.zeros(len(shock_index), dtype=float)
    for (target_type, target_id), channel_values in sorted(channels.items()):
        value = _noisy_or(_independent_strengths(channel_values))
        if target_type == "node":
            node_h[node_index[target_id]] = value
        else:
            shock_h[shock_index[target_id]] = value
    return EvidenceAggregationResult(
        node_h, shock_h, sorted(included, key=lambda item: item["evidence_id"]),
        sorted(excluded, key=lambda item: item["evidence_id"]),
    )


__all__ = [
    "EvidenceAggregationResult", "EvidenceAlignmentError", "EvidenceConflictError",
    "EvidenceQualityError", "EvidenceTimeWindowError", "aggregate_evidence",
]

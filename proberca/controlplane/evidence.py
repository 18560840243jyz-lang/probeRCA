"""Burst evidence aggregation for candidate-level group-penalty adjustment."""

from __future__ import annotations

from collections import defaultdict

from proberca.data.schema import EvidenceObservationRecord

from .model import MetricNode


def _channel_category(channel_id: str) -> str | None:
    value = channel_id.lower()
    mapping = (
        ("localnet", "LocalNet"), ("socket", "LocalNet"),
        ("memory", "Memory"), ("reclaim", "Memory"), ("oom", "Memory"),
        ("futex", "Lock"), ("lock", "Lock"),
        ("block", "IO"), ("io", "IO"),
        ("runqueue", "CPU"), ("wakeup", "CPU"), ("sched", "CPU"),
        ("nic", "NIC"), ("softirq", "NIC"),
        ("dns", "DNS"),
        ("tcp", "TCP"), ("rtt", "TCP"), ("retrans", "TCP"), ("rto", "TCP"),
    )
    return next((category for token, category in mapping if token in value), None)


def _target_entity(target_id: str) -> str:
    if "::shock::" in target_id:
        return target_id.split("::shock::", 1)[0]
    return target_id


def aggregate_burst_evidence(
    evidence: list[EvidenceObservationRecord],
    groups: dict[tuple[str, str], tuple[MetricNode, ...]],
) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], tuple[str, ...]]]:
    """Return H_c using max-per-channel then noisy-OR across independent channels."""
    strengths: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    identifiers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for item in evidence:
        if item.source_type != "burst_event" or not item.independent_from_residual:
            continue
        psi = (
            float(item.normalized_strength)
            * float(item.observation_quality)
            * float(item.reliability_weight)
        )
        if psi <= 0:
            continue
        category = _channel_category(item.channel_id)
        if category is None:
            continue
        target = _target_entity(item.target_id)
        for key, metrics in groups.items():
            entity_id, root_category = key
            exact_metric = any(item.target_id == metric.node_id for metric in metrics)
            entity_match = target in {entity_id, *(metric.node_id for metric in metrics)}
            if not exact_metric and not entity_match:
                continue
            if category != root_category:
                continue
            strengths[key][item.channel_id] = max(
                strengths[key].get(item.channel_id, 0.0), min(psi, 1.0),
            )
            identifiers[key].add(item.evidence_id)
    combined = {}
    for key in groups:
        product = 1.0
        for value in strengths.get(key, {}).values():
            product *= 1.0 - value
        combined[key] = 1.0 - product
    return combined, {
        key: tuple(sorted(identifiers.get(key, set()))) for key in groups
    }

"""Burst evidence aggregation for candidate-level group-penalty adjustment."""

from __future__ import annotations

from collections import defaultdict

from proberca.data.schema import EvidenceObservationRecord

from .config import (
    EXPERIMENTAL_DNS_BURST_CHANNEL_IDS,
    default_burst_channel_roles,
)
from .model import MetricNode


def _target_entity(target_id: str) -> str:
    if "::shock::" in target_id:
        return target_id.split("::shock::", 1)[0]
    return target_id


def aggregate_burst_evidence(
    evidence: list[EvidenceObservationRecord],
    groups: dict[tuple[str, str], tuple[MetricNode, ...]],
) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], tuple[str, ...]]]:
    """Return H_c using max-per-channel then noisy-OR across independent channels."""
    channel_roles = {
        item["channel_id"]: item for item in default_burst_channel_roles()
    }
    strengths: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    identifiers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for item in evidence:
        if item.channel_id in EXPERIMENTAL_DNS_BURST_CHANNEL_IDS:
            continue
        if item.source_type != "burst_event" or not item.independent_from_residual:
            raise ValueError("Burst evidence independence is not established")
        channel_role = channel_roles.get(item.channel_id)
        if channel_role is None:
            raise ValueError(f"unknown Burst channel {item.channel_id}")
        psi = (
            float(item.normalized_strength)
            * float(item.observation_quality)
            * float(item.reliability_weight)
        )
        if psi <= 0:
            continue
        category = channel_role["root_category"]
        target = _target_entity(item.target_id)
        matches = []
        for key, metrics in groups.items():
            entity_id, root_category = key
            exact_metric = any(item.target_id == metric.node_id for metric in metrics)
            entity_match = target in {entity_id, *(metric.node_id for metric in metrics)}
            if not exact_metric and not entity_match:
                continue
            if category != root_category:
                continue
            matches.append(key)
            if metrics[0].entity_type not in channel_role["entity_types"]:
                raise ValueError("Burst channel does not match its candidate group")
            strengths[key][item.channel_id] = max(
                strengths[key].get(item.channel_id, 0.0), min(psi, 1.0),
            )
            identifiers[key].add(item.evidence_id)
        if len(matches) != 1:
            raise ValueError("Burst target must match exactly one candidate group")
    combined = {}
    for key in groups:
        product = 1.0
        for value in strengths.get(key, {}).values():
            product *= 1.0 - value
        combined[key] = 1.0 - product
    return combined, {
        key: tuple(sorted(identifiers.get(key, set()))) for key in groups
    }

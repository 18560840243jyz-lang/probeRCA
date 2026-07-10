"""Dataclass schemas for probeRCA P0 records."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass, dataclass
from typing import Any


@dataclass
class MetricRecord:
    """Metric observation for one service instance node at one timestamp."""

    timestamp: float
    service: str
    instance: str
    node: str
    metric: str
    value: float
    source: str
    incident_id: str | None = None


@dataclass
class EvidenceRecord:
    """Semantic evidence observation used to describe a root-cause type."""

    timestamp: float
    service: str
    instance: str
    node: str
    evidence_type: str
    metric: str
    value: float
    source: str
    probe_id: str
    sampling_rate: float
    incident_id: str | None = None


@dataclass
class IncidentRecord:
    """Fault injection label for offline P0 validation."""

    incident_id: str
    root_service: str
    root_metric: str
    root_type: str
    symptom_service: str
    start_ts: float
    end_ts: float
    injected_path: list[str]


@dataclass
class RCAResult:
    """Root cause analysis output record for one incident."""

    incident_id: str
    symptom_service: str
    top_services: list[dict]
    top_metrics: list[dict]
    root_type: str
    evidence: list[str]
    path: list[str]
    latency_ms: float | None = None


def to_dict(record: Any) -> dict:
    """Convert a dataclass record or dictionary to a plain dictionary."""

    if is_dataclass(record):
        return asdict(record)
    if isinstance(record, dict):
        return dict(record)
    raise TypeError(f"Unsupported record type for to_dict: {type(record).__name__}")

"""Service graph schema for probeRCA P0."""

from __future__ import annotations

from dataclasses import dataclass

ALLOWED_EDGE_TYPES = {"call", "trace", "cohost", "resource", "synthetic"}


@dataclass
class ServiceNode:
    """Service node in the pseudo-distributed service graph."""

    service: str
    instance: str | None = None
    node: str | None = None


@dataclass
class GraphEdge:
    """Directed graph edge between services or service-related nodes."""

    src: str
    dst: str
    edge_type: str
    weight: float = 1.0

    def __post_init__(self) -> None:
        validate_edge_type(self.edge_type)


def validate_edge_type(edge_type: str) -> None:
    """Validate that an edge type belongs to the P0 graph schema."""

    if edge_type not in ALLOWED_EDGE_TYPES:
        allowed = ", ".join(sorted(ALLOWED_EDGE_TYPES))
        raise ValueError(f"Invalid edge_type {edge_type!r}. Allowed edge types: {allowed}")

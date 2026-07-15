"""Explicit runtime call-edge providers; Kubernetes membership is never a call."""
from __future__ import annotations

from dataclasses import asdict

from .contracts import CallEdgeObservation, canonical_hash


class CallEdgeProviderError(ValueError):
    """Call edge observations are invalid or reference unknown services."""


class ExplicitCallEdgeProvider:
    def __init__(self, cluster_id: str, edges: tuple[dict, ...]):
        self.cluster_id = cluster_id
        self.edges = edges
        self.fingerprint = canonical_hash({"cluster_id": cluster_id, "edges": edges})

    @classmethod
    def from_dicts(cls, cluster_id: str, edges: list[dict]):
        required = {"source_service_id", "destination_service_id", "protocol", "request_count"}
        canonical = []
        for edge in edges:
            if set(edge) != required:
                raise CallEdgeProviderError("explicit call edge fields mismatch")
            if not edge["source_service_id"].startswith(cluster_id + "::") or \
                    not edge["destination_service_id"].startswith(cluster_id + "::"):
                raise CallEdgeProviderError("explicit call edge crosses cluster")
            if float(edge["request_count"]) <= 0:
                continue
            canonical.append(dict(edge))
        canonical.sort(key=lambda item: (
            item["source_service_id"], item["destination_service_id"], item["protocol"]))
        return cls(cluster_id, tuple(canonical))

    def collect_window(self, window_start_ns, window_end_ns, inventory_revision):
        known = {f"{self.cluster_id}::{(raw.get('metadata') or {}).get('namespace')}::"
                 f"{(raw.get('metadata') or {}).get('name')}"
                 for raw in inventory_revision.objects_by_kind.get("Service", {}).values()}
        output = []
        for edge in self.edges:
            source, destination = edge["source_service_id"], edge["destination_service_id"]
            if source not in known or destination not in known:
                raise CallEdgeProviderError("call edge endpoint is absent from inventory")
            namespaces = tuple(sorted({source.split("::")[1], destination.split("::")[1]}))
            seed = {**edge, "start": window_start_ns, "end": window_end_ns}
            output.append(CallEdgeObservation(
                canonical_hash(seed), self.cluster_id, namespaces, source, destination,
                edge["protocol"], float(edge["request_count"]), None, None, 1.0,
                window_start_ns, window_end_ns, "explicit", (), self.fingerprint,
            ))
        return tuple(output)


class PrometheusCallEdgeProvider:
    def __init__(self, collector, ttl_windows: int = 1):
        self.collector = collector
        self.ttl_windows = ttl_windows

    def collect_window(self, window_start_ns, window_end_ns, inventory_revision):
        return tuple(item for item in self.collector(
            window_start_ns, window_end_ns, inventory_revision) if item.request_count > 0)

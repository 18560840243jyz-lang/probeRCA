"""Strict, algorithm-free records at the data-plane/control-plane boundary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from proberca.data.schema import (
    EdgeMetricRecord,
    EvidenceObservationRecord,
    NodeMetricRecord,
    TopologySnapshot,
)


FORBIDDEN_INFERENCE_FIELDS = frozenset({
    "root_service",
    "root_metric",
    "root_type",
    "root_edge",
    "target_service",
    "target_metric",
    "target_fault_type",
    "target_edge",
    "injected_path",
    "injection_method",
    "incident_label",
    "ground_truth",
    "expected_root",
})


class GroundTruthFieldError(ValueError):
    """Ground-truth or target-aware content attempted to cross the plane boundary."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def assert_label_safe(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        forbidden = FORBIDDEN_INFERENCE_FIELDS & set(value)
        if forbidden:
            names = ",".join(sorted(forbidden))
            raise GroundTruthFieldError(
                f"ground_truth_field_forbidden at {path}: {names}"
            )
        for key, child in value.items():
            assert_label_safe(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            assert_label_safe(child, f"{path}[{index}]")


def _strict_records(name: str, values: Iterable, expected_type: type) -> tuple:
    result = tuple(values)
    if any(not isinstance(item, expected_type) for item in result):
        raise TypeError(f"{name} must contain {expected_type.__name__}")
    return result


def _record_source_id(record) -> str:
    if isinstance(record, (NodeMetricRecord, EdgeMetricRecord)):
        return (
            f"{record.record_type}:{record.stable_id}:"
            f"{record.timestamp_ns}:{record.series_id}"
        )
    if isinstance(record, TopologySnapshot):
        return f"topology_snapshot:{record.snapshot_id}"
    if isinstance(record, EvidenceObservationRecord):
        return f"evidence_observation:{record.evidence_id}"
    raise TypeError(f"unsupported data-plane record {type(record).__name__}")


@dataclass(frozen=True)
class CollectedWindow:
    """One fully collected, already aggregated window; it contains no algorithm state."""

    schema_version: str
    sequence: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    node_metrics: tuple[NodeMetricRecord, ...]
    edge_metrics: tuple[EdgeMetricRecord, ...]
    topology_events: tuple[TopologySnapshot, ...]
    burst_evidence: tuple[EvidenceObservationRecord, ...]
    source_record_ids: tuple[str, ...]
    collection_metadata: dict[str, Any]
    window_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        node_metrics=(),
        edge_metrics=(),
        topology_events=(),
        burst_evidence=(),
        source_record_ids=(),
        collection_metadata=None,
    ) -> "CollectedWindow":
        nodes = _strict_records("node_metrics", node_metrics, NodeMetricRecord)
        edges = _strict_records("edge_metrics", edge_metrics, EdgeMetricRecord)
        topology = _strict_records(
            "topology_events", topology_events, TopologySnapshot,
        )
        evidence = _strict_records(
            "burst_evidence", burst_evidence, EvidenceObservationRecord,
        )
        metadata = dict(collection_metadata or {})
        sources = tuple(sorted(source_record_ids or (
            _record_source_id(item)
            for item in (*nodes, *edges, *topology, *evidence)
        )))
        all_records = (*nodes, *edges, *topology, *evidence)
        payload = {
            "schema_version": "probeRCA-dataplane-window-v1",
            "sequence": sequence,
            "window_start_ns": window_start_ns,
            "window_end_ns": window_end_ns,
            "cluster_id": (
                next(iter({item.cluster_id for item in all_records}))
                if all_records else ""
            ),
            "node_metrics": [item.to_dict() for item in nodes],
            "edge_metrics": [item.to_dict() for item in edges],
            "topology_events": [item.to_dict() for item in topology],
            "burst_evidence": [item.to_dict() for item in evidence],
            "source_record_ids": list(sources),
            "collection_metadata": metadata,
        }
        assert_label_safe(payload)
        return cls._from_payload(payload | {"window_fingerprint": fingerprint(payload)})

    @classmethod
    def _from_payload(cls, payload: dict[str, Any]) -> "CollectedWindow":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ValueError("invalid CollectedWindow fields")
        assert_label_safe(payload)
        result = cls(
            schema_version=payload["schema_version"],
            sequence=payload["sequence"],
            window_start_ns=payload["window_start_ns"],
            window_end_ns=payload["window_end_ns"],
            cluster_id=payload["cluster_id"],
            node_metrics=tuple(
                NodeMetricRecord.from_dict(item) for item in payload["node_metrics"]
            ),
            edge_metrics=tuple(
                EdgeMetricRecord.from_dict(item) for item in payload["edge_metrics"]
            ),
            topology_events=tuple(
                TopologySnapshot.from_dict(item) for item in payload["topology_events"]
            ),
            burst_evidence=tuple(
                EvidenceObservationRecord.from_dict(item)
                for item in payload["burst_evidence"]
            ),
            source_record_ids=tuple(payload["source_record_ids"]),
            collection_metadata=dict(payload["collection_metadata"]),
            window_fingerprint=payload["window_fingerprint"],
        )
        result.validate()
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CollectedWindow":
        return cls._from_payload(dict(payload))

    def validate(self) -> None:
        if self.schema_version != "probeRCA-dataplane-window-v1":
            raise ValueError("unsupported CollectedWindow schema_version")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) \
                or self.sequence <= 0:
            raise ValueError("data-plane window sequence must be positive")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in (self.window_start_ns, self.window_end_ns)) \
                or self.window_start_ns >= self.window_end_ns:
            raise ValueError("data-plane window interval is invalid")
        records = (*self.node_metrics, *self.edge_metrics)
        if not records:
            raise ValueError("data-plane window must contain metrics")
        if any(not self.window_start_ns <= item.timestamp_ns < self.window_end_ns
               for item in records):
            raise ValueError("metric timestamp is outside collected window")
        clusters = {item.cluster_id for item in (
            *records, *self.topology_events, *self.burst_evidence,
        )}
        if clusters != {self.cluster_id}:
            raise ValueError("collected window crosses cluster identity")
        if tuple(sorted(set(self.source_record_ids))) != self.source_record_ids:
            raise ValueError("source_record_ids must be sorted and unique")
        payload = self.to_dict()
        supplied = payload.pop("window_fingerprint")
        if supplied != fingerprint(payload):
            raise ValueError("CollectedWindow fingerprint mismatch")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["node_metrics"] = [item.to_dict() for item in self.node_metrics]
        payload["edge_metrics"] = [item.to_dict() for item in self.edge_metrics]
        payload["topology_events"] = [item.to_dict() for item in self.topology_events]
        payload["burst_evidence"] = [item.to_dict() for item in self.burst_evidence]
        payload["source_record_ids"] = list(self.source_record_ids)
        assert_label_safe(payload)
        return payload

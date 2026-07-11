"""Deterministic stable IDs and bidirectional integer indexes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

from proberca.data.schema import EdgeMetricRecord, NodeMetricRecord

INDEX_FORMAT_VERSION = "2"
LEGACY_INDEX_FORMAT_VERSION = "1"


def node_id(record: NodeMetricRecord) -> str:
    if not isinstance(record, NodeMetricRecord):
        raise TypeError("node_id requires NodeMetricRecord")
    record.validate()
    return record.stable_id


def edge_id(record: EdgeMetricRecord) -> str:
    if not isinstance(record, EdgeMetricRecord):
        raise TypeError("edge_id requires EdgeMetricRecord")
    record.validate()
    return record.stable_id


def shock_id(record: EdgeMetricRecord) -> str:
    if not isinstance(record, EdgeMetricRecord):
        raise TypeError("shock_id requires EdgeMetricRecord")
    record.validate()
    return record.stable_shock_id


def migrate_v1_stable_id(stable_id: str, *, kind: str, cluster_id: str) -> str:
    """Explicitly migrate a cluster-less P1 ID; v1/v2 mixing is never implicit."""
    if kind not in {"node", "edge", "shock"}:
        raise ValueError("kind must be node, edge, or shock")
    if not isinstance(cluster_id, str) or not cluster_id or "::" in cluster_id or "->" in cluster_id:
        raise ValueError("cluster_id is invalid for stable-ID migration")
    if not isinstance(stable_id, str) or not stable_id:
        raise ValueError("legacy stable ID must be non-empty")
    expected_parts = {"node": 3, "edge": 4, "shock": 5}[kind]
    if len(stable_id.split("::")) != expected_parts:
        raise ValueError("stable ID is not an unmigrated v1 ID of the requested kind")
    migrated = f"{cluster_id}::{stable_id}"
    _validate_v2_id(kind, migrated)
    return migrated


def _validate_v2_id(kind: str, stable_id: str) -> None:
    expected_parts = {"node": 4, "edge": 5, "shock": 6}[kind]
    parts = stable_id.split("::")
    if len(parts) != expected_parts or any(not part for part in parts):
        raise ValueError(f"{kind} ID is not cluster-aware format version 2")
    if kind == "node" and "->" in stable_id:
        raise ValueError("node ID must not contain an edge separator")
    if kind in {"edge", "shock"} and parts[2].count("->") != 1:
        raise ValueError(f"{kind} ID must contain one directed edge")
    if kind == "shock" and parts[-2] != "shock":
        raise ValueError("shock ID must contain the shock marker")


def _validated_ids(kind: str, values: Iterable[str]) -> list[str]:
    result = list(values)
    for value in result:
        if not isinstance(value, str):
            raise TypeError(f"{kind} IDs must be strings")
        if not value:
            raise ValueError(f"{kind} IDs must not be empty")
        _validate_v2_id(kind, value)
    if len(result) != len(set(result)):
        raise ValueError(f"duplicate {kind} ID detected")
    return result


@dataclass(frozen=True)
class StableIndex:
    id_to_index: dict[str, int]
    index_to_id: tuple[str, ...]
    id_kinds: dict[str, str]

    def __post_init__(self) -> None:
        expected_ids = tuple(sorted(self.id_to_index, key=self.id_to_index.get))
        if expected_ids != self.index_to_id:
            raise ValueError("id_to_index and index_to_id are inconsistent")
        if set(self.id_to_index) != set(self.id_kinds):
            raise ValueError("id_kinds does not cover exactly the indexed IDs")
        if sorted(self.id_to_index.values()) != list(range(len(self.id_to_index))):
            raise ValueError("integer indexes must be contiguous from zero")
        if any(kind not in {"node", "edge", "shock"} for kind in self.id_kinds.values()):
            raise ValueError("invalid stable index kind")
        for stable_id, kind in self.id_kinds.items():
            _validate_v2_id(kind, stable_id)

    @classmethod
    def build(
        cls,
        *,
        node_ids: Iterable[str],
        edge_ids: Iterable[str],
        shock_ids: Iterable[str],
    ) -> "StableIndex":
        groups = {
            "node": _validated_ids("node", node_ids),
            "edge": _validated_ids("edge", edge_ids),
            "shock": _validated_ids("shock", shock_ids),
        }
        combined = [stable_id for values in groups.values() for stable_id in values]
        if len(combined) != len(set(combined)):
            raise ValueError("duplicate stable ID detected across index kinds")
        ordered = tuple(sorted(combined))
        mapping = {stable_id: index for index, stable_id in enumerate(ordered)}
        kinds = {
            stable_id: kind for kind, values in groups.items() for stable_id in values
        }
        return cls(mapping, ordered, kinds)

    def index_of(self, stable_id: str) -> int:
        try:
            return self.id_to_index[stable_id]
        except KeyError as exc:
            raise KeyError(f"unknown stable ID {stable_id!r}") from exc

    def id_at(self, integer_index: int) -> str:
        if isinstance(integer_index, bool) or not isinstance(integer_index, int):
            raise TypeError("integer_index must be an integer")
        if integer_index < 0 or integer_index >= len(self.index_to_id):
            raise IndexError(f"integer index out of range: {integer_index}")
        try:
            return self.index_to_id[integer_index]
        except IndexError as exc:
            raise IndexError(f"integer index out of range: {integer_index}") from exc

    def to_dict(self) -> dict:
        return {
            "format_version": INDEX_FORMAT_VERSION,
            "entries": [
                {"id": stable_id, "index": index, "kind": self.id_kinds[stable_id]}
                for stable_id, index in sorted(self.id_to_index.items(), key=lambda item: item[1])
            ],
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "StableIndex":
        if not isinstance(payload, dict) or set(payload) != {"format_version", "entries"}:
            raise ValueError("invalid stable index payload fields")
        if payload["format_version"] != INDEX_FORMAT_VERSION:
            raise ValueError("incompatible stable index format_version")
        entries = payload["entries"]
        if not isinstance(entries, list):
            raise TypeError("stable index entries must be a list")
        mapping: dict[str, int] = {}
        kinds: dict[str, str] = {}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"id", "index", "kind"}:
                raise ValueError("invalid stable index entry")
            stable_id, index, kind = entry["id"], entry["index"], entry["kind"]
            if stable_id in mapping:
                raise ValueError("duplicate stable ID in saved index")
            mapping[stable_id] = index
            kinds[stable_id] = kind
        ordered = tuple(stable_id for stable_id, _ in sorted(mapping.items(), key=lambda item: item[1]))
        return cls(mapping, ordered, kinds)

    def save_json(self, path: str | Path) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path) -> "StableIndex":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def save_npz(self, path: str | Path) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            output,
            format_version=np.asarray([INDEX_FORMAT_VERSION]),
            ids=np.asarray(self.index_to_id),
            kinds=np.asarray([self.id_kinds[stable_id] for stable_id in self.index_to_id]),
        )

    @classmethod
    def load_npz(cls, path: str | Path) -> "StableIndex":
        with np.load(Path(path), allow_pickle=False) as payload:
            if set(payload.files) != {"format_version", "ids", "kinds"}:
                raise ValueError("invalid stable index NPZ members")
            version = payload["format_version"].tolist()
            ids = payload["ids"].tolist()
            kinds = payload["kinds"].tolist()
        if version != [INDEX_FORMAT_VERSION] or len(ids) != len(kinds):
            raise ValueError("invalid or incompatible stable index NPZ")
        entries = [
            {"id": stable_id, "index": index, "kind": kind}
            for index, (stable_id, kind) in enumerate(zip(ids, kinds))
        ]
        return cls.from_dict({"format_version": INDEX_FORMAT_VERSION, "entries": entries})

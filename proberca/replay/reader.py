"""Deterministic streaming merge for P1 metric, topology, and evidence records."""

from __future__ import annotations

import heapq
import json
import tempfile
from pathlib import Path
from typing import Iterable, Iterator

from proberca.data.io import iter_records_jsonl, iter_records_parquet, write_records_jsonl
from proberca.data.schema import (
    EdgeMetricRecord, EvidenceObservationRecord, NodeMetricRecord, StrictRecord,
    TopologySnapshot,
)
from proberca.orchestration.state import EngineWindowInput

from .manifest import ReplayDatasetManifest, ReplayIntegrityError


class ReplayOrderingError(ValueError):
    """A strict Replay input stream is not deterministically ordered."""


class ReplayRecordConflictError(ValueError):
    """The same record identity and timestamp has conflicting content."""


def _timestamp(record: StrictRecord) -> int:
    return record.valid_from_ns if isinstance(record, TopologySnapshot) else record.timestamp_ns


def _stable_id(record: StrictRecord) -> str:
    if isinstance(record, (NodeMetricRecord, EdgeMetricRecord)):
        return record.stable_id
    if isinstance(record, TopologySnapshot):
        return record.snapshot_id
    if isinstance(record, EvidenceObservationRecord):
        return record.evidence_id
    raise TypeError(f"unsupported Replay record type {type(record).__name__}")


def _key(record: StrictRecord):
    return (_timestamp(record), record.record_type, _stable_id(record))


def _source_id(record: StrictRecord) -> str:
    if isinstance(record, (NodeMetricRecord, EdgeMetricRecord)):
        return f"{record.record_type}:{record.stable_id}:{record.timestamp_ns}:{record.series_id}"
    return f"{record.record_type}:{_stable_id(record)}"


def _deduplicate(records: Iterable[StrictRecord]) -> Iterator[StrictRecord]:
    previous_key = None
    previous_payload = None
    for record in records:
        key = _key(record)
        payload = record.to_dict()
        if key == previous_key:
            if payload != previous_payload:
                raise ReplayRecordConflictError(f"conflicting duplicate record {key}")
            continue
        previous_key, previous_payload = key, payload
        yield record


class ReplayRecordReader:
    def __init__(self, dataset_root, manifest: ReplayDatasetManifest, *,
                 strict_order: bool = True, allow_explicit_reorder: bool = False,
                 parquet_batch_size: int = 1024):
        root = Path(dataset_root).resolve()
        if not isinstance(manifest, ReplayDatasetManifest) or root != manifest.dataset_root:
            raise ReplayIntegrityError("reader dataset root does not match manifest")
        if strict_order and allow_explicit_reorder:
            raise ValueError("strict_order and allow_explicit_reorder are mutually exclusive")
        if not strict_order and not allow_explicit_reorder:
            raise ValueError("non-strict Replay requires explicit reorder permission")
        if isinstance(parquet_batch_size, bool) or not isinstance(parquet_batch_size, int) \
                or parquet_batch_size <= 0:
            raise ValueError("parquet_batch_size must be positive")
        self.root = root
        self.manifest = manifest
        self.strict_order = strict_order
        self.allow_explicit_reorder = allow_explicit_reorder
        self.parquet_batch_size = parquet_batch_size
        self._reordered = False

    def _validate_record(self, record, expected_type: str) -> None:
        if record.record_type != expected_type or record.record_type not in self.manifest.allowed_record_types:
            raise ReplayIntegrityError(
                f"{expected_type} stream contains forbidden record_type={record.record_type}")
        if getattr(record, "schema_version", None) not in self.manifest.expected_schema_versions:
            raise ReplayIntegrityError("record schema_version is not allowed")
        if record.cluster_id != self.manifest.cluster_id:
            raise ReplayIntegrityError("record cluster does not match manifest")
        namespaces = set(self.manifest.namespaces)
        if hasattr(record, "namespace") and record.namespace not in namespaces:
            raise ReplayIntegrityError("record namespace does not match manifest")

    def _strict(self, records: Iterable[StrictRecord], expected_type: str):
        previous = None
        for record in records:
            self._validate_record(record, expected_type)
            key = _key(record)
            if previous is not None and key < previous:
                raise ReplayOrderingError(f"{expected_type} input is not ordered: {key} < {previous}")
            previous = key
            yield record

    def _reordered_stream(self, records: Iterable[StrictRecord], expected_type: str, temp: Path):
        run_paths = []
        chunk = []
        previous = None
        for record in records:
            self._validate_record(record, expected_type)
            key = _key(record)
            if previous is not None and key < previous:
                self._reordered = True
            previous = key
            chunk.append(record)
            if len(chunk) >= self.parquet_batch_size:
                run = temp / f"{expected_type}-{len(run_paths)}.jsonl"
                write_records_jsonl(run, sorted(chunk, key=_key)); run_paths.append(run); chunk = []
        if chunk:
            run = temp / f"{expected_type}-{len(run_paths)}.jsonl"
            write_records_jsonl(run, sorted(chunk, key=_key)); run_paths.append(run)
        streams = [iter_records_jsonl(path) for path in run_paths]
        yield from heapq.merge(*streams, key=_key)

    def _source_streams(self, temp: Path):
        sources = [
            (iter_records_parquet(self.manifest.resolve_data_path(self.manifest.node_metrics_file),
                                  batch_size=self.parquet_batch_size), "node_metric"),
            (iter_records_parquet(self.manifest.resolve_data_path(self.manifest.edge_metrics_file),
                                  batch_size=self.parquet_batch_size), "edge_metric"),
            (iter_records_jsonl(self.manifest.resolve_data_path(self.manifest.topology_file)),
             "topology_snapshot"),
        ]
        if self.manifest.evidence_file is not None:
            sources.append((iter_records_parquet(
                self.manifest.resolve_data_path(self.manifest.evidence_file),
                batch_size=self.parquet_batch_size), "evidence_observation"))
        output = []
        for records, expected in sources:
            output.append(self._strict(records, expected) if self.strict_order else
                          self._reordered_stream(records, expected, temp))
        return output

    def iter_windows(self) -> Iterator[EngineWindowInput]:
        with tempfile.TemporaryDirectory(prefix="proberca-replay-") as directory:
            merged = _deduplicate(heapq.merge(*self._source_streams(Path(directory)), key=_key))
            pending_topology = []
            pending_evidence = []
            current_window_end = None
            node_records = []
            edge_records = []
            sequence = 0

            def emit(window_end):
                nonlocal sequence, pending_topology, pending_evidence, node_records, edge_records
                if not node_records and not edge_records:
                    return None
                sequence += 1
                window_ns = self.manifest.window_sec * 1_000_000_000
                sources = [_source_id(item) for item in
                           [*node_records, *edge_records, *pending_topology, *pending_evidence]]
                result = EngineWindowInput(
                    window_end, window_end - window_ns, window_end,
                    sorted(node_records, key=lambda item: item.stable_id),
                    sorted(edge_records, key=lambda item: item.stable_id),
                    sorted(pending_topology, key=lambda item: item.snapshot_id),
                    sorted(pending_evidence, key=lambda item: item.evidence_id),
                    sorted(sources), sequence,
                    ([{"reason_code": "explicit_reorder", "detail": "input stream reordered"}]
                     if self._reordered else []),
                )
                node_records = []; edge_records = []; pending_topology = []; pending_evidence = []
                return result

            for record in merged:
                timestamp = _timestamp(record)
                if isinstance(record, TopologySnapshot):
                    pending_topology.append(record)
                    continue
                if isinstance(record, EvidenceObservationRecord):
                    pending_evidence.append(record)
                    continue
                window_ns = self.manifest.window_sec * 1_000_000_000
                window_end = (timestamp // window_ns + 1) * window_ns
                if current_window_end is not None and window_end != current_window_end:
                    output = emit(current_window_end)
                    if output is not None:
                        yield output
                current_window_end = window_end
                if isinstance(record, NodeMetricRecord):
                    node_records.append(record)
                elif isinstance(record, EdgeMetricRecord):
                    edge_records.append(record)
            if current_window_end is not None:
                output = emit(current_window_end)
                if output is not None:
                    yield output

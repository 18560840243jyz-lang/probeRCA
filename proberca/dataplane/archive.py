"""Write-once sealed archives produced by the collection-only data plane."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from .contracts import (
    CollectedWindow,
    assert_label_safe,
    canonical_json,
    fingerprint,
)


MANIFEST_NAME = "collection-manifest.json"
WINDOWS_NAME = "collected-windows.jsonl"


class CollectionArchiveError(ValueError):
    """The collection archive contract is invalid."""


class CollectionArchiveIntegrityError(CollectionArchiveError):
    """A sealed archive was modified or has inconsistent contents."""


class CollectionArchiveNotSealedError(CollectionArchiveError):
    """The control plane attempted to read an unfinished collection."""


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


_ROLE_FIELDS = frozenset({
    "record_type", "metric_name", "entity_type", "role", "scopes",
    "protocols", "root_category", "root_eligible", "transform", "polarity",
})


def _validate_collection_contract(contract: dict[str, Any]) -> None:
    expected = {
        "schema_version", "normal_metric_roles",
        "burst_evidence_source_type", "burst_evidence_semantics", "window_sec",
    }
    if not isinstance(contract, dict) or set(contract) != expected:
        raise CollectionArchiveError("collection contract fields mismatch")
    if contract["schema_version"] != "probeRCA-final-collection-contract-v1":
        raise CollectionArchiveError("unsupported collection contract schema")
    window_sec = contract["window_sec"]
    if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
        raise CollectionArchiveError("collection contract window_sec must be positive")
    if contract["burst_evidence_source_type"] != "burst_event" \
            or contract["burst_evidence_semantics"] \
            != "normalized_strength_times_quality":
        raise CollectionArchiveError("collection contract Burst semantics mismatch")
    roles = contract["normal_metric_roles"]
    if not isinstance(roles, list) or not roles:
        raise CollectionArchiveError("collection contract requires normal metric roles")
    identities = []
    for role in roles:
        if not isinstance(role, dict) or set(role) != _ROLE_FIELDS:
            raise CollectionArchiveError("collection metric role fields mismatch")
        if not isinstance(role["scopes"], (list, tuple)) or not role["scopes"]:
            raise CollectionArchiveError("collection metric role scopes are invalid")
        if not isinstance(role["protocols"], (list, tuple)):
            raise CollectionArchiveError("collection metric role protocols are invalid")
        identity = (
            role["record_type"], role["metric_name"],
            tuple(role["scopes"]), tuple(role["protocols"]),
        )
        identities.append(identity)
    if len(identities) != len(set(identities)):
        raise CollectionArchiveError("collection metric roles contain duplicates")


def _validate_window_contract(
    window: CollectedWindow, contract: dict[str, Any],
) -> None:
    expected_duration = contract["window_sec"] * 1_000_000_000
    if window.window_end_ns - window.window_start_ns != expected_duration:
        raise CollectionArchiveError("window duration conflicts with collection contract")
    roles = contract["normal_metric_roles"]
    for record in (*window.node_metrics, *window.edge_metrics):
        matches = [
            role for role in roles
            if role["record_type"] == record.record_type
            and role["metric_name"] == record.metric_name
            and record.scope in role["scopes"]
            and (
                record.record_type != "edge_metric"
                or not role["protocols"]
                or record.protocol in role["protocols"]
            )
        ]
        if len(matches) != 1:
            raise CollectionArchiveError(
                f"metric {record.stable_id} matched {len(matches)} collection roles"
            )
        if record.window_sec != contract["window_sec"]:
            raise CollectionArchiveError("metric window_sec conflicts with collection contract")
    observed: dict[tuple[str, str], set[str]] = {}
    for record in window.node_metrics:
        matching = [
            role for role in roles
            if role["record_type"] == record.record_type
            and role["metric_name"] == record.metric_name
            and record.scope in role["scopes"]
        ]
        entity_type = matching[0]["entity_type"]
        if entity_type == "service":
            entity_id = f"{record.cluster_id}::{record.namespace}::{record.service_name}"
        elif entity_type == "host":
            entity_id = f"{record.cluster_id}::host::{record.node_name}"
        else:
            raise CollectionArchiveError("node metric has an invalid entity type")
        entity_key = (entity_type, entity_id)
        if record.metric_name in observed.setdefault(entity_key, set()):
            raise CollectionArchiveError(
                f"duplicate metric {record.metric_name} for {entity_id}"
            )
        observed[entity_key].add(record.metric_name)
    for record in window.edge_metrics:
        entity_id = (
            f"{record.cluster_id}::{record.namespace}::"
            f"{record.src_service}->{record.dst_service}::{record.protocol}"
        )
        entity_key = ("edge", entity_id)
        if record.metric_name in observed.setdefault(entity_key, set()):
            raise CollectionArchiveError(
                f"duplicate metric {record.metric_name} for {entity_id}"
            )
        observed[entity_key].add(record.metric_name)
    for (entity_type, entity_id), names in observed.items():
        if entity_type == "edge":
            protocol = entity_id.rsplit("::", 1)[1]
            expected = {
                role["metric_name"] for role in roles
                if role["entity_type"] == "edge"
                and (not role["protocols"] or protocol in role["protocols"])
            }
        else:
            expected = {
                role["metric_name"] for role in roles
                if role["entity_type"] == entity_type
            }
        if names != expected:
            missing = ",".join(sorted(expected - names))
            extra = ",".join(sorted(names - expected))
            raise CollectionArchiveError(
                f"incomplete {entity_type} metric set for {entity_id}; "
                f"missing={missing or '-'}; extra={extra or '-'}"
            )
    for evidence in window.burst_evidence:
        if evidence.source_type != contract["burst_evidence_source_type"]:
            raise CollectionArchiveError("non-Burst evidence crossed the data-plane boundary")


def _topology_endpoint(snapshot, explicit_namespace: str | None, service: str) -> str:
    if explicit_namespace is not None:
        return f"{snapshot.cluster_id}::{explicit_namespace}::{service}"
    matches = [
        item for item in snapshot.services if item.split("::", 1)[1] == service
    ]
    if len(matches) != 1:
        raise CollectionArchiveError("topology endpoint namespace is ambiguous")
    return f"{snapshot.cluster_id}::{matches[0]}"


def _validate_topology_coverage(window: CollectedWindow, snapshots: list) -> None:
    active = [
        item for item in snapshots
        if item.valid_from_ns <= window.window_end_ns < item.valid_to_ns
    ]
    if len(active) != 1:
        raise CollectionArchiveError(
            f"window {window.sequence} has {len(active)} active topology snapshots"
        )
    snapshot = active[0]
    expected_services = {
        f"{snapshot.cluster_id}::{item}" for item in snapshot.services
    }
    placed_services = {
        f"{snapshot.cluster_id}::{item.namespace}::{item.service_name}"
        for item in snapshot.service_nodes
    }
    if placed_services != expected_services:
        raise CollectionArchiveError("active topology lacks exact service placement coverage")
    expected_hosts = {
        f"{snapshot.cluster_id}::host::{item.node_name}"
        for item in snapshot.service_nodes
    }
    expected_edges = set()
    for edge in snapshot.call_edges:
        if edge.relation_type != "call":
            continue
        source = _topology_endpoint(snapshot, edge.src_namespace, edge.src_service)
        _topology_endpoint(snapshot, edge.dst_namespace, edge.dst_service)
        namespace = edge.src_namespace or source.split("::", 2)[1]
        expected_edges.add(
            f"{snapshot.cluster_id}::{namespace}::"
            f"{edge.src_service}->{edge.dst_service}::{edge.protocol or 'tcp'}"
        )
    observed_services = {
        f"{item.cluster_id}::{item.namespace}::{item.service_name}"
        for item in window.node_metrics if item.scope == "service"
    }
    observed_hosts = {
        f"{item.cluster_id}::host::{item.node_name}"
        for item in window.node_metrics if item.scope == "node"
    }
    observed_edges = {
        f"{item.cluster_id}::{item.namespace}::"
        f"{item.src_service}->{item.dst_service}::{item.protocol}"
        for item in window.edge_metrics
    }
    for name, expected, observed in (
        ("service", expected_services, observed_services),
        ("host", expected_hosts, observed_hosts),
        ("directed edge", expected_edges, observed_edges),
    ):
        if expected != observed:
            missing = ",".join(sorted(expected - observed))
            extra = ",".join(sorted(observed - expected))
            raise CollectionArchiveError(
                f"topology {name} metric coverage mismatch; "
                f"missing={missing or '-'}; extra={extra or '-'}"
            )


@dataclass(frozen=True)
class CollectionArchive:
    schema_version: str
    dataset_id: str
    cluster_id: str
    namespaces: tuple[str, ...]
    window_sec: int
    start_ns: int
    end_ns: int
    window_count: int
    windows_file: str
    windows_sha256: str
    collection_contract: dict[str, Any]
    collection_contract_fingerprint: str
    source_description: str
    collection_metadata: dict[str, Any]
    created_at_ns: int
    sealed: bool
    manifest_fingerprint: str
    root: Path

    @classmethod
    def load(cls, root: str | Path) -> "CollectionArchive":
        directory = Path(root).resolve()
        manifest_path = directory / MANIFEST_NAME
        if not manifest_path.is_file():
            raise CollectionArchiveNotSealedError(
                f"collection archive is not sealed: {manifest_path}"
            )
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CollectionArchiveIntegrityError("cannot read collection manifest") from error
        expected = set(cls.__dataclass_fields__) - {"root"}
        if not isinstance(payload, dict) or set(payload) != expected:
            raise CollectionArchiveIntegrityError("collection manifest fields mismatch")
        assert_label_safe(payload)
        supplied = payload.pop("manifest_fingerprint")
        if supplied != fingerprint(payload):
            raise CollectionArchiveIntegrityError("collection manifest fingerprint mismatch")
        namespaces = payload.get("namespaces")
        if not isinstance(namespaces, list):
            raise CollectionArchiveIntegrityError("collection namespaces must be a list")
        payload["namespaces"] = tuple(namespaces)
        archive = cls(**payload, manifest_fingerprint=supplied, root=directory)
        archive.validate()
        return archive

    def validate(self) -> None:
        if self.schema_version != "probeRCA-dataplane-archive-v1":
            raise CollectionArchiveError("unsupported collection archive schema")
        if self.sealed is not True:
            raise CollectionArchiveNotSealedError("collection manifest is not sealed")
        if not self.dataset_id or not self.cluster_id or not self.source_description:
            raise CollectionArchiveError("collection identity fields must be non-empty")
        if tuple(sorted(set(self.namespaces))) != self.namespaces or not self.namespaces:
            raise CollectionArchiveError("collection namespaces must be sorted and non-empty")
        if isinstance(self.window_sec, bool) or not isinstance(self.window_sec, int) \
                or self.window_sec <= 0:
            raise CollectionArchiveError("collection window_sec must be positive")
        if self.start_ns < 0 or self.start_ns >= self.end_ns or self.window_count <= 0:
            raise CollectionArchiveError("collection range or window count is invalid")
        if self.windows_file != WINDOWS_NAME:
            raise CollectionArchiveError("collection archive uses a non-canonical windows file")
        if self.collection_contract_fingerprint != fingerprint(self.collection_contract):
            raise CollectionArchiveIntegrityError("collection contract fingerprint mismatch")
        windows_path = self.root / self.windows_file
        if not windows_path.is_file() or _file_sha256(windows_path) != self.windows_sha256:
            raise CollectionArchiveIntegrityError("collected windows SHA-256 mismatch")
        assert_label_safe(self.collection_contract)
        assert_label_safe(self.collection_metadata)
        _validate_collection_contract(self.collection_contract)

    def iter_windows(self) -> Iterator[CollectedWindow]:
        previous_sequence = 0
        previous_end = None
        count = 0
        topology_snapshots = []
        with (self.root / self.windows_file).open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    raise CollectionArchiveIntegrityError(
                        f"blank collected window line {line_number}"
                    )
                try:
                    window = CollectedWindow.from_dict(json.loads(line))
                except Exception as error:
                    raise CollectionArchiveIntegrityError(
                        f"invalid collected window line {line_number}: {error}"
                    ) from error
                if window.cluster_id != self.cluster_id:
                    raise CollectionArchiveIntegrityError("window cluster conflicts with archive")
                if window.sequence != previous_sequence + 1:
                    raise CollectionArchiveIntegrityError("window sequence is not contiguous")
                if previous_end is not None and window.window_start_ns < previous_end:
                    raise CollectionArchiveIntegrityError("collected windows overlap")
                if window.window_end_ns - window.window_start_ns \
                        != self.window_sec * 1_000_000_000:
                    raise CollectionArchiveIntegrityError("window duration conflicts with archive")
                _validate_window_contract(window, self.collection_contract)
                topology_snapshots.extend(window.topology_events)
                _validate_topology_coverage(window, topology_snapshots)
                previous_sequence = window.sequence
                previous_end = window.window_end_ns
                count += 1
                yield window
        if count != self.window_count:
            raise CollectionArchiveIntegrityError("window count conflicts with manifest")


class CollectionArchiveWriter:
    """Accumulate collection output and atomically publish one sealed archive."""

    def __init__(
        self,
        root: str | Path,
        *,
        dataset_id: str,
        collection_contract: dict[str, Any],
        source_description: str,
        collection_metadata: dict[str, Any] | None = None,
        clock_ns=time.time_ns,
    ) -> None:
        self.root = Path(root).resolve()
        self.dataset_id = dataset_id
        self.collection_contract = dict(collection_contract)
        self.source_description = source_description
        self.collection_metadata = dict(collection_metadata or {})
        self.clock_ns = clock_ns
        self._windows: list[CollectedWindow] = []
        self._topology_snapshots = []
        self._sealed = False
        if not dataset_id or not source_description:
            raise CollectionArchiveError("dataset_id and source_description are required")
        assert_label_safe(self.collection_contract)
        assert_label_safe(self.collection_metadata)
        _validate_collection_contract(self.collection_contract)

    def append(self, window: CollectedWindow) -> None:
        if self._sealed:
            raise CollectionArchiveError("cannot append to a sealed archive")
        if not isinstance(window, CollectedWindow):
            raise TypeError("data plane accepts only CollectedWindow")
        window.validate()
        _validate_window_contract(window, self.collection_contract)
        topology_snapshots = [*self._topology_snapshots, *window.topology_events]
        _validate_topology_coverage(window, topology_snapshots)
        if self._windows:
            previous = self._windows[-1]
            if window.sequence != previous.sequence + 1:
                raise CollectionArchiveError("data-plane sequence must be contiguous")
            if window.window_start_ns < previous.window_end_ns:
                raise CollectionArchiveError("data-plane windows must not overlap")
            if window.cluster_id != previous.cluster_id:
                raise CollectionArchiveError("one archive cannot cross clusters")
            if window.window_end_ns - window.window_start_ns != \
                    previous.window_end_ns - previous.window_start_ns:
                raise CollectionArchiveError("one archive cannot mix window durations")
        elif window.sequence != 1:
            raise CollectionArchiveError("first data-plane sequence must be 1")
        self._windows.append(window)
        self._topology_snapshots = topology_snapshots

    def extend(self, windows: Iterable[CollectedWindow]) -> None:
        for window in windows:
            self.append(window)

    def seal(self) -> CollectionArchive:
        if self._sealed:
            raise CollectionArchiveError("collection archive is already sealed")
        if not self._windows:
            raise CollectionArchiveError("cannot seal an empty collection archive")
        self.root.mkdir(parents=True, exist_ok=True)
        manifest_path = self.root / MANIFEST_NAME
        windows_path = self.root / WINDOWS_NAME
        if manifest_path.exists() or windows_path.exists():
            raise CollectionArchiveError("collection archive target is not empty")
        temporary = self.root / f".{WINDOWS_NAME}.{os.getpid()}.tmp"
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                for window in self._windows:
                    handle.write(canonical_json(window.to_dict()))
                    handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, windows_path)
            first, last = self._windows[0], self._windows[-1]
            namespaces = tuple(sorted({
                item.namespace
                for window in self._windows
                for item in (*window.node_metrics, *window.edge_metrics, *window.burst_evidence)
            }))
            payload = {
                "schema_version": "probeRCA-dataplane-archive-v1",
                "dataset_id": self.dataset_id,
                "cluster_id": first.cluster_id,
                "namespaces": list(namespaces),
                "window_sec": (first.window_end_ns - first.window_start_ns) // 1_000_000_000,
                "start_ns": first.window_start_ns,
                "end_ns": last.window_end_ns,
                "window_count": len(self._windows),
                "windows_file": WINDOWS_NAME,
                "windows_sha256": _file_sha256(windows_path),
                "collection_contract": self.collection_contract,
                "collection_contract_fingerprint": fingerprint(self.collection_contract),
                "source_description": self.source_description,
                "collection_metadata": self.collection_metadata,
                "created_at_ns": int(self.clock_ns()),
                "sealed": True,
            }
            assert_label_safe(payload)
            payload["manifest_fingerprint"] = fingerprint(payload)
            manifest_temporary = self.root / f".{MANIFEST_NAME}.{os.getpid()}.tmp"
            manifest_temporary.write_text(
                canonical_json(payload) + "\n", encoding="utf-8",
            )
            os.replace(manifest_temporary, manifest_path)
            self._sealed = True
            return CollectionArchive.load(self.root)
        finally:
            if temporary.exists():
                temporary.unlink()

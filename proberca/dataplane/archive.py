"""Write-once sealed archives produced by the collection-only data plane."""

from __future__ import annotations

import hashlib
import json
import os
import time
from bisect import bisect_left, bisect_right
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
    "unit", "metric_kind", "aggregation", "source_scope", "quantile",
    "aggregation_formula",
})
_CONTRACT_FIELDS = frozenset({
    "schema_version", "normal_metric_roles", "aggregation_output_source",
    "aggregation_config_fingerprint", "source_description",
    "burst_evidence_source_type", "burst_evidence_semantics",
    "burst_channel_roles", "burst_config_fingerprint", "window_sec",
})
_BURST_ROLE_FIELDS = frozenset({
    "channel_id", "root_category", "entity_types",
})
_WINDOW_METADATA_FIELDS = frozenset({
    "collector_build_fingerprint",
    "aggregation_config_fingerprint",
    "burst_config_fingerprint",
})
_HEX = frozenset("0123456789abcdef")
_FINAL_AGGREGATIONS = frozenset({
    "counter_delta_then_cross_series_sum_rate",
    "counter_delta_then_cross_series_sum_ratio",
    "ratio_from_summed_components",
    "histogram_merge_quantile",
    "time_weighted_window_ratio",
    "cross_series_sum_delta",
})


def _require_sha256(name: str, value: Any) -> None:
    if not isinstance(value, str) or len(value) != 64 \
            or any(character not in _HEX for character in value):
        raise CollectionArchiveError(f"{name} must be an opaque lowercase SHA-256")


def _validate_collection_contract(contract: dict[str, Any]) -> None:
    if not isinstance(contract, dict) or set(contract) != _CONTRACT_FIELDS:
        raise CollectionArchiveError("collection contract fields mismatch")
    if contract["schema_version"] != "probeRCA-final-collection-contract-v2":
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
        if not isinstance(role["unit"], str) or not role["unit"] \
                or role["metric_kind"] not in {"gauge", "delta_counter", "quantile"}:
            raise CollectionArchiveError("collection metric output semantics are invalid")
        if role["aggregation"] not in _FINAL_AGGREGATIONS \
                or not isinstance(role["aggregation_formula"], str) \
                or not role["aggregation_formula"]:
            raise CollectionArchiveError("collection aggregation formula is invalid")
        if role["metric_kind"] == "quantile":
            if role["aggregation"] != "histogram_merge_quantile" \
                    or role["quantile"] != 0.95:
                raise CollectionArchiveError(
                    "P95 output must be produced by merged histograms"
                )
        elif role["quantile"] is not None:
            raise CollectionArchiveError("non-quantile output declares a quantile")
        if role["metric_kind"] == "delta_counter" \
                and role["aggregation"] != "cross_series_sum_delta":
            raise CollectionArchiveError(
                "final delta counters must be summed only after per-series differencing"
            )
        if role["aggregation"].startswith("counter_delta_then_") \
                and role["metric_kind"] != "gauge":
            raise CollectionArchiveError("counter-derived final rates must be gauges")
        if role["aggregation"] == "ratio_from_summed_components" \
                and (role["metric_kind"] != "gauge" or role["unit"] != "ratio"):
            raise CollectionArchiveError(
                "final ratios must be recomputed from summed components"
            )
        identity = (
            role["record_type"], role["metric_name"],
            tuple(role["scopes"]), tuple(role["protocols"]),
        )
        identities.append(identity)
    if len(identities) != len(set(identities)):
        raise CollectionArchiveError("collection metric roles contain duplicates")
    if contract["aggregation_output_source"] != "final_window_aggregation":
        raise CollectionArchiveError("collection aggregation output source mismatch")
    expected_aggregation_fingerprint = fingerprint({
        "output_source": contract["aggregation_output_source"],
        "roles": roles,
    })
    if contract["aggregation_config_fingerprint"] \
            != expected_aggregation_fingerprint:
        raise CollectionArchiveError("aggregation configuration fingerprint mismatch")
    if contract["source_description"] != "final-window-aggregates-v1":
        raise CollectionArchiveError("collection source description is not canonical")
    burst_roles = contract["burst_channel_roles"]
    if not isinstance(burst_roles, list) or not burst_roles:
        raise CollectionArchiveError("collection contract requires Burst channel roles")
    channel_ids = []
    for role in burst_roles:
        if not isinstance(role, dict) or set(role) != _BURST_ROLE_FIELDS:
            raise CollectionArchiveError("Burst channel role fields mismatch")
        if role["root_category"] not in {
            "CPU", "Memory", "IO", "Lock", "LocalNet", "NIC", "TCP", "DNS",
        }:
            raise CollectionArchiveError("Burst channel root category is invalid")
        entity_types = role["entity_types"]
        if not isinstance(entity_types, list) or not entity_types \
                or any(item not in {"service", "host", "edge"} for item in entity_types) \
                or entity_types != sorted(set(entity_types)):
            raise CollectionArchiveError("Burst channel entity types are invalid")
        channel_ids.append(role["channel_id"])
    if len(channel_ids) != len(set(channel_ids)):
        raise CollectionArchiveError("Burst channel roles contain duplicates")
    expected_burst_fingerprint = fingerprint({
        "roles": burst_roles,
        "semantics": contract["burst_evidence_semantics"],
    })
    if contract["burst_config_fingerprint"] != expected_burst_fingerprint:
        raise CollectionArchiveError("Burst configuration fingerprint mismatch")


def _validate_window_metadata(
    window: CollectedWindow, contract: dict[str, Any],
) -> None:
    metadata = window.collection_metadata
    if set(metadata) != _WINDOW_METADATA_FIELDS:
        raise CollectionArchiveError("collected window metadata fields mismatch")
    _require_sha256("collector_build_fingerprint", metadata["collector_build_fingerprint"])
    if metadata["aggregation_config_fingerprint"] \
            != contract["aggregation_config_fingerprint"]:
        raise CollectionArchiveError(
            "window aggregation configuration fingerprint mismatch"
        )
    if metadata["burst_config_fingerprint"] != contract["burst_config_fingerprint"]:
        raise CollectionArchiveError("window Burst configuration fingerprint mismatch")


def _record_entity(record, role: dict[str, Any]) -> str:
    if role["entity_type"] == "service":
        return f"{record.cluster_id}::{record.namespace}::{record.service_name}"
    if role["entity_type"] == "host":
        return f"{record.cluster_id}::host::{record.node_name}"
    return (
        f"{record.cluster_id}::{record.namespace}::"
        f"{record.src_service}->{record.dst_service}::{record.protocol}"
    )


def _validate_burst_evidence(
    window: CollectedWindow, contract: dict[str, Any],
    matched_roles: dict[str, dict[str, Any]],
) -> None:
    channel_roles = {
        item["channel_id"]: item for item in contract["burst_channel_roles"]
    }
    eligible: dict[tuple[str, str], set[str]] = {}
    entity_types: dict[str, str] = {}
    for record in (*window.node_metrics, *window.edge_metrics):
        role = matched_roles[record.stable_id]
        category = role["root_category"]
        if not role["root_eligible"] or category is None:
            continue
        entity_id = _record_entity(record, role)
        entity_types[entity_id] = role["entity_type"]
        eligible.setdefault((entity_id, category), set()).add(
            f"{entity_id}::{record.metric_name}"
        )
    normal_sources = set(window.residual_source_record_ids)
    for evidence in window.burst_evidence:
        role = channel_roles.get(evidence.channel_id)
        if role is None:
            raise CollectionArchiveError(
                f"unknown Burst channel {evidence.channel_id}"
            )
        if evidence.source_type != contract["burst_evidence_source_type"] \
                or evidence.independent_from_residual is not True:
            raise CollectionArchiveError("Burst evidence independence is not established")
        if evidence.config_fingerprint != contract["burst_config_fingerprint"]:
            raise CollectionArchiveError("Burst evidence configuration mismatch")
        _require_sha256("Burst evidence_id", evidence.evidence_id)
        if set(evidence.provenance) != {
            "calibration_id", "collector_build_fingerprint",
            "source_set_fingerprint",
        }:
            raise CollectionArchiveError("Burst evidence provenance fields mismatch")
        for key, value in evidence.provenance.items():
            _require_sha256(f"Burst provenance.{key}", value)
        if evidence.provenance["collector_build_fingerprint"] \
                != window.collection_metadata["collector_build_fingerprint"]:
            raise CollectionArchiveError(
                "Burst collector build does not match the collected window"
            )
        if any(
            not isinstance(source_id, str)
            or not source_id.startswith("source:")
            or len(source_id) != 71
            or any(character not in _HEX for character in source_id[7:])
            for source_id in evidence.source_record_ids
        ):
            raise CollectionArchiveError(
                "Burst source_record_ids must use opaque source:SHA-256 identities"
            )
        if any(
            not isinstance(source_id, str)
            or not source_id.startswith("object:")
            or len(source_id) != 71
            or any(character not in _HEX for character in source_id[7:])
            for source_id in evidence.source_object_ids
        ):
            raise CollectionArchiveError(
                "Burst source_object_ids must use opaque object:SHA-256 identities"
            )
        if evidence.provenance["source_set_fingerprint"] != fingerprint(
            sorted(evidence.source_record_ids)
        ):
            raise CollectionArchiveError("Burst source-set fingerprint mismatch")
        overlap = normal_sources & set(evidence.source_record_ids)
        if overlap:
            raise CollectionArchiveError(
                "Burst evidence source overlaps residual metric source"
            )
        target = evidence.target_id.split("::shock::", 1)[0]
        matched = [
            (entity_id, category)
            for (entity_id, category), metric_ids in eligible.items()
            if category == role["root_category"]
            and (target == entity_id or evidence.target_id in metric_ids)
        ]
        if len(matched) != 1:
            raise CollectionArchiveError("Burst target is not one eligible candidate group")
        entity_id, category = matched[0]
        entity_type = entity_types[entity_id]
        if category != role["root_category"] or entity_type not in role["entity_types"]:
            raise CollectionArchiveError(
                "Burst channel category or target entity type mismatch"
            )
        expected_target_type = "shock" if entity_type == "edge" else "node"
        if evidence.target_type != expected_target_type:
            raise CollectionArchiveError("Burst target_type conflicts with target entity")


def _validate_window_contract(
    window: CollectedWindow, contract: dict[str, Any],
) -> None:
    expected_duration = contract["window_sec"] * 1_000_000_000
    if window.window_end_ns - window.window_start_ns != expected_duration:
        raise CollectionArchiveError("window duration conflicts with collection contract")
    roles = contract["normal_metric_roles"]
    _validate_window_metadata(window, contract)
    matched_roles = {}
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
        role = matches[0]
        matched_roles[record.stable_id] = role
        if record.unit != role["unit"] or record.metric_kind != role["metric_kind"]:
            raise CollectionArchiveError(
                f"metric {record.stable_id} unit or metric_kind conflicts "
                "with frozen aggregation semantics"
            )
        if record.quantile != role["quantile"]:
            raise CollectionArchiveError(
                f"metric {record.stable_id} quantile conflicts with aggregation semantics"
            )
        if record.source != contract["aggregation_output_source"]:
            raise CollectionArchiveError(
                f"metric {record.stable_id} did not come from the frozen final aggregator"
            )
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
    _validate_burst_evidence(window, contract, matched_roles)


def _topology_endpoint(snapshot, explicit_namespace: str | None, service: str) -> str:
    if explicit_namespace is not None:
        return f"{snapshot.cluster_id}::{explicit_namespace}::{service}"
    matches = [
        item for item in snapshot.services if item.split("::", 1)[1] == service
    ]
    if len(matches) != 1:
        raise CollectionArchiveError("topology endpoint namespace is ambiguous")
    return f"{snapshot.cluster_id}::{matches[0]}"


def _validate_topology_snapshot_coverage(
    window: CollectedWindow, snapshot,
) -> None:
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


def _validate_topology_snapshot(snapshot) -> None:
    _require_sha256("topology snapshot_id", snapshot.snapshot_id)
    _require_sha256(
        "topology structure_fingerprint", snapshot.structure_fingerprint,
    )
    expected_structure_fingerprint = fingerprint({
        "cluster": snapshot.cluster_id,
        "services": snapshot.services,
        "calls": [item.to_dict() for item in snapshot.call_edges],
        "hosts": [item.to_dict() for item in snapshot.host_edges],
        "bindings": [item.to_dict() for item in snapshot.service_resources],
    })
    if snapshot.structure_fingerprint != expected_structure_fingerprint:
        raise CollectionArchiveError(
            "topology structure_fingerprint does not match its structure"
        )
    for name in ("inventory_revision_id", "call_edge_provider_fingerprint"):
        value = getattr(snapshot, name)
        if value is not None:
            _require_sha256(f"topology {name}", value)
    for key, value in snapshot.resource_version_vector.items():
        _require_sha256(f"topology resource_version_vector.{key}", value)
    if snapshot.topology_build_issues:
        raise CollectionArchiveError(
            "topology with unresolved build issues cannot be sealed"
        )


class _TopologyVersionTracker:
    """Validate each topology version once and query coverage in O(log n)."""

    def __init__(self) -> None:
        self._starts: list[int] = []
        self._snapshots: list[Any] = []
        self._identities: set[str] = set()

    def prepare(self, snapshots: Iterable) -> tuple:
        additions = tuple(sorted(
            snapshots,
            key=lambda item: (item.valid_from_ns, item.valid_to_ns),
        ))
        identities = [item.snapshot_id for item in additions]
        if (
            len(identities) != len(set(identities))
            or self._identities & set(identities)
        ):
            raise CollectionArchiveError(
                "topology snapshot_id must identify one version"
            )
        for snapshot in additions:
            _validate_topology_snapshot(snapshot)
        for previous, current in zip(additions, additions[1:]):
            if current.valid_from_ns < previous.valid_to_ns:
                raise CollectionArchiveError(
                    "topology versions must not overlap"
                )
        for snapshot in additions:
            position = bisect_left(
                self._starts, snapshot.valid_from_ns,
            )
            if (
                position
                and snapshot.valid_from_ns
                < self._snapshots[position - 1].valid_to_ns
            ) or (
                position < len(self._snapshots)
                and self._snapshots[position].valid_from_ns
                < snapshot.valid_to_ns
            ):
                raise CollectionArchiveError(
                    "topology versions must not overlap"
                )
        return additions

    def active_for(self, window: CollectedWindow, additions: tuple = ()):
        active = [
            item for item in additions
            if item.valid_from_ns <= window.window_start_ns
            and window.window_end_ns <= item.valid_to_ns
        ]
        position = bisect_right(
            self._starts, window.window_start_ns,
        ) - 1
        if position >= 0:
            candidate = self._snapshots[position]
            if window.window_end_ns <= candidate.valid_to_ns:
                active.append(candidate)
        if len(active) != 1:
            raise CollectionArchiveError(
                f"window {window.sequence} has {len(active)} "
                "active topology snapshots"
            )
        return active[0]

    def commit(self, additions: tuple) -> None:
        for snapshot in additions:
            position = bisect_left(
                self._starts, snapshot.valid_from_ns,
            )
            self._starts.insert(position, snapshot.valid_from_ns)
            self._snapshots.insert(position, snapshot)
            self._identities.add(snapshot.snapshot_id)


def _validate_topology_versions(snapshots: list) -> None:
    tracker = _TopologyVersionTracker()
    additions = tracker.prepare(snapshots)
    tracker.commit(additions)


def _validate_topology_coverage(
    window: CollectedWindow, snapshots: list,
) -> None:
    tracker = _TopologyVersionTracker()
    additions = tracker.prepare(snapshots)
    snapshot = tracker.active_for(window, additions)
    _validate_topology_snapshot_coverage(window, snapshot)


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
        if self.schema_version != "probeRCA-dataplane-archive-v2":
            raise CollectionArchiveError("unsupported collection archive schema")
        if self.sealed is not True:
            raise CollectionArchiveNotSealedError("collection manifest is not sealed")
        if not self.dataset_id or not self.cluster_id or not self.source_description:
            raise CollectionArchiveError("collection identity fields must be non-empty")
        _require_sha256("dataset_id", self.dataset_id)
        if self.source_description != self.collection_contract["source_description"]:
            raise CollectionArchiveError("archive source_description is not canonical")
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
        assert_label_safe(self.collection_metadata)
        _validate_collection_contract(self.collection_contract)
        if set(self.collection_metadata) != _WINDOW_METADATA_FIELDS:
            raise CollectionArchiveError("archive collection_metadata fields mismatch")
        _require_sha256(
            "archive collector_build_fingerprint",
            self.collection_metadata["collector_build_fingerprint"],
        )
        if self.collection_metadata["aggregation_config_fingerprint"] \
                != self.collection_contract["aggregation_config_fingerprint"] \
                or self.collection_metadata["burst_config_fingerprint"] \
                != self.collection_contract["burst_config_fingerprint"]:
            raise CollectionArchiveError("archive collection configuration mismatch")

    def iter_windows(self) -> Iterator[CollectedWindow]:
        previous_sequence = 0
        previous_end = None
        count = 0
        topology_tracker = _TopologyVersionTracker()
        normal_source_ids: set[str] = set()
        burst_source_ids: set[str] = set()
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
                additions = topology_tracker.prepare(
                    window.topology_events
                )
                active_topology = topology_tracker.active_for(
                    window, additions,
                )
                _validate_topology_snapshot_coverage(
                    window, active_topology,
                )
                topology_tracker.commit(additions)
                current_normal = {
                    *window.residual_source_record_ids,
                }
                current_burst = {
                    source_id
                    for evidence in window.burst_evidence
                    for source_id in evidence.source_record_ids
                }
                if current_normal & burst_source_ids or current_burst & normal_source_ids:
                    raise CollectionArchiveIntegrityError(
                        "Burst and residual sources overlap across collected windows"
                    )
                normal_source_ids.update(current_normal)
                burst_source_ids.update(current_burst)
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
        self._topology_tracker = _TopologyVersionTracker()
        self._normal_source_ids: set[str] = set()
        self._burst_source_ids: set[str] = set()
        self._sealed = False
        if not dataset_id or not source_description:
            raise CollectionArchiveError("dataset_id and source_description are required")
        _require_sha256("dataset_id", dataset_id)
        if source_description != self.collection_contract.get("source_description"):
            raise CollectionArchiveError("source_description must match the final contract")
        assert_label_safe(self.collection_metadata)
        _validate_collection_contract(self.collection_contract)
        if set(self.collection_metadata) != _WINDOW_METADATA_FIELDS:
            raise CollectionArchiveError("archive collection_metadata fields mismatch")
        _require_sha256(
            "collector_build_fingerprint",
            self.collection_metadata["collector_build_fingerprint"],
        )
        if self.collection_metadata["aggregation_config_fingerprint"] \
                != self.collection_contract["aggregation_config_fingerprint"] \
                or self.collection_metadata["burst_config_fingerprint"] \
                != self.collection_contract["burst_config_fingerprint"]:
            raise CollectionArchiveError("archive collection configuration mismatch")

    def append(self, window: CollectedWindow) -> None:
        if self._sealed:
            raise CollectionArchiveError("cannot append to a sealed archive")
        if not isinstance(window, CollectedWindow):
            raise TypeError("data plane accepts only CollectedWindow")
        window.validate()
        _validate_window_contract(window, self.collection_contract)
        topology_additions = self._topology_tracker.prepare(
            window.topology_events
        )
        active_topology = self._topology_tracker.active_for(
            window, topology_additions,
        )
        _validate_topology_snapshot_coverage(window, active_topology)
        current_normal = {
            *window.residual_source_record_ids,
        }
        current_burst = {
            source_id
            for evidence in window.burst_evidence
            for source_id in evidence.source_record_ids
        }
        if current_normal & self._burst_source_ids \
                or current_burst & self._normal_source_ids:
            raise CollectionArchiveError(
                "Burst and residual sources overlap across collected windows"
            )
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
        self._topology_tracker.commit(topology_additions)
        self._normal_source_ids.update(current_normal)
        self._burst_source_ids.update(current_burst)

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
                "schema_version": "probeRCA-dataplane-archive-v2",
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

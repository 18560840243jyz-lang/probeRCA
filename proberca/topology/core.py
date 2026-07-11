"""Versioned topology snapshots and explicit P3 relation semantics."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from itertools import combinations
from pathlib import Path

from proberca.config import ImpactDerivationRule
from proberca.data.schema import PROBERCA_SCHEMA_VERSION, TopologyEdge, TopologySnapshot

TOPOLOGY_STORE_VERSION = "1"


class TopologyNotFoundError(LookupError):
    """No topology is valid for the requested cluster and timestamp."""


class TopologyOverlapError(ValueError):
    """More than one topology is valid for a cluster interval."""


class TopologyValidationError(ValueError):
    """Topology structure or query scope is invalid."""


class ImpactRuleConflictError(ValueError):
    """Multiple matching impact rules prescribe conflicting directions."""


@dataclass(frozen=True)
class TopologyRelation:
    relation_id: str
    relation_type: str
    src_service_id: str
    dst_service_id: str
    protocol: str | None
    symmetric: bool
    detail: dict[str, object]


@dataclass(frozen=True)
class TopologyGraph:
    snapshot_id: str
    cluster_id: str
    service_ids: list[str]
    call_edges: list[TopologyRelation]
    impact_edges: list[TopologyRelation]
    host_relations: list[TopologyRelation]
    resource_relations: list[TopologyRelation]
    physical_edges: list[TopologyRelation]


def _service_id(cluster: str, namespace: str, service: str) -> str:
    return f"{cluster}::{namespace}::{service}"


def _resolve_endpoint(snapshot: TopologySnapshot, namespace: str | None, service: str) -> str:
    if namespace is not None:
        candidate = f"{namespace}::{service}"
        if candidate not in snapshot.services:
            raise TopologyValidationError(f"unknown topology endpoint {candidate!r} in {snapshot.snapshot_id}")
        return _service_id(snapshot.cluster_id, namespace, service)
    matches = [item for item in snapshot.services if item.split("::", 1)[1] == service]
    if len(matches) != 1:
        raise TopologyValidationError(f"ambiguous topology endpoint {service!r} in {snapshot.snapshot_id}")
    namespace_value, service_name = matches[0].split("::", 1)
    return _service_id(snapshot.cluster_id, namespace_value, service_name)


def _relation_id(cluster: str, src: str, dst: str, relation_type: str) -> str:
    src_parts, dst_parts = src.split("::"), dst.split("::")
    if src_parts[:2] == dst_parts[:2]:
        return f"{cluster}::{src_parts[1]}::{src_parts[2]}->{dst_parts[2]}::{relation_type}"
    return f"{src}->{dst}::{relation_type}"


def _canonical_snapshot(snapshot: TopologySnapshot) -> TopologySnapshot:
    edge_key = lambda item: (item.relation_type, item.src_namespace or "", item.src_service,
                             item.dst_namespace or "", item.dst_service, item.protocol or "",
                             item.resource_type or "", item.resource_id or "")
    return replace(
        snapshot,
        services=sorted(snapshot.services),
        call_edges=sorted(snapshot.call_edges, key=edge_key),
        host_edges=sorted(snapshot.host_edges, key=edge_key),
        resource_edges=sorted(snapshot.resource_edges, key=edge_key),
        service_nodes=sorted(snapshot.service_nodes, key=lambda item: (item.namespace, item.service_name,
                                                                        item.node_name, item.pod_uid or "")),
        service_resources=sorted(snapshot.service_resources,
                                 key=lambda item: (item.resource_type, item.resource_id,
                                                   item.namespace, item.service_name)),
    )


class TopologyStore:
    """Select exactly one snapshot using half-open validity intervals."""

    def __init__(self, snapshots: list[TopologySnapshot] | None = None):
        self._snapshots: list[TopologySnapshot] = []
        for snapshot in snapshots or []:
            self.add(snapshot)

    def add(self, snapshot: TopologySnapshot) -> None:
        if not isinstance(snapshot, TopologySnapshot):
            raise TypeError("TopologyStore accepts TopologySnapshot")
        snapshot.validate()
        for existing in self._snapshots:
            if existing.cluster_id == snapshot.cluster_id and max(existing.valid_from_ns, snapshot.valid_from_ns) < min(existing.valid_to_ns, snapshot.valid_to_ns):
                raise TopologyOverlapError(
                    f"cluster={snapshot.cluster_id} overlapping snapshots={existing.snapshot_id},{snapshot.snapshot_id}"
                )
        self._snapshots.append(_canonical_snapshot(snapshot))
        self._snapshots.sort(key=lambda item: (item.cluster_id, item.valid_from_ns, item.valid_to_ns, item.snapshot_id))

    def query(self, cluster_id: str, timestamp_ns: int, namespace_scope: list[str] | None = None) -> TopologySnapshot:
        matches = [item for item in self._snapshots if item.cluster_id == cluster_id
                   and item.valid_from_ns <= timestamp_ns < item.valid_to_ns]
        if not matches:
            raise TopologyNotFoundError(f"cluster={cluster_id} timestamp_ns={timestamp_ns} has no valid topology")
        if len(matches) != 1:
            raise TopologyOverlapError(f"cluster={cluster_id} timestamp_ns={timestamp_ns} has {len(matches)} topologies")
        snapshot = matches[0]
        namespaces = {item.split("::", 1)[0] for item in snapshot.services}
        if len(namespaces) > 1 and namespace_scope is None:
            raise TopologyValidationError(
                f"cluster={cluster_id} timestamp_ns={timestamp_ns} requires explicit namespace scope"
            )
        if namespace_scope is not None and not set(namespace_scope) <= namespaces:
            raise TopologyValidationError(f"requested namespace scope is absent from snapshot={snapshot.snapshot_id}")
        return snapshot

    def remove_expired(self, before_ns: int) -> None:
        self._snapshots = [item for item in self._snapshots if item.valid_to_ns > before_ns]

    def to_dict(self) -> dict:
        return {"format_version": TOPOLOGY_STORE_VERSION, "schema_version": PROBERCA_SCHEMA_VERSION,
                "snapshots": [item.to_dict() for item in self._snapshots]}

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path) -> "TopologyStore":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if set(payload) != {"format_version", "schema_version", "snapshots"}:
            raise TopologyValidationError("invalid topology store snapshot fields")
        if payload["format_version"] != TOPOLOGY_STORE_VERSION or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise TopologyValidationError("incompatible topology store snapshot version")
        return cls([TopologySnapshot.from_dict(item) for item in payload["snapshots"]])


def build_topology_graph(snapshot: TopologySnapshot, impact_rules: list[ImpactDerivationRule],
                         allow_cross_namespace: bool) -> TopologyGraph:
    if not isinstance(snapshot, TopologySnapshot):
        raise TypeError("snapshot must be TopologySnapshot")
    if any(not isinstance(rule, ImpactDerivationRule) for rule in impact_rules):
        raise TypeError("impact_rules must contain ImpactDerivationRule")
    service_ids = sorted(_service_id(snapshot.cluster_id, *item.split("::", 1)) for item in snapshot.services)
    call_groups: dict[tuple[str, str], list[TopologyEdge]] = {}
    explicit_impact: dict[tuple[str, str], TopologyRelation] = {}
    physical: dict[str, TopologyRelation] = {}
    for edge in snapshot.call_edges:
        src = _resolve_endpoint(snapshot, edge.src_namespace, edge.src_service)
        dst = _resolve_endpoint(snapshot, edge.dst_namespace, edge.dst_service)
        if not allow_cross_namespace and src.split("::")[1] != dst.split("::")[1]:
            continue
        if edge.relation_type == "impact":
            relation_id = _relation_id(snapshot.cluster_id, src, dst, "impact")
            explicit_impact[(src, dst)] = TopologyRelation(relation_id, "impact", src, dst, edge.protocol,
                                                           False, {"source": "explicit", "snapshot_id": snapshot.snapshot_id})
        elif edge.relation_type == "call":
            call_groups.setdefault((src, dst), []).append(edge)
            if edge.protocol is not None:
                src_parts, dst_parts = src.split("::"), dst.split("::")
                if src_parts[:2] == dst_parts[:2]:
                    physical_id = f"{snapshot.cluster_id}::{src_parts[1]}::{src_parts[2]}->{dst_parts[2]}::{edge.protocol}"
                else:
                    physical_id = f"{src}->{dst}::{edge.protocol}"
                physical[physical_id] = TopologyRelation(physical_id, "physical", src, dst, edge.protocol, False,
                                                         {"source_relation_id": _relation_id(snapshot.cluster_id, src, dst, "call")})

    calls: list[TopologyRelation] = []
    derived: dict[tuple[str, str], TopologyRelation] = {}
    for (src, dst), edges in sorted(call_groups.items()):
        relation_id = _relation_id(snapshot.cluster_id, src, dst, "call")
        protocols = sorted({edge.protocol for edge in edges if edge.protocol is not None})
        calls.append(TopologyRelation(relation_id, "call", src, dst, protocols[0] if len(protocols) == 1 else None,
                                      False, {"protocols": protocols, "snapshot_id": snapshot.snapshot_id}))
        for edge in edges:
            matches = [rule for rule in impact_rules if rule.enabled and rule.source_relation_type == "call"
                       and (rule.protocol is None or rule.protocol == edge.protocol)]
            effective = {rule.direction for rule in matches}
            if len(effective) > 1:
                raise ImpactRuleConflictError(
                    f"snapshot={snapshot.snapshot_id} call={relation_id} conflicting_rules={sorted(rule.rule_id for rule in matches)}"
                )
            if not matches or matches[0].direction == "none":
                continue
            rule = sorted(matches, key=lambda item: item.rule_id)[0]
            pairs = []
            if rule.direction in {"forward", "bidirectional"}: pairs.append((src, dst))
            if rule.direction in {"reverse", "bidirectional"}: pairs.append((dst, src))
            for cause, effect in pairs:
                if (cause, effect) in explicit_impact:
                    continue
                impact_id = _relation_id(snapshot.cluster_id, cause, effect, "impact")
                derived[(cause, effect)] = TopologyRelation(
                    impact_id, "impact", cause, effect, edge.protocol, False,
                    {"source": "derived", "source_relation_id": relation_id,
                     "rule_id": rule.rule_id, "provenance_label": rule.provenance_label},
                )

    node_index: dict[str, set[str]] = {}
    for placement in snapshot.service_nodes:
        node_index.setdefault(placement.node_name, set()).add(
            _service_id(snapshot.cluster_id, placement.namespace, placement.service_name)
        )
    hosts: dict[str, TopologyRelation] = {}
    for node_name, services in sorted(node_index.items()):
        for left, right in combinations(sorted(services), 2):
            if not allow_cross_namespace and left.split("::")[1] != right.split("::")[1]: continue
            relation_id = f"{left}<->{right}::host::{node_name}"
            hosts[relation_id] = TopologyRelation(relation_id, "host", left, right, None, True,
                                                  {"node_name": node_name})

    resource_index: dict[tuple[str, str], set[str]] = {}
    for binding in snapshot.service_resources:
        resource_index.setdefault((binding.resource_type, binding.resource_id), set()).add(
            _service_id(snapshot.cluster_id, binding.namespace, binding.service_name)
        )
    resources: dict[str, TopologyRelation] = {}
    for (resource_type, resource_id), services in sorted(resource_index.items()):
        for left, right in combinations(sorted(services), 2):
            if not allow_cross_namespace and left.split("::")[1] != right.split("::")[1]: continue
            relation_id = f"{left}<->{right}::resource::{resource_type}::{resource_id}"
            resources[relation_id] = TopologyRelation(relation_id, "resource", left, right, None, True,
                                                      {"resource_type": resource_type, "resource_id": resource_id})
    for edge in snapshot.resource_edges:
        if edge.resource_type is None or edge.resource_id is None:
            raise TopologyValidationError(
                f"snapshot={snapshot.snapshot_id} explicit resource edge requires resource_type and resource_id"
            )
        src = _resolve_endpoint(snapshot, edge.src_namespace, edge.src_service)
        dst = _resolve_endpoint(snapshot, edge.dst_namespace, edge.dst_service)
        if not allow_cross_namespace and src.split("::")[1] != dst.split("::")[1]: continue
        left, right = (src, dst) if edge.directed else tuple(sorted((src, dst)))
        relation_id = f"{left}{'->' if edge.directed else '<->'}{right}::resource::{edge.resource_type}::{edge.resource_id}"
        resources[relation_id] = TopologyRelation(relation_id, "resource", left, right, None, not edge.directed,
                                                  {"resource_type": edge.resource_type, "resource_id": edge.resource_id})

    return TopologyGraph(
        snapshot.snapshot_id, snapshot.cluster_id, service_ids,
        sorted(calls, key=lambda item: item.relation_id),
        sorted([*explicit_impact.values(), *derived.values()], key=lambda item: item.relation_id),
        sorted(hosts.values(), key=lambda item: item.relation_id),
        sorted(resources.values(), key=lambda item: item.relation_id),
        sorted(physical.values(), key=lambda item: item.relation_id),
    )

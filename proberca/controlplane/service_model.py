"""Healthy-only service RLS and A_s-driven candidate entity construction."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

import numpy as np

from proberca.data.schema import TopologySnapshot

from .config import FinalControlConfig
from .model import CandidateEntityGraph


def _service_id(cluster_id: str, namespace: str, service: str) -> str:
    return f"{cluster_id}::{namespace}::{service}"


def _edge_id(
    cluster_id: str, namespace: str, source: str, target: str, protocol: str,
) -> str:
    return f"{cluster_id}::{namespace}::{source}->{target}::{protocol}"


def _topology_services(snapshot: TopologySnapshot) -> tuple[str, ...]:
    return tuple(sorted(
        _service_id(snapshot.cluster_id, *item.split("::", 1))
        for item in snapshot.services
    ))


def _edge_endpoints(snapshot: TopologySnapshot, edge) -> tuple[str, str]:
    def namespace(explicit: str | None, service: str) -> str:
        if explicit is not None:
            return explicit
        matches = [
            item.split("::", 1)[0] for item in snapshot.services
            if item.split("::", 1)[1] == service
        ]
        if len(matches) != 1:
            raise ValueError("topology endpoint namespace is ambiguous")
        return matches[0]

    source_namespace = namespace(edge.src_namespace, edge.src_service)
    target_namespace = namespace(edge.dst_namespace, edge.dst_service)
    return (
        _service_id(snapshot.cluster_id, source_namespace, edge.src_service),
        _service_id(snapshot.cluster_id, target_namespace, edge.dst_service),
    )


@dataclass(frozen=True)
class AllowedServiceGraph:
    services: tuple[str, ...]
    relations: tuple[tuple[str, str, str], ...]
    physical_edges: tuple[tuple[str, str, str, str], ...]
    placements: tuple[tuple[str, str], ...]
    snapshot_id: str


def allowed_service_graph(snapshot: TopologySnapshot) -> AllowedServiceGraph:
    if not isinstance(snapshot, TopologySnapshot):
        raise TypeError("service graph requires TopologySnapshot")
    services = _topology_services(snapshot)
    relations = set()
    physical = set()
    for edge in (*snapshot.call_edges, *snapshot.host_edges, *snapshot.resource_edges):
        source, target = _edge_endpoints(snapshot, edge)
        relation_type = edge.relation_type
        relations.add((source, target, relation_type))
        if relation_type == "call":
            namespace = (edge.src_namespace or edge.dst_namespace
                         or source.split("::")[1])
            physical.add((
                _edge_id(
                    snapshot.cluster_id, namespace, edge.src_service,
                    edge.dst_service, edge.protocol or "tcp",
                ), source, target, edge.protocol or "tcp",
            ))
        if edge.directed is False or relation_type in {"host", "resource"}:
            relations.add((target, source, relation_type))
    placements = set()
    by_host = {}
    for placement in snapshot.service_nodes:
        service = _service_id(
            snapshot.cluster_id, placement.namespace, placement.service_name,
        )
        host = f"{snapshot.cluster_id}::host::{placement.node_name}"
        placements.add((service, host))
        by_host.setdefault(host, set()).add(service)
    for colocated in by_host.values():
        for source in colocated:
            for target in colocated:
                if source != target:
                    relations.add((source, target, "host"))
    return AllowedServiceGraph(
        services=services,
        relations=tuple(sorted(relations)),
        physical_edges=tuple(sorted(physical)),
        placements=tuple(sorted(placements)),
        snapshot_id=snapshot.snapshot_id,
    )


@dataclass
class _TargetRLS:
    feature_keys: tuple[tuple[str, int], ...]
    beta: np.ndarray
    covariance: np.ndarray
    updates: int = 0


class ServiceRLS:
    """Masked online RLS; updates are accepted only from Healthy windows."""

    def __init__(self, config: FinalControlConfig):
        self.config = config
        self._models: dict[str, _TargetRLS] = {}
        self._history: dict[int, dict[str, float]] = {}
        self._graph: AllowedServiceGraph | None = None

    def _configure(self, graph: AllowedServiceGraph) -> None:
        parents = {service: set() for service in graph.services}
        for parent, target, _relation_type in graph.relations:
            if parent in parents and target in parents:
                parents[target].add(parent)
        models = {}
        for target in graph.services:
            keys = tuple(
                (parent, lag)
                for parent in sorted(parents[target])
                for lag in self.config.service_lags
            )
            previous = self._models.get(target)
            if previous is not None and previous.feature_keys == keys:
                models[target] = previous
                continue
            size = len(keys)
            models[target] = _TargetRLS(
                keys,
                np.zeros(size, dtype=float),
                np.eye(size, dtype=float) * self.config.rls_initial_covariance,
            )
        self._models = models
        self._graph = graph

    def update(
        self, sequence: int, service_state: dict[str, float],
        graph: AllowedServiceGraph,
    ) -> None:
        if self._graph is None or self._graph.snapshot_id != graph.snapshot_id:
            self._configure(graph)
        current = {key: float(value) for key, value in service_state.items()}
        for target, model in self._models.items():
            if target not in current or not model.feature_keys:
                continue
            values = []
            complete = True
            for parent, lag in model.feature_keys:
                row = self._history.get(sequence - lag)
                if row is None or parent not in row:
                    complete = False
                    break
                values.append(row[parent])
            if not complete:
                continue
            phi = np.asarray(values, dtype=float)
            covariance_phi = model.covariance @ phi
            denominator = (
                self.config.rls_forgetting_factor
                + float(phi @ covariance_phi)
            )
            if not math.isfinite(denominator) or denominator <= 0:
                raise ValueError("service RLS denominator is invalid")
            gain = covariance_phi / denominator
            error = current[target] - float(phi @ model.beta)
            model.beta = model.beta + gain * error
            model.covariance = (
                model.covariance - np.outer(gain, phi) @ model.covariance
            ) / self.config.rls_forgetting_factor
            if not np.isfinite(model.beta).all() \
                    or not np.isfinite(model.covariance).all():
                raise ValueError("service RLS produced non-finite state")
            model.updates += 1
        self._history[sequence] = current
        cutoff = sequence - max(self.config.service_lags)
        self._history = {
            key: value for key, value in self._history.items() if key >= cutoff
        }

    def coefficients(self) -> dict[tuple[str, str, int], float]:
        output = {}
        for target, model in self._models.items():
            for (parent, lag), value in zip(model.feature_keys, model.beta):
                output[(target, parent, lag)] = float(value)
        return output

    def relation_strengths(self) -> dict[tuple[str, str], float]:
        coefficients = self.coefficients()
        relations = set(
            (parent, target)
            for target, parent, _lag in coefficients
        )
        return {
            relation: math.sqrt(sum(
                coefficients.get((relation[1], relation[0], lag), 0.0) ** 2
                for lag in self.config.service_lags
            ))
            for relation in relations
        }


def build_candidate_graph(
    *,
    graph: AllowedServiceGraph,
    service_strengths: dict[tuple[str, str], float],
    seed_services: set[str],
    seed_edges: set[str],
    config: FinalControlConfig,
) -> CandidateEntityGraph:
    edge_lookup = {edge_id: (source, target, protocol)
                   for edge_id, source, target, protocol in graph.physical_edges}
    seeds = set(seed_services)
    for edge_id in seed_edges:
        if edge_id in edge_lookup:
            seeds.update(edge_lookup[edge_id][:2])
    seeds &= set(graph.services)
    if not seeds:
        raise ValueError("candidate graph requires at least one valid alert seed")
    adjacency = {service: set() for service in graph.services}
    for source, target, _kind in graph.relations:
        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set()).add(source)
    raw = set(seeds)
    queue = deque((seed, 0) for seed in sorted(seeds))
    while queue:
        service, hop = queue.popleft()
        if hop >= config.candidate_hops:
            continue
        for neighbor in sorted(adjacency.get(service, ())):
            if neighbor in raw:
                continue
            raw.add(neighbor)
            queue.append((neighbor, hop + 1))
    strong = []
    for parent, target, _kind in graph.relations:
        if parent not in raw or target not in raw:
            continue
        strength = float(service_strengths.get((parent, target), 0.0))
        if strength >= config.service_edge_threshold:
            strong.append((parent, target, strength))
    services = set(seeds)
    for parent, target, _strength in strong:
        services.update((parent, target))
    hosts = {host for service, host in graph.placements if service in services}
    edges = {
        edge_id for edge_id, source, target, _protocol in graph.physical_edges
        if source in services and target in services
    } | seed_edges
    return CandidateEntityGraph(
        seed_services=tuple(sorted(seeds)),
        seed_edges=tuple(sorted(seed_edges)),
        services=tuple(sorted(services)),
        hosts=tuple(sorted(hosts)),
        edges=tuple(sorted(edges)),
        strong_service_relations=tuple(sorted(strong)),
        topology_snapshot_id=graph.snapshot_id,
    )

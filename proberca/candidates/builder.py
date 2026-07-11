"""Build deterministic P3 candidates from alerts, topology, and P1 metrics."""

from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from dataclasses import asdict
from typing import Callable

from proberca.config import MetricSignalSpec, ProbeRCAConfig
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    AlertEvent,
    CandidateProvenance,
    CandidateSubgraph,
    EdgeMetricRecord,
    NodeMetricRecord,
)
from proberca.topology import TopologyGraph, TopologyRelation, TopologyStore, build_topology_graph


class StaleAlertTopologyError(ValueError):
    """An alert trigger is absent from its timestamp-valid topology."""


class CandidateOverflowError(ValueError):
    """A configured candidate size limit was exceeded."""


class CandidateValidationError(ValueError):
    """Candidate graph input or output is structurally invalid."""


class CandidateSerializationError(ValueError):
    """Candidate serialization could not preserve the strict contract."""


class AmbiguousMetricSelectionError(ValueError):
    """More than one signal spec selects the same aggregation output."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _fingerprint(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _relation_dict(relation: TopologyRelation) -> dict:
    return {
        "relation_id": relation.relation_id,
        "src_service_id": relation.src_service_id,
        "dst_service_id": relation.dst_service_id,
        "relation_type": relation.relation_type,
        "protocol": relation.protocol,
        "symmetric": relation.symmetric,
        "detail": relation.detail,
    }


def _physical_dict(relation: TopologyRelation) -> dict:
    return {
        "physical_edge_id": relation.relation_id,
        "src_service_id": relation.src_service_id,
        "dst_service_id": relation.dst_service_id,
        "protocol": relation.protocol,
        "detail": relation.detail,
    }


class CandidateSubgraphBuilder:
    """Construct S_c and its metric/edge objects without scores or root labels."""

    def __init__(self, config: ProbeRCAConfig, signal_specs: list[MetricSignalSpec],
                 clock_ns: Callable[[], int] = time.perf_counter_ns):
        if not isinstance(config, ProbeRCAConfig):
            raise TypeError("config must be ProbeRCAConfig")
        if any(not isinstance(item, MetricSignalSpec) for item in signal_specs):
            raise TypeError("signal_specs must contain MetricSignalSpec")
        output_ids = [item.aggregation_output_id for item in signal_specs]
        if len(output_ids) != len(set(output_ids)):
            raise AmbiguousMetricSelectionError("duplicate aggregation_output_id in MetricSignalSpec registry")
        self.config = config
        self.signal_specs = sorted(signal_specs, key=lambda item: item.aggregation_output_id)
        self.signal_by_id = {item.aggregation_output_id: item for item in self.signal_specs}
        self.clock_ns = clock_ns
        self._soft_snapshots: dict[str, str] = {}

    @property
    def config_fingerprint(self) -> str:
        return _fingerprint({
            "candidate_graph": asdict(self.config.candidate_graph),
            "impact_derivation_rules": [asdict(item) for item in self.config.impact_derivation_rules],
            "rca_metric_families": self.config.rca_metric_families,
            "shock_metric_names": sorted(self.config.shock_templates),
            "signal_specs": [item.to_dict() for item in self.signal_specs],
        })

    def prepare(self, alert: AlertEvent, topology_store: TopologyStore,
                available_node_metrics: list[NodeMetricRecord],
                available_edge_metrics: list[EdgeMetricRecord]) -> CandidateSubgraph:
        started = self.clock_ns()
        if alert.state not in {"soft", "hard", "edge_anomaly"}:
            raise ValueError(f"alert={alert.alert_id} state={alert.state} is not candidate eligible")
        cluster_id = self._alert_cluster(alert)
        namespace_scope = self.config.candidate_graph.allowed_namespaces or None
        snapshot = topology_store.query(cluster_id, alert.timestamp_ns, namespace_scope)
        graph = build_topology_graph(snapshot, self.config.impact_derivation_rules,
                                     self.config.candidate_graph.allow_cross_namespace)
        graph = self._namespace_scoped_graph(graph)
        seeds, seed_provenance = self._seeds(alert, graph)
        service_provenance: dict[str, list[CandidateProvenance]] = {}
        for item in seed_provenance:
            service_provenance.setdefault(item.object_id, []).append(item)
        self._bfs(seeds, graph.impact_edges, self.config.candidate_graph.upstream_hops,
                  incoming=True, reason="impact_ancestor", alert=alert,
                  snapshot_id=snapshot.snapshot_id, output=service_provenance)
        self._bfs(seeds, graph.call_edges, self.config.candidate_graph.downstream_hops,
                  incoming=False, reason="call_descendant", alert=alert,
                  snapshot_id=snapshot.snapshot_id, output=service_provenance)
        if self.config.candidate_graph.include_cohost:
            self._direct_context(seeds, graph.host_relations, "cohost", alert, snapshot.snapshot_id, service_provenance)
        if self.config.candidate_graph.include_shared_resource:
            self._direct_context(seeds, graph.resource_relations, "shared_resource", alert,
                                 snapshot.snapshot_id, service_provenance)
        candidate_services = sorted(service_provenance)
        self._check_limit("services", len(candidate_services), self.config.candidate_graph.max_candidate_services, alert)

        calls = [item for item in graph.call_edges if item.src_service_id in service_provenance and item.dst_service_id in service_provenance]
        impacts = [item for item in graph.impact_edges if item.src_service_id in service_provenance and item.dst_service_id in service_provenance]
        hosts = [item for item in graph.host_relations if item.src_service_id in service_provenance and item.dst_service_id in service_provenance]
        resources = [item for item in graph.resource_relations if item.src_service_id in service_provenance and item.dst_service_id in service_provenance]
        physical = [item for item in graph.physical_edges if item.src_service_id in service_provenance and item.dst_service_id in service_provenance]
        self._check_limit("physical_edges", len(physical), self.config.candidate_graph.max_candidate_physical_edges, alert)
        physical_ids = {item.relation_id for item in physical}

        provenance = [item for values in service_provenance.values() for item in values]
        for item in physical:
            provenance.append(CandidateProvenance(
                item.relation_id, "physical_edge", "call_descendant", item.src_service_id, 1,
                [item.src_service_id, item.dst_service_id], [str(item.detail["source_relation_id"])],
                snapshot.snapshot_id, alert.alert_id, {"protocol": item.protocol},
            ))

        all_node_records = sorted(available_node_metrics, key=lambda item: (item.stable_id, item.series_id))
        all_edge_records = sorted(available_edge_metrics, key=lambda item: (item.stable_id, item.series_id))
        self._validate_metric_clusters(cluster_id, all_node_records, all_edge_records, alert)
        node_records = list({item.stable_id: item for item in reversed(all_node_records)}.values())
        edge_records = list({item.stable_id: item for item in reversed(all_edge_records)}.values())
        node_records.sort(key=lambda item: item.stable_id)
        edge_records.sort(key=lambda item: item.stable_id)
        candidate_nodes: list[str] = []
        missing_nodes: list[str] = []
        observed_node_ids = {item.stable_id for item in node_records}
        for record in node_records:
            service_id = f"{record.cluster_id}::{record.namespace}::{record.service_name}"
            spec = self.signal_by_id.get(record.stable_id)
            if service_id in service_provenance and record.metric_family in self.config.rca_metric_families and spec is not None:
                candidate_nodes.append(record.stable_id)
                provenance.append(CandidateProvenance(
                    record.stable_id, "node_metric", "configured_metric", service_id, 0, [service_id], [],
                    snapshot.snapshot_id, alert.alert_id, {"metric_family": record.metric_family},
                ))
        for spec in self.signal_specs:
            if spec.record_type != "node_metric" or spec.metric_family not in self.config.rca_metric_families:
                continue
            service_id = "::".join(spec.aggregation_output_id.split("::")[:3])
            if service_id in service_provenance and spec.aggregation_output_id not in observed_node_ids:
                missing_nodes.append(spec.aggregation_output_id)
        candidate_nodes = sorted(set(candidate_nodes))
        self._check_limit("node_metrics", len(candidate_nodes), self.config.candidate_graph.max_candidate_node_metrics, alert)

        candidate_edge_ids: list[str] = []
        candidate_shocks: list[str] = []
        missing_edges: list[str] = []
        observed_edge_ids = {item.stable_id for item in edge_records}
        for record in edge_records:
            physical_id = record.stable_id.rsplit("::", 1)[0]
            spec = self.signal_by_id.get(record.stable_id)
            if physical_id in physical_ids and spec is not None:
                candidate_edge_ids.append(record.stable_id)
                provenance.append(CandidateProvenance(
                    record.stable_id, "edge_metric", "observed_edge_metric", physical_id, 0,
                    [physical_id], [], snapshot.snapshot_id, alert.alert_id, {"protocol": record.protocol},
                ))
                if record.metric_name in self.config.shock_templates:
                    shock_id = (f"{record.cluster_id}::{record.namespace}::{record.src_service}->{record.dst_service}::"
                                f"{record.protocol}::shock::{record.metric_name}")
                    candidate_shocks.append(shock_id)
                    provenance.append(CandidateProvenance(
                        shock_id, "shock", "observed_edge_metric", record.stable_id, 0,
                        [physical_id], [], snapshot.snapshot_id, alert.alert_id,
                        {"edge_metric_id": record.stable_id},
                    ))
        for spec in self.signal_specs:
            if spec.record_type != "edge_metric":
                continue
            physical_id = spec.aggregation_output_id.rsplit("::", 1)[0]
            if physical_id in physical_ids and spec.aggregation_output_id not in observed_edge_ids:
                missing_edges.append(spec.aggregation_output_id)

        quality_issues: list[dict] = []
        provenance = self._limit_provenance(provenance, quality_issues)
        signature = _fingerprint({"services": sorted(alert.trigger_services), "edges": sorted(alert.trigger_edges)})
        previous_soft = self._soft_snapshots.get(signature)
        if alert.state == "soft":
            self._soft_snapshots[signature] = snapshot.snapshot_id
        elif alert.state == "hard" and previous_soft is not None and previous_soft != snapshot.snapshot_id:
            quality_issues.append({"reason_code": "topology_changed_since_soft",
                                   "detail": {"soft_snapshot_id": previous_soft,
                                              "hard_snapshot_id": snapshot.snapshot_id}})

        candidate_payload = {
            "cluster_id": cluster_id, "alert_id": alert.alert_id, "alert_state": alert.state,
            "alert_timestamp_ns": alert.timestamp_ns, "snapshot_id": snapshot.snapshot_id,
            "seeds": sorted(seeds), "services": candidate_services, "nodes": candidate_nodes,
            "edges": sorted(candidate_edge_ids), "shocks": sorted(candidate_shocks),
            "config_fingerprint": self.config_fingerprint,
            "metric_fingerprint": _fingerprint({"nodes": sorted(observed_node_ids), "edges": sorted(observed_edge_ids)}),
        }
        candidate_id = _fingerprint(candidate_payload)
        elapsed_ms = (self.clock_ns() - started) / 1_000_000.0
        return CandidateSubgraph(
            schema_version=PROBERCA_SCHEMA_VERSION, candidate_id=candidate_id, cluster_id=cluster_id,
            namespace_scope=sorted({item.split("::")[1] for item in candidate_services}),
            alert_id=alert.alert_id, alert_state=alert.state, alert_timestamp_ns=alert.timestamp_ns,
            topology_snapshot_id=snapshot.snapshot_id, topology_valid_from_ns=snapshot.valid_from_ns,
            topology_valid_to_ns=snapshot.valid_to_ns, seed_services=sorted(seeds),
            trigger_edges=sorted(alert.trigger_edges), candidate_services=candidate_services,
            candidate_node_ids=candidate_nodes, candidate_edge_metric_ids=sorted(set(candidate_edge_ids)),
            candidate_shock_ids=sorted(set(candidate_shocks)), call_edges=[_relation_dict(item) for item in calls],
            impact_edges=[_relation_dict(item) for item in impacts], host_relations=[_relation_dict(item) for item in hosts],
            resource_relations=[_relation_dict(item) for item in resources], physical_edges=[_physical_dict(item) for item in physical],
            provenance=provenance, missing_node_metrics=sorted(set(missing_nodes)),
            missing_edge_metrics=sorted(set(missing_edges)), rca_eligible=alert.state == "hard",
            quality_issues=quality_issues, config_fingerprint=self.config_fingerprint,
            service_count=len(candidate_services), node_metric_count=len(candidate_nodes),
            physical_edge_count=len(physical), shock_count=len(set(candidate_shocks)),
            build_latency_ms=elapsed_ms,
        )

    def _alert_cluster(self, alert: AlertEvent) -> str:
        objects = [*alert.trigger_services, *alert.trigger_edges]
        if not objects:
            raise StaleAlertTopologyError(f"alert={alert.alert_id} has no trigger objects")
        clusters = {item.split("::", 1)[0] for item in objects if "::" in item}
        if len(clusters) != 1:
            raise StaleAlertTopologyError(f"alert={alert.alert_id} has ambiguous cluster triggers={objects}")
        return next(iter(clusters))

    def _namespace_scoped_graph(self, graph: TopologyGraph) -> TopologyGraph:
        allowed = set(self.config.candidate_graph.allowed_namespaces)
        if not allowed:
            return graph
        service_ids = [item for item in graph.service_ids if item.split("::")[1] in allowed]
        service_set = set(service_ids)
        keep = lambda item: item.src_service_id in service_set and item.dst_service_id in service_set
        return TopologyGraph(
            graph.snapshot_id, graph.cluster_id, service_ids,
            [item for item in graph.call_edges if keep(item)],
            [item for item in graph.impact_edges if keep(item)],
            [item for item in graph.host_relations if keep(item)],
            [item for item in graph.resource_relations if keep(item)],
            [item for item in graph.physical_edges if keep(item)],
        )

    def _seeds(self, alert: AlertEvent, graph: TopologyGraph):
        services = set(graph.service_ids)
        seeds = set(alert.trigger_services)
        provenance: list[CandidateProvenance] = []
        for service in sorted(seeds):
            namespace = service.split("::")[1]
            allowed = self.config.candidate_graph.allowed_namespaces
            if allowed and namespace not in allowed:
                raise StaleAlertTopologyError(
                    f"alert={alert.alert_id} trigger_service={service} is outside allowed_namespaces={allowed}"
                )
            if service not in services:
                raise StaleAlertTopologyError(f"alert={alert.alert_id} trigger_service={service} is stale")
            provenance.append(CandidateProvenance(service, "service", "trigger_service", alert.alert_id, 0,
                                                  [service], [], graph.snapshot_id, alert.alert_id, {}))
        physical = {item.relation_id: item for item in graph.physical_edges}
        for edge_id in alert.trigger_edges:
            if edge_id not in physical:
                raise StaleAlertTopologyError(f"alert={alert.alert_id} trigger_edge={edge_id} is stale")
            if self.config.candidate_graph.include_trigger_edge_endpoints:
                for service in (physical[edge_id].src_service_id, physical[edge_id].dst_service_id):
                    seeds.add(service)
                    provenance.append(CandidateProvenance(
                        service, "service", "trigger_edge_endpoint", edge_id, 0, [service], [],
                        graph.snapshot_id, alert.alert_id, {"physical_edge_id": edge_id},
                    ))
        return seeds, provenance

    @staticmethod
    def _bfs(seeds, relations, max_hops, *, incoming, reason, alert, snapshot_id, output):
        adjacency: dict[str, list[TopologyRelation]] = {}
        for relation in relations:
            key = relation.dst_service_id if incoming else relation.src_service_id
            adjacency.setdefault(key, []).append(relation)
        for values in adjacency.values(): values.sort(key=lambda item: item.relation_id)
        for seed in sorted(seeds):
            queue = deque([(seed, [seed], [])])
            shortest: dict[str, int] = {seed: 0}
            while queue:
                current, path, relation_ids = queue.popleft()
                if len(relation_ids) >= max_hops: continue
                for relation in adjacency.get(current, []):
                    target = relation.src_service_id if incoming else relation.dst_service_id
                    if target in path: continue
                    next_path = [target, *path] if incoming else [*path, target]
                    next_relations = ([relation.relation_id, *relation_ids] if incoming
                                      else [*relation_ids, relation.relation_id])
                    hop = len(next_relations)
                    output.setdefault(target, []).append(CandidateProvenance(
                        target, "service", reason, seed, hop, next_path, next_relations,
                        snapshot_id, alert.alert_id, {},
                    ))
                    if hop <= shortest.get(target, hop):
                        shortest[target] = hop
                        queue.append((target, next_path, next_relations))

    @staticmethod
    def _direct_context(seeds, relations, reason, alert, snapshot_id, output):
        for seed in sorted(seeds):
            for relation in relations:
                if seed not in {relation.src_service_id, relation.dst_service_id}: continue
                target = relation.dst_service_id if seed == relation.src_service_id else relation.src_service_id
                detail = ({"shared_node": relation.detail["node_name"]} if reason == "cohost" else
                          {"resource_type": relation.detail["resource_type"], "resource_id": relation.detail["resource_id"]})
                output.setdefault(target, []).append(CandidateProvenance(
                    target, "service", reason, seed, 1, [seed, target], [relation.relation_id],
                    snapshot_id, alert.alert_id, detail,
                ))

    def _limit_provenance(self, provenance, quality_issues):
        grouped: dict[str, list[CandidateProvenance]] = {}
        for item in provenance: grouped.setdefault(item.object_id, []).append(item)
        result = []
        limit = self.config.candidate_graph.max_provenance_paths_per_object
        for object_id, values in sorted(grouped.items()):
            ordered = sorted(values, key=lambda item: (item.hop_count, item.reason_code,
                                                        tuple(item.relation_ids), item.source_object_id))
            if not self.config.candidate_graph.include_all_provenance_paths and len(ordered) > limit:
                quality_issues.append({"reason_code": "provenance_truncated",
                                       "detail": {"object_id": object_id, "original_count": len(ordered), "kept": limit}})
                ordered = ordered[:limit]
            result.extend(ordered)
        return result

    def _check_limit(self, kind, count, limit, alert):
        if count > limit:
            raise CandidateOverflowError(
                f"alert={alert.alert_id} candidate_{kind}={count} exceeds limit={limit}; truncation is forbidden"
            )

    @staticmethod
    def _validate_metric_clusters(cluster_id, nodes, edges, alert):
        invalid = [item.stable_id for item in [*nodes, *edges] if item.cluster_id != cluster_id]
        if invalid:
            raise CandidateValidationError(f"alert={alert.alert_id} cross-cluster metrics={invalid}")


def prepare_candidate_subgraph(alert_event, topology_store, available_node_metrics,
                               available_edge_metrics, config, signal_specs):
    return CandidateSubgraphBuilder(config, signal_specs).prepare(
        alert_event, topology_store, available_node_metrics, available_edge_metrics
    )

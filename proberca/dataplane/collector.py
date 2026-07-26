"""Collection-only orchestration for final ProbeRCA windows.

This module deliberately has no dependency on alerting, propagation, candidate
selection, inversion, or reporting.  It turns source measurements plus two
Kubernetes inventory revisions into a strict :class:`CollectedWindow`.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from itertools import combinations
from typing import Any, Iterable, Protocol

from proberca.config import KubernetesConfig
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    EvidenceObservationRecord,
    ServiceNodePlacement,
    ServiceResourceBinding,
    TopologyEdge,
    TopologySnapshot,
)
from proberca.k8s.client import KubernetesDiscoveryClient
from proberca.k8s.runtime_identity import runtime_identities

from .contracts import CollectedWindow, fingerprint
from .final_aggregation import (
    FINAL_AGGREGATION_VERSION,
    FinalAggregationResult,
    FinalWindowAggregator,
)
from .raw import RawCollectionError, RawCollectionWindow
from .sources import PrimitiveSource, PrometheusSourceConfig


COLLECTOR_CONFIG_SCHEMA_VERSION = "probeRCA-final-live-collector-v1"


class BurstEvidenceSource(Protocol):
    """Independent source of already normalized Burst observations."""

    def collect(
        self,
        *,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision,
    ) -> tuple[EvidenceObservationRecord, ...]:
        ...


@dataclass(frozen=True)
class FinalLiveCollectorConfig:
    schema_version: str
    cluster_id: str
    window_sec: int
    collection_delay_sec: float
    window_lead_sec: float
    kubernetes: KubernetesConfig
    prometheus: PrometheusSourceConfig

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FinalLiveCollectorConfig":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict) or set(payload) != expected:
            raise RawCollectionError("live collector config fields mismatch")
        values = dict(payload)
        values["kubernetes"] = KubernetesConfig.from_dict(values["kubernetes"])
        values["prometheus"] = PrometheusSourceConfig.from_dict(
            values["prometheus"]
        )
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != COLLECTOR_CONFIG_SCHEMA_VERSION:
            raise RawCollectionError("unsupported live collector config")
        if not isinstance(self.cluster_id, str) or not self.cluster_id:
            raise RawCollectionError("collector cluster_id is required")
        if self.window_sec != 1:
            raise RawCollectionError("the frozen final scheme uses 1-second windows")
        for name in ("collection_delay_sec", "window_lead_sec"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) \
                    or float(value) < 0:
                raise RawCollectionError(f"{name} must be non-negative")
        self.kubernetes.validate()
        self.prometheus.validate()
        if not self.kubernetes.enabled:
            raise RawCollectionError("Kubernetes discovery must be enabled")
        if self.kubernetes.cluster_id != self.cluster_id:
            raise RawCollectionError("Kubernetes cluster identity mismatch")

    @property
    def public_fingerprint(self) -> str:
        self.validate()
        return fingerprint({
            "schema_version": self.schema_version,
            "cluster_id": self.cluster_id,
            "window_sec": self.window_sec,
            "collection_delay_sec": self.collection_delay_sec,
            "window_lead_sec": self.window_lead_sec,
            "kubernetes": asdict(self.kubernetes),
            "prometheus": {
                "config_fingerprint": self.prometheus.config_fingerprint,
            },
        })


def collector_build_fingerprint(
    collection_contract: dict[str, Any],
    source_config_fingerprint: str,
) -> str:
    """Content identity for the collection implementation and public config."""
    return fingerprint({
        "implementation": FINAL_AGGREGATION_VERSION,
        "collection_contract": collection_contract,
        "source_config_fingerprint": source_config_fingerprint,
    })


def _service_id(cluster_id: str, namespace: str, service: str) -> str:
    return f"{cluster_id}::{namespace}::{service}"


def _edge_destination_namespaces(
    raw_window: RawCollectionWindow,
) -> dict[tuple[str, str, str, str], str]:
    result: dict[tuple[str, str, str, str], str] = {}
    for sample in raw_window.samples:
        if sample.entity_type != "edge":
            continue
        key = (
            sample.namespace or "", sample.src_service or "",
            sample.dst_service or "", sample.protocol or "",
        )
        destination_namespace = sample.dst_namespace or sample.namespace or ""
        previous = result.setdefault(key, destination_namespace)
        if previous != destination_namespace:
            raise RawCollectionError(
                "one edge identity resolves to multiple destination namespaces"
            )
    return result


def _placements(revision, monitored: set[str]) -> tuple[ServiceNodePlacement, ...]:
    output = set()
    for pod_uid, services in revision.pod_to_services.items():
        matched = monitored & set(services)
        if not matched:
            continue
        pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
        node_name = (pod.get("spec") or {}).get("nodeName")
        if not node_name:
            raise RawCollectionError("monitored Pod has no node placement")
        for service_id in matched:
            _, namespace, service = service_id.split("::")
            output.add(ServiceNodePlacement(
                namespace, service, node_name, pod_uid
            ))
    covered = {
        _service_id(revision.cluster_id, item.namespace, item.service_name)
        for item in output
    }
    if covered != monitored:
        raise RawCollectionError(
            "Kubernetes inventory lacks exact monitored service placement"
        )
    return tuple(sorted(
        output,
        key=lambda item: (
            item.namespace, item.service_name, item.node_name,
            item.pod_uid or "",
        ),
    ))


def _resource_bindings(
    revision, monitored: set[str],
) -> tuple[ServiceResourceBinding, ...]:
    """Derive shared PVC/PV/CSI/host-network/annotated resource identities."""
    output: set[ServiceResourceBinding] = set()
    pvc_by_name = {
        (
            (raw.get("metadata") or {}).get("namespace"),
            (raw.get("metadata") or {}).get("name"),
        ): (uid, raw)
        for uid, raw in revision.objects_by_kind.get(
            "PersistentVolumeClaim", {}
        ).items()
    }
    pv_by_name = {
        (raw.get("metadata") or {}).get("name"): (uid, raw)
        for uid, raw in revision.objects_by_kind.get(
            "PersistentVolume", {}
        ).items()
    }
    node_uid_by_name = {
        (raw.get("metadata") or {}).get("name"): uid
        for uid, raw in revision.objects_by_kind.get("Node", {}).items()
    }
    for pod_uid, services in revision.pod_to_services.items():
        matched = monitored & set(services)
        if not matched:
            continue
        pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
        metadata = pod.get("metadata") or {}
        spec = pod.get("spec") or {}
        namespace = metadata.get("namespace")
        resources: list[tuple[str, str]] = []
        for volume in spec.get("volumes") or []:
            claim_name = (
                volume.get("persistentVolumeClaim") or {}
            ).get("claimName")
            if not claim_name:
                continue
            pvc_uid, pvc = pvc_by_name.get(
                (namespace, claim_name), (claim_name, {})
            )
            resources.append((
                "pvc",
                f"{revision.cluster_id}/{namespace}/pvc/{pvc_uid}",
            ))
            pv_name = (pvc.get("spec") or {}).get("volumeName")
            if pv_name and pv_name in pv_by_name:
                pv_uid, pv = pv_by_name[pv_name]
                resources.append((
                    "pv", f"{revision.cluster_id}/pv/{pv_uid}",
                ))
                csi = (pv.get("spec") or {}).get("csi") or {}
                if csi.get("driver") and csi.get("volumeHandle"):
                    resources.append((
                        "csi",
                        f"{revision.cluster_id}/csi/"
                        f"{csi['driver']}/{csi['volumeHandle']}",
                    ))
        if spec.get("hostNetwork") is True and spec.get("nodeName"):
            node = spec["nodeName"]
            resources.append((
                "host_network",
                f"{revision.cluster_id}/node/"
                f"{node_uid_by_name.get(node, node)}",
            ))
        prefix = "proberca.io/resource-"
        for key, value in sorted((metadata.get("annotations") or {}).items()):
            if key.startswith(prefix) and value:
                resource_type = key[len(prefix):]
                resources.append((
                    resource_type,
                    f"{revision.cluster_id}/{namespace}/"
                    f"{resource_type}/{value}",
                ))
        for service_id in matched:
            _, service_namespace, service = service_id.split("::")
            for resource_type, resource_id in resources:
                output.add(ServiceResourceBinding(
                    service_namespace, service, resource_type, resource_id
                ))
    return tuple(sorted(
        output,
        key=lambda item: (
            item.namespace, item.service_name,
            item.resource_type, item.resource_id,
        ),
    ))


def _runtime_fingerprints(revision, monitored: set[str]) -> tuple[str, ...]:
    selected = []
    services_with_runtime = set()
    for item in runtime_identities(revision):
        matches = monitored & set(item.service_ids)
        if not matches:
            continue
        if not item.ready or not item.started or not item.full_container_id \
                or not item.node_name:
            raise RawCollectionError(
                "monitored container runtime identity is incomplete or unready"
            )
        selected.append(item.identity_fingerprint)
        services_with_runtime.update(matches)
    if services_with_runtime != monitored:
        raise RawCollectionError(
            "monitored services lack exact ready runtime identities"
        )
    return tuple(sorted(set(selected)))


def build_topology_snapshot(
    *,
    raw_window: RawCollectionWindow,
    aggregation: FinalAggregationResult,
    inventory_revision,
) -> TopologySnapshot:
    """Build the exact topology covered by final output metrics."""
    if not inventory_revision.ready:
        raise RawCollectionError(
            "Kubernetes inventory is unsynchronized or stale"
        )
    if inventory_revision.cluster_id != raw_window.cluster_id:
        raise RawCollectionError("inventory/raw cluster identity mismatch")
    if inventory_revision.issues:
        raise RawCollectionError(
            "Kubernetes inventory contains unresolved structural issues"
        )
    monitored = {
        _service_id(
            item.cluster_id, item.namespace, item.service_name
        )
        for item in aggregation.node_metrics
        if item.scope == "service"
    }
    if not monitored:
        raise RawCollectionError("topology has no monitored services")
    known = {
        _service_id(
            inventory_revision.cluster_id,
            (raw.get("metadata") or {}).get("namespace"),
            (raw.get("metadata") or {}).get("name"),
        )
        for raw in inventory_revision.objects_by_kind.get(
            "Service", {}
        ).values()
    }
    if not monitored <= known:
        raise RawCollectionError(
            "normal metrics reference unknown Kubernetes Services"
        )
    destination_namespaces = _edge_destination_namespaces(raw_window)
    call_edges = []
    for item in aggregation.edge_metrics:
        key = (
            item.namespace, item.src_service, item.dst_service, item.protocol,
        )
        destination_namespace = destination_namespaces.get(key)
        if destination_namespace is None:
            raise RawCollectionError(
                "final edge lacks raw destination namespace identity"
            )
        source_id = _service_id(
            item.cluster_id, item.namespace, item.src_service
        )
        destination_id = _service_id(
            item.cluster_id, destination_namespace, item.dst_service
        )
        if source_id not in monitored or destination_id not in monitored:
            raise RawCollectionError(
                "active edge endpoint lacks its complete 9-metric service set"
            )
        call_edges.append(TopologyEdge(
            item.src_service,
            item.dst_service,
            "call",
            item.namespace,
            destination_namespace,
            item.protocol,
            directed=True,
        ))
    call_edges = tuple(sorted(set(call_edges), key=lambda item: (
        item.src_namespace or "", item.src_service,
        item.dst_namespace or "", item.dst_service,
        item.protocol or "",
    )))
    placements = _placements(inventory_revision, monitored)
    expected_hosts = {item.node_name for item in placements}
    observed_hosts = {
        item.node_name for item in aggregation.node_metrics
        if item.scope == "node"
    }
    if observed_hosts != expected_hosts:
        raise RawCollectionError(
            "host metric coverage does not match monitored service placement"
        )
    services_by_node: dict[str, set[str]] = {}
    for item in placements:
        services_by_node.setdefault(item.node_name, set()).add(
            f"{item.namespace}::{item.service_name}"
        )
    host_edges = []
    for node, members in sorted(services_by_node.items()):
        for left, right in combinations(sorted(members), 2):
            left_namespace, left_service = left.split("::")
            right_namespace, right_service = right.split("::")
            host_edges.append(TopologyEdge(
                left_service,
                right_service,
                "host",
                left_namespace,
                right_namespace,
                resource_type="node",
                resource_id=node,
                directed=False,
            ))
    bindings = _resource_bindings(inventory_revision, monitored)
    services = sorted(
        item.split("::", 1)[1] for item in monitored
    )
    structure = {
        "cluster": raw_window.cluster_id,
        "services": services,
        "calls": [item.to_dict() for item in call_edges],
        "hosts": [item.to_dict() for item in host_edges],
        "bindings": [item.to_dict() for item in bindings],
    }
    structure_fingerprint = fingerprint(structure)
    runtime_fingerprints = _runtime_fingerprints(
        inventory_revision, monitored
    )
    resource_versions = {
        item.resource_kind: fingerprint({
            "kind": item.resource_kind,
            "namespace_scope": item.namespace_scope,
            "resource_version": item.resource_version,
            "watch_stream_id": item.watch_stream_id,
        })
        for item in inventory_revision.resource_versions
    }
    snapshot_id = fingerprint({
        "structure_fingerprint": structure_fingerprint,
        "runtime_identity_fingerprints": runtime_fingerprints,
        "window_start_ns": raw_window.window_start_ns,
        "window_end_ns": raw_window.window_end_ns,
    })
    return TopologySnapshot(
        schema_version=PROBERCA_SCHEMA_VERSION,
        snapshot_id=snapshot_id,
        valid_from_ns=raw_window.window_start_ns,
        valid_to_ns=raw_window.window_end_ns,
        cluster_id=raw_window.cluster_id,
        services=services,
        call_edges=list(call_edges),
        host_edges=host_edges,
        resource_edges=[],
        service_nodes=list(placements),
        service_resources=list(bindings),
        structure_fingerprint=structure_fingerprint,
        inventory_revision_id=inventory_revision.revision_id,
        resource_version_vector=resource_versions,
        runtime_identity_fingerprints=list(runtime_fingerprints),
        call_edge_provider_fingerprint=fingerprint({
            "edges": [item.to_dict() for item in call_edges],
            "raw_source_ids": list(
                aggregation.residual_source_record_ids
            ),
        }),
        topology_build_issues=[],
    )


class FinalDataPlaneCollector:
    """Assemble and validate final windows without executing RCA."""

    def __init__(
        self,
        *,
        collection_contract: dict[str, Any],
        collector_build_id: str,
    ):
        if not isinstance(collector_build_id, str) \
                or len(collector_build_id) != 64 \
                or any(character not in "0123456789abcdef"
                       for character in collector_build_id):
            raise RawCollectionError(
                "collector_build_id must be a lowercase SHA-256"
            )
        self.collection_contract = dict(collection_contract)
        self.collector_build_id = collector_build_id
        self.aggregator = FinalWindowAggregator(self.collection_contract)

    def assemble(
        self,
        *,
        raw_window: RawCollectionWindow,
        inventory_at_start,
        inventory_at_end,
        burst_evidence: Iterable[EvidenceObservationRecord] = (),
    ) -> CollectedWindow:
        aggregation = self.aggregator.aggregate(raw_window)
        start_topology = build_topology_snapshot(
            raw_window=raw_window,
            aggregation=aggregation,
            inventory_revision=inventory_at_start,
        )
        end_topology = build_topology_snapshot(
            raw_window=raw_window,
            aggregation=aggregation,
            inventory_revision=inventory_at_end,
        )
        if start_topology.structure_fingerprint \
                != end_topology.structure_fingerprint \
                or start_topology.runtime_identity_fingerprints \
                != end_topology.runtime_identity_fingerprints:
            raise RawCollectionError(
                "topology structure/runtime identity changed inside "
                "the collection window"
            )
        evidence = tuple(burst_evidence)
        if any(not isinstance(item, EvidenceObservationRecord)
               for item in evidence):
            raise RawCollectionError(
                "Burst source returned a non-evidence record"
            )
        metadata = {
            "collector_build_fingerprint": self.collector_build_id,
            "aggregation_config_fingerprint": self.collection_contract[
                "aggregation_config_fingerprint"
            ],
            "burst_config_fingerprint": self.collection_contract[
                "burst_config_fingerprint"
            ],
        }
        return CollectedWindow.create(
            sequence=raw_window.sequence,
            window_start_ns=raw_window.window_start_ns,
            window_end_ns=raw_window.window_end_ns,
            node_metrics=aggregation.node_metrics,
            edge_metrics=aggregation.edge_metrics,
            topology_events=(start_topology,),
            burst_evidence=evidence,
            residual_source_record_ids=(
                aggregation.residual_source_record_ids
            ),
            collection_metadata=metadata,
        )


class FinalLiveCollectionRunner:
    """Capture closed Healthy-only windows from live sources."""

    def __init__(
        self,
        *,
        config: FinalLiveCollectorConfig,
        collection_contract: dict[str, Any],
        primitive_source: PrimitiveSource,
        burst_source: BurstEvidenceSource | None = None,
        discovery_client: KubernetesDiscoveryClient | None = None,
        wall_clock_ns=time.time_ns,
        sleep=time.sleep,
    ):
        config.validate()
        if collection_contract.get("window_sec") != config.window_sec:
            raise RawCollectionError(
                "source and collection contract window sizes differ"
            )
        self.config = config
        self.primitive_source = primitive_source
        self.burst_source = burst_source
        self.discovery = discovery_client or KubernetesDiscoveryClient(
            config.kubernetes
        )
        self.wall_clock_ns = wall_clock_ns
        self.sleep = sleep
        build_id = collector_build_fingerprint(
            collection_contract, config.public_fingerprint
        )
        self.assembler = FinalDataPlaneCollector(
            collection_contract=collection_contract,
            collector_build_id=build_id,
        )

    def _wait_until(self, timestamp_ns: int) -> None:
        remaining = timestamp_ns - self.wall_clock_ns()
        if remaining > 0:
            self.sleep(remaining / 1_000_000_000)

    def collect_one(self, sequence: int) -> CollectedWindow:
        before = self.discovery.discover_once(self.wall_clock_ns()).freeze(
            self.wall_clock_ns()
        )
        lead_ns = int(self.config.window_lead_sec * 1_000_000_000)
        now = self.wall_clock_ns() + lead_ns
        window_ns = self.config.window_sec * 1_000_000_000
        start_ns = ((now + window_ns - 1) // window_ns) * window_ns
        end_ns = start_ns + window_ns
        self._wait_until(
            end_ns + int(
                self.config.collection_delay_sec * 1_000_000_000
            )
        )
        after_inventory = self.discovery.discover_once(
            self.wall_clock_ns()
        ).freeze(self.wall_clock_ns())
        samples = self.primitive_source.collect(
            window_start_ns=start_ns,
            window_end_ns=end_ns,
            inventory_revision=before,
        )
        raw_window = RawCollectionWindow.create(
            sequence=sequence,
            window_start_ns=start_ns,
            window_end_ns=end_ns,
            cluster_id=self.config.cluster_id,
            samples=samples,
        )
        evidence = (
            self.burst_source.collect(
                window_start_ns=start_ns,
                window_end_ns=end_ns,
                inventory_revision=before,
            )
            if self.burst_source is not None else ()
        )
        return self.assembler.assemble(
            raw_window=raw_window,
            inventory_at_start=before,
            inventory_at_end=after_inventory,
            burst_evidence=evidence,
        )

    def collect(self, window_count: int) -> tuple[CollectedWindow, ...]:
        if isinstance(window_count, bool) or not isinstance(window_count, int) \
                or window_count <= 0:
            raise RawCollectionError("window_count must be positive")
        return tuple(
            self.collect_one(sequence)
            for sequence in range(1, window_count + 1)
        )

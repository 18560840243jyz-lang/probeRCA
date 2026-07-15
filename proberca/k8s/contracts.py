"""Strict, credential-free contracts for Kubernetes discovery state."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field, replace
from typing import Any


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")).hexdigest()


def _required(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


@dataclass(frozen=True)
class KubernetesObjectRef:
    api_version: str
    kind: str
    namespace: str | None
    name: str
    uid: str
    resource_version: str
    generation: int | None = None

    def __post_init__(self) -> None:
        for name in ("api_version", "kind", "name", "uid", "resource_version"):
            _required(name, getattr(self, name))
        if self.namespace is not None:
            _required("namespace", self.namespace)
        if self.generation is not None and (
                isinstance(self.generation, bool) or not isinstance(self.generation, int)
                or self.generation < 0):
            raise ValueError("generation must be a non-negative integer or None")

    @classmethod
    def from_raw(cls, raw: dict) -> "KubernetesObjectRef":
        metadata = raw.get("metadata") or {}
        return cls(
            str(raw.get("apiVersion") or ""), str(raw.get("kind") or ""),
            metadata.get("namespace"), str(metadata.get("name") or ""),
            str(metadata.get("uid") or ""), str(metadata.get("resourceVersion") or ""),
            metadata.get("generation"),
        )


@dataclass(frozen=True)
class KubernetesWatchEvent:
    event_type: str
    object_ref: KubernetesObjectRef
    observed_at_ns: int
    raw_object_hash: str
    watch_stream_id: str
    relist_generation: int
    raw_object: dict = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.event_type not in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK", "ERROR"}:
            raise ValueError("invalid Kubernetes watch event type")
        if self.observed_at_ns < 0 or self.relist_generation < 0:
            raise ValueError("watch timestamps/generations must be non-negative")
        _required("watch_stream_id", self.watch_stream_id)
        if self.raw_object_hash != canonical_hash(self.raw_object):
            raise ValueError("raw_object_hash mismatch")

    @classmethod
    def from_raw(cls, event_type: str, raw_object: dict, observed_at_ns: int,
                 watch_stream_id: str, relist_generation: int) -> "KubernetesWatchEvent":
        return cls(
            event_type, KubernetesObjectRef.from_raw(raw_object), observed_at_ns,
            canonical_hash(raw_object), watch_stream_id, relist_generation, raw_object,
        )


@dataclass(frozen=True)
class ResourceVersionVector:
    resource_kind: str
    namespace_scope: tuple[str, ...]
    resource_version: str
    last_event_observed_ns: int
    synced: bool
    relisting: bool
    watch_stream_id: str


@dataclass(frozen=True)
class WorkloadRef:
    api_version: str
    kind: str
    namespace: str
    name: str
    uid: str
    owner_chain: tuple[KubernetesObjectRef, ...]


@dataclass(frozen=True)
class EndpointIdentity:
    service_uid: str
    endpoint_slice_uid: str
    address_type: str
    addresses: tuple[str, ...]
    port_name: str | None
    port: int
    protocol: str
    target_ref_uid: str | None
    node_name: str | None
    ready: bool | None
    serving: bool | None
    terminating: bool | None

    @property
    def dedup_key(self) -> tuple:
        return (self.service_uid, self.address_type, self.addresses, self.protocol,
                self.port, self.target_ref_uid)


@dataclass(frozen=True)
class RuntimeIdentityRecord:
    cluster_id: str
    namespace: str
    pod_uid: str
    pod_name: str
    pod_ips: tuple[str, ...]
    host_ip: str | None
    node_name: str | None
    workload_ref: WorkloadRef | None
    service_ids: tuple[str, ...]
    container_name: str
    container_type: str
    container_runtime: str | None
    full_container_id: str | None
    image_id: str | None
    started: bool | None
    ready: bool
    restart_count: int
    observed_at_ns: int
    resource_version: str
    identity_fingerprint: str = ""

    def __post_init__(self) -> None:
        if self.container_type not in {"app", "init", "ephemeral"}:
            raise ValueError("invalid container_type")
        expected = canonical_hash({
            key: value for key, value in asdict(self).items()
            if key not in {"identity_fingerprint", "observed_at_ns"}
        })
        if self.identity_fingerprint and self.identity_fingerprint != expected:
            raise ValueError("runtime identity fingerprint mismatch")
        object.__setattr__(self, "identity_fingerprint", expected)


@dataclass(frozen=True)
class CallEdgeObservation:
    observation_id: str
    cluster_id: str
    namespace_scope: tuple[str, ...]
    source_service_id: str
    destination_service_id: str
    protocol: str
    request_count: float
    successful_count: float | None
    error_count: float | None
    observation_quality: float
    window_start_ns: int
    window_end_ns: int
    source_provider: str
    source_record_ids: tuple[str, ...]
    config_fingerprint: str

    def __post_init__(self) -> None:
        if self.window_end_ns <= self.window_start_ns:
            raise ValueError("call edge window is invalid")
        for name in ("request_count", "observation_quality"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.observation_quality > 1:
            raise ValueError("observation_quality must be in [0,1]")
        if self.request_count == 0:
            raise ValueError("zero-request observations are not active call edges")


@dataclass(frozen=True)
class InventoryRevision:
    revision_id: str
    cluster_id: str
    resource_versions: tuple[ResourceVersionVector, ...]
    synchronized: bool
    stale: bool
    observed_at_ns: int
    object_counts: dict[str, int]
    issues: tuple[dict, ...]
    fingerprint: str
    objects_by_kind: dict[str, dict[str, dict]] = field(repr=False, compare=False)
    pod_to_services: dict[str, tuple[str, ...]] = field(repr=False, compare=False)
    service_uid_by_name: dict[tuple[str, str], str] = field(repr=False, compare=False)
    pod_uid_by_name: dict[tuple[str, str], str] = field(repr=False, compare=False)
    pod_uids_by_ip: dict[str, tuple[str, ...]] = field(repr=False, compare=False)

    @property
    def ready(self) -> bool:
        return self.synchronized and not self.stale

    def with_stale(self, value: bool) -> "InventoryRevision":
        return replace(self, stale=value)

    def resolve_service_for_pod(self, pod_uid: str, explicit_service: str | None = None) -> str:
        services = self.pod_to_services.get(pod_uid, ())
        if explicit_service is not None:
            pod = self.objects_by_kind.get("Pod", {}).get(pod_uid)
            namespace = (pod or {}).get("metadata", {}).get("namespace")
            service_uid = self.service_uid_by_name.get((namespace, explicit_service))
            if service_uid is None:
                raise ValueError("explicit service does not exist")
            service_id = f"{self.cluster_id}::{namespace}::{explicit_service}"
            if service_id not in services:
                raise ValueError("explicit service does not contain pod")
            return service_id
        if len(services) != 1:
            from .endpoints import AmbiguousPodServiceMappingError
            raise AmbiguousPodServiceMappingError(
                f"pod_uid={pod_uid} has service memberships={list(services)}")
        return services[0]

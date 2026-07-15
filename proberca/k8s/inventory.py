"""Atomic UID-indexed Kubernetes inventory."""
from __future__ import annotations

import copy
from functools import wraps
import threading
import time
from dataclasses import asdict, dataclass

from .contracts import (
    InventoryRevision, KubernetesObjectRef, KubernetesWatchEvent,
    ResourceVersionVector, canonical_hash,
)


def _synchronized(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        started = time.monotonic()
        acquired = self._lock.acquire(timeout=self.lock_timeout_sec)
        waited = time.monotonic() - started
        if not acquired:
            raise InventoryLockTimeout(
                "inventory-write", self._lock_holder, waited,
            )
        outer = self._lock_holder is None
        if outer:
            self._lock_holder = threading.current_thread().name
        try:
            return method(self, *args, **kwargs)
        finally:
            if outer:
                self._lock_holder = None
            self._lock.release()
    return locked


class InventoryConflictError(ValueError):
    """Inventory identity or reverse index is ambiguous."""


class InventoryLockTimeout(TimeoutError):
    def __init__(self, lock_name, holder_thread, wait_duration_sec):
        self.lock_name = str(lock_name)
        self.holder_thread = holder_thread
        self.wait_duration_sec = float(wait_duration_sec)
        super().__init__(
            f"lock={self.lock_name} holder={holder_thread or 'unknown'} "
            f"wait={self.wait_duration_sec:.6f} sec"
        )


@dataclass(frozen=True)
class ReindexResult:
    name_index: dict[tuple[str, str | None, str], str]
    pod_ips: dict[str, set[str]]
    pod_to_services: dict[str, set[str]]
    structural_issues: tuple[dict, ...]


def _canonicalize_structural_issues(issues) -> tuple[dict, ...]:
    by_identity = {}
    for raw in issues:
        reason_code = str(raw.get("reason_code") or "")
        object_id = str(raw.get("object_id") or "")
        if not reason_code or not object_id:
            raise InventoryConflictError(
                "structural issue requires reason_code and object_id"
            )
        related_object_ids = tuple(sorted({
            str(value) for value in raw.get("related_object_ids", ())
            if str(value)
        }))
        details = copy.deepcopy(raw.get("details") or {})
        if not isinstance(details, dict):
            raise InventoryConflictError("structural issue details must be a mapping")
        detail_identity = canonical_hash(details)
        identity = (
            reason_code,
            object_id,
            related_object_ids,
            detail_identity,
        )
        normalized = {
            "reason_code": reason_code,
            "object_id": object_id,
        }
        if related_object_ids:
            normalized["related_object_ids"] = list(related_object_ids)
        if details:
            normalized["details"] = details
        by_identity[identity] = normalized
    return tuple(by_identity[identity] for identity in sorted(by_identity))



class KubernetesInventory:
    def __init__(self, cluster_id: str, *, required_kinds: tuple[str, ...],
                 stale_after_sec: float, namespace_scope: tuple[str, ...] = ("observability",),
                 endpoint_ready_policy: str = "ready_only",
                 include_terminating_endpoints: bool = False,
                 lock_timeout_sec: float = 10.0):
        if not cluster_id:
            raise ValueError("cluster_id is required")
        self.cluster_id = cluster_id
        self._lock = threading.RLock()
        if lock_timeout_sec <= 0:
            raise ValueError("inventory lock timeout must be positive")
        self.lock_timeout_sec = float(lock_timeout_sec)
        self._lock_holder = None
        self.required_kinds = tuple(required_kinds)
        self.stale_after_ns = int(stale_after_sec * 1_000_000_000)
        self.namespace_scope = tuple(namespace_scope)
        if endpoint_ready_policy not in {"ready_only", "ready_or_serving", "all"}:
            raise ValueError("invalid endpoint_ready_policy")
        self.endpoint_ready_policy = endpoint_ready_policy
        self.include_terminating_endpoints = include_terminating_endpoints
        self._objects: dict[str, dict[str, dict]] = {}
        self._versions: dict[str, ResourceVersionVector] = {}
        self._tombstones: dict[str, KubernetesObjectRef] = {}
        self._structural_issues: tuple[dict, ...] = ()
        self._relist_generation: dict[str, int] = {}
        self._reindex()

    @property
    @_synchronized
    def resource_versions(self) -> dict[str, ResourceVersionVector]:
        return dict(self._versions)

    @property
    @_synchronized
    def synchronized(self) -> bool:
        return all(kind in self._versions and self._versions[kind].synced
                   and not self._versions[kind].relisting for kind in self.required_kinds)

    @property
    @_synchronized
    def ready(self) -> bool:
        return self.freeze(time.time_ns()).ready

    @_synchronized
    def mark_relisting(self, kind: str, observed_at_ns: int) -> None:
        previous = self._versions.get(kind)
        self._relist_generation[kind] = self._relist_generation.get(kind, 0) + 1
        self._versions[kind] = ResourceVersionVector(
            kind, self.namespace_scope, previous.resource_version if previous else "unknown",
            observed_at_ns, False, True, previous.watch_stream_id if previous else "relist")

    @_synchronized
    def replace_kind(self, kind: str, objects: list[dict], resource_version: str,
                     observed_at_ns: int, watch_stream_id: str | None = None) -> None:
        replacement: dict[str, dict] = {}
        for raw in objects:
            ref = KubernetesObjectRef.from_raw(raw)
            if ref.kind != kind:
                raise InventoryConflictError(f"listed kind={ref.kind} expected={kind}")
            if ref.uid in replacement:
                raise InventoryConflictError(f"duplicate UID {ref.uid}")
            replacement[ref.uid] = copy.deepcopy(raw)
        self._objects[kind] = replacement
        self._versions[kind] = ResourceVersionVector(
            kind, self.namespace_scope, str(resource_version), observed_at_ns, True, False,
            watch_stream_id or f"{kind}-watch-{self._relist_generation.get(kind, 0)}")
        self._reindex()

    @_synchronized
    def apply_bookmark(
        self, kind: str, resource_version: str, observed_at_ns: int,
        watch_stream_id: str,
    ) -> None:
        if kind not in self.required_kinds or not resource_version:
            raise InventoryConflictError("bookmark identity is invalid")
        previous = self._versions.get(kind)
        self._versions[kind] = ResourceVersionVector(
            kind, self.namespace_scope, resource_version, observed_at_ns,
            bool(previous and previous.synced), False, watch_stream_id,
        )

    @_synchronized
    def apply_event(self, event: KubernetesWatchEvent) -> None:
        kind = event.object_ref.kind
        previous = self._versions.get(kind)
        if event.event_type == "ERROR":
            raise InventoryConflictError(f"watch ERROR kind={kind}")
        if event.event_type == "BOOKMARK":
            self._versions[kind] = ResourceVersionVector(
                kind, self.namespace_scope, event.object_ref.resource_version,
                event.observed_at_ns, bool(previous and previous.synced), False,
                event.watch_stream_id)
            return
        bucket = self._objects.setdefault(kind, {})
        ref = event.object_ref
        if event.event_type == "DELETED":
            bucket.pop(ref.uid, None)
            self._tombstones[ref.uid] = ref
        else:
            key = (ref.namespace, ref.name)
            for uid, raw in list(bucket.items()):
                other = KubernetesObjectRef.from_raw(raw)
                if (other.namespace, other.name) == key and uid != ref.uid:
                    self._tombstones[uid] = other
                    del bucket[uid]
            bucket[ref.uid] = copy.deepcopy(event.raw_object)
        self._versions[kind] = ResourceVersionVector(
            kind, self.namespace_scope, ref.resource_version, event.observed_at_ns,
            bool(previous and previous.synced), False, event.watch_stream_id)
        self._reindex()

    def _derive_indexes_and_issues(self) -> ReindexResult:
        name_index: dict[tuple[str, str | None, str], str] = {}
        pod_ips: dict[str, set[str]] = {}
        for kind in sorted(self._objects):
            values = self._objects[kind]
            for uid in sorted(values):
                raw = values[uid]
                metadata = raw.get("metadata") or {}
                key = (kind, metadata.get("namespace"), metadata.get("name"))
                if key in name_index and name_index[key] != uid:
                    raise InventoryConflictError(f"duplicate object name {key}")
                name_index[key] = uid
                if kind == "Pod":
                    status = raw.get("status") or {}
                    addresses = [status.get("podIP"), *[
                        item.get("ip") for item in status.get("podIPs") or []]]
                    for address in sorted({
                            item for item in addresses if item}):
                        pod_ips.setdefault(address, set()).add(uid)

        structural_issues = []
        pod_to_services: dict[str, set[str]] = {}
        endpoint_slices = self._objects.get("EndpointSlice", {})
        for uid in sorted(endpoint_slices):
            raw = endpoint_slices[uid]
            metadata = raw.get("metadata") or {}
            namespace = metadata.get("namespace")
            service_name = (metadata.get("labels") or {}).get(
                "kubernetes.io/service-name"
            )
            service_uid = name_index.get(("Service", namespace, service_name))
            if not service_uid:
                structural_issues.append({
                    "reason_code": "endpoint_service_missing",
                    "object_id": metadata.get("uid"),
                })
                continue
            owners = metadata.get("ownerReferences") or []
            if owners and not any(
                    item.get("uid") == service_uid for item in owners):
                raise InventoryConflictError(
                    "EndpointSlice owner UID does not match Service"
                )
            service_id = f"{self.cluster_id}::{namespace}::{service_name}"
            for endpoint in raw.get("endpoints") or []:
                conditions = endpoint.get("conditions") or {}
                if (conditions.get("terminating") is True
                        and not self.include_terminating_endpoints):
                    continue
                if (self.endpoint_ready_policy == "ready_only"
                        and conditions.get("ready") is not True):
                    continue
                if self.endpoint_ready_policy == "ready_or_serving" and not (
                        conditions.get("ready") is True
                        or conditions.get("serving") is True):
                    continue
                target = endpoint.get("targetRef") or {}
                pod_uid = (
                    target.get("uid")
                    if target.get("kind") == "Pod"
                    else None
                )
                if pod_uid is None:
                    matches = set()
                    for address in endpoint.get("addresses") or []:
                        matches.update(pod_ips.get(address, set()))
                    if len(matches) == 1:
                        pod_uid = next(iter(matches))
                    elif len(matches) > 1:
                        raise InventoryConflictError(
                            "endpoint address maps to multiple Pods"
                        )
                if pod_uid in self._objects.get("Pod", {}):
                    pod_to_services.setdefault(pod_uid, set()).add(service_id)
        return ReindexResult(
            name_index=name_index,
            pod_ips=pod_ips,
            pod_to_services=pod_to_services,
            structural_issues=_canonicalize_structural_issues(
                structural_issues
            ),
        )

    @_synchronized
    def _reindex(self) -> None:
        result = self._derive_indexes_and_issues()
        self._name_index = result.name_index
        self._pod_ips = result.pod_ips
        self._pod_to_services = result.pod_to_services
        self._structural_issues = result.structural_issues

    @_synchronized
    def object_count(self, kind: str) -> int:
        return len(self._objects.get(kind, {}))

    @_synchronized
    def uid_for_name(self, kind: str, namespace: str | None, name: str) -> str | None:
        return self._name_index.get((kind, namespace, name))

    @_synchronized
    def tombstone(self, uid: str) -> KubernetesObjectRef:
        return self._tombstones[uid]

    @_synchronized
    def resolve_unique_pod_ip(self, address: str) -> str | None:
        matches = sorted(self._pod_ips.get(address, ()))
        if len(matches) > 1:
            raise InventoryConflictError(f"pod IP {address} is ambiguous")
        return matches[0] if matches else None

    def freeze(self, observed_at_ns: int) -> InventoryRevision:
        started = time.monotonic()
        if not self._lock.acquire(timeout=self.lock_timeout_sec):
            raise InventoryLockTimeout(
                "inventory-freeze",
                self._lock_holder,
                time.monotonic() - started,
            )
        outer = self._lock_holder is None
        if outer:
            self._lock_holder = threading.current_thread().name
        try:
            last = min(
                (item.last_event_observed_ns for item in self._versions.values()),
                default=0,
            )
            stale = bool(self._versions) and observed_at_ns - last > self.stale_after_ns
            versions = tuple(sorted(
                self._versions.values(), key=lambda item: item.resource_kind,
            ))
            synchronized = all(
                kind in self._versions
                and self._versions[kind].synced
                and not self._versions[kind].relisting
                for kind in self.required_kinds
            )
            object_refs = {
                kind: dict(values)
                for kind, values in self._objects.items()
            }
            issues = self._structural_issues
            pod_services = {
                uid: tuple(sorted(values))
                for uid, values in self._pod_to_services.items()
            }
            name_index = dict(self._name_index)
            pod_ips = {
                address: tuple(sorted(values))
                for address, values in self._pod_ips.items()
            }
        finally:
            if outer:
                self._lock_holder = None
            self._lock.release()
        objects = copy.deepcopy(object_refs)
        copied_issues = tuple(copy.deepcopy(issues))
        counts = {
            kind: len(values) for kind, values in sorted(objects.items())
        }
        structural = {
            "cluster_id": self.cluster_id,
            "resource_versions": [asdict(item) for item in versions],
            "object_counts": counts,
            "objects": objects,
            "issues": copied_issues,
        }
        fingerprint = canonical_hash(structural)
        return InventoryRevision(
            canonical_hash({
                "fingerprint": fingerprint,
                "observed_at_ns": observed_at_ns,
            }),
            self.cluster_id,
            versions,
            synchronized,
            stale,
            observed_at_ns,
            counts,
            copied_issues,
            fingerprint,
            objects,
            pod_services,
            {
                (namespace or "", name): uid
                for (kind, namespace, name), uid in name_index.items()
                if kind == "Service"
            },
            {
                (namespace or "", name): uid
                for (kind, namespace, name), uid in name_index.items()
                if kind == "Pod"
            },
            pod_ips,
        )

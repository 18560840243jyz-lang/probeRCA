"""UID-only Kubernetes owner chain resolution."""
from __future__ import annotations

from .contracts import KubernetesObjectRef, WorkloadRef


class OwnershipError(ValueError):
    """OwnerReferences are missing, conflicting, cyclic, or cross-namespace."""


def _controller(raw: dict) -> dict | None:
    owners = [item for item in (raw.get("metadata") or {}).get("ownerReferences") or []
              if item.get("controller") is True]
    if len(owners) > 1:
        raise OwnershipError("multiple controller owners")
    return owners[0] if owners else None


def resolve_workload(pod: dict, objects_by_uid: dict[str, dict]) -> WorkloadRef | None:
    metadata = pod.get("metadata") or {}
    namespace = metadata.get("namespace")
    owner = _controller(pod)
    if owner is None:
        return None
    chain = []
    seen = {metadata.get("uid")}
    current = owner
    while current is not None:
        uid = current.get("uid")
        if not uid or uid in seen:
            raise OwnershipError("owner chain is missing UID or cyclic")
        seen.add(uid)
        raw = objects_by_uid.get(uid)
        if raw is None:
            raise OwnershipError(f"owner UID {uid} is unresolved")
        ref = KubernetesObjectRef.from_raw(raw)
        if ref.namespace != namespace:
            raise OwnershipError("namespaced owner crosses namespace")
        if ref.name != current.get("name") or ref.kind != current.get("kind"):
            raise OwnershipError("owner UID/name/kind mismatch")
        chain.append(ref)
        next_owner = _controller(raw)
        if next_owner is None or ref.kind in {"Deployment", "StatefulSet", "DaemonSet", "Job"}:
            return WorkloadRef(ref.api_version, ref.kind, namespace, ref.name, ref.uid,
                               tuple(chain))
        current = next_owner
    raise OwnershipError("owner chain did not resolve")

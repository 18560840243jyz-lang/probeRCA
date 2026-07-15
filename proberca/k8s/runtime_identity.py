"""Container runtime identities derived only from Kubernetes Pod status."""
from __future__ import annotations

from .contracts import RuntimeIdentityRecord
from .ownership import OwnershipError, resolve_workload


def _statuses(pod: dict):
    status = pod.get("status") or {}
    for container_type, field in (
            ("app", "containerStatuses"), ("init", "initContainerStatuses"),
            ("ephemeral", "ephemeralContainerStatuses")):
        for item in status.get(field) or []:
            yield container_type, item


def runtime_identities(revision) -> tuple[RuntimeIdentityRecord, ...]:
    all_objects = {uid: raw for values in revision.objects_by_kind.values()
                   for uid, raw in values.items()}
    output = []
    for pod_uid, pod in sorted(revision.objects_by_kind.get("Pod", {}).items()):
        metadata, spec, status = pod.get("metadata") or {}, pod.get("spec") or {}, pod.get("status") or {}
        namespace = metadata.get("namespace")
        try:
            workload = resolve_workload(pod, all_objects)
        except OwnershipError:
            workload = None
        pod_ips = tuple(sorted({item for item in [status.get("podIP"), *[
            value.get("ip") for value in status.get("podIPs") or []]] if item}))
        for container_type, item in _statuses(pod):
            full_id = item.get("containerID")
            runtime = None
            if full_id:
                if "://" not in full_id:
                    raise ValueError("containerID runtime scheme is missing")
                runtime = full_id.split("://", 1)[0]
                if runtime not in {"containerd", "cri-o", "docker"}:
                    raise ValueError(f"unknown container runtime {runtime}")
            output.append(RuntimeIdentityRecord(
                revision.cluster_id, namespace, pod_uid, metadata.get("name"), pod_ips,
                status.get("hostIP"), spec.get("nodeName"), workload,
                revision.pod_to_services.get(pod_uid, ()), item.get("name"), container_type,
                runtime, full_id, item.get("imageID"), item.get("started"),
                bool(item.get("ready", False)), int(item.get("restartCount", 0)),
                revision.observed_at_ns, str(metadata.get("resourceVersion")),
            ))
    return tuple(sorted(output, key=lambda item: (
        item.namespace, item.pod_uid, item.container_type, item.container_name)))

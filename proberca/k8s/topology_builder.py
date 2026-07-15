"""Build deterministic P3-compatible snapshots from frozen inventory revisions."""
from __future__ import annotations

from itertools import combinations

from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION, ServiceNodePlacement, ServiceResourceBinding,
    TopologyEdge, TopologySnapshot,
)

from .contracts import canonical_hash
from .runtime_identity import runtime_identities


class TopologyBuildError(ValueError):
    """A live inventory revision cannot produce a formal topology."""


class LiveTopologyBuilder:
    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id

    def build(self, window_start_ns: int, window_end_ns: int, revision,
              call_observations) -> TopologySnapshot:
        if not revision.ready:
            raise TopologyBuildError("inventory revision is unsynchronized or stale")
        if revision.cluster_id != self.cluster_id:
            raise TopologyBuildError("inventory cluster mismatch")
        services = {}
        for uid, raw in revision.objects_by_kind.get("Service", {}).items():
            metadata = raw.get("metadata") or {}
            services[uid] = (metadata.get("namespace"), metadata.get("name"))
        service_names = sorted(f"{namespace}::{name}" for namespace, name in services.values())
        known_ids = {f"{self.cluster_id}::{namespace}::{name}" for namespace, name in services.values()}
        calls = []
        for item in sorted(call_observations, key=lambda value: value.observation_id):
            if item.cluster_id != self.cluster_id or item.source_service_id not in known_ids \
                    or item.destination_service_id not in known_ids:
                raise TopologyBuildError("call edge endpoint/cluster is invalid")
            source = item.source_service_id.split("::")
            destination = item.destination_service_id.split("::")
            calls.append(TopologyEdge(
                source[2], destination[2], "call", source[1], destination[1],
                item.protocol, directed=True))
        placements = []
        node_services: dict[str, set[str]] = {}
        for pod_uid, service_ids in revision.pod_to_services.items():
            pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
            node_name = (pod.get("spec") or {}).get("nodeName")
            if not node_name:
                continue
            for service_id in service_ids:
                parts = service_id.split("::")
                placements.append(ServiceNodePlacement(parts[1], parts[2], node_name, pod_uid))
                node_services.setdefault(node_name, set()).add(service_id)
        hosts = []
        for node_name, members in sorted(node_services.items()):
            for left, right in combinations(sorted(members), 2):
                left_parts, right_parts = left.split("::"), right.split("::")
                hosts.append(TopologyEdge(
                    left_parts[2], right_parts[2], "host", left_parts[1], right_parts[1],
                    resource_type="node", resource_id=node_name, directed=False))
        bindings = []
        pvc_by_name = {
            ((raw.get("metadata") or {}).get("namespace"),
             (raw.get("metadata") or {}).get("name")): (uid, raw)
            for uid, raw in revision.objects_by_kind.get("PersistentVolumeClaim", {}).items()
        }
        pv_by_name = {
            (raw.get("metadata") or {}).get("name"): (uid, raw)
            for uid, raw in revision.objects_by_kind.get("PersistentVolume", {}).items()
        }
        node_uid_by_name = {
            (raw.get("metadata") or {}).get("name"): uid
            for uid, raw in revision.objects_by_kind.get("Node", {}).items()
        }
        for pod_uid, service_ids in revision.pod_to_services.items():
            pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
            metadata, spec = pod.get("metadata") or {}, pod.get("spec") or {}
            namespace = metadata.get("namespace")
            for volume in spec.get("volumes") or []:
                claim = (volume.get("persistentVolumeClaim") or {}).get("claimName")
                if claim:
                    pvc = pvc_by_name.get((namespace, claim))
                    pvc_uid, pvc_raw = pvc if pvc else (claim, {})
                    resources = [("pvc", f"{self.cluster_id}/{namespace}/pvc/{pvc_uid}")]
                    pv_name = (pvc_raw.get("spec") or {}).get("volumeName")
                    if pv_name and pv_name in pv_by_name:
                        pv_uid, pv_raw = pv_by_name[pv_name]
                        resources.append(("pv", f"{self.cluster_id}/pv/{pv_uid}"))
                        csi = (pv_raw.get("spec") or {}).get("csi") or {}
                        if csi.get("driver") and csi.get("volumeHandle"):
                            resources.append((
                                "csi", f"{self.cluster_id}/csi/{csi['driver']}/{csi['volumeHandle']}"))
                    for service_id in service_ids:
                        for resource_type, resource_id in resources:
                            bindings.append(ServiceResourceBinding(
                                namespace, service_id.split("::")[2], resource_type, resource_id))
            if spec.get("hostNetwork") is True and spec.get("nodeName"):
                node_id = node_uid_by_name.get(spec["nodeName"], spec["nodeName"])
                for service_id in service_ids:
                    bindings.append(ServiceResourceBinding(
                        namespace, service_id.split("::")[2], "host_network",
                        f"{self.cluster_id}/node/{node_id}"))
            annotations = metadata.get("annotations") or {}
            prefix = "proberca.io/resource-"
            for key, resource_id in sorted(annotations.items()):
                if key.startswith(prefix) and resource_id:
                    resource_type = key[len(prefix):]
                    for service_id in service_ids:
                        bindings.append(ServiceResourceBinding(
                            namespace, service_id.split("::")[2], resource_type,
                            f"{self.cluster_id}/{namespace}/{resource_type}/{resource_id}"))
        structure = {
            "cluster": self.cluster_id, "services": service_names,
            "calls": [item.to_dict() for item in calls],
            "hosts": [item.to_dict() for item in hosts],
            "bindings": [item.to_dict() for item in bindings],
        }
        structure_fingerprint = canonical_hash(structure)
        identities = runtime_identities(revision)
        snapshot_identity = {
            "structure": structure_fingerprint, "start": window_start_ns,
            "end": window_end_ns, "revision": revision.revision_id,
        }
        provider_fingerprints = sorted({
            item.config_fingerprint for item in call_observations})
        return TopologySnapshot(
            schema_version=PROBERCA_SCHEMA_VERSION,
            snapshot_id=canonical_hash(snapshot_identity), valid_from_ns=window_start_ns,
            valid_to_ns=window_end_ns, cluster_id=self.cluster_id,
            services=service_names, call_edges=calls, host_edges=hosts, resource_edges=[],
            service_nodes=sorted(set(placements), key=lambda item: (
                item.namespace, item.service_name, item.node_name, item.pod_uid or "")),
            service_resources=sorted(set(bindings), key=lambda item: (
                item.namespace, item.service_name, item.resource_type, item.resource_id)),
            structure_fingerprint=structure_fingerprint,
            inventory_revision_id=revision.revision_id,
            resource_version_vector={
                item.resource_kind: item.resource_version for item in revision.resource_versions},
            runtime_identity_fingerprints=sorted(
                item.identity_fingerprint for item in identities),
            call_edge_provider_fingerprint=(
                canonical_hash(provider_fingerprints) if provider_fingerprints else None),
            topology_build_issues=list(revision.issues),
        )

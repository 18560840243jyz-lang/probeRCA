"""Time-aware cgroup/process mapping into canonical P1/P7 records."""
from __future__ import annotations

import hashlib
import ipaddress
import json
from dataclasses import dataclass

from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION, BurstEventRecord, EdgeMetricRecord,
    EvidenceObservationRecord, METRIC_RECORD_SCHEMA_VERSION, NodeMetricRecord,
)

from .contracts import EventClass, EventQuality, EventType, KernelEvent


@dataclass(frozen=True)
class RuntimeBinding:
    cgroup_id: int
    cluster_id: str
    namespace: str
    service_name: str
    pod_uid: str
    container_id_fingerprint: str
    pod_identity_fingerprint: str
    node_name: str | None
    valid_from_ns: int
    valid_to_ns: int | None
    process_start_time_ns: int | None = None
    pod_ips: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.cgroup_id < 0 or self.valid_from_ns < 0:
            raise ValueError("mapping cgroup and validity must be non-negative")
        if self.valid_to_ns is not None and self.valid_to_ns <= self.valid_from_ns:
            raise ValueError("mapping validity interval is empty")
        for name in ("cluster_id", "namespace", "service_name", "pod_uid"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} is required")
        for name in ("container_id_fingerprint", "pod_identity_fingerprint"):
            value = getattr(self, name)
            if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
                raise ValueError(f"{name} must be lowercase SHA-256")
        normalized_ips = tuple(sorted({
            str(ipaddress.ip_address(value)) for value in self.pod_ips
        }))
        object.__setattr__(self, "pod_ips", normalized_ips)

    @classmethod
    def from_runtime_identity(
        cls, identity, *, cgroup_id: int, valid_from_ns: int | None = None,
        valid_to_ns: int | None = None,
    ):
        if len(identity.service_ids) != 1:
            raise ValueError("runtime identity must map to exactly one service")
        parts = identity.service_ids[0].split("::", 2)
        if len(parts) != 3:
            raise ValueError("service identity is not canonical")
        cluster_id, namespace, service_name = parts
        if cluster_id != identity.cluster_id or namespace != identity.namespace:
            raise ValueError("runtime and service identity disagree")
        pod_payload = {
            "cluster_id": identity.cluster_id,
            "pod_uid": identity.pod_uid,
            "resource_version": identity.resource_version,
        }
        pod_fingerprint = hashlib.sha256(json.dumps(
            pod_payload, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        return cls(
            cgroup_id=int(cgroup_id), cluster_id=cluster_id,
            namespace=namespace, service_name=service_name,
            pod_uid=identity.pod_uid,
            container_id_fingerprint=identity.identity_fingerprint,
            pod_identity_fingerprint=pod_fingerprint,
            node_name=identity.node_name,
            valid_from_ns=(identity.observed_at_ns if valid_from_ns is None
                           else int(valid_from_ns)),
            valid_to_ns=valid_to_ns, pod_ips=tuple(identity.pod_ips),
        )

    def contains(self, timestamp_ns: int) -> bool:
        return self.valid_from_ns <= timestamp_ns and (
            self.valid_to_ns is None or timestamp_ns < self.valid_to_ns
        )


@dataclass(frozen=True)
class MappedEvent:
    event: KernelEvent
    mapping_status: str
    cluster_id: str | None = None
    namespace: str | None = None
    service_name: str | None = None
    pod_uid: str | None = None
    container_identity_fingerprint: str | None = None
    pod_identity_fingerprint: str | None = None
    node_name: str | None = None
    dst_namespace: str | None = None
    dst_service_name: str | None = None
    dst_pod_uid: str | None = None
    dst_node_name: str | None = None


class IdentityMapper:
    def __init__(self, bindings=()):
        self._bindings: dict[int, list[RuntimeBinding]] = {}
        self._ip_bindings: dict[str, list[RuntimeBinding]] = {}
        for binding in bindings:
            self.register(binding)

    def register(self, binding: RuntimeBinding) -> None:
        if not isinstance(binding, RuntimeBinding):
            raise TypeError("binding must be RuntimeBinding")
        bucket = self._bindings.setdefault(binding.cgroup_id, [])
        if any(existing.valid_from_ns == binding.valid_from_ns for existing in bucket):
            bucket[:] = [x for x in bucket if x.valid_from_ns != binding.valid_from_ns]
        bucket.append(binding)
        bucket.sort(key=lambda item: item.valid_from_ns)
        for address in binding.pod_ips:
            ip_bucket = self._ip_bindings.setdefault(address, [])
            if any(existing.valid_from_ns == binding.valid_from_ns
                   for existing in ip_bucket):
                ip_bucket[:] = [
                    item for item in ip_bucket
                    if item.valid_from_ns != binding.valid_from_ns
                ]
            ip_bucket.append(binding)
            ip_bucket.sort(key=lambda item: item.valid_from_ns)

    @staticmethod
    def _resolve_bucket(bindings, timestamp_ns):
        matches = [item for item in bindings if item.contains(timestamp_ns)]
        if len(matches) > 1:
            raise ValueError("overlapping runtime identity intervals")
        return matches[0] if matches else None

    def _resolve(self, cgroup_id: int, timestamp_ns: int) -> RuntimeBinding | None:
        return self._resolve_bucket(
            self._bindings.get(cgroup_id, ()), timestamp_ns,
        )

    def _resolve_ip(self, address: str, timestamp_ns: int) -> RuntimeBinding | None:
        return self._resolve_bucket(
            self._ip_bindings.get(address, ()), timestamp_ns,
        )

    def map(self, event: KernelEvent) -> MappedEvent:
        primary_id = event.src_cgroup_id if event.event_class is EventClass.EDGE else event.cgroup_id
        source = self._resolve(primary_id, event.timestamp_ns)
        if source is None:
            return MappedEvent(event, "unmapped")
        if source.process_start_time_ns is not None and event.process_start_time_ns not in {
            0, source.process_start_time_ns,
        }:
            return MappedEvent(event, "pid_reused")
        destination = None
        status = "mapped"
        if event.event_class is EventClass.EDGE:
            destination = (
                self._resolve(event.dst_cgroup_id, event.timestamp_ns)
                if event.dst_cgroup_id else None
            )
            if destination is None and event.dst_ip is not None:
                destination = self._resolve_ip(event.dst_ip, event.timestamp_ns)
            if destination is None:
                status = "partial"
        return MappedEvent(
            event=event, mapping_status=status,
            cluster_id=source.cluster_id, namespace=source.namespace,
            service_name=source.service_name, pod_uid=source.pod_uid,
            container_identity_fingerprint=source.container_id_fingerprint,
            pod_identity_fingerprint=source.pod_identity_fingerprint,
            node_name=source.node_name,
            dst_namespace=destination.namespace if destination else None,
            dst_service_name=destination.service_name if destination else None,
            dst_pod_uid=destination.pod_uid if destination else None,
            dst_node_name=destination.node_name if destination else None,
        )


_NODE_FAMILY = {
    EventType.PROCESS_FORK: "cpu", EventType.PROCESS_EXEC: "cpu",
    EventType.PROCESS_EXIT: "cpu", EventType.PROCESS_CGROUP_MIGRATE: "cpu",
    EventType.SCHED_OFFCPU: "cpu",
    EventType.SCHED_RUNQUEUE: "cpu", EventType.FUTEX_WAIT: "lock",
    EventType.FUTEX_WAKE: "lock", EventType.BLOCK_ISSUE: "io",
    EventType.BLOCK_COMPLETE: "io", EventType.BLOCK_LATENCY: "io",
}
_DURATION_TYPES = {
    EventType.SCHED_OFFCPU, EventType.SCHED_RUNQUEUE, EventType.FUTEX_WAIT,
    EventType.BLOCK_LATENCY, EventType.TCP_RTT,
}


def _unit_value(event: KernelEvent) -> tuple[str, float]:
    if event.event_type in _DURATION_TYPES:
        return "nanoseconds", float(event.duration_ns)
    return "count", float(event.value)


def _protocol(value: int) -> str:
    return {6: "tcp", 17: "udp"}.get(value, "unknown")


def event_to_burst_record(
    mapped: MappedEvent, *, burst_id: str, lost_events: int,
    probe_name: str = "libbpf-core",
) -> BurstEventRecord:
    event = mapped.event
    unit, value = _unit_value(event)
    return BurstEventRecord(
        schema_version=PROBERCA_SCHEMA_VERSION,
        event_id=event.event_fingerprint,
        timestamp_ns=event.timestamp_ns,
        event_type=event.event_type_name,
        pid=event.pid or None, tid=event.tid or None,
        cgroup_id=event.cgroup_id or None,
        container_id=mapped.container_identity_fingerprint,
        pod_uid=mapped.pod_uid,
        service_name=mapped.service_name,
        node_name=mapped.node_name,
        src_service=mapped.service_name if event.event_class is EventClass.EDGE else None,
        dst_service=mapped.dst_service_name,
        src_ip=event.src_ip, dst_ip=event.dst_ip,
        src_port=event.src_port or None, dst_port=event.dst_port or None,
        protocol=_protocol(event.protocol) if event.protocol else None,
        value=value, unit=unit, probe_mode="burst", burst_id=burst_id,
        lost_events=lost_events,
        event_class=event.event_class.name.lower(),
        quality=event.quality.name.lower(),
        mapping_status=mapped.mapping_status,
        process_start_time_ns=event.process_start_time_ns,
        container_identity_fingerprint=mapped.container_identity_fingerprint,
        pod_identity_fingerprint=mapped.pod_identity_fingerprint,
        source_cgroup_id=event.src_cgroup_id or None,
        target_cgroup_id=event.dst_cgroup_id or None,
        direction={0: "unknown", 1: "ingress", 2: "egress"}.get(event.direction, "unknown"),
        metric_family=(
            _NODE_FAMILY.get(event.event_type) if event.event_class is EventClass.NODE
            else "edge"
        ),
        probe_name=probe_name,
        attach_epoch=event.attach_epoch,
        event_sequence=event.event_sequence,
        cpu=event.cpu,
    )


def event_to_metric_record(
    mapped: MappedEvent, *, window_sec: int, event_loss_rate: float,
):
    event = mapped.event
    if mapped.mapping_status != "mapped":
        raise ValueError("only fully mapped events become metric records")
    unit, value = _unit_value(event)
    common = {
        "schema_version": METRIC_RECORD_SCHEMA_VERSION,
        "timestamp_ns": event.timestamp_ns,
        "window_sec": window_sec,
        "cluster_id": mapped.cluster_id,
        "value": value,
        "valid": True,
        "invalid_reason": None,
        "unit": unit,
        "sample_count": 1,
        "coverage": 1.0,
        "event_loss_rate": event_loss_rate,
        "mapping_quality": 1.0,
        "source": f"ebpf:{event.event_type_name}",
        "metric_kind": "delta_counter" if unit == "count" else "gauge",
        "histogram_upper_bound": None,
        "histogram_is_inf_bucket": False,
        "histogram_is_cumulative": None,
        "quantile": None,
    }
    if event.event_class is EventClass.NODE:
        return NodeMetricRecord(
            **common, node_name=mapped.node_name, namespace=mapped.namespace,
            service_name=mapped.service_name, pod_uid=mapped.pod_uid,
            container_id=mapped.container_identity_fingerprint,
            metric_family=_NODE_FAMILY[event.event_type],
            metric_name=event.event_type_name, scope="pod",
        )
    if event.event_class is EventClass.EDGE:
        if mapped.namespace != mapped.dst_namespace:
            raise ValueError("cross-namespace edge mapping requires explicit policy")
        return EdgeMetricRecord(
            **common, namespace=mapped.namespace,
            src_service=mapped.service_name, dst_service=mapped.dst_service_name,
            src_pod_uid=mapped.pod_uid, dst_pod_uid=mapped.dst_pod_uid,
            src_node=mapped.node_name, dst_node=mapped.dst_node_name,
            protocol=_protocol(event.protocol), metric_name=event.event_type_name,
            scope="pod_pair",
        )
    raise ValueError("control/loss events are not metric records")


def event_to_evidence(
    mapped: MappedEvent, *, target_type: str, target_id: str,
    window_start_ns: int, window_end_ns: int, analysis_cutoff_ns: int,
    normalized_strength: float, config_fingerprint: str,
) -> EvidenceObservationRecord:
    if mapped.mapping_status != "mapped":
        raise ValueError("unmapped events cannot target P7 variables")
    event = mapped.event
    source_objects = sorted({value for value in (
        mapped.container_identity_fingerprint, mapped.pod_identity_fingerprint,
    ) if value})
    payload = {
        "event": event.event_fingerprint, "target": target_id,
        "cutoff": analysis_cutoff_ns, "config": config_fingerprint,
    }
    evidence_id = hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()
    return EvidenceObservationRecord(
        schema_version=PROBERCA_SCHEMA_VERSION,
        evidence_id=evidence_id, timestamp_ns=event.timestamp_ns,
        evidence_window_start_ns=window_start_ns,
        evidence_window_end_ns=window_end_ns,
        analysis_cutoff_ns=analysis_cutoff_ns,
        cluster_id=mapped.cluster_id, namespace=mapped.namespace,
        target_type=target_type, target_id=target_id,
        channel_id=f"ebpf:{event.event_type_name}", source_type="burst_event",
        normalized_strength=normalized_strength,
        observation_quality={
            EventQuality.EXACT: 1.0, EventQuality.DERIVED: 0.8,
            EventQuality.PARTIAL: 0.5, EventQuality.UNMAPPED: 0.0,
        }[event.quality],
        reliability_weight=1.0,
        source_record_ids=[f"burst_event:{event.event_fingerprint}"],
        source_object_ids=source_objects,
        independent_from_residual=True,
        provenance={
            "calibration_id": "p12-ebpf-v1",
            "probe_name": event.event_type_name.split(".", 1)[0],
            "residual_adjustment": "none",
            "attach_epoch": event.attach_epoch,
        },
        config_fingerprint=config_fingerprint,
    )


__all__ = [
    "IdentityMapper", "MappedEvent", "RuntimeBinding", "event_to_burst_record",
    "event_to_evidence", "event_to_metric_record",
]

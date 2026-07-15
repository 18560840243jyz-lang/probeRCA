from __future__ import annotations

import hashlib
import socket
import struct
from types import SimpleNamespace
from dataclasses import replace

import pytest

from proberca.collectors.ebpf.contracts import (
    EVENT_ABI_SIZE,
    EVENT_SCHEMA_VERSION,
    EventClass,
    EventQuality,
    EventType,
    KernelEvent,
)
from proberca.collectors.ebpf.filters import CandidateFilter, CandidateSnapshot
from proberca.collectors.ebpf.mapping import (
    IdentityMapper,
    RuntimeBinding,
    event_to_burst_record,
    event_to_evidence,
    event_to_metric_record,
)


def event(**changes) -> KernelEvent:
    values = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_type": EventType.PROCESS_EXEC,
        "event_class": EventClass.NODE,
        "quality": EventQuality.EXACT,
        "timestamp_ns": 10_000,
        "process_start_time_ns": 1_000,
        "cgroup_id": 101,
        "src_cgroup_id": 101,
        "dst_cgroup_id": 0,
        "value": 1,
        "duration_ns": 0,
        "attach_epoch": 7,
        "event_sequence": 3,
        "cpu": 0,
        "pid": 123,
        "tid": 123,
        "src_ipv4": 0,
        "dst_ipv4": 0,
        "src_port": 0,
        "dst_port": 0,
        "protocol": 0,
        "direction": 0,
        "mapping_status": 0,
        "comm": "worker",
    }
    values.update(changes)
    return KernelEvent(**values)


def binding(**changes) -> RuntimeBinding:
    values = {
        "cgroup_id": 101,
        "cluster_id": "cluster-a",
        "namespace": "observability",
        "service_name": "service-a",
        "pod_uid": "pod-a",
        "container_id_fingerprint": hashlib.sha256(b"container-a").hexdigest(),
        "pod_identity_fingerprint": hashlib.sha256(b"pod-a").hexdigest(),
        "node_name": "node-a",
        "valid_from_ns": 500,
        "valid_to_ns": None,
        "process_start_time_ns": 1_000,
    }
    values.update(changes)
    return RuntimeBinding(**values)


def test_kernel_event_abi_round_trip_and_alignment():
    original = event()
    payload = original.pack()
    assert EVENT_ABI_SIZE == 136
    assert len(payload) == EVENT_ABI_SIZE
    assert KernelEvent.unpack(payload) == original


def test_incompatible_event_version_fails_fast():
    payload = bytearray(event().pack())
    payload[0:2] = (EVENT_SCHEMA_VERSION + 1).to_bytes(2, "little")
    with pytest.raises(ValueError, match="schema"):
        KernelEvent.unpack(bytes(payload))


@pytest.mark.parametrize(
    ("event_type", "expected_class"),
    [
        (EventType.PROCESS_EXEC, EventClass.NODE),
        (EventType.SCHED_OFFCPU, EventClass.NODE),
        (EventType.FUTEX_WAIT, EventClass.NODE),
        (EventType.BLOCK_LATENCY, EventClass.NODE),
        (EventType.TCP_RETRANSMIT, EventClass.EDGE),
        (EventType.DNS_QUERY, EventClass.EDGE),
        (EventType.LOSS, EventClass.LOSS),
    ],
)
def test_event_type_class_contract(event_type, expected_class):
    assert event(event_type=event_type, event_class=expected_class).event_class is expected_class
    with pytest.raises(ValueError):
        event(event_type=event_type, event_class=EventClass.CONTROL)


def test_candidate_filter_is_directional_and_versioned():
    candidates = CandidateSnapshot(
        version=4,
        cgroup_ids=(101,),
        service_pairs=((101, 202),),
        ttl_sec=30.0,
        max_candidates=8,
    )
    gate = CandidateFilter(candidates)
    assert gate.accepts(event())
    assert gate.accepts(
        event(
            event_type=EventType.TCP_RETRANSMIT,
            event_class=EventClass.EDGE,
            src_cgroup_id=101,
            dst_cgroup_id=202,
        )
    )
    assert not gate.accepts(
        event(
            event_type=EventType.TCP_RETRANSMIT,
            event_class=EventClass.EDGE,
            src_cgroup_id=202,
            dst_cgroup_id=101,
        )
    )
    replacement = CandidateSnapshot(
        version=5,
        cgroup_ids=(303,),
        service_pairs=((303, 404),),
        ttl_sec=30.0,
        max_candidates=8,
    )
    gate.replace(replacement)
    assert not gate.accepts(event())
    assert gate.version == 5


def test_candidate_filter_rejects_unbounded_or_empty_sets():
    with pytest.raises(ValueError):
        CandidateSnapshot(version=1, cgroup_ids=(), service_pairs=(), ttl_sec=30, max_candidates=8)
    with pytest.raises(ValueError):
        CandidateSnapshot(
            version=1,
            cgroup_ids=tuple(range(9)),
            service_pairs=(),
            ttl_sec=30,
            max_candidates=8,
        )


def test_unmapped_event_is_retained_without_fabricated_service():
    mapped = IdentityMapper().map(event())
    assert mapped.mapping_status == "unmapped"
    assert mapped.service_name is None
    assert mapped.event == event()


def test_mapping_rejects_pid_reuse_and_stale_cgroup_identity():
    mapper = IdentityMapper()
    mapper.register(binding())
    assert mapper.map(event()).mapping_status == "mapped"
    assert mapper.map(event(process_start_time_ns=2_000)).mapping_status == "pid_reused"
    assert mapper.map(event(cgroup_id=999, src_cgroup_id=999)).mapping_status == "unmapped"


def test_pod_restart_replaces_mapping_without_cross_instance_leakage():
    mapper = IdentityMapper()
    mapper.register(binding(valid_to_ns=20_000))
    replacement_binding = binding(
        pod_uid="pod-b",
        pod_identity_fingerprint=hashlib.sha256(b"pod-b").hexdigest(),
        valid_from_ns=20_000,
        valid_to_ns=None,
        process_start_time_ns=2_000,
    )
    mapper.register(replacement_binding)
    old = mapper.map(event(timestamp_ns=19_000))
    new = mapper.map(event(timestamp_ns=21_000, process_start_time_ns=2_000))
    assert old.pod_uid == "pod-a"
    assert new.pod_uid == "pod-b"
    assert old.pod_identity_fingerprint != new.pod_identity_fingerprint


def test_burst_record_preserves_extended_kernel_identity():
    mapped = IdentityMapper([binding()]).map(event())
    record = event_to_burst_record(mapped, burst_id="burst-a", lost_events=0)
    assert record.event_class == "node"
    assert record.process_start_time_ns == 1_000
    assert record.attach_epoch == 7
    assert record.event_sequence == 3
    assert record.container_identity_fingerprint == binding().container_id_fingerprint
    assert record.mapping_status == "mapped"


def test_node_and_edge_metric_conversion_stays_separate():
    mapper = IdentityMapper([binding()])
    node = event_to_metric_record(
        mapper.map(event()), window_sec=10, event_loss_rate=0.0
    )
    mapper.register(
        binding(
            cgroup_id=202,
            service_name="service-b",
            pod_uid="pod-b",
            container_id_fingerprint=hashlib.sha256(b"container-b").hexdigest(),
            pod_identity_fingerprint=hashlib.sha256(b"pod-b").hexdigest(),
        )
    )
    edge = event_to_metric_record(
        mapper.map(
            event(
                event_type=EventType.TCP_RETRANSMIT,
                event_class=EventClass.EDGE,
                dst_cgroup_id=202,
                protocol=6,
                value=2,
            )
        ),
        window_sec=10,
        event_loss_rate=0.0,
    )
    assert node.record_type == "node_metric"
    assert edge.record_type == "edge_metric"
    assert node.metric_family in {"cpu", "io", "lock"}
    assert edge.src_service == "service-a"
    assert edge.dst_service == "service-b"


def test_burst_evidence_is_independent_and_p7_compatible():
    mapped = IdentityMapper([binding()]).map(event())
    evidence = event_to_evidence(
        mapped,
        target_type="node",
        target_id="cluster-a::observability::service-a::cpu.offcpu",
        window_start_ns=0,
        window_end_ns=20_000,
        analysis_cutoff_ns=20_000,
        normalized_strength=0.7,
        config_fingerprint="a" * 64,
    )
    assert evidence.source_type == "burst_event"
    assert evidence.independent_from_residual
    assert evidence.provenance["residual_adjustment"] == "none"
    assert "incident" not in str(evidence.to_dict()).lower()


def test_event_identity_does_not_depend_on_business_labels():
    left = event().event_fingerprint
    right = replace(event(), comm="different").event_fingerprint
    assert left != right
    assert "service" not in event().identity_payload
    assert "metric" not in event().identity_payload
    assert "namespace" not in event().identity_payload


def ipv4_identity(address):
    return struct.unpack("=I", socket.inet_aton(address))[0]


def test_kernel_ipv4_identity_uses_network_byte_order():
    value = ipv4_identity("127.0.0.1")
    assert event(dst_ipv4=value).dst_ip == "127.0.0.1"


def test_edge_destination_maps_through_time_bounded_pod_ip():
    source = binding(pod_ips=("10.0.0.1",))
    destination = binding(
        cgroup_id=202, service_name="service-b", pod_uid="pod-b",
        container_id_fingerprint=hashlib.sha256(b"container-b").hexdigest(),
        pod_identity_fingerprint=hashlib.sha256(b"pod-b").hexdigest(),
        pod_ips=("10.0.0.2",),
    )
    mapped = IdentityMapper([source, destination]).map(event(
        event_type=EventType.TCP_RTT, event_class=EventClass.EDGE,
        dst_cgroup_id=0, dst_ipv4=ipv4_identity("10.0.0.2"), protocol=6,
        duration_ns=500,
    ))
    assert mapped.mapping_status == "mapped"
    assert mapped.service_name == "service-a"
    assert mapped.dst_service_name == "service-b"
    assert event_to_metric_record(
        mapped, window_sec=10, event_loss_rate=0.0,
    ).record_type == "edge_metric"


def test_reused_pod_ip_is_resolved_by_event_time_not_current_owner():
    old = binding(pod_ips=("10.0.0.2",), valid_to_ns=20_000)
    new = binding(
        cgroup_id=202, service_name="service-b", pod_uid="pod-b",
        container_id_fingerprint=hashlib.sha256(b"container-b").hexdigest(),
        pod_identity_fingerprint=hashlib.sha256(b"pod-b").hexdigest(),
        pod_ips=("10.0.0.2",), valid_from_ns=20_000,
    )
    mapper = IdentityMapper([old, new])
    assert mapper._resolve_ip("10.0.0.2", 19_000).pod_uid == "pod-a"
    assert mapper._resolve_ip("10.0.0.2", 21_000).pod_uid == "pod-b"


def test_runtime_binding_reuses_p11_runtime_identity_contract():
    identity = SimpleNamespace(
        service_ids=("cluster-a::observability::service-a",),
        cluster_id="cluster-a", namespace="observability", pod_uid="pod-a",
        identity_fingerprint=hashlib.sha256(b"runtime-a").hexdigest(),
        resource_version="17", node_name="node-a", observed_at_ns=500,
        pod_ips=("10.0.0.8",),
    )
    result = RuntimeBinding.from_runtime_identity(identity, cgroup_id=101)
    assert result.service_name == "service-a"
    assert result.pod_ips == ("10.0.0.8",)
    assert result.valid_from_ns == 500

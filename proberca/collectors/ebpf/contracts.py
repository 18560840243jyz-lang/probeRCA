"""Versioned, fixed-width kernel/user ABI for P12 burst events."""
from __future__ import annotations

import hashlib
import json
import socket
import struct
from dataclasses import dataclass
from enum import IntEnum

EVENT_SCHEMA_VERSION = 1
_EVENT_STRUCT = struct.Struct("<HHHHI4xQQQQQQQQQIIIIIHHHHHH16s")
EVENT_ABI_SIZE = _EVENT_STRUCT.size
if EVENT_ABI_SIZE != 136:
    raise RuntimeError(f"unexpected P12 event ABI size {EVENT_ABI_SIZE}")


class EventClass(IntEnum):
    NODE = 1
    EDGE = 2
    UNMAPPED = 3
    CONTROL = 4
    LOSS = 5


class EventQuality(IntEnum):
    EXACT = 1
    DERIVED = 2
    PARTIAL = 3
    UNMAPPED = 4


class EventType(IntEnum):
    PROCESS_FORK = 1
    PROCESS_EXEC = 2
    PROCESS_EXIT = 3
    PROCESS_CGROUP_MIGRATE = 4
    SCHED_OFFCPU = 10
    SCHED_RUNQUEUE = 11
    FUTEX_WAIT = 20
    FUTEX_WAKE = 21
    BLOCK_ISSUE = 30
    BLOCK_COMPLETE = 31
    BLOCK_LATENCY = 32
    TCP_RETRANSMIT = 40
    TCP_RESET = 41
    TCP_CONNECT_FAIL = 42
    TCP_RTT = 43
    DNS_QUERY = 50
    DNS_RESPONSE = 51
    LOSS = 60


EVENT_NAMES = {
    EventType.PROCESS_FORK: "process.fork",
    EventType.PROCESS_EXEC: "process.exec",
    EventType.PROCESS_EXIT: "process.exit",
    EventType.PROCESS_CGROUP_MIGRATE: "process.cgroup_migrate",
    EventType.SCHED_OFFCPU: "sched.offcpu",
    EventType.SCHED_RUNQUEUE: "sched.runqueue_wait",
    EventType.FUTEX_WAIT: "futex.wait",
    EventType.FUTEX_WAKE: "futex.wake",
    EventType.BLOCK_ISSUE: "block.issue",
    EventType.BLOCK_COMPLETE: "block.complete",
    EventType.BLOCK_LATENCY: "block.latency",
    EventType.TCP_RETRANSMIT: "tcp.retransmit",
    EventType.TCP_RESET: "tcp.rst",
    EventType.TCP_CONNECT_FAIL: "tcp.connect_fail",
    EventType.TCP_RTT: "tcp.rtt",
    EventType.DNS_QUERY: "dns.query",
    EventType.DNS_RESPONSE: "dns.response",
    EventType.LOSS: "probe.loss",
}

NODE_TYPES = frozenset({
    EventType.PROCESS_FORK, EventType.PROCESS_EXEC, EventType.PROCESS_EXIT,
    EventType.PROCESS_CGROUP_MIGRATE,
    EventType.SCHED_OFFCPU, EventType.SCHED_RUNQUEUE,
    EventType.FUTEX_WAIT, EventType.FUTEX_WAKE,
    EventType.BLOCK_ISSUE, EventType.BLOCK_COMPLETE, EventType.BLOCK_LATENCY,
})
EDGE_TYPES = frozenset({
    EventType.TCP_RETRANSMIT, EventType.TCP_RESET, EventType.TCP_CONNECT_FAIL,
    EventType.TCP_RTT, EventType.DNS_QUERY, EventType.DNS_RESPONSE,
})


@dataclass(frozen=True)
class KernelEvent:
    schema_version: int
    event_type: EventType
    event_class: EventClass
    quality: EventQuality
    timestamp_ns: int
    process_start_time_ns: int
    cgroup_id: int
    src_cgroup_id: int
    dst_cgroup_id: int
    value: int
    duration_ns: int
    attach_epoch: int
    event_sequence: int
    cpu: int
    pid: int
    tid: int
    src_ipv4: int
    dst_ipv4: int
    src_port: int
    dst_port: int
    protocol: int
    direction: int
    mapping_status: int
    comm: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType(self.event_type))
        object.__setattr__(self, "event_class", EventClass(self.event_class))
        object.__setattr__(self, "quality", EventQuality(self.quality))
        if self.schema_version != EVENT_SCHEMA_VERSION:
            raise ValueError(
                f"incompatible event schema {self.schema_version}; "
                f"expected {EVENT_SCHEMA_VERSION}"
            )
        numeric = (
            self.timestamp_ns, self.process_start_time_ns, self.cgroup_id,
            self.src_cgroup_id, self.dst_cgroup_id, self.value, self.duration_ns,
            self.attach_epoch, self.event_sequence, self.cpu, self.pid, self.tid,
            self.src_ipv4, self.dst_ipv4, self.src_port, self.dst_port,
            self.protocol, self.direction, self.mapping_status,
        )
        if any(isinstance(item, bool) or not isinstance(item, int) or item < 0
               for item in numeric):
            raise ValueError("kernel event numeric fields must be non-negative integers")
        expected = (
            EventClass.NODE if self.event_type in NODE_TYPES else
            EventClass.EDGE if self.event_type in EDGE_TYPES else
            EventClass.LOSS if self.event_type is EventType.LOSS else None
        )
        if expected is not None and self.event_class is not expected:
            raise ValueError("event type and class disagree")
        if not isinstance(self.comm, str) or len(self.comm.encode("utf-8")) > 15:
            raise ValueError("comm must be UTF-8 and fit TASK_COMM_LEN")

    @property
    def event_type_name(self) -> str:
        return EVENT_NAMES[self.event_type]

    @property
    def identity_payload(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "event_type": int(self.event_type),
            "event_class": int(self.event_class),
            "timestamp_ns": self.timestamp_ns,
            "process_start_time_ns": self.process_start_time_ns,
            "cgroup_id": self.cgroup_id,
            "src_cgroup_id": self.src_cgroup_id,
            "dst_cgroup_id": self.dst_cgroup_id,
            "attach_epoch": self.attach_epoch,
            "event_sequence": self.event_sequence,
            "cpu": self.cpu,
            "pid": self.pid,
            "tid": self.tid,
            "comm": self.comm,
        }

    @property
    def event_fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(
            self.identity_payload, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()

    @staticmethod
    def _ipv4(value: int) -> str | None:
        if value == 0:
            return None
        return socket.inet_ntoa(struct.pack("=I", value))

    @property
    def src_ip(self) -> str | None:
        return self._ipv4(self.src_ipv4)

    @property
    def dst_ip(self) -> str | None:
        return self._ipv4(self.dst_ipv4)

    @classmethod
    def from_loader_dict(cls, payload: dict) -> "KernelEvent":
        if payload.get("record_type") != "event":
            raise ValueError("loader payload is not an event")
        fields = {
            "schema_version": payload["schema_version"],
            "event_type": payload["event_type"],
            "event_class": payload["event_class"],
            "quality": payload["quality"],
            "timestamp_ns": payload["timestamp_ns"],
            "process_start_time_ns": payload["process_start_time_ns"],
            "cgroup_id": payload["cgroup_id"],
            "src_cgroup_id": payload["src_cgroup_id"],
            "dst_cgroup_id": payload["dst_cgroup_id"],
            "value": payload["value"],
            "duration_ns": payload["duration_ns"],
            "attach_epoch": payload["attach_epoch"],
            "event_sequence": payload["event_sequence"],
            "cpu": payload["cpu"],
            "pid": payload["pid"],
            "tid": payload["tid"],
            "src_ipv4": payload["src_ipv4"],
            "dst_ipv4": payload["dst_ipv4"],
            "src_port": payload["src_port"],
            "dst_port": payload["dst_port"],
            "protocol": payload["protocol"],
            "direction": payload["direction"],
            "mapping_status": payload["mapping_status"],
            "comm": payload["comm"],
        }
        return cls(**fields)

    def pack(self) -> bytes:
        comm = self.comm.encode("utf-8") + b"\0"
        return _EVENT_STRUCT.pack(
            self.schema_version, int(self.event_type), int(self.event_class),
            int(self.quality), EVENT_ABI_SIZE,
            self.timestamp_ns, self.process_start_time_ns, self.cgroup_id,
            self.src_cgroup_id, self.dst_cgroup_id, self.value, self.duration_ns,
            self.attach_epoch, self.event_sequence,
            self.cpu, self.pid, self.tid, self.src_ipv4, self.dst_ipv4,
            self.src_port, self.dst_port, self.protocol, self.direction,
            self.mapping_status, 0, comm.ljust(16, b"\0"),
        )

    @classmethod
    def unpack(cls, payload: bytes) -> "KernelEvent":
        if len(payload) != EVENT_ABI_SIZE:
            raise ValueError(f"invalid event ABI size {len(payload)}")
        values = list(_EVENT_STRUCT.unpack(payload))
        if values[4] != EVENT_ABI_SIZE:
            raise ValueError(f"event declares invalid ABI size {values[4]}")
        del values[4]
        del values[-2]
        values[-1] = values[-1].split(b"\0", 1)[0].decode("utf-8", "strict")
        return cls(*values)


__all__ = [
    "EDGE_TYPES", "EVENT_ABI_SIZE", "EVENT_NAMES", "EVENT_SCHEMA_VERSION",
    "EventClass", "EventQuality", "EventType", "KernelEvent", "NODE_TYPES",
]

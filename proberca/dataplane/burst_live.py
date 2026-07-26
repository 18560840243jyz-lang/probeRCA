"""Map independent final Burst kernel events into strict raw samples."""

from __future__ import annotations

import ipaddress
import json
import math
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from proberca.k8s.runtime_identity import runtime_identities

from .burst_archive import RawBurstWindow
from .burst_collection import RawBurstSample
from .contracts import fingerprint
from .raw import RawCollectionError


LIVE_BURST_CONFIG_SCHEMA_VERSION = "probeRCA-final-live-burst-v1"
INITIAL_LOG_TAIL_BYTES = 32 * 1024 * 1024

EVENT_SCHED_RUNQUEUE = 1
EVENT_RECLAIM_STALL = 2
EVENT_OOM_VICTIM = 3
EVENT_BLOCK_IO = 4
EVENT_FUTEX_WAIT = 5
EVENT_SOCKET_WAIT = 6
EVENT_SOCKET_BACKLOG = 7
EVENT_SOCKET_FAILURE = 8
EVENT_SOFTIRQ = 9
EVENT_NIC_DROP = 10
EVENT_NIC_ERROR = 11
EVENT_TCP_CONNECTION = 12
EVENT_TCP_RETRANSMIT = 13
EVENT_TCP_RTO = 14
EVENT_TCP_RTT = 15
EVENT_TCP_CONNECT_FAILURE = 16
EVENT_TCP_RST = 17
EVENT_DNS_QUERY = 18
EVENT_DNS_RESPONSE = 19
EVENT_DNS_TIMEOUT = 20

SERVICE_CHANNELS = (
    "sched.runqueue_wait_p95",
    "sched.wakeup_latency_p95",
    "memory.major_page_fault_rate",
    "memory.direct_reclaim_stall",
    "memory.oom_victim",
    "block.latency_p95",
    "block.queue_wait_p95",
    "futex.wait_count",
    "futex.wait_p95",
    "socket.queue_wait_p95",
    "socket.backlog_overflow",
    "socket.accept_connect_failure",
)
HOST_CHANNELS = (
    "host.sched.runqueue_wait_p95",
    "host.sched.wakeup_latency_p95",
    "host.memory.direct_reclaim_stall",
    "host.memory.oom_victim",
    "host.block.latency_p95",
    "host.block.queue_wait_p95",
    "nic.queue_drop_rate",
    "nic.error_rate",
    "nic.softirq_latency_p95",
)
TCP_CHANNELS = (
    "tcp.retrans_rate",
    "tcp.rto_rate",
    "tcp.rtt_p95",
    "tcp.connect_failure_rate",
    "tcp.rst_rate",
)
DNS_CHANNELS = (
    "dns.query_latency_p95",
    "dns.timeout_rate",
    "dns.rcode_failure_rate",
)
RARE_CHANNELS = frozenset({
    "memory.major_page_fault_rate",
    "memory.oom_victim",
    "futex.wait_count",
    "socket.backlog_overflow",
    "socket.accept_connect_failure",
    "host.memory.oom_victim",
    "nic.queue_drop_rate",
    "nic.error_rate",
    "tcp.retrans_rate",
    "tcp.rto_rate",
    "tcp.connect_failure_rate",
    "tcp.rst_rate",
    "dns.timeout_rate",
    "dns.rcode_failure_rate",
})


@dataclass(frozen=True)
class FinalLiveBurstConfig:
    schema_version: str
    cluster_id: str
    event_log_path: str
    cgroup_root: str
    network_class_path: str
    maximum_event_lag_sec: float
    expected_program_count: int
    sampling_profile: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FinalLiveBurstConfig":
        if not isinstance(payload, dict) \
                or set(payload) != set(cls.__dataclass_fields__):
            raise RawCollectionError("live Burst config fields mismatch")
        result = cls(**payload)
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != LIVE_BURST_CONFIG_SCHEMA_VERSION:
            raise RawCollectionError("unsupported live Burst config")
        if (
            not self.cluster_id
            or not self.event_log_path
            or not self.cgroup_root
            or not self.network_class_path
        ):
            raise RawCollectionError("live Burst paths/identity are required")
        if (
            isinstance(self.maximum_event_lag_sec, bool)
            or not isinstance(self.maximum_event_lag_sec, (int, float))
            or not 0 <= float(self.maximum_event_lag_sec) <= 5
        ):
            raise RawCollectionError("maximum_event_lag_sec is invalid")
        if (
            isinstance(self.expected_program_count, bool)
            or not isinstance(self.expected_program_count, int)
            or self.expected_program_count <= 0
        ):
            raise RawCollectionError("expected_program_count is invalid")
        if self.sampling_profile not in {"low", "full"}:
            raise RawCollectionError("sampling_profile is invalid")

    @property
    def public_fingerprint(self) -> str:
        self.validate()
        return fingerprint(asdict(self))


def load_final_live_burst_config(
    payload: dict[str, Any],
) -> FinalLiveBurstConfig:
    return FinalLiveBurstConfig.from_dict(payload)


def _p95(values: Iterable[float]) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def _ipv4(raw: Any) -> str | None:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        return None
    try:
        return str(ipaddress.IPv4Address(
            int(raw).to_bytes(4, byteorder="little", signed=False)
        ))
    except (ValueError, OverflowError):
        return None


def _read_counter(path: Path, key: str) -> int:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RawCollectionError(f"cannot read Burst counter {path}") from error
    values = {}
    for line in lines:
        fields = line.split()
        if len(fields) != 2 or fields[0] in values:
            raise RawCollectionError(f"invalid Burst counter file {path}")
        try:
            values[fields[0]] = int(fields[1])
        except ValueError as error:
            raise RawCollectionError(
                f"non-integer Burst counter in {path}"
            ) from error
    if key not in values or values[key] < 0:
        raise RawCollectionError(f"Burst counter {key} missing from {path}")
    return values[key]


class FinalLiveBurstSource:
    """Read the dedicated event log; never inspect alert or fault labels."""

    def __init__(
        self,
        config: FinalLiveBurstConfig,
        *,
        burst_config_fingerprint: str,
    ):
        config.validate()
        self.config = config
        self.burst_config_fingerprint = burst_config_fingerprint
        self.event_source_fingerprint = fingerprint({
            "implementation": "final-burst-ring-v1",
            "config": config.public_fingerprint,
        })
        self._offset: int | None = None
        self._pending_line = ""
        self._events: deque[dict[str, Any]] = deque()
        self._checkpoints: deque[dict[str, Any]] = deque()
        self._ready_records: deque[dict[str, Any]] = deque()
        self._memory_state: dict[int, tuple[int, int, int]] = {}
        self._nic_state: tuple[int, int, int] | None = None

    @property
    def source_record_ids(self) -> tuple[str, ...]:
        return ()

    def _read_log(self) -> None:
        path = Path(self.config.event_log_path)
        if not path.is_file():
            raise RawCollectionError("final Burst event log is unavailable")
        with path.open("r", encoding="utf-8") as handle:
            size = path.stat().st_size
            if self._offset is None:
                start = max(0, size - INITIAL_LOG_TAIL_BYTES)
                handle.seek(start)
                if start:
                    handle.readline()
            elif size < self._offset:
                raise RawCollectionError(
                    "final Burst event log was truncated"
                )
            else:
                handle.seek(self._offset)
            text = self._pending_line + handle.read()
            self._offset = handle.tell()
        lines = text.splitlines(keepends=True)
        self._pending_line = ""
        if lines and not lines[-1].endswith(("\n", "\r")):
            self._pending_line = lines.pop()
        for line in lines:
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise RawCollectionError("invalid final Burst JSONL") from error
            if record.get("schema_version") != 1:
                raise RawCollectionError("unsupported final Burst event schema")
            record_type = record.get("record_type")
            if record_type == "event":
                divisor = record.setdefault("sampling_divisor", 1)
                if (
                    isinstance(divisor, bool)
                    or not isinstance(divisor, int)
                    or not 1 <= divisor <= 1024
                ):
                    raise RawCollectionError(
                        "invalid final Burst sampling divisor"
                    )
                self._events.append(record)
            elif record_type == "checkpoint":
                self._checkpoints.append(record)
            elif record_type == "control" and record.get("state") == "ready":
                self._ready_records.append(record)
            else:
                raise RawCollectionError("unknown final Burst log record")

    def _identities(
        self, revision, monitored_services, edge_destinations,
    ):
        cgroup_root = Path(self.config.cgroup_root)
        cgroups = {}
        ip_services = {}
        node_names = set()
        service_entities = set()
        for identity in runtime_identities(revision):
            if (
                not identity.ready
                or not identity.started
                or not identity.full_container_id
                or not identity.service_ids
            ):
                continue
            container_id = identity.full_container_id.rsplit(
                "://", 1
            )[-1]
            if (
                len(container_id) != 64
                or any(character not in "0123456789abcdef"
                       for character in container_id.lower())
            ):
                raise RawCollectionError(
                    "Burst runtime container identity is invalid"
                )
            matches = list(cgroup_root.rglob(
                f"cri-containerd-{container_id}.scope"
            ))
            if len(matches) != 1:
                raise RawCollectionError(
                    "Burst cgroup identity is missing or ambiguous"
                )
            services = []
            for service_id in identity.service_ids:
                _, namespace, service = service_id.split("::", 2)
                if (namespace, service) not in monitored_services:
                    continue
                entity = (
                    f"{revision.cluster_id}::{namespace}::{service}"
                )
                services.append({
                    "namespace": namespace,
                    "service": service,
                    "entity": entity,
                })
                service_entities.add((namespace, service, entity))
            if not services:
                continue
            cgroup_id = matches[0].stat().st_ino
            if cgroup_id in cgroups:
                raise RawCollectionError("Burst cgroup identity is duplicated")
            cgroups[cgroup_id] = {
                "services": tuple(services),
                "node": identity.node_name,
                "path": matches[0],
            }
            if identity.node_name:
                node_names.add(identity.node_name)
            for address in identity.pod_ips:
                ip_services.setdefault(address, set()).update(
                    (item["namespace"], item["service"])
                    for item in services
                )
        for raw in revision.objects_by_kind.get("Service", {}).values():
            metadata = raw.get("metadata") or {}
            spec = raw.get("spec") or {}
            address = spec.get("clusterIP")
            key = (metadata.get("namespace"), metadata.get("name"))
            if (
                address
                and address != "None"
                and all(key)
                and key in monitored_services | edge_destinations
            ):
                ip_services.setdefault(address, set()).add(key)
        covered_services = {
            (namespace, service)
            for namespace, service, _entity in service_entities
        }
        if (
            len(node_names) != 1
            or covered_services != monitored_services
        ):
            raise RawCollectionError(
                "single-VM Burst identity coverage is incomplete"
            )
        return cgroups, ip_services, next(iter(node_names)), service_entities

    def _loss(self, start_ns: int, end_ns: int) -> tuple[float, float]:
        before = [
            record for record in self._checkpoints
            if record["timestamp_ns"] <= start_ns
            and record.get("program_count")
            == self.config.expected_program_count
            and record.get("sampling_profile")
            == self.config.sampling_profile
        ]
        after = [
            record for record in self._checkpoints
            if record["timestamp_ns"] >= end_ns
            and record.get("program_count")
            == self.config.expected_program_count
            and record.get("sampling_profile")
            == self.config.sampling_profile
        ]
        if not before or not after:
            return 0.0, 1.0
        left = before[-1]
        right = after[0]
        if (
            right["timestamp_ns"] - end_ns
            > int(self.config.maximum_event_lag_sec * 1_000_000_000)
        ):
            return 0.0, 1.0
        emitted = right["emitted"] - left["emitted"]
        failed = right["reserve_failed"] - left["reserve_failed"]
        if emitted < 0 or failed < 0:
            raise RawCollectionError("Burst loss counters decreased")
        total = emitted + failed
        return 1.0, (failed / total if total else 0.0)

    def _memory_deltas(self, cgroups) -> dict[int, tuple[int, int]]:
        output = {}
        current = {}
        for cgroup_id, identity in cgroups.items():
            path = identity["path"]
            pgfault = _read_counter(path / "memory.stat", "pgfault")
            pgmajfault = _read_counter(path / "memory.stat", "pgmajfault")
            oom_kill = _read_counter(path / "memory.events", "oom_kill")
            current[cgroup_id] = (pgfault, pgmajfault, oom_kill)
            previous = self._memory_state.get(cgroup_id)
            if previous is None:
                output[cgroup_id] = (0, 0)
                continue
            if any(now < old for now, old in zip(current[cgroup_id], previous)):
                raise RawCollectionError("Burst memory counter decreased")
            output[cgroup_id] = (
                pgmajfault - previous[1],
                pgfault - previous[0],
            )
        self._memory_state = current
        return output

    def _nic_delta(self) -> tuple[int, int, int]:
        packets = drops = errors = 0
        root = Path(self.config.network_class_path)
        for interface in root.iterdir():
            if interface.name == "lo":
                continue
            statistics = interface / "statistics"
            packets += sum(
                int((statistics / name).read_text(encoding="ascii"))
                for name in ("rx_packets", "tx_packets")
            )
            drops += sum(
                int((statistics / name).read_text(encoding="ascii"))
                for name in ("rx_dropped", "tx_dropped")
            )
            errors += sum(
                int((statistics / name).read_text(encoding="ascii"))
                for name in ("rx_errors", "tx_errors")
            )
        current = (packets, drops, errors)
        previous = self._nic_state
        self._nic_state = current
        if previous is None:
            return (0, 0, 0)
        if any(now < old for now, old in zip(current, previous)):
            raise RawCollectionError("Burst NIC counter decreased")
        return tuple(now - old for now, old in zip(current, previous))

    def _sample(
        self,
        *,
        timestamp_ns: int,
        namespace: str,
        entity_type: str,
        entity_id: str,
        channel_id: str,
        value: float,
        exposure: float | None,
        coverage: float,
        event_loss_rate: float,
        mapping_quality: float,
    ) -> RawBurstSample:
        return RawBurstSample.create(
            source_object_id="object:" + fingerprint({
                "event_source_fingerprint": self.event_source_fingerprint,
                "channel_id": channel_id,
            }),
            timestamp_ns=timestamp_ns,
            cluster_id=self.config.cluster_id,
            namespace=namespace,
            entity_type=entity_type,
            entity_id=entity_id,
            channel_id=channel_id,
            value=value,
            exposure=exposure,
            coverage=coverage,
            event_loss_rate=event_loss_rate,
            mapping_quality=mapping_quality,
        )

    def collect_window(
        self,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision,
        normal_raw_window,
    ) -> RawBurstWindow:
        self._read_log()
        monitored_services = {
            (sample.namespace, sample.service_name)
            for sample in normal_raw_window.samples
            if sample.entity_type == "service"
        }
        if not monitored_services or any(
            not namespace or not service
            for namespace, service in monitored_services
        ):
            raise RawCollectionError(
                "normal service identity is incomplete for Burst"
            )
        edge_destinations = {
            (
                getattr(sample, "dst_namespace", None)
                or sample.namespace,
                sample.dst_service,
            )
            for sample in normal_raw_window.samples
            if sample.entity_type == "edge"
        }
        cgroups, ip_services, node, service_entities = self._identities(
            inventory_revision, monitored_services, edge_destinations
        )
        coverage, event_loss = self._loss(
            window_start_ns, window_end_ns
        )
        selected = [
            record for record in self._events
            if window_start_ns <= record["timestamp_ns"] < window_end_ns
        ]
        while self._events and self._events[0]["timestamp_ns"] < window_start_ns:
            self._events.popleft()
        timestamp_ns = window_end_ns - 1
        by_service = defaultdict(lambda: defaultdict(list))
        by_host = defaultdict(list)
        tcp = defaultdict(lambda: defaultdict(list))
        dns = defaultdict(lambda: defaultdict(list))
        known_edges = {"tcp": set(), "dns": set()}
        for sample in normal_raw_window.samples:
            if sample.entity_type != "edge" or sample.protocol not in known_edges:
                continue
            if (
                not sample.namespace
                or not sample.src_service
                or not sample.dst_service
            ):
                raise RawCollectionError(
                    "normal edge identity is incomplete for Burst"
                )
            entity = (
                f"{self.config.cluster_id}::{sample.namespace}::"
                f"{sample.src_service}->{sample.dst_service}::"
                f"{sample.protocol}"
            )
            known_edges[sample.protocol].add(entity)
            target = tcp if sample.protocol == "tcp" else dns
            target[entity]["namespace"] = [sample.namespace]
        socket_exposure = defaultdict(int)
        tcp_exposure = defaultdict(int)
        dns_exposure = defaultdict(int)
        mapped = defaultdict(int)
        total = defaultdict(int)

        def edge_for(record, protocol):
            source = cgroups.get(int(record["cgroup_id"]))
            destinations = ip_services.get(
                _ipv4(record["dst_ipv4"]), set()
            )
            if source is None or not destinations:
                return None
            total[protocol] += 1
            matches = set()
            for caller in source["services"]:
                for _dst_namespace, dst_service in destinations:
                    entity = (
                        f"{self.config.cluster_id}::"
                        f"{caller['namespace']}::"
                        f"{caller['service']}->{dst_service}::{protocol}"
                    )
                    if entity in known_edges[protocol]:
                        matches.add((caller["namespace"], entity))
            if len(matches) != 1:
                return None
            mapped[protocol] += 1
            return next(iter(matches))

        for record in selected:
            event_type = int(record["event_type"])
            identity = cgroups.get(int(record["cgroup_id"]))
            if event_type in {
                EVENT_SCHED_RUNQUEUE,
                EVENT_RECLAIM_STALL,
                EVENT_OOM_VICTIM,
                EVENT_BLOCK_IO,
                EVENT_FUTEX_WAIT,
                EVENT_SOCKET_WAIT,
                EVENT_SOCKET_BACKLOG,
                EVENT_SOCKET_FAILURE,
            } and identity is not None:
                services = tuple(
                    item["entity"] for item in identity["services"]
                )
                mapped["service"] += 1
                total["service"] += 1
                if event_type == EVENT_SCHED_RUNQUEUE:
                    for service in services:
                        by_service[service]["sched"].append(
                            record["duration_ns"]
                        )
                    by_host["sched"].append(record["duration_ns"])
                elif event_type == EVENT_RECLAIM_STALL:
                    for service in services:
                        by_service[service]["reclaim"].append(
                            record["duration_ns"]
                        )
                    by_host["reclaim"].append(record["duration_ns"])
                elif event_type == EVENT_OOM_VICTIM:
                    for service in services:
                        by_service[service]["oom"].append(1)
                    by_host["oom"].append(1)
                elif event_type == EVENT_BLOCK_IO:
                    for service in services:
                        by_service[service]["block"].append(
                            record["duration_ns"]
                        )
                        by_service[service]["block_queue"].append(
                            record["auxiliary_ns"]
                        )
                    by_host["block"].append(record["duration_ns"])
                    by_host["block_queue"].append(record["auxiliary_ns"])
                elif event_type == EVENT_FUTEX_WAIT:
                    for service in services:
                        by_service[service]["futex"].append(
                            (
                                record["duration_ns"],
                                record["sampling_divisor"],
                            )
                        )
                elif event_type == EVENT_SOCKET_WAIT:
                    for service in services:
                        by_service[service]["socket_wait"].append(
                            (
                                record["duration_ns"],
                                record["sampling_divisor"],
                            )
                        )
                        socket_exposure[service] += (
                            record["sampling_divisor"]
                        )
                elif event_type == EVENT_SOCKET_BACKLOG:
                    for service in services:
                        by_service[service]["socket_backlog"].append(1)
                elif event_type == EVENT_SOCKET_FAILURE:
                    for service in services:
                        by_service[service]["socket_failure"].append(1)
                continue
            if event_type == EVENT_SOFTIRQ:
                by_host["softirq"].append(record["duration_ns"])
                continue
            if event_type == EVENT_NIC_DROP:
                by_host["nic_drop"].append(1)
                continue
            if event_type == EVENT_NIC_ERROR:
                by_host["nic_error"].append(1)
                continue
            if event_type in {
                EVENT_TCP_CONNECTION,
                EVENT_TCP_RETRANSMIT,
                EVENT_TCP_RTO,
                EVENT_TCP_RTT,
                EVENT_TCP_CONNECT_FAILURE,
                EVENT_TCP_RST,
            }:
                edge = edge_for(record, "tcp")
                if edge is None:
                    continue
                namespace, entity = edge
                tcp[entity]["namespace"] = [namespace]
                if event_type == EVENT_TCP_CONNECTION:
                    tcp_exposure[entity] += record["sampling_divisor"]
                elif event_type == EVENT_TCP_RTT:
                    tcp[entity]["rtt"].append(record["duration_ns"])
                elif event_type == EVENT_TCP_RETRANSMIT:
                    tcp[entity]["retrans"].append(1)
                elif event_type == EVENT_TCP_RTO:
                    tcp[entity]["rto"].append(1)
                elif event_type == EVENT_TCP_CONNECT_FAILURE:
                    tcp[entity]["connect_failure"].append(1)
                elif event_type == EVENT_TCP_RST:
                    tcp[entity]["rst"].append(1)
                continue
            if event_type in {
                EVENT_DNS_QUERY,
                EVENT_DNS_RESPONSE,
                EVENT_DNS_TIMEOUT,
            }:
                edge = edge_for(record, "dns")
                if edge is None:
                    continue
                namespace, entity = edge
                dns[entity]["namespace"] = [namespace]
                if event_type == EVENT_DNS_QUERY:
                    dns_exposure[entity] += record["sampling_divisor"]
                elif event_type == EVENT_DNS_RESPONSE:
                    if int(record["rcode"]) == 0:
                        dns[entity]["latency"].append(
                            record["duration_ns"]
                        )
                    else:
                        dns[entity]["rcode"].append(1)
                elif event_type == EVENT_DNS_TIMEOUT:
                    dns[entity]["timeout"].append(1)
                continue
        memory = self._memory_deltas(cgroups)
        packet_delta, sysfs_drop_delta, sysfs_error_delta = self._nic_delta()
        by_host["nic_drop"].extend([1] * sysfs_drop_delta)
        by_host["nic_error"].extend([1] * sysfs_error_delta)
        samples = []
        service_mapping = (
            mapped["service"] / total["service"] if total["service"] else 1.0
        )

        def continuous(
            namespace, entity_type, entity, channel, values, mapping=1.0,
        ):
            value = _p95(
                item[0] if isinstance(item, tuple) else item
                for item in values
            )
            samples.append(self._sample(
                timestamp_ns=timestamp_ns,
                namespace=namespace,
                entity_type=entity_type,
                entity_id=entity,
                channel_id=channel,
                value=0.0 if value is None else value,
                exposure=None,
                coverage=coverage if value is not None else 0.0,
                event_loss_rate=event_loss,
                mapping_quality=mapping,
            ))

        def rare(
            namespace, entity_type, entity, channel, count, exposure,
            mapping=1.0,
        ):
            samples.append(self._sample(
                timestamp_ns=timestamp_ns,
                namespace=namespace,
                entity_type=entity_type,
                entity_id=entity,
                channel_id=channel,
                value=int(count),
                exposure=float(exposure),
                coverage=coverage,
                event_loss_rate=event_loss,
                mapping_quality=mapping,
            ))

        entity_by_id = {
            entity: (namespace, service)
            for namespace, service, entity in service_entities
        }
        cgroups_by_entity = defaultdict(list)
        for cgroup_id, value in cgroups.items():
            for service in value["services"]:
                cgroups_by_entity[service["entity"]].append(cgroup_id)
        for entity in sorted(entity_by_id):
            namespace, _service = entity_by_id[entity]
            values = by_service[entity]
            continuous(
                namespace, "service", entity,
                "sched.runqueue_wait_p95", values["sched"],
                service_mapping,
            )
            continuous(
                namespace, "service", entity,
                "sched.wakeup_latency_p95", values["sched"],
                service_mapping,
            )
            major = sum(
                memory[cgroup_id][0]
                for cgroup_id in cgroups_by_entity[entity]
            )
            faults = sum(
                memory[cgroup_id][1]
                for cgroup_id in cgroups_by_entity[entity]
            )
            rare(
                namespace, "service", entity,
                "memory.major_page_fault_rate", major, faults,
            )
            continuous(
                namespace, "service", entity,
                "memory.direct_reclaim_stall", values["reclaim"],
                service_mapping,
            )
            rare(
                namespace, "service", entity,
                "memory.oom_victim", len(values["oom"]), 1.0,
                service_mapping,
            )
            continuous(
                namespace, "service", entity,
                "block.latency_p95", values["block"], service_mapping,
            )
            continuous(
                namespace, "service", entity,
                "block.queue_wait_p95", values["block_queue"],
                service_mapping,
            )
            rare(
                namespace, "service", entity,
                "futex.wait_count",
                sum(item[1] for item in values["futex"]),
                1.0,
                service_mapping,
            )
            continuous(
                namespace, "service", entity,
                "futex.wait_p95", values["futex"], service_mapping,
            )
            continuous(
                namespace, "service", entity,
                "socket.queue_wait_p95", values["socket_wait"],
                service_mapping,
            )
            rare(
                namespace, "service", entity,
                "socket.backlog_overflow",
                len(values["socket_backlog"]),
                socket_exposure[entity],
                service_mapping,
            )
            rare(
                namespace, "service", entity,
                "socket.accept_connect_failure",
                len(values["socket_failure"]),
                socket_exposure[entity],
                service_mapping,
            )

        host_entity = f"{self.config.cluster_id}::host::{node}"
        continuous(
            "host", "host", host_entity,
            "host.sched.runqueue_wait_p95", by_host["sched"],
        )
        continuous(
            "host", "host", host_entity,
            "host.sched.wakeup_latency_p95", by_host["sched"],
        )
        continuous(
            "host", "host", host_entity,
            "host.memory.direct_reclaim_stall", by_host["reclaim"],
        )
        rare(
            "host", "host", host_entity,
            "host.memory.oom_victim", len(by_host["oom"]), 1.0,
        )
        continuous(
            "host", "host", host_entity,
            "host.block.latency_p95", by_host["block"],
        )
        continuous(
            "host", "host", host_entity,
            "host.block.queue_wait_p95", by_host["block_queue"],
        )
        rare(
            "host", "host", host_entity,
            "nic.queue_drop_rate", len(by_host["nic_drop"]), packet_delta,
        )
        rare(
            "host", "host", host_entity,
            "nic.error_rate", len(by_host["nic_error"]), packet_delta,
        )
        continuous(
            "host", "host", host_entity,
            "nic.softirq_latency_p95", by_host["softirq"],
        )

        tcp_mapping = mapped["tcp"] / total["tcp"] if total["tcp"] else 1.0
        for entity, values in sorted(tcp.items()):
            namespace = values["namespace"][0]
            exposure = tcp_exposure[entity]
            rare(
                namespace, "edge", entity,
                "tcp.retrans_rate", len(values["retrans"]),
                exposure, tcp_mapping,
            )
            rare(
                namespace, "edge", entity,
                "tcp.rto_rate", len(values["rto"]),
                exposure, tcp_mapping,
            )
            continuous(
                namespace, "edge", entity,
                "tcp.rtt_p95", values["rtt"], tcp_mapping,
            )
            rare(
                namespace, "edge", entity,
                "tcp.connect_failure_rate",
                len(values["connect_failure"]), exposure, tcp_mapping,
            )
            rare(
                namespace, "edge", entity,
                "tcp.rst_rate", len(values["rst"]),
                exposure, tcp_mapping,
            )
        dns_mapping = mapped["dns"] / total["dns"] if total["dns"] else 1.0
        for entity, values in sorted(dns.items()):
            namespace = values["namespace"][0]
            exposure = dns_exposure[entity]
            continuous(
                namespace, "edge", entity,
                "dns.query_latency_p95", values["latency"], dns_mapping,
            )
            rare(
                namespace, "edge", entity,
                "dns.timeout_rate", len(values["timeout"]),
                exposure, dns_mapping,
            )
            rare(
                namespace, "edge", entity,
                "dns.rcode_failure_rate", len(values["rcode"]),
                exposure, dns_mapping,
            )
        return RawBurstWindow.create(
            sequence=sequence,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            cluster_id=self.config.cluster_id,
            samples=samples,
            event_source_fingerprint=self.event_source_fingerprint,
            burst_config_fingerprint=self.burst_config_fingerprint,
            event_loss_rate=event_loss,
        )

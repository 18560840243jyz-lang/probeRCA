from __future__ import annotations

import ipaddress
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from proberca.dataplane.burst_archive import (
    BurstArchive,
    BurstArchiveWriter,
)
from proberca.dataplane.burst_live import (
    DNS_CHANNELS,
    HOST_CHANNELS,
    SERVICE_CHANNELS,
    TCP_CHANNELS,
    FinalLiveBurstConfig,
    FinalLiveBurstSource,
)
from proberca.dataplane.contracts import fingerprint
from proberca.dataplane.raw import RawCollectionError


START = 1_000_000_000
END = 2_000_000_000
CONTAINER_ID = "a" * 64


def _ip(value: str) -> int:
    return int.from_bytes(
        ipaddress.IPv4Address(value).packed,
        byteorder="little",
    )


def _event(event_type: int, cgroup_id: int, **values):
    return {
        "record_type": "event",
        "schema_version": 1,
        "timestamp_ns": values.pop("timestamp_ns", START + 500_000_000),
        "monotonic_ns": values.pop("monotonic_ns", 500_000_000),
        "event_type": event_type,
        "cgroup_id": cgroup_id,
        "value": values.pop("value", 1),
        "duration_ns": values.pop("duration_ns", 1000),
        "auxiliary_ns": values.pop("auxiliary_ns", 100),
        "sequence": values.pop("sequence", event_type),
        "cpu": values.pop("cpu", 0),
        "src_ipv4": values.pop("src_ipv4", _ip("10.0.0.2")),
        "dst_ipv4": values.pop("dst_ipv4", 0),
        "device": values.pop("device", 0),
        "src_port": values.pop("src_port", 12345),
        "dst_port": values.pop("dst_port", 0),
        "protocol": values.pop("protocol", 0),
        "direction": values.pop("direction", 1),
        "transaction_id": values.pop("transaction_id", 0),
        "rcode": values.pop("rcode", 0),
        "sampling_divisor": values.pop("sampling_divisor", 1),
        **values,
    }


def _filesystem(tmp_path: Path):
    cgroup = (
        tmp_path / "cgroup"
        / f"cri-containerd-{CONTAINER_ID}.scope"
    )
    cgroup.mkdir(parents=True)
    (cgroup / "memory.stat").write_text(
        "pgfault 10\npgmajfault 2\n", encoding="utf-8"
    )
    (cgroup / "memory.events").write_text(
        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
        encoding="utf-8",
    )
    statistics = tmp_path / "net" / "eth0" / "statistics"
    statistics.mkdir(parents=True)
    for name, value in {
        "rx_packets": 100,
        "tx_packets": 200,
        "rx_dropped": 0,
        "tx_dropped": 0,
        "rx_errors": 0,
        "tx_errors": 0,
    }.items():
        (statistics / name).write_text(str(value), encoding="ascii")
    return cgroup


def _inventory():
    return SimpleNamespace(
        cluster_id="cluster",
        revision_id="revision-1",
        objects_by_kind={
            "Service": {
                "dst": {
                    "metadata": {"namespace": "ns", "name": "dst"},
                    "spec": {"clusterIP": "10.96.0.20"},
                },
                "dns": {
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns",
                    },
                    "spec": {"clusterIP": "10.96.0.10"},
                },
            },
        },
    )


def _identity():
    return SimpleNamespace(
        ready=True,
        started=True,
        full_container_id=CONTAINER_ID,
        service_ids=("cluster::ns::svc",),
        node_name="node-a",
        pod_ips=("10.0.0.2",),
    )


def _normal_raw_window():
    return SimpleNamespace(samples=(
        SimpleNamespace(
            entity_type="service",
            namespace="ns",
            service_name="svc",
        ),
        SimpleNamespace(
            entity_type="edge",
            protocol="tcp",
            namespace="ns",
            src_service="svc",
            dst_service="dst",
        ),
        SimpleNamespace(
            entity_type="edge",
            protocol="dns",
            namespace="ns",
            src_service="svc",
            dst_service="kube-dns",
            dst_namespace="kube-system",
        ),
    ))


def _config(tmp_path: Path) -> FinalLiveBurstConfig:
    return FinalLiveBurstConfig(
        schema_version="probeRCA-final-live-burst-v2",
        cluster_id="cluster",
        event_log_path=str(tmp_path / "events.jsonl"),
        cgroup_root=str(tmp_path / "cgroup"),
        network_class_path=str(tmp_path / "net"),
        maximum_event_lag_sec=0.5,
        expected_program_count=31,
        sampling_profile="low",
        max_buffered_event_records=250_000,
    )


def test_live_burst_maps_all_29_frozen_channels(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    cgroup_id = cgroup.stat().st_ino
    records = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 100,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 10,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
        _event(1, cgroup_id, duration_ns=100),
        _event(1, cgroup_id + 999_999, duration_ns=999),
        _event(2, cgroup_id, duration_ns=200),
        _event(3, cgroup_id),
        _event(4, cgroup_id, duration_ns=300, auxiliary_ns=50),
        _event(5, cgroup_id, duration_ns=400, sampling_divisor=8),
        _event(6, cgroup_id, duration_ns=500, sampling_divisor=4),
        _event(7, cgroup_id),
        _event(8, cgroup_id),
        _event(9, 0, duration_ns=600),
        _event(10, 0),
        _event(11, 0),
        _event(
            12, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6, sampling_divisor=16,
        ),
        _event(
            13, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6,
        ),
        _event(
            14, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6,
        ),
        _event(
            15, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6, duration_ns=700,
        ),
        _event(
            16, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6,
        ),
        _event(
            17, cgroup_id, dst_ipv4=_ip("10.96.0.20"),
            dst_port=8080, protocol=6,
        ),
        _event(
            18, cgroup_id, dst_ipv4=_ip("10.96.0.10"),
            dst_port=53, protocol=17, sampling_divisor=16,
        ),
        _event(
            19, cgroup_id, dst_ipv4=_ip("10.96.0.10"),
            dst_port=53, protocol=17, duration_ns=700, rcode=0,
            sampling_divisor=16,
        ),
        _event(
            19, cgroup_id, dst_ipv4=_ip("10.96.0.10"),
            dst_port=53, protocol=17, duration_ns=800, rcode=2,
        ),
        _event(
            20, cgroup_id, dst_ipv4=_ip("10.96.0.10"),
            dst_port=53, protocol=17,
        ),
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1_000_000_000,
            "emitted": 100,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    Path(_config(tmp_path).event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    burst_fingerprint = fingerprint({"burst": "contract"})
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=burst_fingerprint,
    )
    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=_inventory(),
        normal_raw_window=_normal_raw_window(),
    )

    channels = {sample.channel_id for sample in window.samples}
    assert channels == set(
        SERVICE_CHANNELS + HOST_CHANNELS + TCP_CHANNELS + DNS_CHANNELS
    )
    assert len(window.samples) == 29
    assert window.event_loss_rate == 0
    assert all(sample.source_record_id.startswith("source:") for sample in window.samples)
    assert all(sample.mapping_quality == 1 for sample in window.samples)
    samples = {sample.channel_id: sample for sample in window.samples}
    assert samples["futex.wait_count"].value == 8
    assert samples["socket.backlog_overflow"].exposure == 4
    assert samples["tcp.retrans_rate"].exposure == 16
    assert samples["dns.timeout_rate"].exposure == 16
    assert samples["dns.query_latency_p95"].value == 700


def test_continuous_counters_use_exact_captured_window_boundaries(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    records = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 1,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1_000_000_000,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    Path(_config(tmp_path).event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    source.begin_capture(_inventory())
    source.capture_boundary(START, _inventory())

    (cgroup / "memory.stat").write_text(
        "pgfault 15\npgmajfault 4\n", encoding="utf-8"
    )
    statistics = tmp_path / "net" / "eth0" / "statistics"
    for name, value in {
        "rx_packets": 110,
        "tx_packets": 205,
        "rx_dropped": 2,
    }.items():
        (statistics / name).write_text(str(value), encoding="ascii")
    source.capture_boundary(END, _inventory())

    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=_inventory(),
        normal_raw_window=_normal_raw_window(),
    )
    samples = {
        (sample.entity_type, sample.channel_id): sample
        for sample in window.samples
    }
    memory = samples[
        ("service", "memory.major_page_fault_rate")
    ]
    assert memory.value == 2
    assert memory.exposure == 5
    nic = samples[("host", "nic.queue_drop_rate")]
    assert nic.value == 2
    assert nic.exposure == 15


def test_boundary_defers_log_read_and_collects_all_timestamped_events(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    cgroup_id = cgroup.stat().st_ino
    event_path = Path(_config(tmp_path).event_log_path)
    initial = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 1,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    event_path.write_text(
        "".join(json.dumps(item) + "\n" for item in initial),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    inventory = _inventory()
    source.begin_capture(inventory)
    later = [
        _event(5, cgroup_id),
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1_000_000_000,
            "emitted": 1,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write("".join(json.dumps(item) + "\n" for item in later))
    original_read_log_until = source._read_log_until
    read_calls = []

    def counted_read_log_until(through_ns):
        read_calls.append(through_ns)
        return original_read_log_until(through_ns)

    monkeypatch.setattr(
        source, "_read_log_until", counted_read_log_until,
    )
    source.capture_boundary(START, inventory)
    source.capture_boundary(END, inventory)
    assert read_calls == []
    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=inventory,
        normal_raw_window=_normal_raw_window(),
    )
    assert read_calls == [END]
    futex = next(
        sample for sample in window.samples
        if sample.channel_id == "futex.wait_count"
    )
    assert futex.value == 1
    assert window.event_loss_rate == 0


def test_event_log_is_consumed_one_window_at_a_time(tmp_path, monkeypatch):
    cgroup = _filesystem(tmp_path)
    cgroup_id = cgroup.stat().st_ino
    event_path = Path(_config(tmp_path).event_log_path)
    event_path.write_text(
        "".join(json.dumps(item) + "\n" for item in (
            {
                "record_type": "checkpoint",
                "schema_version": 1,
                "timestamp_ns": START,
                "monotonic_ns": 0,
                "emitted": 0,
                "reserve_failed": 0,
                "program_count": 31,
                "sampling_profile": "low",
            },
        )),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    inventory = _inventory()
    source.begin_capture(inventory)
    records = []
    for index in range(3):
        start = START + index * 1_000_000_000
        end = start + 1_000_000_000
        records.extend((
            _event(
                5,
                cgroup_id,
                timestamp_ns=start + 500_000_000,
                sequence=index + 1,
            ),
            {
                "record_type": "checkpoint",
                "schema_version": 1,
                "timestamp_ns": end,
                "monotonic_ns": end - START,
                "emitted": index + 1,
                "reserve_failed": 0,
                "program_count": 31,
                "sampling_profile": "low",
            },
        ))
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write("".join(json.dumps(item) + "\n" for item in records))
    file_size = event_path.stat().st_size

    for index in range(4):
        source.capture_boundary(
            START + index * 1_000_000_000,
            inventory,
        )

    for index in range(3):
        start = START + index * 1_000_000_000
        end = start + 1_000_000_000
        window = source.collect_window(
            sequence=index + 1,
            window_start_ns=start,
            window_end_ns=end,
            inventory_revision=inventory,
            normal_raw_window=_normal_raw_window(),
        )
        futex = next(
            sample for sample in window.samples
            if sample.channel_id == "futex.wait_count"
        )
        assert futex.value == 1
        assert len(source._events) == 0
        assert len(source._checkpoints) <= 2
        if index < 2:
            assert source._offset < file_size
        else:
            assert source._offset == file_size


def test_event_log_buffer_limit_fails_closed(tmp_path, monkeypatch):
    cgroup = _filesystem(tmp_path)
    cgroup_id = cgroup.stat().st_ino
    config = replace(_config(tmp_path), max_buffered_event_records=1)
    Path(config.event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in (
            _event(5, cgroup_id, sequence=1),
            _event(5, cgroup_id, sequence=2),
            {
                "record_type": "checkpoint",
                "schema_version": 1,
                "timestamp_ns": END,
                "monotonic_ns": 1_000_000_000,
                "emitted": 2,
                "reserve_failed": 0,
                "program_count": 31,
                "sampling_profile": "low",
            },
        )),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        config,
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    with pytest.raises(
        RawCollectionError,
        match="event buffer exceeded its configured limit",
    ):
        source.collect_window(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            inventory_revision=_inventory(),
            normal_raw_window=_normal_raw_window(),
        )


def test_event_log_transport_stays_bounded_for_1600_windows(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    cgroup_id = cgroup.stat().st_ino
    event_path = Path(_config(tmp_path).event_log_path)
    event_path.write_text(
        json.dumps({
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        }) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    source.begin_capture(_inventory())
    records = []
    for index in range(1600):
        start = START + index * 1_000_000_000
        end = start + 1_000_000_000
        records.extend((
            _event(
                5,
                cgroup_id,
                timestamp_ns=start + 500_000_000,
                sequence=index + 1,
            ),
            {
                "record_type": "checkpoint",
                "schema_version": 1,
                "timestamp_ns": end,
                "monotonic_ns": end - START,
                "emitted": index + 1,
                "reserve_failed": 0,
                "program_count": 31,
                "sampling_profile": "low",
            },
        ))
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write("".join(json.dumps(item) + "\n" for item in records))
    file_size = event_path.stat().st_size

    for index in range(1600):
        end = START + (index + 1) * 1_000_000_000
        source._read_log_until(end)
        assert len(source._events) == 1
        assert len(source._checkpoints) <= 2
        source._prune_log_state(end)
        assert len(source._events) == 0
        assert len(source._checkpoints) <= 2

    assert source._offset == file_size


def test_memory_totals_reads_each_counter_file_once(tmp_path, monkeypatch):
    cgroup = _filesystem(tmp_path)
    original_read_text = Path.read_text
    reads = []

    def counted_read_text(path, *args, **kwargs):
        if path.name in {"memory.stat", "memory.events"}:
            reads.append(path.name)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted_read_text)
    assert FinalLiveBurstSource._memory_totals(cgroup) == (10, 2, 0)
    assert reads == ["memory.stat", "memory.events"]


def test_boundary_reuses_frozen_cgroup_and_nic_paths(tmp_path, monkeypatch):
    _filesystem(tmp_path)
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    event_path = Path(_config(tmp_path).event_log_path)
    event_path.write_text("", encoding="utf-8")
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    inventory = _inventory()
    source.begin_capture(inventory)
    monkeypatch.setattr(
        source,
        "_refresh_cgroup_paths",
        lambda _root: pytest.fail("boundary rescanned cgroup directories"),
    )
    original_iterdir = Path.iterdir

    def guarded_iterdir(path):
        if path == Path(source.config.network_class_path):
            pytest.fail("boundary rescanned NIC directories")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", guarded_iterdir)
    source.capture_boundary(START, inventory)
    source.capture_boundary(END, inventory)


def test_boundary_rejects_revision_or_cgroup_identity_change(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    Path(_config(tmp_path).event_log_path).write_text("", encoding="utf-8")
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    inventory = _inventory()
    source.begin_capture(inventory)
    changed = _inventory()
    changed.revision_id = "revision-2"
    with pytest.raises(RawCollectionError, match="runtime identity changed"):
        source.capture_boundary(START, changed)

    replacement = cgroup.with_name(cgroup.name + ".old")
    cgroup.rename(replacement)
    cgroup.mkdir()
    with pytest.raises(RawCollectionError, match="cgroup identity changed"):
        source.capture_boundary(START, inventory)


def test_tcp_kernel_reverse_events_use_frozen_call_direction(
    tmp_path, monkeypatch,
):
    source_cgroup = _filesystem(tmp_path)
    destination_container_id = "b" * 64
    destination_cgroup = (
        tmp_path / "cgroup"
        / f"cri-containerd-{destination_container_id}.scope"
    )
    destination_cgroup.mkdir()
    (destination_cgroup / "memory.stat").write_text(
        "pgfault 10\npgmajfault 2\n", encoding="utf-8"
    )
    (destination_cgroup / "memory.events").write_text(
        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
        encoding="utf-8",
    )
    records = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 1,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
        # The server-side RTO is emitted with the kernel packet direction,
        # but belongs to the frozen caller->callee edge svc->dst.
        _event(
            14, destination_cgroup.stat().st_ino,
            src_ipv4=_ip("10.0.0.3"),
            dst_ipv4=_ip("10.0.0.2"),
            src_port=8080,
            dst_port=12345,
            protocol=6,
        ),
        # Both endpoints are mapped services, but this self-pair is not a
        # candidate in the frozen topology and must not dilute quality.
        _event(
            12, source_cgroup.stat().st_ino,
            src_ipv4=_ip("10.0.0.2"),
            dst_ipv4=_ip("10.0.0.2"),
            protocol=6,
        ),
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1_000_000_000,
            "emitted": 2,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    Path(_config(tmp_path).event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    identities = (
        _identity(),
        SimpleNamespace(
            ready=True,
            started=True,
            full_container_id=destination_container_id,
            service_ids=("cluster::ns::dst",),
            node_name="node-a",
            pod_ips=("10.0.0.3",),
        ),
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: identities,
    )
    normal = SimpleNamespace(samples=(
        SimpleNamespace(
            entity_type="service",
            namespace="ns",
            service_name="svc",
        ),
        SimpleNamespace(
            entity_type="service",
            namespace="ns",
            service_name="dst",
        ),
        SimpleNamespace(
            entity_type="edge",
            protocol="tcp",
            namespace="ns",
            src_service="svc",
            dst_service="dst",
        ),
    ))
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=_inventory(),
        normal_raw_window=normal,
    )

    tcp = {
        sample.channel_id: sample
        for sample in window.samples
        if sample.entity_type == "edge"
    }
    assert tcp["tcp.rto_rate"].value == 1
    assert all(sample.mapping_quality == 1 for sample in tcp.values())


def test_formal_tcp_scope_excludes_observed_reverse_edge_from_burst(
    tmp_path, monkeypatch,
):
    source_cgroup = _filesystem(tmp_path)
    destination_container_id = "b" * 64
    destination_cgroup = (
        tmp_path / "cgroup"
        / f"cri-containerd-{destination_container_id}.scope"
    )
    destination_cgroup.mkdir()
    (destination_cgroup / "memory.stat").write_text(
        "pgfault 10\npgmajfault 2\n", encoding="utf-8"
    )
    (destination_cgroup / "memory.events").write_text(
        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
        encoding="utf-8",
    )
    records = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 1,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
        _event(
            13, source_cgroup.stat().st_ino,
            src_ipv4=_ip("10.0.0.2"),
            dst_ipv4=_ip("10.96.0.20"),
            protocol=6,
        ),
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1_000_000_000,
            "emitted": 1,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    Path(_config(tmp_path).event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    identities = (
        _identity(),
        SimpleNamespace(
            ready=True,
            started=True,
            full_container_id=destination_container_id,
            service_ids=("cluster::ns::dst",),
            node_name="node-a",
            pod_ips=("10.0.0.3",),
        ),
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: identities,
    )
    normal = SimpleNamespace(samples=(
        SimpleNamespace(
            entity_type="service",
            namespace="ns",
            service_name="svc",
        ),
        SimpleNamespace(
            entity_type="service",
            namespace="ns",
            service_name="dst",
        ),
        SimpleNamespace(
            entity_type="edge",
            protocol="tcp",
            namespace="ns",
            src_service="svc",
            dst_service="dst",
        ),
        SimpleNamespace(
            entity_type="edge",
            protocol="tcp",
            namespace="ns",
            src_service="dst",
            dst_service="svc",
        ),
    ))
    formal_edge = "cluster::ns::svc->dst::tcp"
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
        formal_tcp_edge_entity_ids=(formal_edge,),
    )
    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=_inventory(),
        normal_raw_window=normal,
    )

    tcp = [
        sample for sample in window.samples
        if sample.entity_type == "edge"
    ]
    assert {sample.entity_id for sample in tcp} == {formal_edge}
    assert {sample.channel_id for sample in tcp} == set(TCP_CHANNELS)
    assert all(sample.mapping_quality == 1 for sample in tcp)
    retransmit = next(
        sample for sample in tcp
        if sample.channel_id == "tcp.retrans_rate"
    )
    assert retransmit.value == 1


def test_raw_burst_archive_is_write_once_and_integrity_checked(
    tmp_path, monkeypatch,
):
    cgroup = _filesystem(tmp_path)
    records = [
        {
            "record_type": "control",
            "schema_version": 1,
            "state": "ready",
            "timestamp_ns": START - 1,
            "program_count": 31,
            "timeout_ms": 5000,
            "sampling_profile": "low",
        },
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": START,
            "monotonic_ns": 0,
            "emitted": 0,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
        _event(1, cgroup.stat().st_ino),
        {
            "record_type": "checkpoint",
            "schema_version": 1,
            "timestamp_ns": END,
            "monotonic_ns": 1,
            "emitted": 1,
            "reserve_failed": 0,
            "program_count": 31,
            "sampling_profile": "low",
        },
    ]
    Path(_config(tmp_path).event_log_path).write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "proberca.dataplane.burst_live.runtime_identities",
        lambda revision: (_identity(),),
    )
    source = FinalLiveBurstSource(
        _config(tmp_path),
        burst_config_fingerprint=fingerprint({"burst": "contract"}),
    )
    window = source.collect_window(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=_inventory(),
        normal_raw_window=_normal_raw_window(),
    )
    root = tmp_path / "archive"
    writer = BurstArchiveWriter(
        root,
        dataset_id=fingerprint({"dataset": "test"}),
        cluster_id="cluster",
        event_source_fingerprint=source.event_source_fingerprint,
        burst_config_fingerprint=window.burst_config_fingerprint,
    )
    writer.append(window)
    archive = writer.seal()
    assert len(tuple(archive.iter_windows())) == 1
    with pytest.raises(RawCollectionError, match="sealed"):
        writer.append(window)

    windows = root / "burst-windows.jsonl"
    windows.write_text(
        windows.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RawCollectionError, match="content hash"):
        BurstArchive.load(root)


def test_live_burst_config_rejects_wrong_program_count(tmp_path):
    payload = {
        **_config(tmp_path).__dict__,
        "expected_program_count": 0,
    }
    with pytest.raises(RawCollectionError, match="program_count"):
        FinalLiveBurstConfig.from_dict(payload)


def test_live_burst_config_rejects_unknown_sampling_profile(tmp_path):
    payload = {
        **_config(tmp_path).__dict__,
        "sampling_profile": "adaptive",
    }
    with pytest.raises(RawCollectionError, match="sampling_profile"):
        FinalLiveBurstConfig.from_dict(payload)

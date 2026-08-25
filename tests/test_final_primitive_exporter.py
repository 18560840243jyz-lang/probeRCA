from __future__ import annotations

from concurrent.futures import Future
import json
from pathlib import Path
from types import SimpleNamespace
import socket
import threading
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
import yaml

import scripts.check_final_dataplane_readiness as readiness_module
import scripts.install_final_dataplane as install_module
import proberca.dataplane.primitive_exporter as primitive_module
from scripts.install_final_dataplane import (
    _service_matches_contract,
)
from proberca.dataplane.primitive_exporter import (
    FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION,
    FinalPrimitiveExporter,
    FinalPrimitiveExporterConfig,
    _select_metric_lines,
)
from proberca.dataplane.dns_policy import DnsAggregationPolicy
from proberca.dataplane.prometheus_text import (
    PrometheusSample,
    parse_prometheus_text,
    render_prometheus_text,
)
from proberca.dataplane.raw import RawCollectionError


def test_prometheus_text_labels_are_parsed_once_and_escaped():
    samples = parse_prometheus_text(
        'metric_total{le="0.00025",server="dns://:53",'
        'route="a\\\\b\\\"c"} 7\n'
    )
    assert len(samples) == 1
    assert samples[0].label_dict == {
        "le": "0.00025",
        "route": 'a\\b"c',
        "server": "dns://:53",
    }
    rendered = render_prometheus_text(
        samples, timestamp_ms=1_234_000
    )
    assert 'le="0.00025"' in rendered
    assert 'server="dns://:53"' in rendered
    assert rendered.endswith(" 7 1234000\n")


def test_metric_selection_ignores_unrelated_invalid_beyla_family():
    exposition = (
        'messaging_duration_bucket{destination="\x00"} 1\n'
        'rpc_server_duration_seconds_count{service_name="payment"} 2\n'
    )
    with pytest.raises(RawCollectionError, match="labels"):
        parse_prometheus_text(exposition)
    selected = _select_metric_lines(
        exposition,
        frozenset({"rpc_server_duration_seconds_count"}),
    )
    samples = parse_prometheus_text(selected)
    assert [item.name for item in samples] == [
        "rpc_server_duration_seconds_count"
    ]


def test_renderer_rejects_negative_or_duplicate_output():
    with pytest.raises(RawCollectionError, match="non-negative"):
        PrometheusSample.create("counter_total", {}, -1)
    sample = PrometheusSample.create("counter_total", {"a": "b"}, 1)
    with pytest.raises(RawCollectionError, match="duplicate"):
        render_prometheus_text(
            (sample, sample), timestamp_ms=1_000
        )


def test_final_exporter_config_is_frozen_and_one_second():
    payload = yaml.safe_load(Path(
        "configs/final_primitive_exporter.example.yaml"
    ).read_text(encoding="utf-8"))
    config = FinalPrimitiveExporterConfig.from_dict(payload)
    assert config.schema_version == FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION
    assert config.snapshot_period_sec == 1
    assert config.acquisition_max_pending == 6
    assert config.beyla_acquisition_workers == 6
    assert config.raw_acquisition_workers == 36
    assert config.publish_queue_max_pending == 4
    assert config.publish_visibility_sec == 0.75
    assert config.inventory_max_staleness_sec == 30.0
    assert config.experimental_dns_enabled is False
    assert "kube-system/kube-dns" in config.include_services
    assert len(config.include_services) == 12
    invalid = dict(payload)
    invalid["snapshot_period_sec"] = 2
    with pytest.raises(RawCollectionError, match="frozen range"):
        FinalPrimitiveExporterConfig.from_dict(invalid)
    underprovisioned = dict(payload)
    underprovisioned["acquisition_max_pending"] = 5
    underprovisioned["beyla_acquisition_workers"] = 5
    underprovisioned["raw_acquisition_workers"] = 30
    with pytest.raises(RawCollectionError, match="cannot cover"):
        FinalPrimitiveExporterConfig.from_dict(underprovisioned)
    missing_worker = dict(payload)
    missing_worker["beyla_acquisition_workers"] = 5
    with pytest.raises(RawCollectionError, match="every pending"):
        FinalPrimitiveExporterConfig.from_dict(missing_worker)


def test_formal_live_collector_has_tcp_queries_but_no_dns_queries():
    payload = yaml.safe_load(Path(
        "configs/final_live_collector.example.yaml"
    ).read_text(encoding="utf-8"))
    queries = payload["prometheus"]["queries"]
    components = {item["component"] for item in queries}

    assert {
        "edge_request_total",
        "edge_error_total",
        "edge_timeout_total",
        "edge_latency_observation_total",
        "edge_latency_histogram",
    } <= components
    assert not any(
        item["query_id"].startswith("dns-")
        or item["component"].startswith("dns_")
        for item in queries
    )


def test_inventory_refresh_is_single_inflight_and_installed_atomically(
    monkeypatch,
):
    clock = {"ns": 1_000_000_000}
    monkeypatch.setattr(
        primitive_module.time, "perf_counter_ns", lambda: clock["ns"]
    )
    stale = SimpleNamespace(containers=(
        SimpleNamespace(container_id="a" * 64),
    ))
    refreshed = SimpleNamespace(containers=(
        SimpleNamespace(container_id="b" * 64),
    ))
    future = Future()
    submissions = []
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(inventory_max_staleness_sec=30.0)
    exporter._inventory_cache = stale
    exporter._inventory_cache_accepted_perf_ns = clock["ns"]
    exporter._inventory_refresh_last_error = None
    exporter._inventory_refresh_lock = threading.Lock()
    exporter._inventory_refresh_future = None
    exporter._inventory_refresh_executor = SimpleNamespace(
        submit=lambda function: submissions.append(function) or future
    )

    exporter._start_inventory_refresh()
    exporter._start_inventory_refresh()
    assert submissions == [primitive_module._inventory_worker]
    clock["ns"] += 2_000_000_000
    assert exporter._accept_inventory_refresh() is False
    assert exporter._inventory_cache is stale

    future.set_result(refreshed)
    assert exporter._accept_inventory_refresh() is True
    assert exporter._inventory_cache is refreshed
    assert exporter._inventory_cache_accepted_perf_ns == clock["ns"]
    assert exporter._inventory_refresh_future is None


def test_inventory_refresh_staleness_is_bounded(monkeypatch):
    clock = {"ns": 10_000_000_000}
    monkeypatch.setattr(
        primitive_module.time, "perf_counter_ns", lambda: clock["ns"]
    )
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(inventory_max_staleness_sec=5.0)
    exporter._inventory_cache = object()
    exporter._inventory_cache_accepted_perf_ns = clock["ns"]
    exporter._inventory_refresh_last_error = None
    exporter._inventory_refresh_lock = threading.Lock()
    exporter._inventory_refresh_future = Future()

    clock["ns"] += 5_000_000_000
    assert exporter._accept_inventory_refresh() is False
    clock["ns"] += 1
    with pytest.raises(RawCollectionError, match="inventory_refresh_stale"):
        exporter._accept_inventory_refresh()


def test_failed_inventory_refresh_retries_with_verified_cache(monkeypatch):
    clock = {"ns": 20_000_000_000}
    monkeypatch.setattr(
        primitive_module.time, "perf_counter_ns", lambda: clock["ns"]
    )
    stale = object()
    failed = Future()
    failed.set_exception(RuntimeError("temporary Kubernetes API pressure"))
    replacement = Future()
    submissions = []
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(inventory_max_staleness_sec=30.0)
    exporter._inventory_cache = stale
    exporter._inventory_cache_accepted_perf_ns = clock["ns"]
    exporter._inventory_refresh_last_error = None
    exporter._inventory_refresh_lock = threading.Lock()
    exporter._inventory_refresh_future = failed
    exporter._inventory_refresh_executor = SimpleNamespace(
        submit=lambda function: submissions.append(function) or replacement
    )

    clock["ns"] += 2_000_000_000
    assert exporter._accept_inventory_refresh() is False
    assert exporter._inventory_cache is stale
    assert exporter._inventory_refresh_future is None
    assert "temporary Kubernetes API pressure" in (
        exporter._inventory_refresh_last_error or ""
    )
    exporter._start_inventory_refresh()
    assert submissions == [primitive_module._inventory_worker]
    assert exporter._inventory_refresh_future is replacement


def test_source_parser_warmup_is_read_only_and_uses_frozen_inventory():
    pod = SimpleNamespace(container_id="coredns-container")
    inventory = SimpleNamespace(coredns_pods=(pod,))
    calls = []
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(node_exporter_url="http://node/metrics")
    exporter._inventory_and_cgroup_paths = lambda: (inventory, {})
    exporter._beyla = lambda selected: calls.append(("beyla", selected))
    exporter._fetch_url = lambda url: calls.append(("node", url))
    exporter._coredns = lambda selected: calls.append(("coredns", selected))

    exporter._warm_source_parsers()

    assert sorted(name for name, _value in calls) == [
        "beyla", "coredns", "node",
    ]
    assert next(value for name, value in calls if name == "beyla") \
        is inventory
    assert next(value for name, value in calls if name == "coredns") \
        is pod
    assert next(value for name, value in calls if name == "node") \
        == "http://node/metrics"


def test_host_exporter_reads_beyla_from_its_frozen_monitored_node():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        runtime_mode="host", monitored_node_name="worker-2", beyla_port=9400,
    )
    inventory = SimpleNamespace(
        node_names=("s0", "worker-1", "worker-2", "worker-3"),
        node_internal_ips={
            "s0": "10.0.0.1", "worker-1": "10.0.0.2",
            "worker-2": "10.0.0.3", "worker-3": "10.0.0.4",
        },
    )
    calls = []
    exporter._fetch_url = lambda url, **kwargs: calls.append(
        (url, kwargs["metric_names"])
    ) or ()

    assert exporter._beyla(inventory) == ()
    assert calls == [
        ("http://10.0.0.3:9400/metrics", primitive_module._BEYLA_REQUEST_METRICS)
    ]


def test_host_exporter_fails_closed_when_monitored_node_is_absent():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        runtime_mode="host", monitored_node_name="worker-2", beyla_port=9400,
    )
    inventory = SimpleNamespace(
        node_names=("s0", "worker-1"),
        node_internal_ips={"s0": "10.0.0.1", "worker-1": "10.0.0.2"},
    )

    with pytest.raises(RawCollectionError, match="monitored Kubernetes node"):
        exporter._beyla(inventory)


def test_final_bpf_normal_path_is_map_aggregated_and_window_safe():
    bpf = Path(
        "bpf/final_normal/final_normal.bpf.c"
    ).read_text(encoding="utf-8")
    header = Path(
        "bpf/final_normal/final_normal.h"
    ).read_text(encoding="utf-8")
    loader = Path(
        "bpf/user/proberca_final_loader.c"
    ).read_text(encoding="utf-8")
    assert "BPF_MAP_TYPE_RINGBUF" not in bpf
    assert "BPF_MAP_TYPE_PERF_EVENT_ARRAY" not in bpf
    assert "futex_wait_ns_total" in bpf
    assert "dns_edge_counters" in bpf
    assert "tcp_edge_counters" in bpf
    assert "final_tcp_preconnect_failure" in bpf
    assert "&local->socket_accept_fail_total" in bpf
    assert "result == -EAGAIN" in bpf
    assert "result == -ERESTARTSYS" in bpf
    assert bpf.index("result == -EAGAIN") \
        < bpf.index("&counters->socket_ops_total")
    assert "preconnect_failure_total" in header
    assert "success_latency_buckets" in loader
    assert "servfail_total" in bpf
    assert "PROBERCA_FINAL_DNS_QNAME_MAX" in bpf
    assert "record_dns_parse_failure" in bpf
    assert "__be32 client_ipv4;" in header
    assert "__be16 qtype;" in header
    assert "__u64 qname_hash;" in header
    assert '"futex_starts"' in loader
    assert '"tcp_edge_counters"' in loader
    assert "tcp_edge_transport" in loader
    assert "collect_active_futex_waits" in loader
    assert "resolve_stable_futex_entries" in loader
    assert "all_futex_entries_resolved" in loader
    assert "FUTEX_SNAPSHOT_ATTEMPTS" in loader
    assert "entry->completed_before_ns + entry->active_ns" in loader
    assert "--snapshot" in loader
    assert "--cgroup-id" in loader

    burst_bpf = Path(
        "bpf/final_burst/final_burst.bpf.c"
    ).read_text(encoding="utf-8")
    tcp_failure = burst_bpf.index("PROBERCA_BURST_TCP_CONNECT_FAILURE")
    local_failure = burst_bpf.index(
        "PROBERCA_BURST_SOCKET_FAILURE", tcp_failure
    )
    assert local_failure > tcp_failure


def test_final_burst_runtime_log_is_epoch_scoped_and_bounded():
    loader = Path(
        "bpf/user/proberca_final_burst_loader.c"
    ).read_text(encoding="utf-8")
    service = Path(
        "deploy/final-dataplane/proberca-final-burst.service"
    ).read_text(encoding="utf-8")

    assert 'fopen(options->output_path, "w")' in loader
    assert 'fopen(options->output_path, "a")' not in loader
    assert '"max-output-bytes"' in loader
    assert "final Burst output byte limit reached" in loader
    assert "--max-output-bytes 4294967296" in service


def test_bpf_snapshot_filters_to_sorted_active_cgroups(monkeypatch):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        bpf_loader_path="/loader",
        bpf_map_directory="/maps",
        dns_timeout_ms=5_000,
        source_timeout_sec=2,
    )
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            stdout=(
                '{"record_type":"cgroup","cgroup_id":3}\n'
                '{"record_type":"dns","cgroup_id":9}\n'
            )
        )

    monkeypatch.setattr(
        "proberca.dataplane.primitive_exporter.subprocess.run",
        fake_run,
    )
    records = exporter._bpf_snapshot((9, 3, 9))
    assert [item["record_type"] for item in records] == [
        "cgroup", "dns",
    ]
    assert captured["command"] == [
        "/loader", "--snapshot", "/maps", "--timeout-ms", "5000",
        "--cgroup-id", "3", "--cgroup-id", "9",
    ]
    assert captured["kwargs"]["check"] is True


def test_bpf_snapshot_rejects_empty_active_cgroup_set():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    with pytest.raises(
        RawCollectionError, match="active positive cgroup IDs",
    ):
        exporter._bpf_snapshot(())


def test_futex_counter_is_monotonic_and_bounded_by_thread_capacity():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._futex_raw_high_water_ns = {}
    exporter._futex_wait_ns = {}

    assert exporter._bounded_futex_counter("container", 100.0, 0.0) == 0.0
    assert exporter._bounded_futex_counter("container", 106.0, 10.0) == 6.0
    assert exporter._bounded_futex_counter("container", 104.0, 10.0) == 6.0
    assert exporter._bounded_futex_counter(
        "container", 1_000.0, 10.0
    ) == 16.0
    assert exporter._bounded_futex_counter(
        "container", 1_005.0, 10.0
    ) == 21.0


def test_capacity_gap_fails_once_rebases_and_recovers(tmp_path):
    cgroup = tmp_path / "container"
    cgroup.mkdir()
    (cgroup / "cpu.stat").write_text(
        "usage_usec 100\nnr_throttled 0\nnr_periods 10\n",
        encoding="utf-8",
    )
    (cgroup / "cpu.max").write_text("100000 100000\n", encoding="utf-8")
    (cgroup / "memory.current").write_text("1024\n", encoding="utf-8")
    (cgroup / "memory.max").write_text("2048\n", encoding="utf-8")
    (cgroup / "memory.stat").write_text(
        "inactive_file 128\n", encoding="utf-8"
    )
    (cgroup / "io.pressure").write_text(
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        encoding="utf-8",
    )
    (cgroup / "cgroup.procs").write_text("1\n2\n", encoding="utf-8")
    (cgroup / "cgroup.threads").write_text("1\n2\n3\n", encoding="utf-8")

    identity = SimpleNamespace(
        container_id="container-id",
        namespace="online-boutique",
        pod="service-pod",
        container="server",
        cpu_request_cores=1.0,
        series="container-series",
    )
    inventory = SimpleNamespace(containers=(identity,))
    cgroup_id = cgroup.stat().st_ino
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._active_task_ns = {}
    exporter._active_thread_ns = {}
    exporter._futex_raw_high_water_ns = {}
    exporter._futex_wait_ns = {}
    exporter._last_capacity_ns = None

    def bpf(raw_wait_ns):
        return ({
            "record_type": "cgroup",
            "cgroup_id": cgroup_id,
            "futex_wait_ns_total": raw_wait_ns,
        },)

    exporter._resource_samples(inventory, {"container-id": cgroup}, bpf(100), 1_000_000_000)
    exporter._resource_samples(inventory, {"container-id": cgroup}, bpf(106), 2_000_000_000)
    assert exporter._active_task_ns["container-id"] == 2_000_000_000
    assert exporter._active_thread_ns["container-id"] == 3_000_000_000
    assert exporter._futex_wait_ns["container-id"] == 6

    with pytest.raises(
        RawCollectionError, match="capacity_integration_gap_rebased"
    ):
        exporter._resource_samples(
            inventory, {"container-id": cgroup}, bpf(160), 10_000_000_000
        )
    assert exporter._last_capacity_ns == 10_000_000_000
    assert exporter._active_task_ns["container-id"] == 2_000_000_000
    assert exporter._active_thread_ns["container-id"] == 3_000_000_000
    assert exporter._futex_raw_high_water_ns["container-id"] == 160
    assert exporter._futex_wait_ns["container-id"] == 6

    recovered = exporter._resource_samples(
        inventory, {"container-id": cgroup}, bpf(165), 11_000_000_000
    )
    values = {item.name: item.value for item in recovered}
    assert values[
        "proberca_cgroup_active_task_nanoseconds_total"
    ] == 4_000_000_000
    assert values[
        "proberca_cgroup_active_thread_nanoseconds_total"
    ] == 6_000_000_000
    assert values[
        "proberca_cgroup_futex_wait_nanoseconds_total"
    ] == 11


def test_snapshot_http_gate_recovers_after_one_gap(monkeypatch):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._lock = threading.Lock()
    exporter._snapshot = ""
    exporter._snapshot_ns = 0
    exporter._last_error = None
    exporter.wall_clock_ns = lambda: 11_000_000_000
    outcomes = iter((
        RawCollectionError("capacity_integration_gap_rebased"),
        "metric_total 1 11000\n",
    ))

    def collect_snapshot(_timestamp_ns):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(exporter, "collect_snapshot", collect_snapshot)
    with pytest.raises(
        RawCollectionError, match="capacity_integration_gap_rebased"
    ):
        exporter.snapshot_once(10_000_000_000)
    assert "capacity_integration_gap_rebased" in exporter._response()[2]

    exporter.snapshot_once(11_000_000_000)
    snapshot, timestamp_ns, error = exporter._response()
    assert snapshot == "metric_total 1 11000\n"
    assert timestamp_ns == 11_000_000_000
    assert error == ""
    assert exporter._last_error is None


def test_http_endpoints_return_to_200_after_rebased_gap(monkeypatch):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        listen_host="127.0.0.1", listen_port=port
    )
    exporter._lock = threading.Lock()
    exporter._stop = threading.Event()
    exporter._snapshot = ""
    exporter._snapshot_ns = 0
    exporter._last_error = None
    exporter._inventory_refresh_executor = SimpleNamespace(
        shutdown=lambda **_kwargs: None
    )
    exporter.wall_clock_ns = lambda: 11_000_000_000
    outcomes = iter((
        RawCollectionError("capacity_integration_gap_rebased"),
        "metric_total 1 11000\n",
    ))
    gap_ready = threading.Event()
    continue_after_gap = threading.Event()
    recovery_ready = threading.Event()
    server_ready = threading.Event()
    server_holder = {}

    def collect_snapshot(_timestamp_ns):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def snapshot_loop():
        try:
            exporter.snapshot_once(10_000_000_000)
        except RawCollectionError:
            gap_ready.set()
        assert continue_after_gap.wait(timeout=5)
        exporter.snapshot_once(11_000_000_000)
        recovery_ready.set()

    server_type = primitive_module.ThreadingHTTPServer

    def server_factory(address, handler):
        server = server_type(address, handler)
        server_holder["server"] = server
        server_ready.set()
        return server

    monkeypatch.setattr(exporter, "collect_snapshot", collect_snapshot)
    monkeypatch.setattr(exporter, "_snapshot_loop", snapshot_loop)
    monkeypatch.setattr(exporter, "_warm_source_parsers", lambda: None)
    monkeypatch.setattr(
        primitive_module, "ThreadingHTTPServer", server_factory
    )
    thread = threading.Thread(target=exporter.serve_forever, daemon=True)
    thread.start()
    assert server_ready.wait(timeout=5)
    assert gap_ready.wait(timeout=5)

    def http_status(path):
        try:
            with urlopen(
                f"http://127.0.0.1:{port}{path}", timeout=2
            ) as response:
                return response.status
        except HTTPError as error:
            return error.code

    assert http_status("/healthz") == 503
    assert http_status("/metrics") == 503
    continue_after_gap.set()
    assert recovery_ready.wait(timeout=5)
    assert http_status("/healthz") == 200
    assert http_status("/metrics") == 200

    server_holder["server"].shutdown()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_fatal_pipeline_failure_exits_http_server_for_service_restart(
    monkeypatch,
):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        listen_host="127.0.0.1", listen_port=port
    )
    exporter._lock = threading.Lock()
    exporter._stop = threading.Event()
    exporter._snapshot = ""
    exporter._snapshot_ns = 0
    exporter._last_error = None
    exporter._inventory_refresh_executor = SimpleNamespace(
        shutdown=lambda **_kwargs: None
    )
    exporter._beyla_executor = SimpleNamespace(
        shutdown=lambda **_kwargs: None
    )
    exporter._raw_source_executor = SimpleNamespace(
        shutdown=lambda **_kwargs: None
    )
    monkeypatch.setattr(exporter, "_warm_source_parsers", lambda: None)
    monkeypatch.setattr(exporter, "_assembly_loop", lambda: None)
    monkeypatch.setattr(exporter, "_publication_loop", lambda: None)

    def fail_pipeline():
        assert exporter._stop.wait(0.2) is False
        exporter._record_pipeline_failure(
            "RawCollectionError: inventory refresh missed the next "
            "snapshot deadline",
            fatal=True,
        )

    monkeypatch.setattr(exporter, "_snapshot_loop", fail_pipeline)
    errors = []

    def serve():
        try:
            exporter.serve_forever()
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RawCollectionError)
    assert "primitive acquisition pipeline stopped" in str(errors[0])
    assert "inventory refresh missed" in str(errors[0])


def test_incomplete_beyla_coverage_fails_one_snapshot_without_killing_pipeline():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._lock = threading.Lock()
    exporter._last_error = None
    exporter._ensure_pipeline_state()
    error = RawCollectionError(
        "Beyla/CoreDNS request coverage is incomplete: "
        "[('online-boutique', 'paymentservice')]"
    )

    assert primitive_module._is_transient_snapshot_error(error) is True
    exporter._record_pipeline_failure(
        f"{type(error).__name__}: {error}",
        fatal=not primitive_module._is_transient_snapshot_error(error),
    )

    assert exporter._pipeline_failed.is_set() is False
    assert "request coverage is incomplete" in exporter._last_error
    assert primitive_module._is_transient_snapshot_error(
        RawCollectionError("inventory refresh missed the next snapshot deadline")
    ) is False


def test_snapshot_loop_uses_fixed_one_second_deadlines():
    clock = {"ns": 100_000_000}
    targets = []

    class Stop:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            clock["ns"] += int(seconds * 1_000_000_000)
            return False

    stop = Stop()
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        snapshot_period_sec=1, acquisition_max_pending=4,
    )
    exporter.wall_clock_ns = lambda: clock["ns"]
    exporter._stop = stop
    exporter._lock = threading.Lock()
    exporter._last_error = None
    exporter._snapshot_deadline_misses_total = 0

    def launch_target(target_ns):
        targets.append(target_ns)
        clock["ns"] += 200_000_000
        if len(targets) == 3:
            stop.stopped = True
        return SimpleNamespace(target_ns=target_ns)

    exporter._launch_target = launch_target
    exporter._snapshot_loop()

    assert targets == [1_000_000_000, 2_000_000_000, 3_000_000_000]
    assert exporter._snapshot_deadline_misses_total == 0


def test_snapshot_loop_launches_next_target_while_prior_source_is_slow():
    clock = {"ns": 100_000_000}
    targets = []

    class Stop:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            clock["ns"] += int(seconds * 1_000_000_000)
            return False

    stop = Stop()
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(
        snapshot_period_sec=1, acquisition_max_pending=4,
    )
    exporter.wall_clock_ns = lambda: clock["ns"]
    exporter._stop = stop
    exporter._lock = threading.Lock()
    exporter._last_error = None
    exporter._snapshot_deadline_misses_total = 0

    def launch_target(target_ns):
        targets.append(target_ns)
        clock["ns"] += 20_000_000
        if len(targets) == 2:
            stop.stopped = True
        return SimpleNamespace(target_ns=target_ns)

    exporter._launch_target = launch_target
    exporter._snapshot_loop()

    assert targets == [1_000_000_000, 2_000_000_000]
    assert exporter._snapshot_deadline_misses_total == 0
    assert exporter._missing_targets_total == 0


def test_request_rows_reuses_precomputed_beyla_indexes(monkeypatch):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    inventory = SimpleNamespace(services=frozenset())

    def unexpected_rebuild(*_args, **_kwargs):
        raise AssertionError("Beyla indexes were rebuilt")

    monkeypatch.setattr(primitive_module, "_sample_index", unexpected_rebuild)
    monkeypatch.setattr(
        primitive_module, "_histogram_index", unexpected_rebuild
    )

    rows = exporter._request_rows(
        (), inventory, edge=False,
        sample_index={}, histogram_index={},
    )

    assert rows == ()


def test_service_rpc_rows_merge_concrete_and_wildcard_business_buckets():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    inventory = SimpleNamespace(services=frozenset({
        ("online-boutique", "cartservice"),
        ("online-boutique", "recommendationservice"),
    }))

    def family(service, pod, method, count):
        labels = {
            "k8s_namespace_name": "online-boutique",
            "service_name": service,
            "k8s_pod_name": pod,
            "k8s_container_name": "server",
            "rpc_method": method,
            "rpc_grpc_status_code": "0",
        }
        samples = [PrometheusSample.create(
            "rpc_server_duration_seconds_count", labels, count,
        )]
        samples.extend(
            PrometheusSample.create(
                "rpc_server_duration_seconds_bucket",
                {**labels, "le": bound}, count,
            )
            for bound in ("0.01", "+Inf")
        )
        return samples

    samples = tuple([
        *family("cartservice", "cart-pod", "*", 2),
        *family(
            "cartservice", "cart-pod",
            "/hipstershop.CartService/AddItem", 20,
        ),
        *family(
            "cartservice", "cart-pod",
            "/hipstershop.CartService/GetCart", 30,
        ),
        *family(
            "cartservice", "cart-pod",
            "/grpc.health.v1.Health/Check", 100,
        ),
        *family(
            "cartservice", "cart-pod",
            "00-0123456789abcdef-0123456789abcdef-01", 1,
        ),
        *family("recommendationservice", "recommendation-pod", "*", 12),
    ])
    rows = exporter._request_rows(samples, inventory, edge=False)

    assert {
        (
            row.service,
            row.count.label_dict["rpc_method"],
            row.count.value,
        )
        for row in rows
    } == {
        ("cartservice", "*", 2),
        ("cartservice", "/hipstershop.CartService/AddItem", 20),
        ("cartservice", "/hipstershop.CartService/GetCart", 30),
        ("recommendationservice", "*", 12),
    }


def test_exporter_uses_persistent_source_workers_and_slow_stage_logging():
    source = Path(
        "proberca/dataplane/primitive_exporter.py"
    ).read_text(encoding="utf-8")
    assert "self._beyla_executor = ThreadPoolExecutor(" in source
    assert "self._raw_source_executor = ThreadPoolExecutor(" in source
    assert "self._inventory_refresh_executor = ProcessPoolExecutor(" in source
    assert 'multiprocessing.get_context("spawn")' in source
    assert "wait(futures)" in source
    assert "final primitive snapshot published:" in source
    assert '"beyla_duration_ns"' in source


def test_tcp_preconnect_failures_join_frozen_edge_without_latency():
    inventory = SimpleNamespace(service_cluster_ips={
        "10.96.53.134": (
            "online-boutique", "productcatalogservice",
        ),
    })
    identity = SimpleNamespace(
        namespace="online-boutique", service="frontend",
    )
    records = ({
        "record_type": "tcp_edge_transport",
        "cgroup_id": 7,
        "destination_ipv4": "10.96.53.134",
        "destination_port": 3550,
        "preconnect_failure_total": 13,
    },)

    samples = FinalPrimitiveExporter._tcp_transport_failure_samples(
        inventory, records, {7: identity}
    )

    assert {item.name for item in samples} == {
        "proberca_tcp_edge_request_total",
        "proberca_tcp_edge_error_total",
        "proberca_tcp_edge_timeout_total",
    }
    values = {item.name: item.value for item in samples}
    assert values == {
        "proberca_tcp_edge_request_total": 13,
        "proberca_tcp_edge_error_total": 13,
        "proberca_tcp_edge_timeout_total": 0,
    }
    assert all(
        item.label_dict["src_service"] == "frontend"
        and item.label_dict["dst_service"]
        == "productcatalogservice"
        and item.label_dict["source_coverage"] == "1"
        for item in samples
    )
    assert not any("latency" in item.name for item in samples)


def test_dns_query_counter_is_completed_responses_plus_timeouts():
    inventory = SimpleNamespace(
        service_cluster_ips={"10.96.0.10": ("kube-system", "kube-dns")}
    )
    identity = SimpleNamespace(
        namespace="online-boutique", service="frontend",
        container="server",
    )
    records = ({
        "record_type": "dns",
        "cgroup_id": 7,
        "server_ipv4": "10.96.0.10",
        "qname": "paymentservice.online-boutique.svc.cluster.local.",
        "qtype": 1,
        "query_total": 10,
        "success_total": 5,
        "timeout_total": 2,
        "servfail_total": 1,
        "refused_total": 0,
        "nxdomain_total": 0,
        "transport_error_total": 0,
        "retry_total": 3,
        "truncated_total": 0,
        "success_latency_buckets": [0] * 15 + [5],
    },)
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.dns_policy = DnsAggregationPolicy.from_dict(
        yaml.safe_load(Path(
            "configs/final_dns_aggregation_policy.yaml"
        ).read_text(encoding="utf-8"))
    )
    samples = exporter._dns_samples(
        inventory, records, {7: identity}
    )
    values = {
        item.name: item.value for item in samples
        if item.name.startswith("proberca_dns_edge_")
    }
    assert values["proberca_dns_edge_query_total"] == 8
    assert values["proberca_dns_edge_success_total"] == 5
    assert values["proberca_dns_edge_timeout_total"] == 2
    assert values[
        "proberca_dns_edge_success_latency_milliseconds_bucket"
    ] == 5


def test_dns_tcp_fallback_fails_closed_until_stream_merge_exists():
    inventory = SimpleNamespace(
        service_cluster_ips={"10.96.0.10": ("kube-system", "kube-dns")}
    )
    identity = SimpleNamespace(
        namespace="online-boutique", service="frontend",
        container="server",
    )
    record = {
        "record_type": "dns",
        "cgroup_id": 7,
        "server_ipv4": "10.96.0.10",
        "qname": "paymentservice.online-boutique.svc.cluster.local.",
        "qtype": 1,
        "query_total": 1,
        "success_total": 0,
        "timeout_total": 0,
        "servfail_total": 0,
        "refused_total": 0,
        "nxdomain_total": 0,
        "transport_error_total": 0,
        "retry_total": 0,
        "truncated_total": 1,
        "success_latency_buckets": [0] * 16,
    }
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.dns_policy = DnsAggregationPolicy.from_dict(
        yaml.safe_load(Path(
            "configs/final_dns_aggregation_policy.yaml"
        ).read_text(encoding="utf-8"))
    )
    with pytest.raises(
        RawCollectionError,
        match="TCP fallback is not completely observed",
    ):
        exporter._dns_samples(
            inventory, (record,), {7: identity}
        )


def test_dns_sidecar_and_metadata_probe_are_audit_only():
    inventory = SimpleNamespace(
        service_cluster_ips={"10.96.0.10": ("kube-system", "kube-dns")}
    )
    records = (
        {
            "record_type": "dns",
            "cgroup_id": 7,
            "server_ipv4": "10.96.0.10",
            "qname": "metadata.google.internal.",
            "qtype": 1,
            "query_total": 1,
            "success_total": 0,
            "timeout_total": 0,
            "servfail_total": 1,
            "refused_total": 0,
            "nxdomain_total": 0,
            "transport_error_total": 0,
            "retry_total": 0,
            "truncated_total": 0,
            "success_latency_buckets": [0] * 16,
        },
        {
            "record_type": "dns",
            "cgroup_id": 8,
            "server_ipv4": "10.96.0.10",
            "qname": (
                "paymentservice.online-boutique.svc.cluster.local."
            ),
            "qtype": 1,
            "query_total": 1,
            "success_total": 1,
            "timeout_total": 0,
            "servfail_total": 0,
            "refused_total": 0,
            "nxdomain_total": 0,
            "transport_error_total": 0,
            "retry_total": 0,
            "truncated_total": 0,
            "success_latency_buckets": [0] * 15 + [1],
        },
    )
    identities = {
        7: SimpleNamespace(
            namespace="online-boutique", service="frontend",
            container="server",
        ),
        8: SimpleNamespace(
            namespace="online-boutique", service="frontend",
            container="proberca-healthy-dns-exposure",
        ),
    }
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.dns_policy = DnsAggregationPolicy.from_dict(
        yaml.safe_load(Path(
            "configs/final_dns_aggregation_policy.yaml"
        ).read_text(encoding="utf-8"))
    )
    samples = exporter._dns_samples(
        inventory, records, identities
    )
    assert not any(
        item.name.startswith("proberca_dns_edge_")
        for item in samples
    )
    nonzero = {
        (
            item.label_dict["src_container_role"],
            item.label_dict["qname_class"],
            item.label_dict["formal_action"],
            item.label_dict["final_outcome"],
            item.value,
        )
        for item in samples
        if item.name == "proberca_dns_policy_transaction_total"
        and item.value
    }
    assert nonzero == {
        ("application", "metadata_probe", "record_only", "SERVFAIL", 1),
        ("dns-sidecar", "cluster_service", "separate", "SUCCESS", 1),
    }


def test_directed_edge_series_rebase_after_counter_reset():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
    exporter._edge_sample_raw = {}
    labels = {
        "namespace": "online-boutique",
        "dst_namespace": "online-boutique",
        "src_service": "checkoutservice",
        "dst_service": "paymentservice",
        "protocol": "tcp",
        "source_series": "series-a",
    }
    first = PrometheusSample.create(
        "proberca_tcp_edge_request_total", labels, 10
    )
    present = exporter._persistent_edge_samples((first,))[0]
    assert present.value == 10
    assert present.label_dict["source_coverage"] == "1"
    absent = exporter._persistent_edge_samples(())[0]
    assert absent.value == 10
    assert absent.label_dict["source_coverage"] == "0"
    reset = PrometheusSample.create(
        "proberca_tcp_edge_request_total", labels, 2
    )
    assert exporter._persistent_edge_samples((reset,))[0].value == 12
    advanced = PrometheusSample.create(
        "proberca_tcp_edge_request_total", labels, 12
    )
    assert exporter._persistent_edge_samples((advanced,))[0].value == 22


def test_histogram_buckets_remain_cumulative_across_series_reset():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
    exporter._edge_sample_raw = {}
    common = {
        "namespace": "online-boutique",
        "dst_namespace": "online-boutique",
        "src_service": "checkoutservice",
        "dst_service": "paymentservice",
        "protocol": "tcp",
        "source_series": "series-a",
    }

    def histogram(values):
        return tuple(
            PrometheusSample.create(
                "proberca_tcp_edge_latency_milliseconds_bucket",
                {**common, "le": bound},
                value,
            )
            for bound, value in zip(("1", "10", "+Inf"), values)
        )

    first = exporter._persistent_edge_samples(
        histogram((2, 7, 10))
    )
    reset = exporter._persistent_edge_samples(
        histogram((1, 2, 3))
    )
    advanced = exporter._persistent_edge_samples(
        histogram((2, 4, 6))
    )

    def by_bound(samples):
        return {
            item.label_dict["le"]: item.value
            for item in samples
        }

    assert by_bound(first) == {"1": 2, "10": 7, "+Inf": 10}
    assert by_bound(reset) == {"1": 3, "10": 9, "+Inf": 13}
    assert by_bound(advanced) == {"1": 4, "10": 11, "+Inf": 16}
    reset_values = by_bound(reset)
    assert [
        reset_values["1"], reset_values["10"], reset_values["+Inf"],
    ] == sorted(reset_values.values())
    reset_values = by_bound(reset)
    advanced_values = by_bound(advanced)
    assert [
        advanced_values[bound] - reset_values[bound]
        for bound in ("1", "10", "+Inf")
    ] == [1, 2, 3]


def test_request_histogram_family_rebases_atomically():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
    exporter._edge_sample_raw = {}
    common = {
        "namespace": "online-boutique",
        "dst_namespace": "online-boutique",
        "src_service": "checkoutservice",
        "dst_service": "paymentservice",
        "protocol": "tcp",
        "source_series": "series-a",
    }

    def family(count, buckets):
        output = [
            PrometheusSample.create(
                "proberca_tcp_edge_request_total", common, count
            ),
            PrometheusSample.create(
                "proberca_tcp_edge_error_total", common, 0
            ),
            PrometheusSample.create(
                "proberca_tcp_edge_timeout_total", common, 0
            ),
        ]
        output.extend(
            PrometheusSample.create(
                "proberca_tcp_edge_latency_milliseconds_bucket",
                {**common, "le": bound},
                value,
            )
            for bound, value in zip(("1", "10", "+Inf"), buckets)
        )
        return tuple(output)

    exporter._persistent_edge_samples(family(10, (2, 7, 10)))
    absent = exporter._persistent_edge_samples(())
    assert {
        item.label_dict["source_coverage"] for item in absent
    } == {"0"}
    rebased = exporter._persistent_edge_samples(
        family(3, (1, 2, 3))
    )
    by_name = {
        (item.name, item.label_dict.get("le")): item.value
        for item in rebased
    }
    assert by_name[
        ("proberca_tcp_edge_request_total", None)
    ] == 13
    assert [
        by_name[
            (
                "proberca_tcp_edge_latency_milliseconds_bucket",
                bound,
            )
        ]
        for bound in ("1", "10", "+Inf")
    ] == [3, 9, 13]


def test_inconsistent_raw_histogram_does_not_poison_family_state():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
    exporter._edge_sample_raw = {}
    common = {
        "namespace": "online-boutique",
        "dst_namespace": "online-boutique",
        "src_service": "checkoutservice",
        "dst_service": "paymentservice",
        "protocol": "tcp",
        "source_series": "series-a",
    }

    def family(count, buckets):
        output = [
            PrometheusSample.create(
                "proberca_tcp_edge_request_total", common, count
            ),
            PrometheusSample.create(
                "proberca_tcp_edge_error_total", common, 0
            ),
            PrometheusSample.create(
                "proberca_tcp_edge_timeout_total", common, 0
            ),
        ]
        output.extend(
            PrometheusSample.create(
                "proberca_tcp_edge_latency_milliseconds_bucket",
                {**common, "le": bound},
                value,
            )
            for bound, value in zip(("1", "10", "+Inf"), buckets)
        )
        return tuple(output)

    exporter._persistent_edge_samples(family(10, (2, 7, 10)))
    inconsistent = exporter._persistent_edge_samples(
        family(15, (3, 9, 14))
    )
    assert {
        item.label_dict["histogram_consistent"]
        for item in inconsistent
        if item.name.endswith("_bucket")
    } == {"0"}
    recovered = exporter._persistent_edge_samples(
        family(16, (4, 10, 16))
    )
    assert {
        item.label_dict["histogram_consistent"]
        for item in recovered
        if item.name.endswith("_bucket")
    } == {"1"}
    by_bound = {
        item.label_dict["le"]: item.value
        for item in recovered
        if item.name.endswith("_bucket")
    }
    assert by_bound == {"1": 4, "10": 10, "+Inf": 16}


def test_service_series_persist_only_for_the_active_container():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._service_sample_high_water = {}
    exporter._service_sample_raw = {}
    inventory = SimpleNamespace(containers=(
        SimpleNamespace(
            namespace="online-boutique",
            pod="frontend-pod",
            container="frontend",
        ),
    ))
    labels = {
        "namespace": "online-boutique",
        "pod": "frontend-pod",
        "container": "frontend",
        "source_series": "series-a",
    }
    first = PrometheusSample.create(
        "proberca_service_request_total", labels, 10
    )
    present = exporter._persistent_service_samples(
        (first,), inventory
    )[0]
    assert present.value == 10
    assert present.label_dict["source_coverage"] == "1"
    absent = exporter._persistent_service_samples(
        (), inventory
    )[0]
    assert absent.value == 10
    assert absent.label_dict["source_coverage"] == "0"
    reset = PrometheusSample.create(
        "proberca_service_request_total", labels, 2
    )
    assert exporter._persistent_service_samples(
        (reset,), inventory
    )[0].value == 12
    assert exporter._persistent_service_samples(
        (), SimpleNamespace(containers=())
    ) == ()


def test_service_series_ignore_retired_container_after_rollout():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._service_sample_high_water = {}
    inventory = SimpleNamespace(containers=(
        SimpleNamespace(
            namespace="online-boutique",
            pod="frontend-new",
            container="frontend",
        ),
    ))
    retired = PrometheusSample.create(
        "proberca_service_request_total",
        {
            "namespace": "online-boutique",
            "pod": "frontend-old",
            "container": "frontend",
            "source_series": "retired",
        },
        100,
    )
    active = PrometheusSample.create(
        "proberca_service_request_total",
        {
            "namespace": "online-boutique",
            "pod": "frontend-new",
            "container": "frontend",
            "source_series": "active",
        },
        10,
    )

    output = exporter._persistent_service_samples(
        (retired, active), inventory
    )

    assert len(output) == 1
    assert output[0].label_dict["pod"] == "frontend-new"
    assert output[0].label_dict["source_coverage"] == "1"
    assert set(exporter._service_sample_high_water) == {active.identity}


def test_dynamic_service_series_merge_into_one_stable_counter():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._service_sample_high_water = {}
    inventory = SimpleNamespace(containers=(
        SimpleNamespace(
            namespace="online-boutique",
            pod="shipping-pod",
            container="server",
        ),
    ))
    common = {
        "namespace": "online-boutique",
        "pod": "shipping-pod",
        "container": "server",
    }

    first_raw = PrometheusSample.create(
        "proberca_service_request_total",
        {**common, "source_series": "status-200"},
        10,
    )
    first = exporter._stable_request_samples(
        exporter._persistent_service_samples((first_raw,), inventory),
        edge=False,
    )

    second_raw = (
        PrometheusSample.create(
            "proberca_service_request_total",
            {**common, "source_series": "status-200"},
            11,
        ),
        PrometheusSample.create(
            "proberca_service_request_total",
            {**common, "source_series": "status-500"},
            2,
        ),
    )
    second = exporter._stable_request_samples(
        exporter._persistent_service_samples(second_raw, inventory),
        edge=False,
    )

    assert len(first) == len(second) == 1
    assert first[0].value == 10
    assert second[0].value == 13
    assert first[0].labels == second[0].labels
    assert first[0].label_dict["source_series"] not in {
        "status-200", "status-500",
    }


def test_dynamic_edge_series_merge_into_one_stable_counter():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
    exporter._edge_sample_raw = {}
    common = {
        "namespace": "online-boutique",
        "dst_namespace": "online-boutique",
        "src_service": "checkoutservice",
        "dst_service": "paymentservice",
        "protocol": "tcp",
    }

    first_raw = PrometheusSample.create(
        "proberca_tcp_edge_request_total",
        {**common, "source_series": "route-a"},
        20,
    )
    diagnostic_mapping = {}
    first = exporter._stable_request_samples(
        exporter._persistent_edge_samples((first_raw,)),
        edge=True,
        diagnostic_mapping=diagnostic_mapping,
    )

    second_raw = (
        PrometheusSample.create(
            "proberca_tcp_edge_request_total",
            {**common, "source_series": "route-a"},
            21,
        ),
        PrometheusSample.create(
            "proberca_tcp_edge_request_total",
            {**common, "source_series": "route-b"},
            3,
        ),
    )
    second = exporter._stable_request_samples(
        exporter._persistent_edge_samples(second_raw),
        edge=True,
        diagnostic_mapping=diagnostic_mapping,
    )

    assert len(first) == len(second) == 1
    assert first[0].value == 20
    assert second[0].value == 24
    assert first[0].labels == second[0].labels
    stable_series = second[0].label_dict["source_series"]
    assert diagnostic_mapping[stable_series] == {"route-a", "route-b"}


def test_stable_request_aggregation_keeps_histogram_buckets_separate():
    common = {
        "namespace": "online-boutique",
        "pod": "shipping-pod",
        "container": "server",
        "source_coverage": "1",
    }
    samples = (
        PrometheusSample.create(
            "proberca_service_request_latency_milliseconds_bucket",
            {**common, "source_series": "a", "le": "10"},
            7,
        ),
        PrometheusSample.create(
            "proberca_service_request_latency_milliseconds_bucket",
            {**common, "source_series": "b", "le": "10"},
            5,
        ),
        PrometheusSample.create(
            "proberca_service_request_latency_milliseconds_bucket",
            {**common, "source_series": "a", "le": "+Inf"},
            8,
        ),
        PrometheusSample.create(
            "proberca_service_request_latency_milliseconds_bucket",
            {**common, "source_series": "b", "le": "+Inf"},
            6,
        ),
    )
    stable = FinalPrimitiveExporter._stable_request_samples(
        samples, edge=False,
    )
    by_boundary = {
        item.label_dict["le"]: item.value for item in stable
    }
    assert by_boundary == {"10": 12, "+Inf": 14}
    assert len({
        item.label_dict["source_series"] for item in stable
    }) == 1


def test_qdisc_drop_counter_survives_qdisc_removal_and_recreation(
    monkeypatch,
):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(source_timeout_sec=1)
    exporter._kind_node_pid = 4321
    exporter._qdisc_drop_state = {}
    payloads = iter((
        '[{"dev":"veth0","drops":3}]',
        '[{"dev":"veth0","drops":8}]',
        '[{"dev":"veth0","drops":0}]',
        '[{"dev":"veth0","drops":2}]',
    ))

    commands = []

    def fake_run(arguments, **_kwargs):
        commands.append(arguments)
        return SimpleNamespace(
            returncode=0, stdout=next(payloads), stderr=""
        )

    monkeypatch.setattr(
        "proberca.dataplane.primitive_exporter.subprocess.run",
        fake_run,
    )
    assert exporter._qdisc_transmit_drop_totals()["veth0"] == 3
    assert exporter._qdisc_transmit_drop_totals()["veth0"] == 8
    assert exporter._qdisc_transmit_drop_totals()["veth0"] == 8
    assert exporter._qdisc_transmit_drop_totals()["veth0"] == 10
    assert commands[0][:5] == [
        "nsenter", "-t", "4321", "-n", "tc",
    ]


def test_kind_node_qdisc_drops_are_emitted_without_host_device_match(
    monkeypatch,
):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(source_timeout_sec=1)
    inventory = SimpleNamespace(node_names=("kind-node",))
    samples = []
    for source_name in (
        "node_pressure_cpu_waiting_seconds_total",
        "node_pressure_memory_waiting_seconds_total",
        "node_pressure_io_waiting_seconds_total",
    ):
        samples.append(PrometheusSample.create(source_name, {}, 1.0))
    for source_name in (
        "node_network_receive_drop_total",
        "node_network_transmit_drop_total",
        "node_network_receive_errs_total",
        "node_network_transmit_errs_total",
    ):
        samples.append(PrometheusSample.create(
            source_name, {"device": "eth0"}, 10.0,
        ))
    monkeypatch.setattr(
        exporter, "_qdisc_transmit_drop_totals",
        lambda _raw: {"veth-pod": 7.0},
    )

    output = exporter._host_samples(
        inventory, tuple(samples), qdisc_raw=(("veth-pod", 7.0),),
    )

    transmit = {
        item.label_dict["interface"]: item.value
        for item in output
        if item.name == "proberca_node_network_transmit_drop_total"
    }
    qdisc = {
        item.label_dict["interface"]: item.value
        for item in output
        if item.name == "proberca_node_qdisc_transmit_drop_total"
    }
    assert transmit == {"eth0": 10.0}
    assert qdisc == {"kind:veth-pod": 7.0}


def test_deployment_uses_pinned_beyla_without_unused_service_graph():
    manifest = Path(
        "deploy/final-dataplane/beyla.yaml"
    ).read_text(encoding="utf-8")
    assert "grafana/beyla:3.15.0@sha256:" in manifest
    assert "application_service_graph" not in manifest
    assert "context_propagation" not in manifest
    scrape = yaml.safe_load(Path(
        "deploy/final-dataplane/prometheus-scrape-job.yaml"
    ).read_text(encoding="utf-8"))
    assert scrape["honor_timestamps"] is True
    assert scrape["scrape_interval"] == "250ms"
    documents = tuple(yaml.safe_load_all(manifest))
    beyla_map = next(
        item for item in documents
        if item["kind"] == "ConfigMap"
        and item["metadata"]["name"] == "proberca-beyla"
    )
    discovery = yaml.safe_load(
        beyla_map["data"]["beyla.yaml"]
    )["discovery"]["instrument"]
    online_deployments = {
        item["k8s_deployment_name"]
        for item in discovery
        if item["k8s_namespace"] == "online-boutique"
    }
    assert online_deployments == {
        "adservice",
        "cartservice",
        "checkoutservice",
        "currencyservice",
        "emailservice",
        "frontend",
        "paymentservice",
        "productcatalogservice",
        "recommendationservice",
        "redis-cart",
        "shippingservice",
    }
    assert "loadgenerator" not in online_deployments
    assert "proberca-healthy-checkout-load" not in online_deployments
    assert "proberca-healthy-rpc-load" not in online_deployments


def test_container_resources_use_direct_cgroup_v2_primitives():
    source = Path(
        "proberca/dataplane/primitive_exporter.py"
    ).read_text(encoding="utf-8")
    assert "self._active_cgroup_paths(inventory)" in source
    assert '"usage_usec" not in cpu_stat' in source
    assert '(path / "memory.current").read_text' in source
    assert '"inactive_file" not in memory_stat' in source
    assert "cAdvisor" not in source


def test_exporter_service_has_deadline_priority():
    service = Path(
        "deploy/final-dataplane/proberca-final-primitive-exporter.service"
    ).read_text(encoding="utf-8")
    assert "Nice=-5" in service
    assert "CPUWeight=200" in service


def test_single_vm_scope_freezes_v2_collection_runtime():
    scope = yaml.safe_load(Path(
        "configs/final_single_vm_scope.yaml"
    ).read_text(encoding="utf-8"))
    assert scope["status"] == "frozen_before_healthy_pilot"
    assert scope["load_profile"] == "single-vm-qualified-55"
    assert scope["load_profile_fingerprint"] == (
        "d3f0bffc6c26cb4d3f837d66eb62b5e377bae78b97522c5cefa75f3f0c2ee848"
    )
    assert scope["checkout_load_replicas"] == 3
    assert scope["checkout_interval_pattern_seconds"] == [
        0.127273, 0.145455, 0.163636, 0.136364, 0.154545,
    ]
    assert scope["direct_rpc_load_replicas"] == 1
    assert scope["direct_rpc_period_seconds"] == 0.218182
    assert scope["direct_rpc_workers_per_service"] == 3
    assert scope["online_boutique_loadgenerator_replicas"] == 1
    assert scope["online_boutique_loadgenerator_users"] == 28
    assert scope["online_boutique_loadgenerator_rate"] == 1
    assert len(scope["direct_rpc_services"]) == 8
    assert scope["primitive_exporter_schema"] == (
        FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION
    )
    assert scope["container_resource_source"] == "direct_cgroup_v2"
    assert scope["beyla_retired_series_ttl"] == "30s"
    assert scope["experimental_dns"] == {
        "enabled": False,
        "formal_scope": "excluded_from_formal_rca",
        "required_for_readiness": False,
        "archived_diagnostics_compatible": True,
    }


def test_snapshot_loop_reports_source_failures():
    source = Path(
        "proberca/dataplane/primitive_exporter.py"
    ).read_text(encoding="utf-8")
    assert "final primitive snapshot failed:" in source
    assert "file=sys.stderr" in source


def test_active_cgroup_paths_resolve_exact_runtime_identities(tmp_path):
    first_id = "a" * 64
    second_id = "b" * 64
    first_path = (
        tmp_path / "pod-a" / f"cri-containerd-{first_id}.scope"
    )
    second_path = (
        tmp_path / "pod-b" / f"cri-containerd-{second_id}.scope"
    )
    first_path.mkdir(parents=True)
    second_path.mkdir(parents=True)
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._node_cgroup_root = tmp_path
    exporter._cgroup_path_cache = None
    inventory = SimpleNamespace(containers=(
        SimpleNamespace(container_id=first_id),
        SimpleNamespace(container_id=second_id),
    ))

    resolved = exporter._active_cgroup_paths(inventory)

    assert resolved == {
        first_id: first_path,
        second_id: second_path,
    }
    assert exporter._active_cgroup_paths(inventory) == resolved


def test_stale_runtime_inventory_refreshes_after_pod_rollout(monkeypatch):
    stale = SimpleNamespace(containers=(
        SimpleNamespace(container_id="a" * 64),
    ))
    refreshed = SimpleNamespace(containers=(
        SimpleNamespace(container_id="b" * 64),
    ))
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._inventory_cache = stale
    exporter._inventory = lambda: refreshed
    attempts = []

    def cgroup_paths(inventory):
        attempts.append(inventory)
        if inventory is stale:
            raise RawCollectionError(
                "active container cgroups are incomplete"
            )
        return {"b" * 64: Path("/new-cgroup")}

    monkeypatch.setattr(exporter, "_active_cgroup_paths", cgroup_paths)

    inventory, paths = exporter._inventory_and_cgroup_paths()

    assert attempts == [stale, refreshed]
    assert inventory is refreshed
    assert paths == {"b" * 64: Path("/new-cgroup")}
    assert exporter._inventory_cache is refreshed


def test_unchanged_runtime_inventory_keeps_cgroup_failure_closed(
    monkeypatch,
):
    inventory = SimpleNamespace(containers=(
        SimpleNamespace(container_id="a" * 64),
    ))
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._inventory_cache = inventory
    exporter._inventory = lambda: inventory

    def fail(_inventory):
        raise RawCollectionError(
            "active container cgroups are incomplete"
        )

    monkeypatch.setattr(exporter, "_active_cgroup_paths", fail)

    with pytest.raises(
        RawCollectionError,
        match="active container cgroups are incomplete",
    ):
        exporter._inventory_and_cgroup_paths()


def test_coredns_cpu_accounting_profile_provides_throttle_denominator():
    patch_path = Path(
        "deploy/final-dataplane/coredns-cpu-accounting-patch.yaml"
    )
    patch = yaml.safe_load(patch_path.read_text(encoding="utf-8"))
    assert patch["metadata"] == {
        "name": "coredns",
        "namespace": "kube-system",
    }
    template = patch["spec"]["template"]
    assert template["metadata"]["annotations"][
        "proberca.io/cpu-accounting-profile"
    ] == "single-vm-v1"
    container = template["spec"]["containers"][0]
    assert container["name"] == "coredns"
    assert container["resources"]["limits"]["cpu"] == "500m"
    installer = Path(
        "scripts/install_final_dataplane.py"
    ).read_text(encoding="utf-8")
    assert str(patch_path) in installer
    assert '"patch", "deployment/coredns"' in installer
    assert '"rollout", "status", "deployment/coredns"' in installer


def test_healthy_calibration_load_is_frozen_and_fault_free():
    documents = tuple(yaml.safe_load_all(Path(
        "deploy/final-dataplane/healthy-calibration-load.yaml"
    ).read_text(encoding="utf-8")))
    config_map, deployment, rpc_deployment = documents
    assert config_map["metadata"]["namespace"] == "online-boutique"
    assert deployment["metadata"]["annotations"][
        "proberca.io/load-profile"
    ] == "single-vm-qualified-55"
    assert deployment["metadata"]["annotations"][
        "proberca.io/load-profile-fingerprint"
    ] == "d3f0bffc6c26cb4d3f837d66eb62b5e377bae78b97522c5cefa75f3f0c2ee848"
    assert deployment["spec"]["replicas"] == 3
    containers = {
        item["name"]: item
        for item in deployment["spec"]["template"]["spec"]["containers"]
    }
    assert set(containers) == {"checkout-load"}
    assert all(
        "@sha256:" in item["image"]
        for item in containers.values()
    )
    checkout = containers["checkout-load"]
    environment = {
        item["name"]: item["value"]
        for item in checkout["env"]
        if "value" in item
    }
    assert environment == {
        "TARGET_URL": (
            "http://frontend"
        ),
        "INTERVAL_PATTERN_SECONDS": (
            "0.127273,0.145455,0.163636,0.136364,0.154545"
        ),
        "PHASE_SECONDS": "20",
    }
    pod_uid = next(
        item for item in checkout["env"] if item["name"] == "POD_UID"
    )
    assert pod_uid["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    driver = config_map["data"]["checkout_driver.py"]
    compile(driver, "checkout_driver.py", "exec")
    assert "/cart/checkout" in driver
    assert "INTERVAL_PATTERN_SECONDS" in driver
    assert "hashlib.sha256(POD_UID.encode" in driver
    assert "phase_offset_seconds" in driver
    assert "tc " not in driver
    assert "iptables" not in driver
    assert "stress" not in driver
    rpc_driver = config_map["data"]["rpc_driver.py"]
    compile(rpc_driver, "rpc_driver.py", "exec")
    assert "WORKERS_PER_SERVICE" in rpc_driver
    assert "PERIOD_SECONDS" in rpc_driver
    assert "grpc.channel_ready_future" in rpc_driver
    assert "worker_slot = service_index * WORKERS_PER_SERVICE + index" in (
        rpc_driver
    )
    assert "worker_slot * PERIOD_SECONDS / worker_count" in rpc_driver
    assert "if deadline <= completed:" in rpc_driver
    assert "deadline = completed + PERIOD_SECONDS" in rpc_driver
    assert (
        "index * PERIOD_SECONDS / WORKERS_PER_SERVICE" not in rpc_driver
    )
    assert "tc " not in rpc_driver
    assert "iptables" not in rpc_driver
    assert "stress" not in rpc_driver
    assert rpc_deployment["metadata"]["name"] \
        == "proberca-healthy-rpc-load"
    assert rpc_deployment["metadata"]["annotations"][
        "proberca.io/load-profile"
    ] == "single-vm-qualified-55"
    assert rpc_deployment["metadata"]["annotations"][
        "proberca.io/load-profile-fingerprint"
    ] == "d3f0bffc6c26cb4d3f837d66eb62b5e377bae78b97522c5cefa75f3f0c2ee848"
    assert rpc_deployment["spec"]["replicas"] == 1
    rpc = rpc_deployment["spec"]["template"]["spec"]["containers"][0]
    assert "@sha256:" in rpc["image"]
    assert {
        item["name"]: item["value"] for item in rpc["env"]
    } == {
        "PYTHONPATH": "/email_server",
        "PERIOD_SECONDS": "0.218182",
        "WORKERS_PER_SERVICE": "3",
    }
    installer = Path(
        "scripts/install_final_dataplane.py"
    ).read_text(encoding="utf-8")
    assert "deploy/final-dataplane/healthy-calibration-load.yaml" in installer
    assert (
        '"deployment/proberca-healthy-checkout-load"' in installer
    )
    assert '"deployment/proberca-healthy-rpc-load"' in installer


def test_formal_installer_executes_only_formal_workloads(monkeypatch):
    commands = []
    probe_configurations = []
    prometheus_configurations = []

    def capture(arguments, **kwargs):
        commands.append(tuple(str(item) for item in arguments))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(install_module, "_require_root", lambda: None)
    monkeypatch.setattr(
        install_module, "_find_bpftool", lambda: "/usr/bin/bpftool",
    )
    monkeypatch.setattr(install_module, "_run", capture)
    monkeypatch.setattr(
        install_module, "_atomic_copy", lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        install_module,
        "_install_prometheus_job",
        lambda repository: prometheus_configurations.append(repository),
    )
    monkeypatch.setattr(
        install_module,
        "_configure_healthy_probe_cadence",
        lambda repository: probe_configurations.append(repository),
    )
    monkeypatch.setattr(
        install_module.Path, "mkdir", lambda *_args, **_kwargs: None,
    )

    repository = Path("/home/jyz/probeRCA")
    install_module.install(repository)

    rendered = tuple(" ".join(command) for command in commands)
    assert not any(
        forbidden in command
        for command in rendered
        for forbidden in (
            "healthy-dns-exposure-patch.yaml",
            "proberca-healthy-dns-exposure",
            "patch deployment/frontend",
        )
    )
    assert any(
        "deploy/final-dataplane/healthy-calibration-load.yaml" in command
        for command in rendered
    )
    assert any(
        "deploy/final-dataplane/beyla.yaml" in command
        for command in rendered
    )
    cadence = yaml.safe_load(Path(
        "deploy/final-dataplane/healthy-probe-cadence.yaml"
    ).read_text(encoding="utf-8"))
    formal_deployments = set(cadence["deployments"])
    formal_restart_order = cadence["instrumentation_restart_order"]
    restarted_deployments = [
        command.split("deployment/", 1)[1]
        for command in rendered
        if "rollout restart deployment/" in command
    ]
    assert restarted_deployments[:len(formal_restart_order)] \
        == formal_restart_order
    assert set(formal_restart_order) == formal_deployments
    load_restart_order = restarted_deployments[len(formal_restart_order):]
    assert load_restart_order == [
        "proberca-healthy-checkout-load",
        "proberca-healthy-rpc-load",
        "loadgenerator",
    ]
    beyla_restart = next(
        index for index, command in enumerate(rendered)
        if "rollout restart daemonset/proberca-beyla" in command
    )
    formal_workload_restarts = [
        index for index, command in enumerate(rendered)
        if any(
            f"rollout restart deployment/{name}" in command
            for name in formal_deployments
        )
    ]
    load_restarts = [
        index for index, command in enumerate(rendered)
        if any(
            f"rollout restart deployment/{name}" in command
            for name in load_restart_order
        )
    ]
    primitive_restart = next(
        index for index, command in enumerate(rendered)
        if "systemctl restart proberca-final-primitive-exporter.service"
        in command
    )
    assert beyla_restart < min(formal_workload_restarts)
    assert max(formal_workload_restarts) < min(load_restarts)
    assert max(load_restarts) < primitive_restart
    assert any(
        "scripts/check_final_dataplane_readiness.py" in command
        and "configs/final_live_collector.example.yaml" in command
        and "http://127.0.0.1:9477/metrics" in command
        and "--timeout-sec 300" in command
        for command in rendered
    )
    assert any(
        "patch deployment/coredns" in command
        for command in rendered
    )
    assert any(
        "set env deployment/loadgenerator" in command
        and "USERS=28" in command and "RATE=1" in command
        for command in rendered
    )
    assert any(
        "proberca.io/load-profile=single-vm-qualified-55" in command
        and "d3f0bffc6c26cb4d3f837d66eb62b5e377bae78b97522c5cefa75f3f0c2ee848" in command
        for command in rendered
    )
    assert any(
        "proberca-final-ebpf.service" in command
        and "proberca-final-burst.service" in command
        and "proberca-final-primitive-exporter.service" in command
        for command in rendered
    )
    assert probe_configurations == [repository]
    assert prometheus_configurations == [repository]


def test_formal_readiness_requires_exporter_and_every_tcp_edge():
    cluster_id = "cluster"
    required_edges = frozenset({
        "cluster::ns::caller-a->callee-a::tcp",
        "cluster::ns::caller-b->callee-b::tcp",
    })
    ready = PrometheusSample.create(
        "proberca_final_primitive_exporter_ready",
        {"cluster_id": cluster_id},
        1.0,
    )

    def edge(caller, callee):
        return PrometheusSample.create(
            "proberca_tcp_edge_request_total",
            {
                "namespace": "ns",
                "src_service": caller,
                "dst_namespace": "ns",
                "dst_service": callee,
                "protocol": "tcp",
                "source_series": f"{caller}-{callee}",
                "source_coverage": "1",
            },
            1.0,
        )

    incomplete = readiness_module.evaluate_formal_coverage(
        render_prometheus_text(
            (ready, edge("caller-a", "callee-a")),
            timestamp_ms=1_000,
        ),
        cluster_id=cluster_id,
        required_edges=required_edges,
    )
    assert incomplete == {
        "ready": False,
        "exporter_ready": True,
        "required_tcp_edges": 2,
        "observed_required_tcp_edges": 1,
        "missing_tcp_edges": [
            "cluster::ns::caller-b->callee-b::tcp",
        ],
    }

    complete = readiness_module.evaluate_formal_coverage(
        render_prometheus_text((
            ready,
            edge("caller-a", "callee-a"),
            edge("caller-b", "callee-b"),
        ), timestamp_ms=1_000),
        cluster_id=cluster_id,
        required_edges=required_edges,
    )
    assert complete["ready"] is True
    assert complete["observed_required_tcp_edges"] == 2
    assert complete["missing_tcp_edges"] == []


def test_experimental_dns_exposure_is_retained_but_not_formally_installed():
    patch_path = Path(
        "deploy/final-dataplane/healthy-dns-exposure-patch.yaml"
    )
    patch = yaml.safe_load(patch_path.read_text(encoding="utf-8"))
    template = patch["spec"]["template"]
    assert template["metadata"]["annotations"][
        "proberca.io/healthy-dns-exposure-profile"
    ] == "single-vm-dns-v1"
    containers = template["spec"]["containers"]
    assert len(containers) == 1
    exposure = containers[0]
    assert exposure["name"] == "proberca-healthy-dns-exposure"
    assert "@sha256:" in exposure["image"]
    source = exposure["args"][0]
    assert "socket.getaddrinfo" in source
    assert "except socket.gaierror as error" in source
    assert "healthy DNS lookup failed" in source
    assert "AF_INET" in source
    assert all(
        forbidden not in source
        for forbidden in ("tc ", "iptables", "stress", "fault")
    )
    environment = {
        item["name"]: item["value"]
        for item in exposure["env"]
    }
    assert environment["INTERVAL_PATTERN_SECONDS"] == (
        "0.07,0.08,0.09,0.075,0.085"
    )
    assert {
        item.strip()
        for item in environment["DNS_NAMES"].split(",")
    } == {
        "cartservice.online-boutique.svc.cluster.local.",
        "checkoutservice.online-boutique.svc.cluster.local.",
        "currencyservice.online-boutique.svc.cluster.local.",
        "paymentservice.online-boutique.svc.cluster.local.",
        "productcatalogservice.online-boutique.svc.cluster.local.",
        "shippingservice.online-boutique.svc.cluster.local.",
    }
    installer = Path(
        "scripts/install_final_dataplane.py"
    ).read_text(encoding="utf-8")
    assert str(patch_path) not in installer
    assert "proberca-healthy-dns-exposure" not in installer


def test_healthy_probe_cadence_is_explicit_and_reproducible():
    configuration = yaml.safe_load(Path(
        "deploy/final-dataplane/healthy-probe-cadence.yaml"
    ).read_text(encoding="utf-8"))
    assert set(configuration) == {
        "schema_version", "namespace", "readiness_period_seconds",
        "instrumentation_restart_order", "probe_profiles", "deployments",
    }
    assert configuration["schema_version"] \
        == "proberca-healthy-probe-cadence-v6"
    assert configuration["namespace"] == "online-boutique"
    assert configuration["readiness_period_seconds"] == 1
    assert configuration["instrumentation_restart_order"] == [
        "redis-cart",
        "adservice",
        "currencyservice",
        "emailservice",
        "paymentservice",
        "productcatalogservice",
        "shippingservice",
        "cartservice",
        "recommendationservice",
        "checkoutservice",
        "frontend",
    ]
    assert configuration["probe_profiles"] == {
        "default": {
            "liveness_initial_delay_seconds": 0,
            "liveness_failure_threshold": 3,
            "liveness_timeout_seconds": 1,
            "readiness_initial_delay_seconds": 0,
            "readiness_failure_threshold": 3,
            "readiness_timeout_seconds": 1,
        },
    }
    expected = {
        "adservice": ("server", 15, "default"),
        "cartservice": ("server", 10, "default"),
        "checkoutservice": ("server", 10, "default"),
        "currencyservice": ("server", 10, "default"),
        "frontend": ("server", 10, "default"),
        "paymentservice": ("server", 10, "default"),
        "productcatalogservice": ("server", 10, "default"),
        "recommendationservice": ("server", 5, "default"),
        "redis-cart": ("redis", 5, "default"),
        "shippingservice": ("server", 10, "default"),
    }
    assert {
        name: (
            profile["container"],
            profile["liveness_period_seconds"],
            profile["probe_profile"],
        )
        for name, profile in configuration["deployments"].items()
        if name != "emailservice"
    } == expected
    expected_capacity = {
        "cartservice": {
            "requests": {"cpu": "200m", "memory": "64Mi"},
            "limits": {"cpu": "500m", "memory": "128Mi"},
        },
        "checkoutservice": {
            "requests": {"cpu": "150m", "memory": "64Mi"},
            "limits": {"cpu": "500m", "memory": "128Mi"},
        },
        "currencyservice": {
            "requests": {"cpu": "150m", "memory": "64Mi"},
            "limits": {"cpu": "500m", "memory": "128Mi"},
        },
        "frontend": {
            "requests": {"cpu": "300m", "memory": "64Mi"},
            "limits": {"cpu": "1000m", "memory": "128Mi"},
        },
        "productcatalogservice": {
            "requests": {"cpu": "200m", "memory": "64Mi"},
            "limits": {"cpu": "600m", "memory": "128Mi"},
        },
        "recommendationservice": {
            "requests": {"cpu": "200m", "memory": "220Mi"},
            "limits": {"cpu": "500m", "memory": "450Mi"},
        },
    }
    assert {
        name: profile["capacity_resources"]
        for name, profile in configuration["deployments"].items()
        if "capacity_resources" in profile
    } == expected_capacity
    emailservice = configuration["deployments"]["emailservice"]
    assert emailservice["service_contract"] == {
        "name": "emailservice",
        "port": 5000,
        "targetPort": 8080,
    }
    rendered = emailservice["strategic_merge_patch"]
    container = rendered["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "server"
    expected_probes = {
        "startupProbe": {
            "$patch": "replace",
            "grpc": {"port": 8080},
            "periodSeconds": 2,
            "timeoutSeconds": 3,
            "failureThreshold": 30,
        },
        "readinessProbe": {
            "$patch": "replace",
            "grpc": {"port": 8080},
            "periodSeconds": 5,
            "timeoutSeconds": 3,
            "failureThreshold": 3,
            "successThreshold": 1,
        },
        "livenessProbe": {
            "$patch": "replace",
            "grpc": {"port": 8080},
            "periodSeconds": 10,
            "timeoutSeconds": 3,
            "failureThreshold": 3,
        },
    }
    assert {
        name: container[name] for name in expected_probes
    } == expected_probes
    for probe_name in (
        "startupProbe", "readinessProbe", "livenessProbe",
    ):
        assert container[probe_name]["grpc"]["port"] == 8080
        assert container[probe_name]["$patch"] == "replace"
        assert "initialDelaySeconds" not in container[probe_name]
    assert _service_matches_contract(
        {
            "metadata": {"name": "emailservice"},
            "spec": {
                "ports": [{
                    "name": "grpc",
                    "port": 5000,
                    "targetPort": 8080,
                    "protocol": "TCP",
                }],
            },
        },
        emailservice["service_contract"],
    )
    installer = Path(
        "scripts/install_final_dataplane.py"
    ).read_text(encoding="utf-8")
    assert "_configure_healthy_probe_cadence(repository)" in installer
    assert "deploy/final-dataplane/healthy-probe-cadence.yaml" in installer
    assert '"livenessProbe"' in installer
    assert '"readinessProbe"' in installer
    assert '"capacity_resources"' in installer
    assert '"proberca.io/healthy-capacity"' in installer


def test_healthy_probe_installer_applies_frozen_capacity(monkeypatch):
    configuration = yaml.safe_load(Path(
        "deploy/final-dataplane/healthy-probe-cadence.yaml"
    ).read_text(encoding="utf-8"))
    commands = []

    def capture(arguments, **kwargs):
        command = tuple(str(item) for item in arguments)
        commands.append(command)
        if "get" not in command:
            return SimpleNamespace(returncode=0, stdout="")
        target = command[command.index("get") + 1]
        if target == "service/emailservice":
            payload = {
                "metadata": {"name": "emailservice"},
                "spec": {"ports": [{
                    "port": 5000,
                    "targetPort": 8080,
                    "protocol": "TCP",
                }]},
            }
        else:
            deployment = target.split("/", 1)[1]
            profile = configuration["deployments"][deployment]
            payload = {
                "spec": {"template": {"spec": {"containers": [{
                    "name": profile["container"],
                    "livenessProbe": {},
                    "readinessProbe": {},
                }]}}},
            }
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(payload),
        )

    monkeypatch.setattr(
        install_module, "_load_mapping", lambda _path: configuration,
    )
    monkeypatch.setattr(install_module, "_run", capture)
    install_module._configure_healthy_probe_cadence(
        Path("/home/jyz/probeRCA")
    )

    expected = {
        name: profile["capacity_resources"]
        for name, profile in configuration["deployments"].items()
        if "capacity_resources" in profile
    }
    for deployment, resources in expected.items():
        command = next(
            item for item in commands
            if "patch" in item
            and f"deployment/{deployment}" in item
        )
        payload = json.loads(command[command.index("--patch") + 1])
        template = payload["spec"]["template"]
        assert template["metadata"]["annotations"][
            "proberca.io/healthy-capacity"
        ] == (
            f"cpu-{resources['limits']['cpu']}_"
            f"memory-{resources['limits']['memory']}"
        )
        container = template["spec"]["containers"][0]
        assert container["name"] \
            == configuration["deployments"][deployment]["container"]
        assert container["resources"] == resources
        assert container["livenessProbe"]["periodSeconds"] \
            == configuration["deployments"][deployment][
                "liveness_period_seconds"
            ]
        assert container["readinessProbe"]["periodSeconds"] == 1

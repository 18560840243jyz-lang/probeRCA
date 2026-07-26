from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from proberca.dataplane.primitive_exporter import (
    FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION,
    FinalPrimitiveExporter,
    FinalPrimitiveExporterConfig,
    _select_metric_lines,
)
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
    assert "kube-system/kube-dns" in config.include_services
    assert len(config.include_services) == 12
    invalid = dict(payload)
    invalid["snapshot_period_sec"] = 2
    with pytest.raises(RawCollectionError, match="frozen range"):
        FinalPrimitiveExporterConfig.from_dict(invalid)


def test_final_bpf_normal_path_is_map_aggregated_and_window_safe():
    bpf = Path(
        "bpf/final_normal/final_normal.bpf.c"
    ).read_text(encoding="utf-8")
    loader = Path(
        "bpf/user/proberca_final_loader.c"
    ).read_text(encoding="utf-8")
    assert "BPF_MAP_TYPE_RINGBUF" not in bpf
    assert "BPF_MAP_TYPE_PERF_EVENT_ARRAY" not in bpf
    assert "futex_wait_ns_total" in bpf
    assert "dns_edge_counters" in bpf
    assert '"futex_starts"' in loader
    assert "collect_active_futex_waits" in loader
    assert "resolve_stable_futex_entries" in loader
    assert "all_futex_entries_resolved" in loader
    assert "FUTEX_SNAPSHOT_ATTEMPTS" in loader
    assert "entry->completed_before_ns + entry->active_ns" in loader
    assert "--snapshot" in loader


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


def test_dns_query_counter_is_completed_responses_plus_timeouts():
    inventory = SimpleNamespace(
        service_cluster_ips={"10.96.0.10": ("kube-system", "kube-dns")}
    )
    identity = SimpleNamespace(
        namespace="online-boutique", service="frontend"
    )
    records = ({
        "record_type": "dns",
        "cgroup_id": 7,
        "server_ipv4": "10.96.0.10",
        "query_total": 10,
        "timeout_total": 2,
        "error_rcode_total": 1,
        "latency_buckets": [0] * 15 + [6],
    },)
    samples = FinalPrimitiveExporter._dns_samples(
        inventory, records, {7: identity}
    )
    values = {item.name: item.value for item in samples}
    assert values["proberca_dns_edge_query_total"] == 8
    assert values["proberca_dns_edge_timeout_total"] == 2
    assert values[
        "proberca_dns_edge_latency_milliseconds_bucket"
    ] == 6


def test_directed_edge_series_persist_at_their_high_water_mark():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._edge_sample_high_water = {}
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
    assert exporter._persistent_edge_samples((first,))[0].value == 10
    assert exporter._persistent_edge_samples(())[0].value == 10
    reset = PrometheusSample.create(
        "proberca_tcp_edge_request_total", labels, 2
    )
    assert exporter._persistent_edge_samples((reset,))[0].value == 10
    advanced = PrometheusSample.create(
        "proberca_tcp_edge_request_total", labels, 12
    )
    assert exporter._persistent_edge_samples((advanced,))[0].value == 12


def test_service_series_persist_only_for_the_active_container():
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter._service_sample_high_water = {}
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
    assert exporter._persistent_service_samples(
        (first,), inventory
    )[0].value == 10
    assert exporter._persistent_service_samples(
        (), inventory
    )[0].value == 10
    reset = PrometheusSample.create(
        "proberca_service_request_total", labels, 2
    )
    assert exporter._persistent_service_samples(
        (reset,), inventory
    )[0].value == 10
    assert exporter._persistent_service_samples(
        (), SimpleNamespace(containers=())
    ) == ()


def test_qdisc_drop_counter_survives_qdisc_removal_and_recreation(
    monkeypatch,
):
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.config = SimpleNamespace(source_timeout_sec=1)
    exporter._qdisc_drop_state = {}
    payloads = iter((
        '[{"dev":"veth0","drops":3}]',
        '[{"dev":"veth0","drops":8}]',
        '[{"dev":"veth0","drops":0}]',
        '[{"dev":"veth0","drops":2}]',
    ))

    def fake_run(*_args, **_kwargs):
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

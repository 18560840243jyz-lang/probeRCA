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
    assert config.kubelet_port == 10250
    assert config.kubelet_ca_path.endswith("/kubelet.crt")
    assert "kube-system/kube-dns" in config.include_services
    assert len(config.include_services) == 12
    invalid = dict(payload)
    invalid["snapshot_period_sec"] = 2
    with pytest.raises(RawCollectionError, match="frozen range"):
        FinalPrimitiveExporterConfig.from_dict(invalid)


def test_inventory_refresh_is_pipelined_for_the_next_snapshot():
    source = Path(
        "proberca/dataplane/primitive_exporter.py"
    ).read_text(encoding="utf-8")
    cached = source.index("inventory = self._inventory_cache")
    refresh = source.index(
        "next_inventory_future = executor.submit(self._inventory)"
    )
    publish = source.index("self._inventory_cache = next_inventory")
    aggregate = source.index(
        "service_rows = self._request_rows(", publish
    )
    assert cached < refresh < publish < aggregate


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
    assert "--cgroup-id" in loader


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
    present = exporter._persistent_edge_samples((first,))[0]
    assert present.value == 10
    assert present.label_dict["source_coverage"] == "1"
    absent = exporter._persistent_edge_samples(())[0]
    assert absent.value == 10
    assert absent.label_dict["source_coverage"] == "0"
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
    )[0].value == 10
    assert exporter._persistent_service_samples(
        (), SimpleNamespace(containers=())
    ) == ()


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
    first = exporter._stable_request_samples(
        exporter._persistent_edge_samples((first_raw,)),
        edge=True,
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
    )

    assert len(first) == len(second) == 1
    assert first[0].value == 20
    assert second[0].value == 24
    assert first[0].labels == second[0].labels


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


def test_cadvisor_uses_pinned_direct_kubelet_transport():
    source = Path(
        "proberca/dataplane/primitive_exporter.py"
    ).read_text(encoding="utf-8")
    installer = Path(
        "scripts/install_final_dataplane.py"
    ).read_text(encoding="utf-8")
    assert "urllib3.HTTPSConnectionPool(" in source
    assert "assert_hostname=node" in source
    assert "cert_reqs=ssl.CERT_REQUIRED" in source
    assert "connect_get_node_proxy_with_path" not in source
    assert '"/var/lib/kubelet/pki/kubelet.crt"' in installer
    assert '"-----BEGIN CERTIFICATE-----"' in installer


def test_healthy_calibration_load_is_frozen_and_fault_free():
    documents = tuple(yaml.safe_load_all(Path(
        "deploy/final-dataplane/healthy-calibration-load.yaml"
    ).read_text(encoding="utf-8")))
    config_map, deployment = documents
    assert config_map["metadata"]["namespace"] == "online-boutique"
    assert deployment["metadata"]["annotations"][
        "proberca.io/load-profile"
    ] == "single-vm-healthy-v1"
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert "@sha256:" in container["image"]
    environment = {
        item["name"]: item["value"]
        for item in container["env"]
    }
    assert environment == {
        "TARGET_URL": (
            "http://frontend"
        ),
        "INTERVAL_SECONDS": "0.4",
    }
    driver = config_map["data"]["checkout_driver.py"]
    assert "/cart/checkout" in driver
    assert "tc " not in driver
    assert "iptables" not in driver
    assert "stress" not in driver

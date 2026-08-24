from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import proberca.dataplane.burst_collection as burst_collection_module
from proberca.dataplane.burst import burst_event_rate, rare_event_strength
from proberca.dataplane.archive import CollectionArchive, CollectionArchiveWriter
from proberca.dataplane.burst_archive import (
    BurstArchive,
    BurstArchiveWriter,
    RawBurstWindow,
)
from proberca.dataplane.burst_collection import (
    BURST_CHANNEL_MODES,
    BurstChannelCalibration,
    BurstEvidenceCollector,
    RawBurstSample,
)
from proberca.dataplane.burst_live import (
    DNS_CHANNELS,
    HOST_CHANNELS,
    RARE_CHANNELS,
    SERVICE_CHANNELS,
    TCP_CHANNELS,
)
from proberca.dataplane.burst_replay import (
    BurstCalibrationArtifact,
    BurstCalibrationPolicy,
    BurstJoinedCollectionArchive,
    calibrate_healthy_burst,
)
from proberca.dataplane.collector import FinalDataPlaneCollector
from proberca.dataplane.collector import FinalLiveCollectorConfig
from proberca.dataplane.collector import FinalLiveCollectionRunner
from proberca.dataplane.contracts import canonical_json, fingerprint
from proberca.data.schema import (
    METRIC_INVALID_REASONS,
    METRIC_RECORD_SCHEMA_VERSION,
    NodeMetricRecord,
)
from proberca.dataplane.final_aggregation import COMPONENTS, FinalWindowAggregator
from proberca.dataplane.raw import (
    RawCollectionError,
    RawCollectionWindow,
    RawMetricSample,
)
from proberca.dataplane.raw_archive import (
    RawPrimitiveArchive,
    RawPrimitiveArchiveWriter,
)
from proberca.campaign.dataset_package import (
    DatasetPackageError,
    seal_dataset_directory,
)
from proberca.dataplane.sources import (
    PrometheusPrimitiveQuery,
    PrometheusPrimitiveSource,
    PrometheusSourceConfig,
)
from proberca.cli.collect_final import _write_aligned_windows
from proberca.k8s.contracts import (
    KubernetesObjectRef,
    ResourceVersionVector,
    RuntimeIdentityRecord,
    WorkloadRef,
)


START = 1_000_000_000
END = 2_000_000_000
CLUSTER = "cluster-a"
NAMESPACE = "shop"

FORMAL_SERVICE_METRICS = frozenset({
    "request_rate",
    "request_failure_rate",
    "request_latency_p95",
    "cpu_usage_rate",
    "cpu_throttle_ratio",
    "memory_working_set_ratio",
    "io_psi",
    "futex_wait_time_rate",
    "local_socket_failure_rate",
})
FORMAL_HOST_METRICS = frozenset({
    "cpu_psi",
    "memory_psi",
    "io_psi",
    "nic_drop_error_rate",
})
FORMAL_TCP_METRICS = frozenset({
    "edge_request_count",
    "edge_latency_p95",
    "edge_failure_rate",
})


def _runtime_identity(*, owner_resource_version="100", owner_generation=1,
                      owner_uid="owner-a", container_id="containerd://abc"):
    owner = KubernetesObjectRef(
        "apps/v1", "ReplicaSet", NAMESPACE, "checkout-abc", owner_uid,
        owner_resource_version, owner_generation,
    )
    workload = WorkloadRef(
        "apps/v1", "Deployment", NAMESPACE, "checkout", "deployment-a",
        (owner,),
    )
    return RuntimeIdentityRecord(
        CLUSTER, NAMESPACE, "pod-a", "checkout-abc-1", ("10.0.0.1",),
        "10.0.0.10", "node-a", workload,
        (f"{CLUSTER}::{NAMESPACE}::checkout",), "server", "app",
        "containerd", container_id, "image@sha256:abc", True, True, 0,
        START, "pod-rv",
    )


def test_runtime_identity_ignores_mutable_owner_versions():
    original = _runtime_identity()
    status_updated = _runtime_identity(
        owner_resource_version="999", owner_generation=42,
    )
    assert status_updated.identity_fingerprint == original.identity_fingerprint

    replaced_owner = _runtime_identity(owner_uid="owner-b")
    replaced_container = _runtime_identity(container_id="containerd://def")
    assert replaced_owner.identity_fingerprint != original.identity_fingerprint
    assert (
        replaced_container.identity_fingerprint
        != original.identity_fingerprint
    )
EXPERIMENTAL_DNS_COMPONENTS = frozenset({
    "dns_query_total",
    "dns_success_total",
    "dns_timeout_total",
    "dns_servfail_total",
    "dns_refused_total",
    "dns_nxdomain_failure_total",
    "dns_transport_error_total",
    "dns_success_latency_histogram",
})


def _counter(
    output, component, start, delta, *, entity, series, **identity,
):
    spec = COMPONENTS[component]
    for timestamp, value in ((START, start), (END, start + delta)):
        output.append(RawMetricSample.create(
            timestamp_ns=timestamp,
            cluster_id=CLUSTER,
            entity_type=entity,
            component=component,
            metric_family=spec.metric_family,
            metric_kind=spec.metric_kind,
            unit=spec.unit,
            scope=spec.scope,
            series_id=series,
            value=value,
            **identity,
        ))


def _gauge(output, component, value, *, series, **identity):
    spec = COMPONENTS[component]
    output.append(RawMetricSample.create(
        timestamp_ns=END,
        cluster_id=CLUSTER,
        entity_type="service",
        component=component,
        metric_family=spec.metric_family,
        metric_kind=spec.metric_kind,
        unit=spec.unit,
        scope=spec.scope,
        series_id=series,
        value=value,
        **identity,
    ))


def _histogram(
    output, component, deltas, *, entity, series, **identity,
):
    spec = COMPONENTS[component]
    for bound, start_value, delta in zip(
        (1.0, 10.0, None), (100.0, 200.0, 300.0), deltas,
    ):
        for timestamp, value in (
            (START, start_value), (END, start_value + delta),
        ):
            output.append(RawMetricSample.create(
                timestamp_ns=timestamp,
                cluster_id=CLUSTER,
                entity_type=entity,
                component=component,
                metric_family=spec.metric_family,
                metric_kind=spec.metric_kind,
                unit=spec.unit,
                scope=spec.scope,
                series_id=series,
                value=value,
                histogram_upper_bound=bound,
                histogram_is_inf_bucket=bound is None,
                **identity,
            ))


def _service_samples(
    output, service, node, *, series, request_delta=100, error_delta=2,
    request_histogram_deltas=None,
    socket_failure_deltas=(1, 0, 0, 0),
    socket_ops_delta=100,
):
    identity = {
        "namespace": NAMESPACE,
        "service_name": service,
        "pod_uid": f"pod-{service}",
        "container_id": f"container-{service}",
        "node_name": node,
    }
    for component, delta in (
        ("request_total", request_delta),
        ("request_error_total", error_delta),
        ("request_timeout_total", 0),
        ("cpu_time_ns_total", 500_000_000),
        ("cpu_nr_throttled_total", 2),
        ("cpu_nr_periods_total", 100),
        ("io_psi_some_ns_total", 100_000_000),
        ("active_task_ns_total", 1_000_000_000),
        ("futex_wait_ns_total", 10_000_000),
        ("active_thread_ns_total", 1_000_000_000),
        ("socket_backlog_overflow_total", socket_failure_deltas[0]),
        ("socket_accept_fail_total", socket_failure_deltas[1]),
        ("socket_local_rst_total", socket_failure_deltas[2]),
        ("socket_local_drop_total", socket_failure_deltas[3]),
        ("socket_ops_total", socket_ops_delta),
    ):
        _counter(
            output, component, 100, delta,
            entity="service", series=series, **identity,
        )
    _gauge(
        output, "allocated_cpu_cores", 1.0,
        series=series, **identity,
    )
    _gauge(
        output, "memory_working_set_bytes", 50.0,
        series=series, **identity,
    )
    _gauge(
        output, "memory_limit_bytes", 100.0,
        series=series, **identity,
    )
    _histogram(
        output, "request_latency_histogram", (
            (
                request_delta // 2,
                (request_delta * 95 + 99) // 100,
                request_delta,
            )
            if request_histogram_deltas is None
            else request_histogram_deltas
        ),
        entity="service", series=series, **identity,
    )


def _host_samples(output, node, *, qdisc_delta=0):
    identity = {"node_name": node}
    for component, delta in (
        ("node_cpu_psi_some_ns_total", 10_000_000),
        ("node_memory_psi_some_ns_total", 20_000_000),
        ("node_io_psi_some_ns_total", 30_000_000),
        ("node_nic_rx_drop_total", 1),
        ("node_nic_tx_drop_total", 2),
        ("node_nic_rx_error_total", 3),
        ("node_nic_tx_error_total", 4),
        ("node_qdisc_tx_drop_total", qdisc_delta),
    ):
        _counter(
            output, component, 100, delta,
            entity="host", series=node, **identity,
        )


def _edge_samples(
    output, protocol, *, count_delta=None, error_delta=None,
    timeout_delta=None, histogram_deltas=None,
    src_service="frontend", dst_service="payment", series=None,
):
    identity = {
        "namespace": NAMESPACE,
        "src_service": src_service,
        "dst_service": dst_service,
        "dst_namespace": NAMESPACE,
        "src_pod_uid": "pod-frontend",
        "dst_pod_uid": "pod-payment",
        "src_node": "node-a",
        "dst_node": "node-b",
        "protocol": protocol,
    }
    if protocol == "tcp":
        latency_delta = 50 if count_delta is None else count_delta
        components = (
            ("edge_request_total", 50 if count_delta is None else count_delta),
            ("edge_error_total", 2 if error_delta is None else error_delta),
            ("edge_timeout_total", 1 if timeout_delta is None else timeout_delta),
            ("edge_latency_observation_total", latency_delta),
        )
        histogram = "edge_latency_histogram"
    else:
        dns_count = 21 if count_delta is None else count_delta
        dns_error = 1 if error_delta is None else error_delta
        dns_timeout = 1 if timeout_delta is None else timeout_delta
        dns_success = dns_count - dns_error - dns_timeout
        components = (
            ("dns_query_total", dns_count),
            ("dns_success_total", dns_success),
            ("dns_timeout_total", dns_timeout),
            ("dns_servfail_total", dns_error),
            ("dns_refused_total", 0),
            ("dns_nxdomain_failure_total", 0),
            ("dns_transport_error_total", 0),
        )
        histogram = "dns_success_latency_histogram"
    for component, delta in components:
        _counter(
            output, component, 100, delta,
            entity="edge", series=series or f"{protocol}-flow", **identity,
        )
    _histogram(
        output, histogram, (
            (
                (25, 48, 50)
                if protocol == "tcp" else (10, dns_success, dns_success)
            )
            if histogram_deltas is None else histogram_deltas
        ),
        entity="edge", series=series or f"{protocol}-flow", **identity,
    )


def _edge_id(source="frontend", destination="payment"):
    return (
        f"{CLUSTER}::{NAMESPACE}::{source}->{destination}::tcp"
    )


def _raw_window(*, include_dns=True):
    samples = []
    _service_samples(
        samples, "frontend", "node-a", series="frontend-series"
    )
    _service_samples(
        samples, "payment", "node-b", series="payment-series"
    )
    _host_samples(samples, "node-a")
    _host_samples(samples, "node-b")
    _edge_samples(samples, "tcp")
    if include_dns:
        _edge_samples(samples, "dns")
    return RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    )


def _revision(version="rv-1"):
    services = {}
    pods = {}
    nodes = {}
    pod_to_services = {}
    service_uid_by_name = {}
    pod_uid_by_name = {}
    for service, node in (("frontend", "node-a"), ("payment", "node-b")):
        service_uid = f"service-{service}"
        pod_uid = f"pod-{service}"
        services[service_uid] = {
            "metadata": {
                "namespace": NAMESPACE,
                "name": service,
                "uid": service_uid,
                "resourceVersion": version,
            },
        }
        pods[pod_uid] = {
            "metadata": {
                "namespace": NAMESPACE,
                "name": f"{service}-pod",
                "uid": pod_uid,
                "resourceVersion": version,
            },
            "spec": {"nodeName": node, "volumes": []},
            "status": {
                "podIP": "10.0.0.1" if service == "frontend" else "10.0.0.2",
                "hostIP": "192.0.2.1",
                "containerStatuses": [{
                    "name": service,
                    "containerID": f"containerd://container-{service}",
                    "imageID": f"sha256:{'1' * 64}",
                    "started": True,
                    "ready": True,
                    "restartCount": 0,
                }],
            },
        }
        nodes[node] = {
            "metadata": {
                "name": node, "uid": node,
                "resourceVersion": version,
            },
        }
        service_id = f"{CLUSTER}::{NAMESPACE}::{service}"
        pod_to_services[pod_uid] = (service_id,)
        service_uid_by_name[(NAMESPACE, service)] = service_uid
        pod_uid_by_name[(NAMESPACE, f"{service}-pod")] = pod_uid
    resource_versions = tuple(
        ResourceVersionVector(
            kind, (NAMESPACE,), version, START, True, False, f"{kind}-watch"
        )
        for kind in ("Pod", "Service", "Node")
    )
    return SimpleNamespace(
        ready=True,
        issues=(),
        cluster_id=CLUSTER,
        observed_at_ns=START,
        revision_id=fingerprint({"version": version}),
        resource_versions=resource_versions,
        objects_by_kind={
            "Pod": pods,
            "Service": services,
            "Node": nodes,
            "PersistentVolumeClaim": {},
            "PersistentVolume": {},
        },
        pod_to_services=pod_to_services,
        service_uid_by_name=service_uid_by_name,
        pod_uid_by_name=pod_uid_by_name,
    )


@pytest.fixture
def contract():
    with open(
        "configs/final_collection_contract.yaml", encoding="utf-8"
    ) as handle:
        return yaml.safe_load(handle)


def test_real_shape_9_4_3_3_and_exact_math(contract):
    result = FinalWindowAggregator(contract).aggregate(_raw_window())
    services = {}
    hosts = {}
    for item in result.node_metrics:
        target = services if item.scope == "service" else hosts
        key = item.service_name if item.scope == "service" else item.node_name
        target.setdefault(key, {})[item.metric_name] = item.value
    edges = {}
    for item in result.edge_metrics:
        edges.setdefault(item.protocol, {})[item.metric_name] = item.value
    assert all(len(values) == 9 for values in services.values())
    assert all(len(values) == 4 for values in hosts.values())
    assert all(len(values) == 3 for values in edges.values())
    assert services["frontend"]["request_failure_rate"] == pytest.approx(0.02)
    assert services["frontend"]["request_latency_p95"] == 10.0
    assert services["frontend"]["cpu_usage_rate"] == pytest.approx(0.5)
    assert services["frontend"]["memory_working_set_ratio"] == pytest.approx(0.5)
    assert hosts["node-a"]["nic_drop_error_rate"] == 10.0
    assert edges["tcp"]["edge_failure_rate"] == pytest.approx(3 / 50)
    assert edges["dns"]["dns_failure_rate"] == pytest.approx(2 / 21)


def test_host_nic_rate_adds_independently_covered_qdisc_drops(contract):
    samples = []
    _host_samples(samples, "node-a", qdisc_delta=7)
    raw = RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    )

    result = FinalWindowAggregator(contract).aggregate(raw)

    nic = next(
        item for item in result.node_metrics
        if item.metric_name == "nic_drop_error_rate"
    )
    assert nic.value == 17.0


def test_tcp_preconnect_failures_extend_count_and_failure_not_latency(contract):
    raw = _raw_window(include_dns=False)
    samples = list(raw.samples)
    identity = {
        "namespace": NAMESPACE,
        "src_service": "frontend",
        "dst_service": "payment",
        "dst_namespace": NAMESPACE,
        "src_pod_uid": "pod-frontend",
        "dst_pod_uid": "pod-payment",
        "src_node": "node-a",
        "dst_node": "node-b",
        "protocol": "tcp",
    }
    for component, delta in (
        ("edge_request_total", 3),
        ("edge_error_total", 3),
        ("edge_timeout_total", 0),
    ):
        _counter(
            samples, component, 0, delta,
            entity="edge", series="tcp-preconnect", **identity,
        )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {item.metric_name: item for item in result.edge_metrics}
    assert metrics["edge_request_count"].value == 53
    assert metrics["edge_failure_rate"].value == pytest.approx(6 / 53)
    assert metrics["edge_latency_p95"].value == 10
    assert metrics["edge_latency_p95"].sample_count == 50


def test_ratio_is_recomputed_after_cross_pod_sum(contract):
    raw = _raw_window()
    samples = list(raw.samples)
    _service_samples(
        samples, "frontend", "node-a",
        series="frontend-second", request_delta=900, error_delta=0,
    )
    second = RawCollectionWindow.create(
        sequence=1, window_start_ns=START, window_end_ns=END,
        cluster_id=CLUSTER, samples=samples,
    )
    result = FinalWindowAggregator(contract).aggregate(second)
    value = next(
        item.value for item in result.node_metrics
        if item.service_name == "frontend"
        and item.metric_name == "request_failure_rate"
    )
    assert value == pytest.approx(2 / 1000)


def test_local_socket_failure_events_are_deduplicated_per_operation(contract):
    raw = _raw_window()
    samples = [
        item for item in raw.samples
        if item.service_name != "frontend"
    ]
    _service_samples(
        samples,
        "frontend",
        "node-a",
        series="frontend-series",
        socket_failure_deltas=(1, 1, 0, 0),
        socket_ops_delta=1,
    )
    window = RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    )
    result = FinalWindowAggregator(contract).aggregate(window)
    record = next(
        item for item in result.node_metrics
        if item.service_name == "frontend"
        and item.metric_name == "local_socket_failure_rate"
    )
    assert record.value == pytest.approx(1.0)
    assert record.sample_count == 1


def test_tcp_non_monotonic_histogram_invalidates_only_latency(contract):
    samples = [
        item for item in _raw_window(include_dns=False).samples
        if item.entity_type != "edge"
    ]
    _edge_samples(
        samples,
        "tcp",
        count_delta=5,
        error_delta=0,
        timeout_delta=0,
        histogram_deltas=(6, 5, 5),
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {item.metric_name: item for item in result.edge_metrics}
    assert metrics["edge_request_count"].value == 5
    assert metrics["edge_request_count"].valid is True
    assert metrics["edge_failure_rate"].value == 0
    assert metrics["edge_failure_rate"].valid is True
    latency = metrics["edge_latency_p95"]
    assert latency.value is None
    assert latency.valid is False
    assert latency.invalid_reason == "inconsistent_histogram"
    assert latency.sample_count == 5
    assert latency.coverage == 1
    assert latency.mapping_quality == 1


def test_service_non_monotonic_histogram_invalidates_only_latency(contract):
    samples = [
        item for item in _raw_window(include_dns=False).samples
        if item.service_name != "frontend"
    ]
    _service_samples(
        samples,
        "frontend",
        "node-a",
        series="frontend-series",
        request_delta=5,
        error_delta=0,
        request_histogram_deltas=(6, 5, 5),
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {
        item.metric_name: item
        for item in result.node_metrics
        if item.service_name == "frontend"
    }
    assert metrics["request_rate"].value == 5
    assert metrics["request_failure_rate"].value == 0
    assert metrics["request_latency_p95"].invalid_reason \
        == "inconsistent_histogram"


def test_histogram_count_mismatch_invalidates_latency_only(contract):
    samples = [
        item for item in _raw_window(include_dns=False).samples
        if item.entity_type != "edge"
    ]
    _edge_samples(
        samples,
        "tcp",
        count_delta=5,
        error_delta=0,
        timeout_delta=0,
        histogram_deltas=(2, 4, 4),
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {item.metric_name: item for item in result.edge_metrics}
    assert metrics["edge_request_count"].value == 5
    assert metrics["edge_failure_rate"].value == 0
    assert metrics["edge_latency_p95"].invalid_reason \
        == "inconsistent_histogram"
    assert metrics["edge_latency_p95"].sample_count == 4


def test_formal_tcp_series_birth_is_explicitly_invalid_for_one_window(contract):
    samples = [
        item for item in _raw_window(include_dns=False).samples
        if item.entity_type != "edge"
    ]
    edge_samples = []
    _edge_samples(edge_samples, "tcp")
    samples.extend(
        item for item in edge_samples if item.timestamp_ns == END
    )
    result = FinalWindowAggregator(
        contract,
        formal_tcp_edge_entity_ids=(_edge_id(),),
    ).aggregate(RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    ))
    metrics = {item.metric_name: item for item in result.edge_metrics}
    assert set(metrics) == FORMAL_TCP_METRICS
    assert all(item.value is None for item in metrics.values())
    assert all(item.valid is False for item in metrics.values())
    assert {
        item.invalid_reason for item in metrics.values()
    } == {"series_lifecycle_transition"}
    assert all(item.sample_count == 0 for item in metrics.values())

    recovered = FinalWindowAggregator(
        contract,
        formal_tcp_edge_entity_ids=(_edge_id(),),
    ).aggregate(_raw_window(include_dns=False))
    recovered_metrics = {
        item.metric_name: item for item in recovered.edge_metrics
    }
    assert recovered_metrics["edge_request_count"].value == 50
    assert recovered_metrics["edge_failure_rate"].value == pytest.approx(3 / 50)
    assert recovered_metrics["edge_latency_p95"].value == 10.0
    assert all(item.valid is True for item in recovered_metrics.values())


def test_out_of_scope_dynamic_tcp_edge_cannot_block_formal_projection(contract):
    samples = list(_raw_window(include_dns=False).samples)
    reverse_samples = []
    _edge_samples(
        reverse_samples,
        "tcp",
        src_service="payment",
        dst_service="frontend",
        series="reverse-flow",
    )
    samples.extend(
        item for item in reverse_samples if item.timestamp_ns == END
    )
    result = FinalWindowAggregator(
        contract,
        formal_tcp_edge_entity_ids=(_edge_id(),),
    ).aggregate(RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    ))
    assert len(result.edge_metrics) == 3
    assert {
        item.stable_id.rsplit("::", 1)[0] for item in result.edge_metrics
    } == {_edge_id()}
    assert all(item.valid is True for item in result.edge_metrics)
    assert not any(
        item.source_record_id in result.residual_source_record_ids
        for item in reverse_samples
    )


def test_missing_formal_tcp_edge_fails_the_frozen_projection(contract):
    with pytest.raises(RawCollectionError, match="formal TCP edge scope mismatch"):
        FinalWindowAggregator(
            contract,
            formal_tcp_edge_entity_ids=(
                _edge_id(),
                _edge_id("payment", "frontend"),
            ),
        ).aggregate(_raw_window(include_dns=False))


def test_idle_pressure_and_lock_zero_over_zero_are_no_exposure(contract):
    raw = _raw_window()
    targets = {
        "io_psi_some_ns_total",
        "active_task_ns_total",
        "futex_wait_ns_total",
        "active_thread_ns_total",
    }
    starts = {
        (item.component, item.series_id): item.value
        for item in raw.samples
        if (
            item.service_name == "frontend"
            and item.component in targets
            and item.timestamp_ns == START
        )
    }
    samples = []
    for item in raw.samples:
        if (
            item.service_name == "frontend"
            and item.component in targets
            and item.timestamp_ns == END
        ):
            values = item.to_dict()
            values["value"] = starts[(item.component, item.series_id)]
            values.pop("source_record_id")
            item = RawMetricSample.create(**values)
        samples.append(item)
    idle = RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    )
    result = FinalWindowAggregator(contract).aggregate(idle)
    values = {
        item.metric_name: item
        for item in result.node_metrics
        if item.service_name == "frontend"
    }
    for name in ("io_psi", "futex_wait_time_rate"):
        assert values[name].value is None
        assert values[name].valid is False
        assert values[name].invalid_reason == "no_exposure"


def test_counter_reset_missing_component_and_wrong_unit_fail_closed(contract):
    raw = _raw_window()
    samples = list(raw.samples)
    index = next(
        index for index, item in enumerate(samples)
        if item.component == "request_total"
        and item.service_name == "frontend"
        and item.timestamp_ns == END
    )
    item = samples[index]
    values = item.to_dict()
    values.update(value=0.0)
    values.pop("source_record_id")
    samples[index] = RawMetricSample.create(**values)
    reset = RawCollectionWindow.create(
        sequence=1, window_start_ns=START, window_end_ns=END,
        cluster_id=CLUSTER, samples=samples,
    )
    with pytest.raises(RawCollectionError, match="counter reset"):
        FinalWindowAggregator(contract).aggregate(reset)

    missing = RawCollectionWindow.create(
        sequence=1, window_start_ns=START, window_end_ns=END,
        cluster_id=CLUSTER,
        samples=[
            item for item in raw.samples
            if not (
                item.component == "memory_limit_bytes"
                and item.service_name == "frontend"
            )
        ],
    )
    with pytest.raises(RawCollectionError, match="missing raw component"):
        FinalWindowAggregator(contract).aggregate(missing)

    first = raw.samples[0]
    bad = first.to_dict()
    bad["unit"] = "bytes"
    bad.pop("source_record_id")
    samples = [RawMetricSample.create(**bad), *raw.samples[1:]]
    wrong_unit = RawCollectionWindow.create(
        sequence=1, window_start_ns=START, window_end_ns=END,
        cluster_id=CLUSTER, samples=samples,
    )
    with pytest.raises(RawCollectionError, match="semantics mismatch"):
        FinalWindowAggregator(contract).aggregate(wrong_unit)


def test_collector_builds_versioned_topology_and_seals(contract, tmp_path):
    assert contract["schema_version"] == "probeRCA-final-collection-contract-v4"
    roles = contract["normal_metric_roles"]
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "service"
    } == FORMAL_SERVICE_METRICS
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "host"
    } == FORMAL_HOST_METRICS
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "edge"
    } == FORMAL_TCP_METRICS
    assert all(
        role["protocols"] == ["tcp"]
        for role in roles
        if role["entity_type"] == "edge"
    )

    raw = _raw_window(include_dns=False)
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "test"}),
    )
    window = collector.assemble(
        raw_window=raw,
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    assert window.schema_version == "probeRCA-dataplane-window-v3"
    assert all(
        item.schema_version == METRIC_RECORD_SCHEMA_VERSION
        for item in (*window.node_metrics, *window.edge_metrics)
    )
    snapshot = window.topology_events[0]
    assert len(snapshot.services) == 2
    assert {item.protocol for item in snapshot.call_edges} == {"tcp"}
    assert len(snapshot.service_nodes) == 2
    assert snapshot.valid_from_ns == START
    assert snapshot.valid_to_ns == END
    expected_topology_fingerprint = fingerprint({
        "cluster": CLUSTER,
        "services": list(snapshot.services),
        "calls": [item.to_dict() for item in snapshot.call_edges],
        "hosts": [item.to_dict() for item in snapshot.host_edges],
        "bindings": [item.to_dict() for item in snapshot.service_resources],
    })
    assert snapshot.structure_fingerprint == expected_topology_fingerprint
    assert snapshot.snapshot_id == fingerprint({
        "structure_fingerprint": expected_topology_fingerprint,
        "runtime_identity_fingerprints": list(
            snapshot.runtime_identity_fingerprints
        ),
        "window_start_ns": START,
        "window_end_ns": END,
    })
    assert snapshot.snapshot_id != snapshot.structure_fingerprint
    assert set(snapshot.service_runtime_identity_fingerprints) == {
        f"{CLUSTER}::{NAMESPACE}::frontend",
        f"{CLUSTER}::{NAMESPACE}::payment",
    }
    assert set().union(*map(
        set, snapshot.service_runtime_identity_fingerprints.values()
    )) == set(snapshot.runtime_identity_fingerprints)
    assert {
        item.metric_name for item in window.edge_metrics
    } == FORMAL_TCP_METRICS
    assert {item.protocol for item in window.edge_metrics} == {"tcp"}

    metadata = window.collection_metadata
    dataset_id = fingerprint({"dataset": "healthy"})
    archive_root = tmp_path / "archive"
    writer = CollectionArchiveWriter(
        archive_root,
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )
    writer.append(window)
    archive = writer.seal()
    assert archive.schema_version == "probeRCA-dataplane-archive-v3"
    assert archive.window_count == 1
    assert archive.dataset_id == dataset_id
    assert archive.windows_sha256 == hashlib.sha256(
        (archive.root / archive.windows_file).read_bytes()
    ).hexdigest()

    reloaded = CollectionArchive.load(archive_root)
    assert reloaded.dataset_id == dataset_id
    assert reloaded.windows_sha256 == archive.windows_sha256
    assert reloaded.manifest_fingerprint == archive.manifest_fingerprint
    assert tuple(
        item.to_dict() for item in reloaded.iter_windows()
    ) == (window.to_dict(),)


def test_new_archive_serializes_null_and_legacy_v2_is_projected_in_memory(
    contract,
    tmp_path,
):
    samples = []
    _service_samples(
        samples, "frontend", "node-a", series="frontend-series",
        request_delta=0, error_delta=0,
        request_histogram_deltas=(0, 0, 0),
    )
    _service_samples(
        samples, "payment", "node-b", series="payment-series",
    )
    _host_samples(samples, "node-a")
    _host_samples(samples, "node-b")
    _edge_samples(
        samples, "tcp", count_delta=0, error_delta=0,
        timeout_delta=0, histogram_deltas=(0, 0, 0),
    )
    raw = RawCollectionWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
    )
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "missing-value-test"}),
    )
    window = collector.assemble(
        raw_window=raw,
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    current_root = tmp_path / "current"
    current = CollectionArchiveWriter(
        current_root,
        dataset_id=fingerprint({"dataset": "missing-value-current"}),
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=window.collection_metadata,
    )
    current.append(window)
    current_archive = current.seal()
    raw_json = (
        current_archive.root / current_archive.windows_file
    ).read_text(encoding="utf-8")
    payload = json.loads(raw_json)
    invalid_records = [
        item
        for item in (*payload["node_metrics"], *payload["edge_metrics"])
        if item["valid"] is False
    ]
    assert invalid_records
    assert all(item["value"] is None for item in invalid_records)
    assert {
        item["invalid_reason"] for item in invalid_records
    } == {"no_exposure"}
    assert '"value":null' in raw_json
    assert '"valid":false' in raw_json
    assert '"invalid_reason":"no_exposure"' in raw_json

    reloaded_window = next(current_archive.iter_windows())
    assert any(
        item.value is None
        and item.valid is False
        and item.invalid_reason == "no_exposure"
        for item in (
            *reloaded_window.node_metrics,
            *reloaded_window.edge_metrics,
        )
    )
    services = {}
    hosts = {}
    for item in reloaded_window.node_metrics:
        target = services if item.scope == "service" else hosts
        target.setdefault(item.stable_id.rsplit("::", 1)[0], set()).add(
            item.metric_name
        )
    edges = {}
    for item in reloaded_window.edge_metrics:
        edge_id = (
            item.cluster_id,
            item.namespace,
            item.src_service,
            item.dst_service,
            item.protocol,
        )
        edges.setdefault(edge_id, set()).add(item.metric_name)
    assert all(len(names) == 9 for names in services.values())
    assert all(len(names) == 4 for names in hosts.values())
    assert all(len(names) == 3 for names in edges.values())

    legacy_window = window.to_dict()
    for item in (
        *legacy_window["node_metrics"],
        *legacy_window["edge_metrics"],
    ):
        if item["valid"] is False:
            item["value"] = 0.0
            item["sample_count"] = 0
            item["coverage"] = 0.0
        item["schema_version"] = "1.0"
        item.pop("valid")
        item.pop("invalid_reason")
        item.pop("mapping_quality")
    legacy_window["schema_version"] = "probeRCA-dataplane-window-v2"
    legacy_window.pop("window_fingerprint")
    legacy_window["window_fingerprint"] = fingerprint(legacy_window)
    legacy_root = tmp_path / "legacy-v2"
    legacy_root.mkdir()
    legacy_windows_path = legacy_root / "collected-windows.jsonl"
    legacy_windows_path.write_text(
        canonical_json(legacy_window) + "\n",
        encoding="utf-8",
    )
    legacy_sha = hashlib.sha256(legacy_windows_path.read_bytes()).hexdigest()
    manifest = json.loads(
        (current_archive.root / "collection-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    manifest.update(
        schema_version="probeRCA-dataplane-archive-v2",
        windows_sha256=legacy_sha,
    )
    manifest.pop("manifest_fingerprint")
    manifest["manifest_fingerprint"] = fingerprint(manifest)
    (legacy_root / "collection-manifest.json").write_text(
        canonical_json(manifest) + "\n",
        encoding="utf-8",
    )

    legacy_archive = CollectionArchive.load(legacy_root)
    projected = next(legacy_archive.iter_windows())
    assert hashlib.sha256(legacy_windows_path.read_bytes()).hexdigest() == legacy_sha
    assert (
        legacy_archive.collection_contract_fingerprint
        == manifest["collection_contract_fingerprint"]
    )
    assert projected.to_dict() == legacy_window
    edge_records = {
        item.metric_name: item for item in projected.edge_metrics
    }
    assert edge_records["edge_request_count"].value == 0.0
    assert edge_records["edge_request_count"].valid is True
    for name in ("edge_latency_p95", "edge_failure_rate"):
        assert edge_records[name].value is None
        assert edge_records[name].valid is False
        assert edge_records[name].invalid_reason == "no_exposure"
    new_writer = CollectionArchiveWriter(
        tmp_path / "must-not-reseal-legacy",
        dataset_id=fingerprint({"dataset": "must-not-reseal-legacy"}),
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=projected.collection_metadata,
    )
    with pytest.raises(
        ValueError,
        match="only current-schema windows",
    ):
        new_writer.append(projected)


def _aligned_test_pair(contract, sequence):
    offset = (sequence - 1) * 1_000_000_000
    raw = _raw_window(include_dns=False)
    shifted = []
    for item in raw.samples:
        payload = item.to_dict()
        payload["timestamp_ns"] += offset
        payload.pop("source_record_id")
        shifted.append(RawMetricSample.create(**payload))
    raw = RawCollectionWindow.create(
        sequence=sequence,
        window_start_ns=START + offset,
        window_end_ns=END + offset,
        cluster_id=CLUSTER,
        samples=shifted,
    )
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "aligned-writer"}),
    )
    normal = collector.assemble(
        raw_window=raw,
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    burst = RawBurstWindow.create(
        sequence=sequence,
        window_start_ns=START + offset,
        window_end_ns=END + offset,
        cluster_id=CLUSTER,
        samples=(),
        event_source_fingerprint=fingerprint({"source": "burst"}),
        burst_config_fingerprint=contract["burst_config_fingerprint"],
        event_loss_rate=0.0,
    )
    return normal, burst


def _aligned_test_writers(contract, tmp_path, metadata):
    dataset_id = fingerprint({"dataset": str(tmp_path)})
    normal = CollectionArchiveWriter(
        tmp_path / "normal",
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )
    burst = BurstArchiveWriter(
        tmp_path / "burst",
        dataset_id=dataset_id,
        cluster_id=CLUSTER,
        event_source_fingerprint=fingerprint({"source": "burst"}),
        burst_config_fingerprint=contract["burst_config_fingerprint"],
    )
    return normal, burst


def test_aligned_incremental_failure_keeps_equal_partial_archives(
    contract, tmp_path, capsys,
):
    first = _aligned_test_pair(contract, 1)
    normal_writer, burst_writer = _aligned_test_writers(
        contract, tmp_path, first[0].collection_metadata,
    )

    class Runner:
        @staticmethod
        def iter_collect_aligned(_window_count):
            yield first
            for sequence in range(2, 165):
                yield _aligned_test_pair(contract, sequence)
            raise RawCollectionError("deterministic sequence 165 failure")

    with pytest.raises(
        RawCollectionError, match="deterministic sequence 165"
    ):
        _write_aligned_windows(
            runner=Runner(),
            normal_writer=normal_writer,
            burst_writer=burst_writer,
            window_count=200,
        )
    normal_lines = (
        tmp_path / "normal" / "collected-windows.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    burst_lines = (
        tmp_path / "burst" / "burst-windows.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    assert len(normal_lines) == len(burst_lines) == 164
    assert [
        json.loads(line)["sequence"] for line in normal_lines
    ] == list(range(1, 165))
    assert [
        json.loads(line)["sequence"] for line in burst_lines
    ] == list(range(1, 165))
    assert not (tmp_path / "normal" / "collection-manifest.json").exists()
    assert not (tmp_path / "burst" / "burst-manifest.json").exists()
    failure = json.loads(capsys.readouterr().err)
    assert failure["last_committed_sequence"] == 164
    assert failure["partial_archives_aligned"] is True


def test_aligned_incremental_success_seals_matching_archives(
    contract, tmp_path,
):
    pairs = tuple(_aligned_test_pair(contract, index) for index in range(1, 4))
    normal_writer, burst_writer = _aligned_test_writers(
        contract, tmp_path, pairs[0][0].collection_metadata,
    )

    class Runner:
        @staticmethod
        def iter_collect_aligned(_window_count):
            yield from pairs

    _write_aligned_windows(
        runner=Runner(),
        normal_writer=normal_writer,
        burst_writer=burst_writer,
        window_count=3,
    )
    normal = normal_writer.seal()
    burst = burst_writer.seal()
    assert normal.window_count == burst.window_count == 3
    assert normal.dataset_id == burst.dataset_id
    assert [
        (item.sequence, item.window_start_ns, item.window_end_ns)
        for item in normal.iter_windows()
    ] == [
        (item.sequence, item.window_start_ns, item.window_end_ns)
        for item in burst.iter_windows()
    ]


def test_raw_primitive_archive_round_trip_is_write_once_and_content_verified(
    tmp_path,
):
    dataset_id = fingerprint({"dataset": "raw-primitives"})
    source_fingerprint = fingerprint({"source": "worker-1-prometheus"})
    writer = RawPrimitiveArchiveWriter(
        tmp_path / "raw",
        dataset_id=dataset_id,
        source_fingerprint=source_fingerprint,
    )
    raw = _raw_window(include_dns=False)
    writer.append(raw)
    manifest = writer.seal()
    archive = RawPrimitiveArchive(tmp_path / "raw")
    assert archive.manifest == manifest
    assert tuple(item.to_dict() for item in archive.iter_windows()) == (
        raw.to_dict(),
    )
    with pytest.raises(RawCollectionError, match="lowercase SHA-256"):
        RawPrimitiveArchiveWriter(
            tmp_path / "bad-id",
            dataset_id="G" * 64,
            source_fingerprint=source_fingerprint,
        )
    windows_path = tmp_path / "raw" / "raw-primitive-windows.jsonl"
    windows_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(RawCollectionError, match="integrity"):
        RawPrimitiveArchive(tmp_path / "raw")


def test_campaign_dataset_package_requires_three_aligned_raw_worker_archives(
    contract, tmp_path,
):
    normal_window, burst_window = _aligned_test_pair(contract, 1)
    normal_writer, burst_writer = _aligned_test_writers(
        contract, tmp_path, normal_window.collection_metadata,
    )
    normal_writer.append(normal_window)
    burst_writer.append(burst_window)
    normal = normal_writer.seal()
    burst_writer.seal()
    raw = _raw_window(include_dns=False)
    for worker in ("worker-1", "worker-2", "worker-3"):
        writer = RawPrimitiveArchiveWriter(
            tmp_path / "primitives" / worker,
            dataset_id=normal.dataset_id,
            source_fingerprint=fingerprint({"source": worker}),
        )
        writer.append(raw)
        writer.seal()
        audit = tmp_path / "raw-events" / worker
        audit.mkdir(parents=True)
        events = audit / "filtered-ebpf-events.jsonl"
        checkpoints = audit / "checkpoints.jsonl"
        events.write_text("", encoding="utf-8")
        checkpoints.write_text("", encoding="utf-8")
        event_core = {
            "schema_version": "probeRCA-filtered-burst-raw-events-v1",
            "event_source_fingerprint": fingerprint({"events": worker}),
            "burst_config_fingerprint": contract["burst_config_fingerprint"],
            "start_ns": normal.start_ns,
            "end_ns": normal.end_ns,
            "boundary_count": normal.window_count + 1,
            "event_count": 0,
            "checkpoint_count": 0,
            "events_sha256": hashlib.sha256(b"").hexdigest(),
            "checkpoints_sha256": hashlib.sha256(b"").hexdigest(),
        }
        (audit / "raw-event-manifest.json").write_text(
            canonical_json({
                **event_core,
                "manifest_fingerprint": fingerprint(event_core),
            }) + "\n",
            encoding="utf-8",
        )
    result = seal_dataset_directory(tmp_path, {
        "dataset_id": normal.dataset_id,
        "case_id": "H-DEV",
        "git_sha": "a" * 40,
        "load_profile_id": "multi-node-open-loop-40",
        "load_profile_fingerprint": "b" * 64,
        "public_manifest_fingerprint": "c" * 64,
        "campaign_config_fingerprint": "d" * 64,
    })
    assert result["sealed"] is True
    assert (tmp_path / "dataset-manifest.json").is_file()
    sums = (tmp_path / "SHA256SUMS").read_text(encoding="utf-8")
    assert "normal/collection-manifest.json" in sums
    assert "burst/burst-manifest.json" in sums
    assert sums.count("raw-primitive-manifest.json") == 3
    assert sums.count("raw-event-manifest.json") == 3
    with pytest.raises(DatasetPackageError, match="already sealed"):
        seal_dataset_directory(tmp_path, {
            "dataset_id": normal.dataset_id, "case_id": "H-DEV",
            "git_sha": "a" * 40,
            "load_profile_id": "multi-node-open-loop-40",
            "load_profile_fingerprint": "b" * 64,
            "public_manifest_fingerprint": "c" * 64,
            "campaign_config_fingerprint": "d" * 64,
        })


def test_global_resource_watermark_change_does_not_fake_layout_change(
    contract,
):
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "test"}),
    )
    before = _revision("rv-1")
    after = _revision("rv-2")
    window = collector.assemble(
        raw_window=_raw_window(),
        inventory_at_start=before,
        inventory_at_end=after,
    )
    assert len(window.topology_events) == 1


def test_runtime_identity_change_inside_window_is_rejected(contract):
    before = _revision("rv-1")
    after = copy.deepcopy(before)
    after.objects_by_kind["Pod"]["pod-payment"]["status"][
        "containerStatuses"
    ][0]["containerID"] = "containerd://replacement-payment"
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "test"}),
    )
    with pytest.raises(RawCollectionError, match="runtime identity"):
        collector.assemble(
            raw_window=_raw_window(),
            inventory_at_start=before,
            inventory_at_end=after,
        )


def test_empty_service_window_distinguishes_zero_count_from_no_exposure(
    contract,
):
    samples = []
    _service_samples(
        samples, "frontend", "node-a", series="frontend-series",
        request_delta=0, error_delta=0,
        request_histogram_deltas=(0, 0, 0),
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {item.metric_name: item for item in result.node_metrics}
    assert len(metrics) == 9
    assert metrics["request_rate"].value == 0.0
    assert metrics["request_rate"].valid is True
    assert metrics["request_rate"].invalid_reason is None
    assert metrics["request_failure_rate"].value is None
    assert metrics["request_failure_rate"].valid is False
    assert metrics["request_failure_rate"].invalid_reason == "no_exposure"
    assert metrics["request_failure_rate"].sample_count == 0
    assert metrics["request_failure_rate"].coverage > 0
    latency = metrics["request_latency_p95"]
    assert latency.value is None
    assert latency.valid is False
    assert latency.invalid_reason == "no_exposure"
    assert latency.sample_count == 0
    assert latency.coverage > 0


def test_inactive_edge_keeps_stable_identity_with_missing_latency(
    contract,
):
    samples = []
    _service_samples(
        samples, "frontend", "node-a", series="frontend-series"
    )
    _service_samples(
        samples, "payment", "node-b", series="payment-series"
    )
    _host_samples(samples, "node-a")
    _host_samples(samples, "node-b")
    _edge_samples(
        samples, "tcp", count_delta=0, error_delta=0,
        timeout_delta=0, histogram_deltas=(0, 0, 0),
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    metrics = {item.metric_name: item for item in result.edge_metrics}
    assert set(metrics) == {
        "edge_request_count", "edge_latency_p95", "edge_failure_rate",
    }
    assert metrics["edge_request_count"].value == 0.0
    assert metrics["edge_request_count"].valid is True
    assert metrics["edge_request_count"].invalid_reason is None
    assert metrics["edge_failure_rate"].value is None
    assert metrics["edge_failure_rate"].valid is False
    assert metrics["edge_failure_rate"].invalid_reason == "no_exposure"
    assert metrics["edge_failure_rate"].sample_count == 0
    assert metrics["edge_failure_rate"].coverage > 0
    assert metrics["edge_latency_p95"].value is None
    assert metrics["edge_latency_p95"].valid is False
    assert metrics["edge_latency_p95"].invalid_reason == "no_exposure"
    assert metrics["edge_latency_p95"].coverage > 0


def test_metric_record_validity_invariants(contract):
    record = FinalWindowAggregator(contract).aggregate(
        _raw_window(include_dns=False)
    ).node_metrics[0]
    assert isinstance(record, NodeMetricRecord)
    assert record.schema_version == METRIC_RECORD_SCHEMA_VERSION
    assert record.valid is True
    assert record.invalid_reason is None

    with pytest.raises((TypeError, ValueError)):
        replace(record, value=None)
    with pytest.raises(ValueError, match="value=None"):
        replace(
            record,
            value=0.0,
            valid=False,
            invalid_reason="no_exposure",
        )
    with pytest.raises(ValueError, match="must not have invalid_reason"):
        replace(record, invalid_reason="no_exposure")
    with pytest.raises(ValueError, match="non-empty invalid_reason"):
        replace(record, value=None, valid=False, invalid_reason=None)
    with pytest.raises(ValueError, match="non-empty invalid_reason"):
        replace(record, value=None, valid=False, invalid_reason="")
    with pytest.raises(ValueError, match="finite"):
        replace(record, value=float("nan"))
    with pytest.raises(ValueError, match="finite"):
        replace(record, value=float("inf"))
    with pytest.raises(ValueError, match="unsupported metric invalid_reason"):
        replace(
            record,
            value=None,
            valid=False,
            invalid_reason="unknown_reason",
        )
    for reason in {
        "no_exposure",
        "zero_coverage",
        "insufficient_sample_count",
        "excessive_event_loss",
        "missing_component",
        "series_lifecycle_transition",
    }:
        projected = replace(
            record,
            value=None,
            valid=False,
            invalid_reason=reason,
        )
        assert projected.invalid_reason in METRIC_INVALID_REASONS


def test_zero_socket_operations_are_no_exposure(contract):
    samples = []
    _service_samples(
        samples,
        "frontend",
        "node-a",
        series="frontend-series",
        socket_failure_deltas=(0, 0, 0, 0),
        socket_ops_delta=0,
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    record = next(
        item for item in result.node_metrics
        if item.metric_name == "local_socket_failure_rate"
    )
    assert record.value is None
    assert record.valid is False
    assert record.invalid_reason == "no_exposure"
    assert record.coverage > 0


def test_positive_requests_with_zero_failures_are_valid_zero(contract):
    samples = []
    _service_samples(
        samples,
        "frontend",
        "node-a",
        series="frontend-series",
        request_delta=100,
        error_delta=0,
    )
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    record = next(
        item for item in result.node_metrics
        if item.metric_name == "request_failure_rate"
    )
    assert record.value == 0.0
    assert record.valid is True
    assert record.invalid_reason is None


def test_zero_input_coverage_is_explicitly_invalid(contract):
    raw = _raw_window(include_dns=False)
    samples = []
    for item in raw.samples:
        if (
            item.component == "request_total"
            and item.service_name == "frontend"
        ):
            payload = item.to_dict()
            payload["coverage"] = 0.0
            payload.pop("source_record_id")
            item = RawMetricSample.create(**payload)
        samples.append(item)
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    record = next(
        item for item in result.node_metrics
        if item.service_name == "frontend"
        and item.metric_name == "request_rate"
    )
    assert record.value is None
    assert record.valid is False
    assert record.invalid_reason == "zero_coverage"
    assert record.coverage == 0.0


def test_coverage_and_mapping_quality_remain_separate(contract):
    raw = _raw_window(include_dns=False)
    samples = []
    for item in raw.samples:
        if (
            item.component == "request_total"
            and item.service_name == "frontend"
        ):
            payload = item.to_dict()
            payload["coverage"] = 0.8
            payload["mapping_quality"] = 0.5
            payload.pop("source_record_id")
            item = RawMetricSample.create(**payload)
        samples.append(item)
    result = FinalWindowAggregator(contract).aggregate(
        RawCollectionWindow.create(
            sequence=1,
            window_start_ns=START,
            window_end_ns=END,
            cluster_id=CLUSTER,
            samples=samples,
        )
    )
    record = next(
        item for item in result.node_metrics
        if item.service_name == "frontend"
        and item.metric_name == "request_rate"
    )
    assert record.valid is True
    assert record.value == 100.0
    assert record.coverage == pytest.approx(0.8)
    assert record.mapping_quality == pytest.approx(0.5)


def _calibrations(contract):
    output = []
    for role in contract["burst_channel_roles"]:
        mode = BURST_CHANNEL_MODES[role["channel_id"]]
        output.append(BurstChannelCalibration.create(
            channel_id=role["channel_id"],
            mode=mode,
            rare_event_threshold=0.1 if mode == "rare" else None,
            healthy_values=[] if mode == "rare" else [1, 2, 3, 4, 5],
            transform="identity",
            polarity=1,
            z_cap=5.0,
            minimum_healthy_samples=5,
            minimum_scale=1.0e-6,
        ))
    return output


def test_burst_event_and_continuous_channels_are_disjoint():
    channels = set(
        SERVICE_CHANNELS + HOST_CHANNELS + TCP_CHANNELS + DNS_CHANNELS
    )
    assert set(BURST_CHANNEL_MODES) == channels
    assert {
        channel for channel, mode in BURST_CHANNEL_MODES.items()
        if mode == "rare"
    } == set(RARE_CHANNELS)
    assert {
        channel for channel, mode in BURST_CHANNEL_MODES.items()
        if mode == "continuous"
    } == channels - set(RARE_CHANNELS)

    with pytest.raises(
        RawCollectionError, match="only a positive threshold"
    ):
        BurstChannelCalibration.create(
            channel_id="tcp.rto_rate",
            mode="rare",
            rare_event_threshold=0.1,
            healthy_values=[0.0],
            transform="identity",
            polarity=1,
            z_cap=5.0,
            minimum_healthy_samples=5,
            minimum_scale=1.0e-6,
        )
    with pytest.raises(
        RawCollectionError, match="cannot declare a rare threshold"
    ):
        BurstChannelCalibration.create(
            channel_id="tcp.rtt_p95",
            mode="continuous",
            rare_event_threshold=0.1,
            healthy_values=[1, 2, 3, 4, 5],
            transform="identity",
            polarity=1,
            z_cap=5.0,
            minimum_healthy_samples=5,
            minimum_scale=1.0e-6,
        )
    common = {
        "source_object_id": "object:" + fingerprint({"probe": "typed"}),
        "timestamp_ns": END - 1,
        "cluster_id": CLUSTER,
        "namespace": NAMESPACE,
        "entity_type": "edge",
        "entity_id": f"{CLUSTER}::{NAMESPACE}::frontend->payment::tcp",
        "value": 1,
        "coverage": 1.0,
        "event_loss_rate": 0.0,
        "mapping_quality": 1.0,
    }
    with pytest.raises(RawCollectionError, match="requires exposure"):
        RawBurstSample.create(
            **common,
            channel_id="tcp.rto_rate",
            exposure=None,
        )
    with pytest.raises(RawCollectionError, match="cannot declare exposure"):
        RawBurstSample.create(
            **common,
            channel_id="tcp.rtt_p95",
            exposure=10,
        )


def test_burst_is_normalized_from_independent_sources(contract):
    build_id = fingerprint({"build": "test"})
    builder = BurstEvidenceCollector(
        collection_contract=contract,
        collector_build_id=build_id,
        calibrations=_calibrations(contract),
    )
    entity = f"{CLUSTER}::{NAMESPACE}::frontend->payment::tcp"
    sample = RawBurstSample.create(
        source_object_id="object:" + fingerprint({"probe": "tcp-rto"}),
        timestamp_ns=END - 1,
        cluster_id=CLUSTER,
        namespace=NAMESPACE,
        entity_type="edge",
        entity_id=entity,
        channel_id="tcp.rto_rate",
        value=1,
        exposure=10,
        coverage=1.0,
        event_loss_rate=0.0,
        mapping_quality=1.0,
    )
    evidence = builder.collect(
        samples=(sample,),
        window_start_ns=START,
        window_end_ns=END,
        residual_source_record_ids=(),
    )
    assert len(evidence) == 1
    assert evidence[0].normalized_strength == pytest.approx(1.0)
    assert evidence[0].reliability_weight == pytest.approx(1.0)
    with pytest.raises(RawCollectionError, match="overlaps"):
        builder.collect(
            samples=(sample,),
            window_start_ns=START,
            window_end_ns=END,
            residual_source_record_ids=(sample.source_record_id,),
        )


def test_zero_exposure_rare_event_uses_window_count_not_inverse_epsilon():
    assert burst_event_rate(3, 0.0) == 3.0
    assert rare_event_strength(1, 0.0, 10.0) == pytest.approx(0.1)


def test_continuous_burst_calibration_is_target_scoped(contract):
    channel_id = "tcp.rtt_p95"
    calibrations = [
        item for item in _calibrations(contract)
        if item.channel_id != channel_id
    ]
    target_a = f"{CLUSTER}::{NAMESPACE}::frontend->payment::tcp"
    target_b = f"{CLUSTER}::{NAMESPACE}::frontend->currency::tcp"
    for target_id, values in (
        (target_a, [1.0] * 5),
        (target_b, [100.0] * 5),
    ):
        calibrations.append(BurstChannelCalibration.create(
            channel_id=channel_id,
            target_id=target_id,
            mode="continuous",
            rare_event_threshold=None,
            healthy_values=values,
            transform="identity",
            polarity=1,
            z_cap=5.0,
            minimum_healthy_samples=5,
            minimum_scale=1.0,
        ))
    builder = BurstEvidenceCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "target-scoped"}),
        calibrations=calibrations,
    )

    def strength(target_id):
        sample = RawBurstSample.create(
            source_object_id="object:" + fingerprint({"target": target_id}),
            timestamp_ns=END - 1,
            cluster_id=CLUSTER,
            namespace=NAMESPACE,
            entity_type="edge",
            entity_id=target_id,
            channel_id=channel_id,
            value=10.0,
            exposure=None,
            coverage=1.0,
            event_loss_rate=0.0,
            mapping_quality=1.0,
        )
        return builder.collect(
            samples=(sample,),
            window_start_ns=START,
            window_end_ns=END,
            residual_source_record_ids=(),
        )[0].normalized_strength

    assert strength(target_a) == 1.0
    assert strength(target_b) == 0.0


def test_continuous_burst_reference_is_fitted_once_per_collector(
    contract, monkeypatch,
):
    calls = []
    original = burst_collection_module.fit_continuous_burst_reference

    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        burst_collection_module,
        "fit_continuous_burst_reference",
        counted,
    )
    builder = BurstEvidenceCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "cached-reference"}),
        calibrations=_calibrations(contract),
    )
    fitted_count = len(calls)
    formal_channels = {
        role["channel_id"] for role in contract["burst_channel_roles"]
    }
    assert fitted_count == sum(
        BURST_CHANNEL_MODES[channel_id] == "continuous"
        for channel_id in formal_channels
    )
    sample = RawBurstSample.create(
        source_object_id="object:" + fingerprint({"probe": "tcp-rtt-cache"}),
        timestamp_ns=END - 1,
        cluster_id=CLUSTER,
        namespace=NAMESPACE,
        entity_type="edge",
        entity_id=f"{CLUSTER}::{NAMESPACE}::frontend->payment::tcp",
        channel_id="tcp.rtt_p95",
        value=10,
        exposure=None,
        coverage=1.0,
        event_loss_rate=0.0,
        mapping_quality=1.0,
    )

    first = builder.collect(
        samples=(sample,),
        window_start_ns=START,
        window_end_ns=END,
        residual_source_record_ids=(),
    )
    second = builder.collect(
        samples=(sample,),
        window_start_ns=START,
        window_end_ns=END,
        residual_source_record_ids=(),
    )

    assert len(calls) == fitted_count
    assert first[0].normalized_strength == second[0].normalized_strength


def _burst_sample_for_role(role, *, suffix):
    channel_id = role["channel_id"]
    entity_type = role["entity_types"][0]
    entity_id = {
        "service": f"{CLUSTER}::{NAMESPACE}::frontend",
        "host": f"{CLUSTER}::host::node-a",
        "edge": _edge_id(),
    }[entity_type]
    mode = BURST_CHANNEL_MODES[channel_id]
    return RawBurstSample.create(
        source_object_id="object:" + fingerprint({
            "channel": channel_id, "suffix": suffix,
        }),
        timestamp_ns=END - 1,
        cluster_id=CLUSTER,
        namespace=NAMESPACE if entity_type != "host" else "_host",
        entity_type=entity_type,
        entity_id=entity_id,
        channel_id=channel_id,
        value=0.0,
        exposure=1.0 if mode == "rare" else None,
        coverage=1.0,
        event_loss_rate=0.0,
        mapping_quality=1.0,
    )


def _sealed_raw_burst(tmp_path, contract, samples, *, dataset_id):
    writer = BurstArchiveWriter(
        tmp_path,
        dataset_id=dataset_id,
        cluster_id=CLUSTER,
        event_source_fingerprint=fingerprint({"source": str(tmp_path)}),
        burst_config_fingerprint=contract["burst_config_fingerprint"],
    )
    writer.append(RawBurstWindow.create(
        sequence=1,
        window_start_ns=START,
        window_end_ns=END,
        cluster_id=CLUSTER,
        samples=samples,
        event_source_fingerprint=writer.event_source_fingerprint,
        burst_config_fingerprint=contract["burst_config_fingerprint"],
        event_loss_rate=0.0,
    ))
    return writer.seal()


def test_healthy_burst_calibration_is_frozen_and_round_trips(
    contract, tmp_path,
):
    samples = tuple(
        _burst_sample_for_role(role, suffix="healthy")
        for role in contract["burst_channel_roles"]
    )
    archive = _sealed_raw_burst(
        tmp_path / "healthy-burst",
        contract,
        samples,
        dataset_id=fingerprint({"dataset": "healthy-burst"}),
    )
    rare = {
        role["channel_id"]: 0.1
        for role in contract["burst_channel_roles"]
        if BURST_CHANNEL_MODES[role["channel_id"]] == "rare"
    }
    policy = BurstCalibrationPolicy.create(
        rare_event_thresholds=rare,
        continuous_transform="identity",
        continuous_polarity=1,
        continuous_z_cap=5.0,
        continuous_minimum_healthy_samples=1,
        continuous_minimum_scale=1.0e-6,
    )
    artifact = calibrate_healthy_burst(
        archive=archive,
        collection_contract=contract,
        policy=policy,
    )
    assert {item.channel_id for item in artifact.calibrations} == {
        role["channel_id"] for role in contract["burst_channel_roles"]
    }
    assert all(item.target_id != "*" for item in artifact.calibrations)
    assert artifact.source_dataset_id == archive.dataset_id
    path = tmp_path / "burst-calibration.json"
    artifact.save(path)
    assert BurstCalibrationArtifact.load(path) == artifact
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(RawCollectionError, match="already exists"):
        artifact.save(path)

    missing = dict(rare)
    missing.pop(next(iter(missing)))
    bad_policy = BurstCalibrationPolicy.create(
        rare_event_thresholds=missing,
        continuous_transform="identity",
        continuous_polarity=1,
        continuous_z_cap=5.0,
        continuous_minimum_healthy_samples=1,
        continuous_minimum_scale=1.0e-6,
    )
    with pytest.raises(RawCollectionError, match="cover every formal"):
        calibrate_healthy_burst(
            archive=archive,
            collection_contract=contract,
            policy=bad_policy,
        )


def test_formal_burst_policy_is_fingerprinted_and_target_calibrated():
    payload = yaml.safe_load(Path(
        "configs/final_burst_calibration_policy.yaml"
    ).read_text(encoding="utf-8"))
    policy = BurstCalibrationPolicy.from_dict(payload)
    assert policy.rare_event_quantile == pytest.approx(0.999)
    assert policy.continuous_transform == "log1p"
    assert policy.continuous_minimum_healthy_samples == 300


def test_read_only_burst_join_aligns_evidence_without_mutating_archives(
    contract, tmp_path,
):
    build_id = fingerprint({"build": "burst-join"})
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=build_id,
    )
    normal_window = collector.assemble(
        raw_window=_raw_window(include_dns=False),
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    dataset_id = fingerprint({"dataset": "burst-join"})
    normal_writer = CollectionArchiveWriter(
        tmp_path / "normal",
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata={
            "collector_build_fingerprint": build_id,
            "aggregation_config_fingerprint": contract[
                "aggregation_config_fingerprint"
            ],
            "burst_config_fingerprint": contract[
                "burst_config_fingerprint"
            ],
        },
    )
    normal_writer.append(normal_window)
    normal = normal_writer.seal()
    raw_sample = RawBurstSample.create(
        source_object_id="object:" + fingerprint({"probe": "joined-rto"}),
        timestamp_ns=END - 1,
        cluster_id=CLUSTER,
        namespace=NAMESPACE,
        entity_type="edge",
        entity_id=_edge_id(),
        channel_id="tcp.rto_rate",
        value=1.0,
        exposure=10.0,
        coverage=1.0,
        event_loss_rate=0.0,
        mapping_quality=1.0,
    )
    burst = _sealed_raw_burst(
        tmp_path / "burst",
        contract,
        (raw_sample,),
        dataset_id=dataset_id,
    )
    artifact = BurstCalibrationArtifact.create(
        source_archive=burst,
        policy_fingerprint=fingerprint({"policy": "test"}),
        calibrations=_calibrations(contract),
    )
    before_normal = normal.windows_sha256
    before_burst = burst.windows_sha256
    joined = BurstJoinedCollectionArchive.create(
        normal_archive=normal,
        burst_archive=burst,
        calibration_artifact=artifact,
    )
    windows = tuple(joined.iter_windows())
    assert len(windows) == 1
    assert len(windows[0].burst_evidence) == 1
    assert windows[0].burst_evidence[0].channel_id == "tcp.rto_rate"
    assert windows[0].burst_evidence[0].normalized_strength == pytest.approx(
        1.0
    )
    assert windows[0].burst_evidence[0].source_record_ids == [
        raw_sample.source_record_id
    ]
    assert CollectionArchive.load(normal.root).windows_sha256 == before_normal
    assert BurstArchive.load(burst.root).windows_sha256 == before_burst


def test_read_only_burst_join_rejects_boundary_mismatch(contract, tmp_path):
    dataset_id = fingerprint({"dataset": "mismatch"})
    build_id = fingerprint({"build": "mismatch"})
    normal_window = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=build_id,
    ).assemble(
        raw_window=_raw_window(include_dns=False),
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    normal_writer = CollectionArchiveWriter(
        tmp_path / "normal-mismatch",
        dataset_id=dataset_id,
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata={
            "collector_build_fingerprint": build_id,
            "aggregation_config_fingerprint": contract[
                "aggregation_config_fingerprint"
            ],
            "burst_config_fingerprint": contract[
                "burst_config_fingerprint"
            ],
        },
    )
    normal_writer.append(normal_window)
    normal = normal_writer.seal()
    burst = _sealed_raw_burst(
        tmp_path / "burst-mismatch",
        contract,
        (),
        dataset_id=dataset_id,
    )
    object.__setattr__(burst, "end_ns", burst.end_ns + 1)
    artifact = BurstCalibrationArtifact.create(
        source_archive=burst,
        policy_fingerprint=fingerprint({"policy": "test"}),
        calibrations=_calibrations(contract),
    )
    with pytest.raises(RawCollectionError, match="identities do not align"):
        BurstJoinedCollectionArchive.create(
            normal_archive=normal,
            burst_archive=burst,
            calibration_artifact=artifact,
        )


def test_prometheus_source_rejects_preaggregated_queries():
    payload = {
        "query_id": "bad",
        "component": "request_total",
        "promql": "sum(rate(request_total[1m])) by (service)",
        "label_mapping": {
            "namespace": "namespace",
            "pod": "pod",
            "series": "pod",
        },
        "required_labels": ["namespace", "pod"],
        "optional_labels": [],
        "series_labels": ["series"],
        "histogram_le_label": None,
        "value_scale": 1.0,
        "histogram_bound_scale": 1.0,
    }
    with pytest.raises(RawCollectionError, match="pre-aggregation"):
        PrometheusPrimitiveQuery.from_dict(payload)


def test_example_live_source_config_is_complete():
    with open(
        "configs/final_live_collector.example.yaml", encoding="utf-8"
    ) as handle:
        config = FinalLiveCollectorConfig.from_dict(yaml.safe_load(handle))
    actual_components = {
        query.component for query in config.prometheus.queries
    }
    assert len(config.prometheus.queries) == len(actual_components)
    assert EXPERIMENTAL_DNS_COMPONENTS <= set(COMPONENTS)
    formal_expected = set(COMPONENTS) - EXPERIMENTAL_DNS_COMPONENTS
    assert actual_components == formal_expected
    assert actual_components.isdisjoint(EXPERIMENTAL_DNS_COMPONENTS)
    assert {
        "edge_request_total",
        "edge_error_total",
        "edge_timeout_total",
        "edge_latency_histogram",
    } <= actual_components
    assert len(config.formal_tcp_edge_entity_ids) == 15
    assert len(set(config.formal_tcp_edge_entity_ids)) == 15
    assert all(
        item.startswith(f"{config.cluster_id}::")
        and item.endswith("::tcp")
        for item in config.formal_tcp_edge_entity_ids
    )
    assert (
        "kind-proberca-ob::online-boutique::"
        "emailservice->checkoutservice::tcp"
    ) not in config.formal_tcp_edge_entity_ids
    with open("configs/final_control.yaml", encoding="utf-8") as handle:
        control = yaml.safe_load(handle)
    required_control_edges = {
        coordinate.rsplit("::", 1)[0]
        for coordinate in control["calibration_required_root_coordinates"]
        if "->" in coordinate and coordinate.endswith(
            ("::edge_latency_p95", "::edge_failure_rate")
        )
    }
    assert set(config.formal_tcp_edge_entity_ids) == required_control_edges

    with open(
        "configs/final_collection_contract.yaml", encoding="utf-8"
    ) as handle:
        contract = yaml.safe_load(handle)
    roles = contract["normal_metric_roles"]
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "service"
    } == FORMAL_SERVICE_METRICS
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "host"
    } == FORMAL_HOST_METRICS
    assert {
        role["metric_name"] for role in roles
        if role["entity_type"] == "edge"
        and role["protocols"] == ["tcp"]
    } == FORMAL_TCP_METRICS


def test_prometheus_source_preserves_raw_boundary_series_identity():
    query = PrometheusPrimitiveQuery.from_dict({
        "query_id": "request-total",
        "component": "request_total",
        "promql": "proberca_service_request_total",
        "label_mapping": {
            "namespace": "namespace",
            "pod": "pod",
            "container_id": "container",
            "source_coverage": "source_coverage",
        },
        "required_labels": [
            "namespace", "pod", "container", "source_coverage",
        ],
        "optional_labels": ["job", "instance"],
        "series_labels": ["namespace", "pod", "container_id"],
        "histogram_le_label": None,
        "value_scale": 1.0,
        "histogram_bound_scale": 1.0,
    })
    config = PrometheusSourceConfig(
        "http://prometheus.test", 1.0, 2.0, True, (query,)
    )

    class Response:
        status_code = 200

        content = b"response"

        def json(self):
            return {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "__name__": "proberca_service_request_total",
                                "namespace": NAMESPACE,
                                "pod": "frontend-pod",
                                "container": "frontend",
                                "source_coverage": "1",
                                "job": "proberca",
                                "instance": "127.0.0.1:9999",
                            },
                            "values": [[1.0, "100"]],
                        },
                        {
                            "metric": {
                            "__name__": "proberca_service_request_total",
                            "namespace": NAMESPACE,
                            "pod": "frontend-pod",
                            "container": "frontend",
                            "source_coverage": "0",
                            "job": "proberca",
                            "instance": "127.0.0.1:9999",
                            },
                            "values": [[2.0, "100"], [3.0, "100"]],
                        },
                    ],
                },
            }

    class Session:
        def __init__(self):
            self.ranges = []
            self.queries = []

        def get(self, _url, *, params, timeout):
            assert timeout == 1.0
            assert _url.endswith("/api/v1/query_range")
            self.ranges.append((
                float(params["start"]),
                float(params["end"]),
                params["step"],
            ))
            self.queries.append(params["query"])
            return Response()

    class Revision(SimpleNamespace):
        def resolve_service_for_pod(self, pod_uid, explicit_service=None):
            assert pod_uid == "pod-frontend"
            assert explicit_service is None
            raise ValueError("ambiguous service membership")

    revision = Revision(
        cluster_id=CLUSTER,
        pod_uid_by_name={(NAMESPACE, "frontend-pod"): "pod-frontend"},
        pod_to_services={
            "pod-frontend": (
                f"{CLUSTER}::{NAMESPACE}::frontend",
                f"{CLUSTER}::{NAMESPACE}::frontend-external",
            )
        },
        objects_by_kind={
            "Pod": {"pod-frontend": {
                "metadata": {"labels": {"app": "frontend"}},
                "spec": {"nodeName": "node-a"},
            }},
        },
        service_uid_by_name={(NAMESPACE, "frontend"): "service-frontend"},
    )
    session = Session()
    windows = PrometheusPrimitiveSource(
        config, session=session,
    ).collect_windows(
        bounds=((START, END), (END, END + (END - START))),
        inventory_revision=revision,
    )
    samples = windows[0]
    assert len(samples) == 2
    assert {item.timestamp_ns for item in samples} == {START, END}
    assert len({item.series_id for item in samples}) == 1
    assert {item.coverage for item in samples} == {0.0, 1.0}
    assert len({item.source_object_id for item in samples}) == 1
    assert len({item.source_record_id for item in samples}) == 2
    assert len(windows[1]) == 2
    assert session.ranges == [(1.0, 3.0, "1")]
    assert all(
        "timestamp(proberca_service_request_total) == time()" in item
        and "time() - timestamp(proberca_service_request_total)" in item
        and "<= 2.000000000" in item
        for item in session.queries
    )
    assert windows[0][-1].source_record_id \
        == windows[1][0].source_record_id
    assert PrometheusPrimitiveSource(
        config, session=Session(),
    ).config.range_query_chunk_windows == 120
    assert PrometheusPrimitiveSource(
        config, session=Session(),
    ).config.range_query_max_workers == 1


def test_query_range_chunks_bound_request_fanout_and_memory(monkeypatch):
    with open(
        "configs/final_live_collector.example.yaml", encoding="utf-8"
    ) as handle:
        config = FinalLiveCollectorConfig.from_dict(yaml.safe_load(handle))
    source = PrometheusPrimitiveSource(config.prometheus)
    requests_seen = []
    concurrency_lock = threading.Lock()
    concurrency = {"current": 0, "maximum": 0}

    def fake_range(query, *, expected_timestamps_ns):
        with concurrency_lock:
            concurrency["current"] += 1
            concurrency["maximum"] = max(
                concurrency["maximum"], concurrency["current"]
            )
        time.sleep(0.005)
        requests_seen.append((query.component, expected_timestamps_ns))
        with concurrency_lock:
            concurrency["current"] -= 1
        return (), 0.001, 10

    monkeypatch.setattr(source, "_range", fake_range)
    monkeypatch.setattr(
        source,
        "_samples_from_responses",
        lambda **_kwargs: (),
    )
    bounds = tuple(
        (
            START + index * 1_000_000_000,
            START + (index + 1) * 1_000_000_000,
        )
        for index in range(1200)
    )
    chunks = list(source.iter_collect_window_chunks(
        bounds=bounds,
        inventory_revision=SimpleNamespace(),
    ))
    assert len(chunks) == 40
    assert all(len(chunk) == 30 for chunk in chunks)
    assert len(config.prometheus.queries) == 32
    assert len(requests_seen) == 32 * 40 == 1280
    assert source.last_range_query_stats["request_count"] == 1280
    assert source.last_range_query_stats["max_loaded_windows"] == 30
    assert config.prometheus.range_query_max_workers == 1
    assert concurrency["maximum"] == 1
    for component, timestamps in requests_seen:
        expected_count = (
            31
            if COMPONENTS[component].metric_kind in {
                "monotonic_counter", "histogram_bucket",
            }
            else 30
        )
        assert len(timestamps) == expected_count


@pytest.mark.parametrize(
    "values,match",
    [
        ([[1.0, "1"]], "omitted evaluation timestamps"),
        (
            [[1.0, "1"], [2.0, "2"], [3.0, "3"]],
            "chunk-external timestamp",
        ),
        (
            [[1.0, "1"], [1.0, "2"], [2.0, "2"]],
            "duplicate sample",
        ),
        ([[1.0, "NaN"], [2.0, "2"]], "stale/non-finite"),
    ],
)
def test_query_range_rejects_missing_extra_duplicate_and_stale(
    values, match,
):
    query = PrometheusPrimitiveQuery.from_dict({
        "query_id": "request-total",
        "component": "request_total",
        "promql": "proberca_service_request_total",
        "label_mapping": {"namespace": "namespace"},
        "required_labels": ["namespace"],
        "optional_labels": [],
        "series_labels": ["namespace"],
        "histogram_le_label": None,
        "value_scale": 1.0,
        "histogram_bound_scale": 1.0,
    })

    class Response:
        status_code = 200
        content = b"response"

        @staticmethod
        def json():
            return {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [{
                        "metric": {"namespace": NAMESPACE},
                        "values": values,
                    }],
                },
            }

    class Session:
        @staticmethod
        def get(*_args, **_kwargs):
            return Response()

    source = PrometheusPrimitiveSource(
        PrometheusSourceConfig(
            "http://prometheus.test", 1.0, 2.0, True, (query,),
        ),
        session=Session(),
    )
    with pytest.raises(RawCollectionError, match=match):
        source._range(
            query,
            expected_timestamps_ns=(START, END),
        )


def test_dataplane_does_not_import_control_or_algorithm_modules():
    import ast
    from pathlib import Path

    forbidden = (
        "proberca.controlplane", "proberca.alerting",
        "proberca.propagation", "proberca.inversion",
        "proberca.diagnosis", "proberca.orchestration",
    )
    for path in Path("proberca/dataplane").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(
            name.startswith(prefix)
            for name in imports for prefix in forbidden
        ), (path, imports)


def test_live_runner_reports_capture_complete_before_history_query():
    events = []
    samples = []
    _gauge(
        samples, "memory_working_set_bytes", 1.0, series="series",
        namespace=NAMESPACE, service_name="checkoutservice",
        pod_uid="pod", container_id="container", node_name="node",
    )
    samples = tuple(samples)



    class Revision:
        def freeze(self, _timestamp_ns):
            return self

    class Discovery:
        calls = 0

        def discover_once(self, _timestamp_ns):
            self.calls += 1
            events.append(f"discover-{self.calls}")
            return Revision()

    class Primitive:
        @staticmethod
        def wait_for_target_timestamp(**_kwargs):
            events.append("primitive-target-ready")

        @staticmethod
        def iter_collect_window_chunks(**_kwargs):
            events.append("history-query")
            return ((samples,),)

    class Burst:
        @staticmethod
        def begin_capture(_revision):
            events.append("burst-begin")

        @staticmethod
        def capture_boundary(_timestamp_ns, _revision):
            events.append("burst-boundary")

        @staticmethod
        def collect_window(
            *, sequence, window_start_ns, window_end_ns, **_kwargs,
        ):
            return SimpleNamespace(
                sequence=sequence,
                window_start_ns=window_start_ns,
                window_end_ns=window_end_ns,
                cluster_id=CLUSTER,
                samples=(),
            )

    class Assembler:
        @staticmethod
        def assemble(*, raw_window, **_kwargs):
            return SimpleNamespace(
                sequence=raw_window.sequence,
                window_start_ns=raw_window.window_start_ns,
                window_end_ns=raw_window.window_end_ns,
                cluster_id=raw_window.cluster_id,
                residual_source_record_ids=(),
            )

    runner = FinalLiveCollectionRunner.__new__(
        FinalLiveCollectionRunner
    )
    runner.config = SimpleNamespace(
        cluster_id=CLUSTER, window_lead_sec=0,
        window_sec=1, collection_delay_sec=0,
    )
    runner.discovery = Discovery()
    runner.primitive_source = Primitive()
    runner.raw_burst_source = Burst()
    runner.assembler = Assembler()
    runner.wall_clock_ns = lambda: START
    runner.sleep = lambda _seconds: None

    pairs = list(runner.iter_collect_aligned(
        1,
        capture_complete_callback=lambda target: events.append(
            f"capture-complete-{target}"
        ),
        boundary_callback=lambda sequence, target: events.append(
            f"boundary-{sequence}-{target}"
        ),
    ))

    assert len(pairs) == 1
    boundary_index = events.index(f"boundary-1-{END}")
    assert events[boundary_index - 1] == "burst-boundary"
    assert boundary_index < events.index("primitive-target-ready")
    capture_index = events.index(f"capture-complete-{END}")
    assert events[capture_index - 1] == "primitive-target-ready"
    assert events[capture_index + 1] == "discover-2"
    assert capture_index < events.index("history-query")


def test_aligned_writer_atomically_records_capture_completion(
    contract, tmp_path,
):
    pair = _aligned_test_pair(contract, 1)
    normal_writer, burst_writer = _aligned_test_writers(
        contract, tmp_path, pair[0].collection_metadata,
    )
    marker = tmp_path / "lifecycle" / "capture-complete.json"

    class Runner:
        @staticmethod
        def iter_collect_aligned(
            _window_count, capture_complete_callback=None,
        ):
            assert capture_complete_callback is not None
            capture_complete_callback(END)
            yield pair

    _write_aligned_windows(
        runner=Runner(),
        normal_writer=normal_writer,
        burst_writer=burst_writer,
        window_count=1,
        capture_complete_marker=marker,
    )

    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["phase"] == "capture_complete"
    assert payload["final_target_ns"] == END
    assert isinstance(payload["timestamp_ns"], int)
    assert not tuple(marker.parent.glob(f".{marker.name}.*.tmp"))


def test_aligned_writer_atomically_records_internal_phase_boundary(
    contract, tmp_path,
):
    pairs = (
        _aligned_test_pair(contract, 1),
        _aligned_test_pair(contract, 2),
    )
    normal_writer, burst_writer = _aligned_test_writers(
        contract, tmp_path, pairs[0][0].collection_metadata,
    )
    marker = tmp_path / "lifecycle" / "phase-boundary.json"

    class Runner:
        @staticmethod
        def iter_collect_aligned(
            _window_count, capture_complete_callback=None,
            boundary_callback=None,
        ):
            assert capture_complete_callback is None
            assert boundary_callback is not None
            boundary_callback(1, END)
            boundary_callback(2, END + 1_000_000_000)
            yield from pairs

    _write_aligned_windows(
        runner=Runner(),
        normal_writer=normal_writer,
        burst_writer=burst_writer,
        window_count=2,
        phase_boundary_window=1,
        phase_boundary_marker=marker,
    )

    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["phase"] == "phase_boundary"
    assert payload["boundary_sequence"] == 1
    assert payload["boundary_target_ns"] == END
    assert isinstance(payload["timestamp_ns"], int)
    assert not tuple(marker.parent.glob(f".{marker.name}.*.tmp"))


def test_contiguous_capture_splits_into_two_aligned_phase_archives(
    contract, tmp_path,
):
    from scripts.run_final_fault_matrix import split_aligned_archive_slice

    pairs = tuple(_aligned_test_pair(contract, index) for index in range(1, 5))
    source_root = tmp_path / "source"
    normal_writer, burst_writer = _aligned_test_writers(
        contract, source_root, pairs[0][0].collection_metadata,
    )
    for normal, burst in pairs:
        normal_writer.append(normal)
        burst_writer.append(burst)
    normal_writer.seal()
    burst_writer.seal()

    first = split_aligned_archive_slice(
        source_normal_root=source_root / "normal",
        source_burst_root=source_root / "burst",
        output_root=tmp_path / "phase-one",
        start_index=0,
        window_count=2,
    )
    second = split_aligned_archive_slice(
        source_normal_root=source_root / "normal",
        source_burst_root=source_root / "burst",
        output_root=tmp_path / "phase-two",
        start_index=2,
        window_count=2,
    )

    assert first["window_count"] == second["window_count"] == 2
    assert first["window_end_ns"] == second["window_start_ns"]
    assert first["dataset_id"] != second["dataset_id"]
    for phase_root in (tmp_path / "phase-one", tmp_path / "phase-two"):
        normal = CollectionArchive.load(phase_root / "normal")
        burst = BurstArchive.load(phase_root / "burst")
        assert normal.dataset_id == burst.dataset_id
        assert [item.sequence for item in normal.iter_windows()] == [1, 2]
        assert [item.sequence for item in burst.iter_windows()] == [1, 2]

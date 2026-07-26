from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest
import yaml

from proberca.dataplane.archive import CollectionArchiveWriter
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
from proberca.dataplane.collector import FinalDataPlaneCollector
from proberca.dataplane.collector import FinalLiveCollectorConfig
from proberca.dataplane.contracts import fingerprint
from proberca.dataplane.final_aggregation import COMPONENTS, FinalWindowAggregator
from proberca.dataplane.raw import (
    RawCollectionError,
    RawCollectionWindow,
    RawMetricSample,
)
from proberca.dataplane.sources import (
    PrometheusPrimitiveQuery,
    PrometheusPrimitiveSource,
    PrometheusSourceConfig,
)
from proberca.k8s.contracts import ResourceVersionVector


START = 1_000_000_000
END = 2_000_000_000
CLUSTER = "cluster-a"
NAMESPACE = "shop"


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


def _host_samples(output, node):
    identity = {"node_name": node}
    for component, delta in (
        ("node_cpu_psi_some_ns_total", 10_000_000),
        ("node_memory_psi_some_ns_total", 20_000_000),
        ("node_io_psi_some_ns_total", 30_000_000),
        ("node_nic_rx_drop_total", 1),
        ("node_nic_tx_drop_total", 2),
        ("node_nic_rx_error_total", 3),
        ("node_nic_tx_error_total", 4),
    ):
        _counter(
            output, component, 100, delta,
            entity="host", series=node, **identity,
        )


def _edge_samples(
    output, protocol, *, count_delta=None, error_delta=None,
    timeout_delta=None, histogram_deltas=None,
):
    identity = {
        "namespace": NAMESPACE,
        "src_service": "frontend",
        "dst_service": "payment",
        "dst_namespace": NAMESPACE,
        "src_pod_uid": "pod-frontend",
        "dst_pod_uid": "pod-payment",
        "src_node": "node-a",
        "dst_node": "node-b",
        "protocol": protocol,
    }
    if protocol == "tcp":
        components = (
            ("edge_request_total", 50 if count_delta is None else count_delta),
            ("edge_error_total", 2 if error_delta is None else error_delta),
            ("edge_timeout_total", 1 if timeout_delta is None else timeout_delta),
        )
        histogram = "edge_latency_histogram"
    else:
        components = (
            ("dns_query_total", 21 if count_delta is None else count_delta),
            ("dns_timeout_total", 1 if timeout_delta is None else timeout_delta),
            ("dns_error_rcode_total", 1 if error_delta is None else error_delta),
        )
        histogram = "dns_latency_histogram"
    for component, delta in components:
        _counter(
            output, component, 100, delta,
            entity="edge", series=f"{protocol}-flow", **identity,
        )
    _histogram(
        output, histogram, (
            (
                (25, 48, 50)
                if protocol == "tcp" else (10, 19, 20)
            )
            if histogram_deltas is None else histogram_deltas
        ),
        entity="edge", series=f"{protocol}-flow", **identity,
    )


def _raw_window():
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
    value = next(
        item.value for item in result.node_metrics
        if item.service_name == "frontend"
        and item.metric_name == "local_socket_failure_rate"
    )
    assert value == pytest.approx(1.0)


def test_idle_pressure_and_lock_zero_over_zero_use_formula_epsilon(contract):
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
        item.metric_name: item.value
        for item in result.node_metrics
        if item.service_name == "frontend"
    }
    assert values["io_psi"] == 0.0
    assert values["futex_wait_time_rate"] == 0.0


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
    raw = _raw_window()
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "test"}),
    )
    window = collector.assemble(
        raw_window=raw,
        inventory_at_start=_revision(),
        inventory_at_end=_revision(),
    )
    snapshot = window.topology_events[0]
    assert len(snapshot.services) == 2
    assert {item.protocol for item in snapshot.call_edges} == {"tcp", "dns"}
    assert len(snapshot.service_nodes) == 2
    metadata = window.collection_metadata
    writer = CollectionArchiveWriter(
        tmp_path / "archive",
        dataset_id=fingerprint({"dataset": "healthy"}),
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )
    writer.append(window)
    archive = writer.seal()
    assert archive.window_count == 1
    assert len(tuple(archive.iter_windows())) == 1


def test_global_resource_watermark_change_does_not_fake_layout_change(
    contract,
):
    collector = FinalDataPlaneCollector(
        collection_contract=contract,
        collector_build_id=fingerprint({"build": "test"}),
    )
    before = _revision("rv-1")
    after = _revision("rv-2")
    for objects in after.objects_by_kind.values():
        for raw in objects.values():
            raw["metadata"]["resourceVersion"] = "rv-1"
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


def test_empty_service_histogram_has_zero_coverage_not_imputation(
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
    assert metrics["request_rate"].value == 0
    latency = metrics["request_latency_p95"]
    assert latency.value == 0
    assert latency.sample_count == 0
    assert latency.coverage == 0


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
    assert metrics["edge_request_count"].value == 0
    assert metrics["edge_failure_rate"].value == 0
    assert metrics["edge_latency_p95"].coverage == 0


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
    assert len(config.prometheus.queries) == len({
        query.component for query in config.prometheus.queries
    })
    assert {query.component for query in config.prometheus.queries} == set(
        COMPONENTS
    )


def test_prometheus_source_preserves_raw_boundary_series_identity():
    query = PrometheusPrimitiveQuery.from_dict({
        "query_id": "request-total",
        "component": "request_total",
        "promql": "proberca_service_request_total",
        "label_mapping": {
            "namespace": "namespace",
            "pod": "pod",
            "container_id": "container",
        },
        "required_labels": ["namespace", "pod", "container"],
        "optional_labels": ["job", "instance"],
        "series_labels": ["namespace", "pod", "container_id"],
        "histogram_le_label": None,
        "value_scale": 1.0,
        "histogram_bound_scale": 1.0,
    })
    config = PrometheusSourceConfig(
        "http://prometheus.test", 1.0, True, (query,)
    )

    class Response:
        status_code = 200

        def __init__(self, timestamp):
            self.timestamp = timestamp

        def json(self):
            return {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{
                        "metric": {
                            "__name__": "proberca_service_request_total",
                            "namespace": NAMESPACE,
                            "pod": "frontend-pod",
                            "container": "frontend",
                            "job": "proberca",
                            "instance": "127.0.0.1:9999",
                        },
                        "value": [self.timestamp, "100"],
                    }],
                },
            }

    class Session:
        def get(self, _url, *, params, timeout):
            assert timeout == 1.0
            return Response(float(params["time"]))

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
    samples = PrometheusPrimitiveSource(
        config, session=Session()
    ).collect(
        window_start_ns=START,
        window_end_ns=END,
        inventory_revision=revision,
    )
    assert len(samples) == 2
    assert {item.timestamp_ns for item in samples} == {START, END}
    assert len({item.series_id for item in samples}) == 1
    assert len({item.source_record_id for item in samples}) == 2


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

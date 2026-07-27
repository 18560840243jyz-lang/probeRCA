"""Exact 9/4/3/3 aggregation for the frozen ProbeRCA-BPF scheme.

The aggregator accepts only raw source primitives.  It differences each
monotonic series before any cross-Pod/flow sum, recomputes ratios from summed
components, and derives P95 only from merged histogram buckets.  Missing or
ambiguous components reject the whole window; no metric is imputed with zero.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    EdgeMetricRecord,
    NodeMetricRecord,
)

from .raw import RawCollectionError, RawCollectionWindow, RawMetricSample


FINAL_AGGREGATION_VERSION = "probeRCA-final-window-aggregation-v1"
FINAL_OUTPUT_SOURCE = "final_window_aggregation"
NANOSECONDS_PER_SECOND = 1_000_000_000
RATIO_EPSILON = 1.0e-12


@dataclass(frozen=True)
class ComponentSpec:
    entity_type: str
    metric_family: str
    metric_kind: str
    unit: str
    scope: str


COMPONENTS: dict[str, ComponentSpec] = {
    # Service request symptoms.
    "request_total": ComponentSpec(
        "service", "request", "monotonic_counter", "requests", "pod"
    ),
    "request_error_total": ComponentSpec(
        "service", "request", "monotonic_counter", "requests", "pod"
    ),
    "request_timeout_total": ComponentSpec(
        "service", "request", "monotonic_counter", "requests", "pod"
    ),
    "request_latency_histogram": ComponentSpec(
        "service", "request", "histogram_bucket", "milliseconds", "pod"
    ),
    # Service CPU, memory, I/O, lock, and local socket.
    "cpu_time_ns_total": ComponentSpec(
        "service", "cpu", "monotonic_counter", "nanoseconds", "pod"
    ),
    "allocated_cpu_cores": ComponentSpec(
        "service", "cpu", "gauge", "cores", "pod"
    ),
    "cpu_nr_throttled_total": ComponentSpec(
        "service", "cpu", "monotonic_counter", "periods", "pod"
    ),
    "cpu_nr_periods_total": ComponentSpec(
        "service", "cpu", "monotonic_counter", "periods", "pod"
    ),
    "memory_working_set_bytes": ComponentSpec(
        "service", "memory", "gauge", "bytes", "pod"
    ),
    "memory_limit_bytes": ComponentSpec(
        "service", "memory", "gauge", "bytes", "pod"
    ),
    "io_psi_some_ns_total": ComponentSpec(
        "service", "io", "monotonic_counter", "nanoseconds", "pod"
    ),
    "active_task_ns_total": ComponentSpec(
        "service", "io", "monotonic_counter", "nanoseconds", "pod"
    ),
    "futex_wait_ns_total": ComponentSpec(
        "service", "lock", "monotonic_counter", "nanoseconds", "pod"
    ),
    "active_thread_ns_total": ComponentSpec(
        "service", "lock", "monotonic_counter", "nanoseconds", "pod"
    ),
    "socket_backlog_overflow_total": ComponentSpec(
        "service", "net_local", "monotonic_counter", "events", "pod"
    ),
    "socket_accept_fail_total": ComponentSpec(
        "service", "net_local", "monotonic_counter", "events", "pod"
    ),
    "socket_local_rst_total": ComponentSpec(
        "service", "net_local", "monotonic_counter", "events", "pod"
    ),
    "socket_local_drop_total": ComponentSpec(
        "service", "net_local", "monotonic_counter", "events", "pod"
    ),
    "socket_ops_total": ComponentSpec(
        "service", "net_local", "monotonic_counter", "operations", "pod"
    ),
    # Host pressure and NIC.
    "node_cpu_psi_some_ns_total": ComponentSpec(
        "host", "cpu", "monotonic_counter", "nanoseconds", "node"
    ),
    "node_memory_psi_some_ns_total": ComponentSpec(
        "host", "memory", "monotonic_counter", "nanoseconds", "node"
    ),
    "node_io_psi_some_ns_total": ComponentSpec(
        "host", "io", "monotonic_counter", "nanoseconds", "node"
    ),
    "node_nic_rx_drop_total": ComponentSpec(
        "host", "net_local", "monotonic_counter", "events", "node"
    ),
    "node_nic_tx_drop_total": ComponentSpec(
        "host", "net_local", "monotonic_counter", "events", "node"
    ),
    "node_nic_rx_error_total": ComponentSpec(
        "host", "net_local", "monotonic_counter", "events", "node"
    ),
    "node_nic_tx_error_total": ComponentSpec(
        "host", "net_local", "monotonic_counter", "events", "node"
    ),
    # TCP directed edge.
    "edge_request_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "requests", "flow"
    ),
    "edge_error_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "requests", "flow"
    ),
    "edge_timeout_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "requests", "flow"
    ),
    "edge_latency_histogram": ComponentSpec(
        "edge", "request", "histogram_bucket", "milliseconds", "flow"
    ),
    # DNS directed edge.
    "dns_query_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "queries", "flow"
    ),
    "dns_timeout_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "queries", "flow"
    ),
    "dns_error_rcode_total": ComponentSpec(
        "edge", "request", "monotonic_counter", "queries", "flow"
    ),
    "dns_latency_histogram": ComponentSpec(
        "edge", "request", "histogram_bucket", "milliseconds", "flow"
    ),
}


SERVICE_OUTPUTS = frozenset({
    "request_rate", "request_failure_rate", "request_latency_p95",
    "cpu_usage_rate", "cpu_throttle_ratio", "memory_working_set_ratio",
    "io_psi", "futex_wait_time_rate", "local_socket_failure_rate",
})
HOST_OUTPUTS = frozenset({
    "cpu_psi", "memory_psi", "io_psi", "nic_drop_error_rate",
})
TCP_OUTPUTS = frozenset({
    "edge_request_count", "edge_latency_p95", "edge_failure_rate",
})
DNS_OUTPUTS = frozenset({
    "dns_query_count", "dns_latency_p95", "dns_failure_rate",
})


@dataclass(frozen=True)
class FinalAggregationResult:
    node_metrics: tuple[NodeMetricRecord, ...]
    edge_metrics: tuple[EdgeMetricRecord, ...]
    residual_source_record_ids: tuple[str, ...]
    source_object_ids: tuple[str, ...]


@dataclass(frozen=True)
class _Value:
    value: float
    sample_count: int
    coverage: float
    event_loss_rate: float
    mapping_quality: float
    source_ids: frozenset[str]
    object_ids: frozenset[str]

    @property
    def effective_coverage(self) -> float:
        return self.coverage * self.mapping_quality


def _combine(values: Iterable[_Value], result: float) -> _Value:
    items = tuple(values)
    if not items:
        raise RawCollectionError("cannot combine an empty component set")
    return _Value(
        float(result),
        sum(item.sample_count for item in items),
        min(item.coverage for item in items),
        max(item.event_loss_rate for item in items),
        min(item.mapping_quality for item in items),
        frozenset().union(*(item.source_ids for item in items)),
        frozenset().union(*(item.object_ids for item in items)),
    )


class FinalWindowAggregator:
    """Fail-closed implementation of the final window formulas."""

    def __init__(self, collection_contract: dict):
        self.contract = dict(collection_contract)
        if self.contract.get("aggregation_output_source") != FINAL_OUTPUT_SOURCE:
            raise RawCollectionError("final aggregation output source mismatch")
        if self.contract.get("window_sec") != 1:
            raise RawCollectionError("the frozen final collector requires 1-second windows")
        roles = self.contract.get("normal_metric_roles")
        if not isinstance(roles, list):
            raise RawCollectionError("collection contract metric roles are missing")
        self._role_semantics = {
            (
                item["record_type"], item["metric_name"],
                tuple(item["scopes"]), tuple(item["protocols"]),
            ): item
            for item in roles
        }

    def aggregate(self, window: RawCollectionWindow) -> FinalAggregationResult:
        window.validate()
        if window.window_end_ns - window.window_start_ns \
                != NANOSECONDS_PER_SECOND:
            raise RawCollectionError("final aggregation requires an exact 1-second window")
        samples = tuple(window.samples)
        self._validate_component_semantics(samples)
        by_entity: dict[tuple[str, ...], list[RawMetricSample]] = defaultdict(list)
        for sample in samples:
            by_entity[sample.entity_key].append(sample)
        nodes: list[NodeMetricRecord] = []
        edges: list[EdgeMetricRecord] = []
        used_sources: set[str] = set()
        used_objects: set[str] = set()
        for entity_key, entity_samples in sorted(by_entity.items()):
            try:
                if entity_key[0] == "service":
                    records, sources, objects = self._service(
                        window, entity_key, entity_samples
                    )
                    nodes.extend(records)
                elif entity_key[0] == "host":
                    records, sources, objects = self._host(
                        window, entity_key, entity_samples
                    )
                    nodes.extend(records)
                else:
                    records, sources, objects = self._edge(
                        window, entity_key, entity_samples
                    )
                    edges.extend(records)
            except RawCollectionError as error:
                raise RawCollectionError(
                    f"entity {entity_key!r}: {error}"
                ) from error
            used_sources.update(sources)
            used_objects.update(objects)
        if not nodes:
            raise RawCollectionError("final window has no service/host metrics")
        supplied_sources = {item.source_record_id for item in samples}
        unused = supplied_sources - used_sources
        if unused:
            raise RawCollectionError(
                "raw window contains unused or protocol-incompatible components"
            )
        return FinalAggregationResult(
            node_metrics=tuple(sorted(nodes, key=lambda item: (
                item.scope, item.stable_id, item.metric_name,
            ))),
            edge_metrics=tuple(sorted(edges, key=lambda item: (
                item.protocol, item.stable_id, item.metric_name,
            ))),
            residual_source_record_ids=tuple(sorted(used_sources)),
            source_object_ids=tuple(sorted(used_objects)),
        )

    @staticmethod
    def _validate_component_semantics(samples: tuple[RawMetricSample, ...]) -> None:
        for sample in samples:
            spec = COMPONENTS.get(sample.component)
            if spec is None:
                raise RawCollectionError(
                    f"unknown final raw component {sample.component!r}"
                )
            actual = (
                sample.entity_type, sample.metric_family, sample.metric_kind,
                sample.unit, sample.scope,
            )
            expected = (
                spec.entity_type, spec.metric_family, spec.metric_kind,
                spec.unit, spec.scope,
            )
            if actual != expected:
                raise RawCollectionError(
                    f"raw component semantics mismatch for {sample.component}"
                )

    @staticmethod
    def _component(
        samples: list[RawMetricSample], name: str,
    ) -> list[RawMetricSample]:
        output = [item for item in samples if item.component == name]
        if not output:
            raise RawCollectionError(f"missing raw component {name}")
        return output

    @staticmethod
    def _series_sets_equal(
        values: dict[str, dict[str, _Value]], names: tuple[str, ...],
    ) -> None:
        series_sets = {name: set(values[name]) for name in names}
        first = series_sets[names[0]]
        if any(series_sets[name] != first for name in names[1:]):
            raise RawCollectionError(
                "ratio component series coverage is inconsistent: "
                + ",".join(f"{name}={sorted(series_sets[name])}" for name in names)
            )

    def _deltas(
        self,
        window: RawCollectionWindow,
        samples: list[RawMetricSample],
        *components: str,
    ) -> dict[str, dict[str, _Value]]:
        output: dict[str, dict[str, _Value]] = {}
        for component in components:
            grouped: dict[str, list[RawMetricSample]] = defaultdict(list)
            for sample in self._component(samples, component):
                grouped[sample.series_id].append(sample)
            component_values: dict[str, _Value] = {}
            for series_id, records in sorted(grouped.items()):
                by_time: dict[int, list[RawMetricSample]] = defaultdict(list)
                for record in records:
                    by_time[record.timestamp_ns].append(record)
                if set(by_time) != {
                    window.window_start_ns, window.window_end_ns,
                } or any(len(items) != 1 for items in by_time.values()):
                    raise RawCollectionError(
                        f"{component}/{series_id} requires exactly one "
                        "counter sample at each window boundary"
                    )
                start = by_time[window.window_start_ns][0]
                end = by_time[window.window_end_ns][0]
                delta = end.value - start.value
                if delta < 0:
                    raise RawCollectionError(
                        f"counter reset in {component}/{series_id}"
                    )
                component_values[series_id] = _Value(
                    delta, 1, min(start.coverage, end.coverage),
                    max(start.event_loss_rate, end.event_loss_rate),
                    min(start.mapping_quality, end.mapping_quality),
                    frozenset({start.source_record_id, end.source_record_id}),
                    frozenset({start.source_object_id, end.source_object_id}),
                )
            output[component] = component_values
        return output

    def _gauges(
        self,
        window: RawCollectionWindow,
        samples: list[RawMetricSample],
        *components: str,
    ) -> dict[str, dict[str, _Value]]:
        output: dict[str, dict[str, _Value]] = {}
        for component in components:
            grouped: dict[str, list[RawMetricSample]] = defaultdict(list)
            for sample in self._component(samples, component):
                if not window.window_start_ns <= sample.timestamp_ns <= window.window_end_ns:
                    raise RawCollectionError("gauge timestamp is outside its window")
                grouped[sample.series_id].append(sample)
            component_values: dict[str, _Value] = {}
            for series_id, records in sorted(grouped.items()):
                latest_time = max(item.timestamp_ns for item in records)
                latest = [item for item in records if item.timestamp_ns == latest_time]
                if len(latest) != 1:
                    raise RawCollectionError(
                        f"ambiguous latest gauge {component}/{series_id}"
                    )
                item = latest[0]
                component_values[series_id] = _Value(
                    item.value, 1, item.coverage, item.event_loss_rate,
                    item.mapping_quality, frozenset({item.source_record_id}),
                    frozenset({item.source_object_id}),
                )
            output[component] = component_values
        return output

    @staticmethod
    def _sum(values: dict[str, _Value]) -> _Value:
        return _combine(values.values(), sum(item.value for item in values.values()))

    @staticmethod
    def _ratio(
        numerator: _Value,
        denominator: _Value,
        name: str,
        *,
        bounded: bool = True,
    ) -> _Value:
        if denominator.value <= 0:
            if numerator.value == 0:
                combined = _combine((numerator, denominator), 0.0)
                # A zero denominator has no statistical exposure. Preserve
                # lineage but mark the derived ratio explicitly missing.
                return _Value(
                    0.0,
                    0,
                    0.0,
                    combined.event_loss_rate,
                    combined.mapping_quality,
                    combined.source_ids,
                    combined.object_ids,
                )
            raise RawCollectionError(f"{name} has a non-positive denominator")
        result = numerator.value / (denominator.value + RATIO_EPSILON)
        if not math.isfinite(result) or result < 0 \
                or (bounded and result > 1.0):
            raise RawCollectionError(
                f"{name} ratio is invalid: numerator={numerator.value}, "
                f"denominator={denominator.value}, result={result}"
            )
        return _combine((numerator, denominator), result)

    def _histogram_p95(
        self,
        window: RawCollectionWindow,
        samples: list[RawMetricSample],
        component: str,
        required_series_ids: set[str] | None = None,
        *,
        allow_empty: bool = False,
    ) -> _Value:
        records = self._component(samples, component)
        by_series: dict[str, list[RawMetricSample]] = defaultdict(list)
        for item in records:
            by_series[item.series_id].append(item)
        if required_series_ids is not None \
                and set(by_series) != required_series_ids:
            raise RawCollectionError(
                f"{component} series coverage does not match its count component"
            )
        merged: dict[tuple[float | None, bool], float] = defaultdict(float)
        source_ids: set[str] = set()
        object_ids: set[str] = set()
        qualities: list[RawMetricSample] = []
        expected_buckets: set[tuple[float | None, bool]] | None = None
        for series_id, series in sorted(by_series.items()):
            by_time: dict[int, dict[tuple[float | None, bool], RawMetricSample]] = {
                window.window_start_ns: {},
                window.window_end_ns: {},
            }
            for item in series:
                if item.timestamp_ns not in by_time:
                    raise RawCollectionError(
                        f"{component}/{series_id} histogram sample is not on a boundary"
                    )
                if item.bucket_key in by_time[item.timestamp_ns]:
                    raise RawCollectionError(
                        f"duplicate histogram bucket {component}/{series_id}"
                    )
                by_time[item.timestamp_ns][item.bucket_key] = item
            start_keys = set(by_time[window.window_start_ns])
            end_keys = set(by_time[window.window_end_ns])
            if not start_keys or start_keys != end_keys:
                raise RawCollectionError(
                    f"{component}/{series_id} histogram boundaries are incomplete"
                )
            if expected_buckets is None:
                expected_buckets = start_keys
            elif start_keys != expected_buckets:
                raise RawCollectionError(
                    f"{component} histogram series have different buckets"
                )
            ordered_keys = sorted(
                start_keys,
                key=lambda item: math.inf if item[1] else float(item[0]),
            )
            if not ordered_keys[-1][1]:
                raise RawCollectionError(f"{component} histogram lacks +Inf")
            start_values = [
                by_time[window.window_start_ns][key].value for key in ordered_keys
            ]
            end_values = [
                by_time[window.window_end_ns][key].value for key in ordered_keys
            ]
            if start_values != sorted(start_values) \
                    or end_values != sorted(end_values):
                raise RawCollectionError(
                    f"{component}/{series_id} cumulative buckets are non-monotonic"
                )
            deltas = [
                end_value - start_value
                for start_value, end_value in zip(start_values, end_values)
            ]
            if any(value < 0 for value in deltas) or deltas != sorted(deltas):
                raise RawCollectionError(
                    f"{component}/{series_id} histogram reset or invalid delta"
                )
            for key, value in zip(ordered_keys, deltas):
                merged[key] += value
            for timestamp in (window.window_start_ns, window.window_end_ns):
                for item in by_time[timestamp].values():
                    source_ids.add(item.source_record_id)
                    object_ids.add(item.source_object_id)
                    qualities.append(item)
        ordered = sorted(
            merged,
            key=lambda item: math.inf if item[1] else float(item[0]),
        )
        cumulative = [merged[key] for key in ordered]
        total = cumulative[-1]
        if total <= 0:
            if allow_empty and total == 0:
                return _Value(
                    0.0, 0, 0.0,
                    max(item.event_loss_rate for item in qualities),
                    min(item.mapping_quality for item in qualities),
                    frozenset(source_ids), frozenset(object_ids),
                )
            raise RawCollectionError(f"{component} histogram has no observations")
        threshold = 0.95 * total
        selected = next(
            key for key, count in zip(ordered, cumulative) if count >= threshold
        )
        finite = [float(key[0]) for key in ordered if not key[1]]
        value = finite[-1] if selected[1] else float(selected[0])
        return _Value(
            value, int(total), min(item.coverage for item in qualities),
            max(item.event_loss_rate for item in qualities),
            min(item.mapping_quality for item in qualities),
            frozenset(source_ids), frozenset(object_ids),
        )

    def _node_record(
        self,
        window: RawCollectionWindow,
        entity_key: tuple[str, ...],
        metric_family: str,
        metric_name: str,
        value: _Value,
        *,
        unit: str,
        metric_kind: str = "gauge",
        quantile: float | None = None,
    ) -> NodeMetricRecord:
        if entity_key[0] == "service":
            _, cluster, namespace, service = entity_key
            node_name, scope = None, "service"
        else:
            _, cluster, node_name = entity_key
            namespace, service, scope = "_host", node_name, "node"
        return NodeMetricRecord(
            PROBERCA_SCHEMA_VERSION,
            window.window_end_ns - 1,
            1,
            cluster,
            node_name,
            namespace,
            service,
            None,
            None,
            metric_family,
            metric_name,
            value.value,
            unit,
            value.sample_count,
            value.effective_coverage,
            value.event_loss_rate,
            FINAL_OUTPUT_SOURCE,
            metric_kind,
            scope,
            None,
            False,
            None,
            quantile,
        )

    @staticmethod
    def _sources(values: Iterable[_Value]) -> tuple[set[str], set[str]]:
        items = tuple(values)
        return (
            set().union(*(item.source_ids for item in items)),
            set().union(*(item.object_ids for item in items)),
        )

    def _service(self, window, entity_key, samples):
        request = self._deltas(
            window, samples, "request_total", "request_error_total",
            "request_timeout_total",
        )
        self._series_sets_equal(
            request,
            ("request_total", "request_error_total", "request_timeout_total"),
        )
        request_total = self._sum(request["request_total"])
        errors = self._sum(request["request_error_total"])
        timeouts = self._sum(request["request_timeout_total"])
        request_rate = _combine((request_total,), request_total.value)
        request_failure = self._ratio(
            _combine((errors, timeouts), errors.value + timeouts.value),
            request_total,
            "request_failure_rate",
        )
        request_p95 = self._histogram_p95(
            window, samples, "request_latency_histogram",
            set(request["request_total"]), allow_empty=True,
        )
        if request_p95.sample_count != request_total.value:
            raise RawCollectionError(
                "service request histogram count does not match request_total"
            )

        cpu = self._deltas(
            window, samples, "cpu_time_ns_total",
            "cpu_nr_throttled_total", "cpu_nr_periods_total",
        )
        allocation = self._gauges(
            window, samples, "allocated_cpu_cores"
        )
        self._series_sets_equal(
            {**cpu, **allocation},
            ("cpu_time_ns_total", "allocated_cpu_cores"),
        )
        self._series_sets_equal(
            cpu, ("cpu_nr_throttled_total", "cpu_nr_periods_total")
        )
        cpu_time = self._sum(cpu["cpu_time_ns_total"])
        cpu_cores = self._sum(allocation["allocated_cpu_cores"])
        cpu_denominator = _combine(
            (cpu_cores,), cpu_cores.value * (
                window.window_end_ns - window.window_start_ns
            )
        )
        cpu_usage = self._ratio(
            cpu_time, cpu_denominator, "cpu_usage_rate", bounded=False
        )
        cpu_throttle = self._ratio(
            self._sum(cpu["cpu_nr_throttled_total"]),
            self._sum(cpu["cpu_nr_periods_total"]),
            "cpu_throttle_ratio",
        )

        memory = self._gauges(
            window, samples, "memory_working_set_bytes", "memory_limit_bytes"
        )
        self._series_sets_equal(
            memory, ("memory_working_set_bytes", "memory_limit_bytes")
        )
        memory_ratio = self._ratio(
            self._sum(memory["memory_working_set_bytes"]),
            self._sum(memory["memory_limit_bytes"]),
            "memory_working_set_ratio",
            bounded=False,
        )

        io = self._deltas(
            window, samples, "io_psi_some_ns_total", "active_task_ns_total"
        )
        self._series_sets_equal(
            io, ("io_psi_some_ns_total", "active_task_ns_total")
        )
        io_ratio = self._ratio(
            self._sum(io["io_psi_some_ns_total"]),
            self._sum(io["active_task_ns_total"]),
            "io_psi",
        )
        lock = self._deltas(
            window, samples, "futex_wait_ns_total", "active_thread_ns_total"
        )
        self._series_sets_equal(
            lock, ("futex_wait_ns_total", "active_thread_ns_total")
        )
        lock_ratio = self._ratio(
            self._sum(lock["futex_wait_ns_total"]),
            self._sum(lock["active_thread_ns_total"]),
            "futex_wait_time_rate",
        )
        local = self._deltas(
            window, samples,
            "socket_backlog_overflow_total", "socket_accept_fail_total",
            "socket_local_rst_total", "socket_local_drop_total",
            "socket_ops_total",
        )
        self._series_sets_equal(local, tuple(local))
        local_bad = tuple(
            self._sum(local[name]) for name in (
                "socket_backlog_overflow_total", "socket_accept_fail_total",
                "socket_local_rst_total", "socket_local_drop_total",
            )
        )
        socket_operations = self._sum(local["socket_ops_total"])
        # One failed socket operation can produce more than one raw kernel
        # symptom (for example, a drop followed by a reset).  The metric is a
        # failed-operation ratio, so conservatively de-duplicate overlapping
        # symptom counters at the number of observed operations.
        local_failed_operations = _combine(
            local_bad,
            min(
                sum(item.value for item in local_bad),
                socket_operations.value,
            ),
        )
        local_failure = self._ratio(
            local_failed_operations,
            socket_operations,
            "local_socket_failure_rate",
        )
        outputs = (
            ("request", "request_rate", request_rate, "requests_per_second", "gauge", None),
            ("request", "request_failure_rate", request_failure, "ratio", "gauge", None),
            ("request", "request_latency_p95", request_p95, "milliseconds", "quantile", 0.95),
            ("cpu", "cpu_usage_rate", cpu_usage, "ratio", "gauge", None),
            ("cpu", "cpu_throttle_ratio", cpu_throttle, "ratio", "gauge", None),
            ("memory", "memory_working_set_ratio", memory_ratio, "ratio", "gauge", None),
            ("io", "io_psi", io_ratio, "ratio", "gauge", None),
            ("lock", "futex_wait_time_rate", lock_ratio, "ratio", "gauge", None),
            ("net_local", "local_socket_failure_rate", local_failure, "ratio", "gauge", None),
        )
        records = tuple(
            self._node_record(
                window, entity_key, family, name, value,
                unit=unit, metric_kind=kind, quantile=quantile,
            )
            for family, name, value, unit, kind, quantile in outputs
        )
        if {item.metric_name for item in records} != SERVICE_OUTPUTS:
            raise AssertionError("service final metric set is not 9")
        sources, objects = self._sources(item[2] for item in outputs)
        return records, sources, objects

    def _host(self, window, entity_key, samples):
        pressure = self._deltas(
            window, samples,
            "node_cpu_psi_some_ns_total", "node_memory_psi_some_ns_total",
            "node_io_psi_some_ns_total",
        )
        self._series_sets_equal(pressure, tuple(pressure))
        window_ns = float(window.window_end_ns - window.window_start_ns)
        cpu = _combine(
            (self._sum(pressure["node_cpu_psi_some_ns_total"]),),
            self._sum(pressure["node_cpu_psi_some_ns_total"]).value / window_ns,
        )
        memory = _combine(
            (self._sum(pressure["node_memory_psi_some_ns_total"]),),
            self._sum(pressure["node_memory_psi_some_ns_total"]).value / window_ns,
        )
        io = _combine(
            (self._sum(pressure["node_io_psi_some_ns_total"]),),
            self._sum(pressure["node_io_psi_some_ns_total"]).value / window_ns,
        )
        nic = self._deltas(
            window, samples,
            "node_nic_rx_drop_total", "node_nic_tx_drop_total",
            "node_nic_rx_error_total", "node_nic_tx_error_total",
        )
        self._series_sets_equal(nic, tuple(nic))
        nic_parts = tuple(self._sum(nic[name]) for name in nic)
        nic_rate = _combine(
            nic_parts,
            sum(item.value for item in nic_parts),
        )
        outputs = (
            ("cpu", "cpu_psi", cpu, "ratio"),
            ("memory", "memory_psi", memory, "ratio"),
            ("io", "io_psi", io, "ratio"),
            ("net_local", "nic_drop_error_rate", nic_rate, "events_per_second"),
        )
        records = tuple(
            self._node_record(
                window, entity_key, family, name, value, unit=unit
            )
            for family, name, value, unit in outputs
        )
        if {item.metric_name for item in records} != HOST_OUTPUTS:
            raise AssertionError("host final metric set is not 4")
        sources, objects = self._sources(item[2] for item in outputs)
        return records, sources, objects

    def _edge_record(
        self, window, entity_key, metric_name, value, unit, metric_kind,
        quantile=None,
    ):
        (
            _, cluster, namespace, source, destination, _destination_namespace,
            protocol,
        ) = entity_key
        return EdgeMetricRecord(
            PROBERCA_SCHEMA_VERSION,
            window.window_end_ns - 1,
            1,
            cluster,
            namespace,
            source,
            destination,
            None,
            None,
            None,
            None,
            protocol,
            metric_name,
            value.value,
            unit,
            value.sample_count,
            value.effective_coverage,
            value.event_loss_rate,
            FINAL_OUTPUT_SOURCE,
            metric_kind,
            "service_pair",
            None,
            False,
            None,
            quantile,
        )

    def _edge(self, window, entity_key, samples):
        protocol = entity_key[-1]
        if protocol == "dns":
            values = self._deltas(
                window, samples, "dns_query_total", "dns_timeout_total",
                "dns_error_rcode_total",
            )
            self._series_sets_equal(
                values,
                ("dns_query_total", "dns_timeout_total", "dns_error_rcode_total"),
            )
            count = self._sum(values["dns_query_total"])
            bad = (
                self._sum(values["dns_timeout_total"]),
                self._sum(values["dns_error_rcode_total"]),
            )
            latency = self._histogram_p95(
                window, samples, "dns_latency_histogram",
                set(values["dns_query_total"]), allow_empty=True,
            )
            if count.value == 0:
                if any(item.value != 0 for item in bad) \
                        or latency.sample_count != 0:
                    raise RawCollectionError(
                        "inactive DNS edge has failure/latency observations"
                    )
            if (
                latency.sample_count + bad[0].value
                != count.value
            ):
                raise RawCollectionError(
                    "DNS response histogram plus timeouts does not "
                    "match completed query_total"
                )
            failure = self._ratio(
                _combine(bad, sum(item.value for item in bad)),
                count,
                "dns_failure_rate",
            )
            outputs = (
                ("dns_query_count", count, "queries", "delta_counter", None),
                ("dns_latency_p95", latency, "milliseconds", "quantile", 0.95),
                ("dns_failure_rate", failure, "ratio", "gauge", None),
            )
            expected = DNS_OUTPUTS
        elif protocol == "tcp":
            values = self._deltas(
                window, samples, "edge_request_total", "edge_error_total",
                "edge_timeout_total",
            )
            self._series_sets_equal(
                values,
                ("edge_request_total", "edge_error_total", "edge_timeout_total"),
            )
            count = self._sum(values["edge_request_total"])
            bad = (
                self._sum(values["edge_error_total"]),
                self._sum(values["edge_timeout_total"]),
            )
            latency = self._histogram_p95(
                window, samples, "edge_latency_histogram",
                set(values["edge_request_total"]), allow_empty=True,
            )
            if count.value == 0:
                if any(item.value != 0 for item in bad) \
                        or latency.sample_count != 0:
                    raise RawCollectionError(
                        "inactive TCP edge has failure/latency observations"
                    )
            if latency.sample_count != count.value:
                raise RawCollectionError(
                    "TCP latency histogram count does not match request_total"
                )
            failure = self._ratio(
                _combine(bad, sum(item.value for item in bad)),
                count,
                "edge_failure_rate",
            )
            outputs = (
                ("edge_request_count", count, "requests", "delta_counter", None),
                ("edge_latency_p95", latency, "milliseconds", "quantile", 0.95),
                ("edge_failure_rate", failure, "ratio", "gauge", None),
            )
            expected = TCP_OUTPUTS
        else:
            raise RawCollectionError(
                f"final edge protocol must be tcp or dns, got {protocol!r}"
            )
        records = tuple(
            self._edge_record(
                window, entity_key, name, value, unit, kind, quantile
            )
            for name, value, unit, kind, quantile in outputs
        )
        if {item.metric_name for item in records} != expected:
            raise AssertionError("edge final metric set is not 3")
        sources, objects = self._sources(item[1] for item in outputs)
        return records, sources, objects

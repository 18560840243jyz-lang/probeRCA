"""Configuration and metric taxonomy for the final ProbeRCA-BPF control plane."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from proberca.dataplane.contracts import fingerprint


ROOT_CATEGORIES = frozenset({
    "CPU", "Memory", "IO", "Lock", "LocalNet", "NIC", "TCP",
})
FORMAL_EDGE_PROTOCOLS = frozenset({"tcp"})
EXPERIMENTAL_DNS_BURST_CHANNEL_IDS = frozenset({
    "dns.query_latency_p95",
    "dns.timeout_rate",
    "dns.rcode_failure_rate",
})
ENTITY_TYPES = frozenset({"service", "host", "edge"})
RECORD_TYPES = frozenset({"node_metric", "edge_metric"})
TRANSFORMS = frozenset({"identity", "log1p"})
FINAL_METRIC_KINDS = frozenset({"gauge", "delta_counter", "quantile"})
FINAL_AGGREGATIONS = frozenset({
    "counter_delta_then_cross_series_sum_rate",
    "counter_delta_then_cross_series_sum_ratio",
    "ratio_from_summed_components",
    "histogram_merge_quantile",
    "time_weighted_window_ratio",
    "cross_series_sum_delta",
})
FINAL_AGGREGATION_OUTPUT_SOURCE = "final_window_aggregation"
FINAL_SOURCE_DESCRIPTION = "final-window-aggregates-v1"
SCALE_FAMILIES = frozenset({"latency", "ratio", "count", "psi"})


def default_burst_channel_roles() -> tuple[dict[str, Any], ...]:
    """Exact Burst channels allowed to cross the final collection boundary."""
    entries = (
        ("sched.runqueue_wait_p95", "CPU", ("service",)),
        ("sched.wakeup_latency_p95", "CPU", ("service",)),
        ("memory.major_page_fault_rate", "Memory", ("service",)),
        ("memory.direct_reclaim_stall", "Memory", ("service",)),
        ("memory.oom_victim", "Memory", ("service",)),
        ("block.latency_p95", "IO", ("service",)),
        ("block.queue_wait_p95", "IO", ("service",)),
        ("futex.wait_count", "Lock", ("service",)),
        ("futex.wait_p95", "Lock", ("service",)),
        ("socket.queue_wait_p95", "LocalNet", ("service",)),
        ("socket.backlog_overflow", "LocalNet", ("service",)),
        ("socket.accept_connect_failure", "LocalNet", ("service",)),
        ("host.sched.runqueue_wait_p95", "CPU", ("host",)),
        ("host.sched.wakeup_latency_p95", "CPU", ("host",)),
        ("host.memory.direct_reclaim_stall", "Memory", ("host",)),
        ("host.memory.oom_victim", "Memory", ("host",)),
        ("host.block.latency_p95", "IO", ("host",)),
        ("host.block.queue_wait_p95", "IO", ("host",)),
        ("nic.queue_drop_rate", "NIC", ("host",)),
        ("nic.error_rate", "NIC", ("host",)),
        ("nic.softirq_latency_p95", "NIC", ("host",)),
        ("tcp.retrans_rate", "TCP", ("edge",)),
        ("tcp.rto_rate", "TCP", ("edge",)),
        ("tcp.rtt_p95", "TCP", ("edge",)),
        ("tcp.connect_failure_rate", "TCP", ("edge",)),
        ("tcp.rst_rate", "TCP", ("edge",)),
    )
    return tuple({
        "channel_id": channel_id,
        "root_category": root_category,
        "entity_types": list(entity_types),
    } for channel_id, root_category, entity_types in entries)


@dataclass(frozen=True)
class MetricRoleSpec:
    record_type: str
    metric_name: str
    entity_type: str
    role: str
    scopes: tuple[str, ...]
    protocols: tuple[str, ...] = ()
    root_category: str | None = None
    root_eligible: bool = False
    transform: str = "identity"
    polarity: int = 1
    unit: str = "ratio"
    metric_kind: str = "gauge"
    aggregation: str = "time_weighted_window_ratio"
    aggregation_formula: str = "window_time_weighted_ratio"
    source_scope: str = "service"
    quantile: float | None = None

    def __post_init__(self) -> None:
        if self.record_type not in RECORD_TYPES:
            raise ValueError("metric role has invalid record_type")
        if not self.metric_name or not self.role:
            raise ValueError("metric role requires metric_name and role")
        if self.entity_type not in ENTITY_TYPES:
            raise ValueError("metric role has invalid entity_type")
        if not self.scopes or tuple(sorted(set(self.scopes))) != self.scopes:
            raise ValueError("metric role scopes must be sorted and unique")
        if tuple(sorted(set(self.protocols))) != self.protocols:
            raise ValueError("metric role protocols must be sorted and unique")
        if self.entity_type == "edge" and self.record_type != "edge_metric":
            raise ValueError("edge entity roles require edge_metric records")
        if self.entity_type != "edge" and self.record_type != "node_metric":
            raise ValueError("service and host roles require node_metric records")
        if self.root_category is not None and self.root_category not in ROOT_CATEGORIES:
            raise ValueError("metric role has invalid root_category")
        if self.root_eligible != (self.root_category is not None):
            raise ValueError("root eligibility must match root_category presence")
        if self.transform not in TRANSFORMS:
            raise ValueError("metric role has invalid transform")
        if self.polarity not in {-1, 1}:
            raise ValueError("metric role polarity must be -1 or +1")
        if not self.unit:
            raise ValueError("metric role requires an output unit")
        if self.metric_kind not in FINAL_METRIC_KINDS:
            raise ValueError("metric role has invalid final metric_kind")
        if self.aggregation not in FINAL_AGGREGATIONS:
            raise ValueError("metric role has invalid aggregation semantics")
        if not isinstance(self.aggregation_formula, str) \
                or not self.aggregation_formula:
            raise ValueError("metric role requires an exact aggregation formula")
        if self.source_scope not in {"pod", "service", "node", "flow", "pod_pair", "service_pair"}:
            raise ValueError("metric role has invalid source_scope")
        if self.metric_kind == "quantile":
            if self.aggregation != "histogram_merge_quantile" \
                    or self.quantile != 0.95:
                raise ValueError("final P95 metrics require merged histogram quantile 0.95")
        elif self.quantile is not None:
            raise ValueError("non-quantile final metrics cannot declare quantile")
        if self.metric_kind == "delta_counter" \
                and self.aggregation != "cross_series_sum_delta":
            raise ValueError("final delta counters require cross-series delta sums")
        if self.aggregation.startswith("counter_delta_then_") \
                and self.metric_kind != "gauge":
            raise ValueError("final counter-derived rates/ratios must be gauges")
        if self.aggregation == "ratio_from_summed_components" \
                and (self.metric_kind != "gauge" or self.unit != "ratio"):
            raise ValueError("component ratios must emit ratio gauges")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MetricRoleSpec":
        expected = set(cls.__dataclass_fields__)
        values = dict(payload)
        values.setdefault("protocols", ())
        values.setdefault("root_category", None)
        values.setdefault("root_eligible", False)
        values.setdefault("transform", "identity")
        values.setdefault("polarity", 1)
        values.setdefault("unit", "ratio")
        values.setdefault("metric_kind", "gauge")
        values.setdefault("aggregation", "time_weighted_window_ratio")
        values.setdefault("aggregation_formula", "window_time_weighted_ratio")
        values.setdefault("source_scope", "service")
        values.setdefault("quantile", None)
        if set(values) != expected:
            raise ValueError("metric role fields mismatch")
        values["scopes"] = tuple(values["scopes"])
        values["protocols"] = tuple(values["protocols"])
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def scale_family(self) -> str:
        """Return the frozen statistical-scale family in transformed space."""
        if self.metric_kind == "quantile":
            return "latency"
        if "psi" in self.metric_name:
            return "psi"
        if self.metric_kind == "delta_counter" or self.unit in {
            "requests", "queries", "requests_per_second", "events_per_second",
        }:
            return "count"
        return "ratio"


def default_metric_roles() -> tuple[MetricRoleSpec, ...]:
    service_scopes = ("service",)
    edge_scopes = ("service_pair",)
    return tuple(sorted((
        MetricRoleSpec("node_metric", "request_rate", "service", "request_rate", service_scopes,
                       unit="requests_per_second",
                       aggregation="counter_delta_then_cross_series_sum_rate",
                       aggregation_formula="sum_pod(delta(request_total))/window_sec",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "request_failure_rate", "service", "request_failure", service_scopes,
                       aggregation="ratio_from_summed_components",
                       aggregation_formula="(sum_pod(delta(error_total))+sum_pod(delta(timeout_total)))/(sum_pod(delta(request_total))+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "request_latency_p95", "service", "request_latency", service_scopes,
                       transform="log1p", unit="milliseconds", metric_kind="quantile",
                       aggregation="histogram_merge_quantile",
                       aggregation_formula="q0.95(merge_pod(request_latency_histogram))",
                       source_scope="pod", quantile=0.95),
        MetricRoleSpec("node_metric", "cpu_usage_rate", "service", "service_cpu_usage", service_scopes,
                       root_category="CPU", root_eligible=True,
                       aggregation="counter_delta_then_cross_series_sum_ratio",
                       aggregation_formula="sum_container(delta(cpu_time_ns))/(window_ns*allocated_cpu_cores)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "cpu_throttle_ratio", "service", "service_cpu_throttle", service_scopes,
                       root_category="CPU", root_eligible=True,
                       aggregation="ratio_from_summed_components",
                       aggregation_formula="sum_container(delta(nr_throttled))/sum_container(delta(nr_periods)+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "memory_working_set_ratio", "service", "service_memory", service_scopes,
                       root_category="Memory", root_eligible=True,
                       aggregation="ratio_from_summed_components",
                       aggregation_formula="sum_container(working_set_bytes)/(sum_container(memory_limit_bytes)+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "io_psi", "service", "service_io", service_scopes,
                       root_category="IO", root_eligible=True,
                       aggregation_formula="sum_cgroup(io_psi_some_ns)/(window_ns*active_task_capacity+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "futex_wait_time_rate", "service", "service_lock", service_scopes,
                       root_category="Lock", root_eligible=True,
                       aggregation="counter_delta_then_cross_series_sum_ratio",
                       aggregation_formula="sum_cgroup(delta(futex_wait_ns))/(sum_cgroup(active_thread_ns)+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "local_socket_failure_rate", "service", "service_localnet", service_scopes,
                       root_category="LocalNet", root_eligible=True,
                       aggregation="ratio_from_summed_components",
                       aggregation_formula="sum_pod(delta(backlog_overflow+accept_fail+local_rst+local_drop))/(sum_pod(delta(socket_ops))+epsilon)",
                       source_scope="pod"),
        MetricRoleSpec("node_metric", "cpu_psi", "host", "host_cpu", ("node",),
                       root_category="CPU", root_eligible=True,
                       aggregation_formula="node_cpu_psi_some_ns/window_ns", source_scope="node"),
        MetricRoleSpec("node_metric", "memory_psi", "host", "host_memory", ("node",),
                       root_category="Memory", root_eligible=True,
                       aggregation_formula="node_memory_psi_some_ns/window_ns", source_scope="node"),
        MetricRoleSpec("node_metric", "io_psi", "host", "host_io", ("node",),
                       root_category="IO", root_eligible=True,
                       aggregation_formula="node_io_psi_some_ns/window_ns", source_scope="node"),
        MetricRoleSpec("node_metric", "nic_drop_error_rate", "host", "host_nic", ("node",),
                       root_category="NIC", root_eligible=True, unit="events_per_second",
                       aggregation="counter_delta_then_cross_series_sum_rate",
                       aggregation_formula="sum_interface(delta(rx_drop+tx_drop+rx_error+tx_error))/window_sec",
                       source_scope="node"),
        MetricRoleSpec("edge_metric", "edge_request_count", "edge", "edge_count", edge_scopes,
                       protocols=("tcp",), unit="requests", metric_kind="delta_counter",
                       aggregation="cross_series_sum_delta",
                       aggregation_formula="sum_flow(delta(request_total))", source_scope="flow"),
        MetricRoleSpec("edge_metric", "edge_latency_p95", "edge", "edge_latency", edge_scopes,
                       protocols=("tcp",), root_category="TCP", root_eligible=True,
                       transform="log1p", unit="milliseconds", metric_kind="quantile",
                       aggregation="histogram_merge_quantile",
                       aggregation_formula="q0.95(merge_flow(edge_latency_histogram))",
                       source_scope="flow", quantile=0.95),
        MetricRoleSpec("edge_metric", "edge_failure_rate", "edge", "edge_failure", edge_scopes,
                       protocols=("tcp",), root_category="TCP", root_eligible=True,
                       aggregation="ratio_from_summed_components",
                       aggregation_formula="sum_flow(delta(error_total+timeout_total))/(sum_flow(delta(request_total))+epsilon)",
                       source_scope="flow"),
    ), key=lambda item: (
        item.record_type, item.metric_name, item.entity_type, item.scopes, item.protocols,
    )))


def experimental_dns_metric_roles() -> tuple[MetricRoleSpec, ...]:
    """Compatibility-only DNS roles; they can be read but never enter formal RCA."""
    edge_scopes = ("service_pair",)
    return (
        MetricRoleSpec(
            "edge_metric", "dns_failure_rate", "edge", "edge_failure",
            edge_scopes, protocols=("dns",),
            root_eligible=False,
            aggregation="ratio_from_summed_components",
            aggregation_formula=(
                "sum_flow(delta(dns_timeout+dns_error_rcode))/"
                "(sum_flow(delta(dns_query_total))+epsilon)"
            ),
            source_scope="flow",
        ),
        MetricRoleSpec(
            "edge_metric", "dns_latency_p95", "edge", "edge_latency",
            edge_scopes, protocols=("dns",),
            root_eligible=False, transform="log1p", unit="milliseconds",
            metric_kind="quantile",
            aggregation="histogram_merge_quantile",
            aggregation_formula=(
                "q0.95(merge_flow(dns_latency_histogram))"
            ),
            source_scope="flow", quantile=0.95,
        ),
        MetricRoleSpec(
            "edge_metric", "dns_query_count", "edge", "edge_count",
            edge_scopes, protocols=("dns",), unit="queries",
            metric_kind="delta_counter",
            aggregation="cross_series_sum_delta",
            aggregation_formula="sum_flow(delta(dns_query_total))",
            source_scope="flow",
        ),
    )


@dataclass(frozen=True)
class FinalControlConfig:
    window_sec: int = 1
    baseline_min_windows: int = 6
    # Numeric protection only. Statistical lower bounds are family-specific.
    baseline_min_scale: float = 1.0e-12
    baseline_family_min_scales: dict[str, float | None] = field(
        default_factory=lambda: {
            family: None for family in sorted(SCALE_FAMILIES)
        }
    )
    latency_min_samples: int = 5
    failure_min_requests: int = 5
    service_lags: tuple[int, ...] = (1, 2)
    metric_lags: tuple[int, ...] = (1, 2)
    rls_forgetting_factor: float = 0.99
    rls_initial_covariance: float = 100.0
    service_min_training_updates: int = 4
    metric_ridge: float = 0.1
    metric_min_training_rows: int = 4
    metric_rows_per_feature: float = 2.0
    metric_rank_tolerance: float = 1.0e-10
    metric_max_condition_number: float = 1.0e8
    calibration_learning_windows: int = 600
    calibration_validation_windows: int = 300
    calibration_required_entity_types: tuple[str, ...] = (
        "edge", "host", "service",
    )
    calibration_required_root_coordinates: tuple[str, ...] = ()
    alpha_latency: float = 0.5
    alpha_failure: float = 0.5
    soft_threshold: float = 3.0
    soft_consecutive_windows: int = 3
    hard_threshold: float = 5.0
    hard_consecutive_windows: int = 2
    recovery_threshold: float = 1.0
    recovery_windows: int = 2
    candidate_hops: int = 2
    service_edge_threshold: float = 0.05
    burst_window_count: int = 1
    burst_eta: float = 2.0
    l1_penalty: float = 0.15
    group_penalties: dict[str, float] = field(default_factory=lambda: {
        category: 0.25 for category in sorted(ROOT_CATEGORIES)
    })
    fista_max_iterations: int = 500
    fista_tolerance: float = 1.0e-7
    top_k: int = 5
    strict_metric_contract: bool = True
    metric_roles: tuple[MetricRoleSpec, ...] = field(default_factory=default_metric_roles)

    def __post_init__(self) -> None:
        for name in (
            "window_sec", "baseline_min_windows", "metric_min_training_rows",
            "latency_min_samples", "failure_min_requests",
            "service_min_training_updates", "calibration_learning_windows",
            "calibration_validation_windows",
            "soft_consecutive_windows", "hard_consecutive_windows",
            "recovery_windows", "candidate_hops", "burst_window_count",
            "fista_max_iterations", "top_k",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"control.{name} must be a positive integer")
        for name in ("service_lags", "metric_lags"):
            values = getattr(self, name)
            if not values or tuple(sorted(set(values))) != values \
                    or any(isinstance(item, bool) or not isinstance(item, int) or item <= 0
                           for item in values):
                raise ValueError(f"control.{name} must contain sorted positive lags")
        finite_positive = (
            "baseline_min_scale", "rls_initial_covariance", "metric_ridge",
            "soft_threshold", "hard_threshold", "l1_penalty",
            "fista_tolerance", "metric_rows_per_feature",
            "metric_rank_tolerance", "metric_max_condition_number",
        )
        for name in finite_positive:
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool) \
                    or not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"control.{name} must be finite and positive")
        if not 0.0 < self.rls_forgetting_factor <= 1.0:
            raise ValueError("control.rls_forgetting_factor must be in (0,1]")
        if self.hard_threshold <= self.soft_threshold:
            raise ValueError("control hard threshold must exceed soft threshold")
        if not 0.0 <= self.recovery_threshold < self.soft_threshold:
            raise ValueError("control recovery threshold must be below soft threshold")
        if self.alpha_latency < 0 or self.alpha_failure < 0 \
                or not math.isclose(self.alpha_latency + self.alpha_failure, 1.0):
            raise ValueError("service state weights must be non-negative and sum to one")
        if any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or value < 0
            for value in (self.service_edge_threshold, self.burst_eta)
        ):
            raise ValueError("candidate threshold and burst eta must be finite and non-negative")
        if type(self.strict_metric_contract) is not bool:
            raise TypeError("strict_metric_contract must be boolean")
        if set(self.baseline_family_min_scales) != SCALE_FAMILIES:
            raise ValueError(
                "baseline family floors must cover every scale family"
            )
        for family, value in self.baseline_family_min_scales.items():
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= self.baseline_min_scale
            ):
                raise ValueError(
                    f"baseline family floor {family} must exceed numeric epsilon"
                )
        if not self.calibration_required_entity_types \
                or tuple(sorted(set(self.calibration_required_entity_types))) \
                != self.calibration_required_entity_types \
                or any(
                    item not in ENTITY_TYPES
                    for item in self.calibration_required_entity_types
                ):
            raise ValueError(
                "calibration entity types must be sorted and unique"
            )
        if tuple(sorted(set(
            self.calibration_required_root_coordinates
        ))) != self.calibration_required_root_coordinates or any(
            not isinstance(item, str) or not item
            for item in self.calibration_required_root_coordinates
        ):
            raise ValueError(
                "calibration root coordinates must be sorted, unique strings"
            )
        if set(self.group_penalties) != ROOT_CATEGORIES \
                or any(not math.isfinite(value) or value < 0
                       for value in self.group_penalties.values()):
            raise ValueError("group_penalties must cover every root category")
        if not self.metric_roles or any(
            not isinstance(item, MetricRoleSpec) for item in self.metric_roles
        ):
            raise TypeError("metric_roles must contain MetricRoleSpec")
        identities = [(
            item.record_type, item.metric_name, item.entity_type,
            item.scopes, item.protocols,
        ) for item in self.metric_roles]
        if len(identities) != len(set(identities)):
            raise ValueError("metric role definitions contain duplicates")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FinalControlConfig":
        values = dict(payload)
        expected = set(cls.__dataclass_fields__)
        defaults = cls()
        for name in expected:
            if name not in values:
                values[name] = getattr(defaults, name)
        if set(values) != expected:
            raise ValueError("final control config fields mismatch")
        values["service_lags"] = tuple(values["service_lags"])
        values["metric_lags"] = tuple(values["metric_lags"])
        values["calibration_required_entity_types"] = tuple(
            values["calibration_required_entity_types"]
        )
        values["calibration_required_root_coordinates"] = tuple(
            values["calibration_required_root_coordinates"]
        )
        values["metric_roles"] = tuple(
            item if isinstance(item, MetricRoleSpec) else MetricRoleSpec.from_dict(item)
            for item in values["metric_roles"]
        )
        values["group_penalties"] = {
            str(key): float(value) for key, value in values["group_penalties"].items()
        }
        values["baseline_family_min_scales"] = {
            str(key): (None if value is None else float(value))
            for key, value in values["baseline_family_min_scales"].items()
        }
        return cls(**values)

    @property
    def config_fingerprint(self) -> str:
        return fingerprint(self.to_dict())

    @property
    def collection_contract(self) -> dict[str, Any]:
        roles = [
            item.to_dict() for item in sorted(
                self.metric_roles,
                key=lambda value: (
                    value.record_type, value.metric_name, value.entity_type,
                    value.scopes, value.protocols,
                ),
            )
        ]
        burst_roles = list(default_burst_channel_roles())
        aggregation_fingerprint = fingerprint({
            "output_source": FINAL_AGGREGATION_OUTPUT_SOURCE,
            "roles": roles,
        })
        burst_fingerprint = fingerprint({
            "roles": burst_roles,
            "semantics": "normalized_strength_times_quality",
        })
        return {
            "schema_version": "probeRCA-final-collection-contract-v4",
            "normal_metric_roles": roles,
            "aggregation_output_source": FINAL_AGGREGATION_OUTPUT_SOURCE,
            "aggregation_config_fingerprint": aggregation_fingerprint,
            "source_description": FINAL_SOURCE_DESCRIPTION,
            "burst_evidence_source_type": "burst_event",
            "burst_evidence_semantics": "normalized_strength_times_quality",
            "burst_channel_roles": burst_roles,
            "burst_config_fingerprint": burst_fingerprint,
            "window_sec": self.window_sec,
        }

    @property
    def collection_contract_fingerprint(self) -> str:
        return fingerprint(self.collection_contract)

    def project_collection_contract(
        self, contract: dict[str, Any],
    ) -> dict[str, Any]:
        """Project only explicit v2/v3 DNS-era contracts into formal v4."""
        schema = contract.get("schema_version")
        if schema == "probeRCA-final-collection-contract-v4":
            return dict(contract)
        if schema not in {
            "probeRCA-final-collection-contract-v2",
            "probeRCA-final-collection-contract-v3",
        }:
            raise ValueError("unsupported legacy collection contract")
        archived_roles = contract["normal_metric_roles"]
        discarded_roles = [
            role for role in archived_roles if "dns" in role["protocols"]
        ]
        allowed_dns_roles = {
            (spec.metric_name, spec.role)
            for spec in experimental_dns_metric_roles()
        }
        if any(
            tuple(role["protocols"]) != ("dns",)
            or (role["metric_name"], role["role"]) not in allowed_dns_roles
            for role in discarded_roles
        ):
            raise ValueError("legacy DNS metric role is ambiguous")
        roles = [
            role for role in archived_roles if role not in discarded_roles
        ]
        archived_burst = contract["burst_channel_roles"]
        discarded_burst = [
            role for role in archived_burst
            if role["root_category"] == "DNS"
        ]
        if any(
            role["channel_id"] not in EXPERIMENTAL_DNS_BURST_CHANNEL_IDS
            for role in discarded_burst
        ):
            raise ValueError("legacy DNS Burst role is unknown")
        burst_roles = [
            role for role in archived_burst if role not in discarded_burst
        ]
        return {
            "schema_version": "probeRCA-final-collection-contract-v4",
            "normal_metric_roles": roles,
            "aggregation_output_source": contract[
                "aggregation_output_source"
            ],
            "aggregation_config_fingerprint": fingerprint({
                "output_source": contract["aggregation_output_source"],
                "roles": roles,
            }),
            "source_description": contract["source_description"],
            "burst_evidence_source_type": contract[
                "burst_evidence_source_type"
            ],
            "burst_evidence_semantics": contract[
                "burst_evidence_semantics"
            ],
            "burst_channel_roles": burst_roles,
            "burst_config_fingerprint": fingerprint({
                "roles": burst_roles,
                "semantics": contract["burst_evidence_semantics"],
            }),
            "window_sec": contract["window_sec"],
        }

    @property
    def required_scope_fingerprint(self) -> str:
        return fingerprint({
            "entity_types": sorted(
                self.calibration_required_entity_types
            ),
            "root_coordinates": sorted(
                self.calibration_required_root_coordinates
            ),
        })

    @property
    def scale_config_fingerprint(self) -> str:
        return fingerprint({
            "numeric_epsilon": self.baseline_min_scale,
            "family_floors": dict(sorted(
                self.baseline_family_min_scales.items()
            )),
            "metric_families": [
                {
                    "record_type": item.record_type,
                    "metric_name": item.metric_name,
                    "entity_type": item.entity_type,
                    "scopes": list(item.scopes),
                    "protocols": list(item.protocols),
                    "transform": item.transform,
                    "scale_family": item.scale_family,
                }
                for item in sorted(
                    self.metric_roles,
                    key=lambda value: (
                        value.record_type, value.metric_name,
                        value.entity_type, value.scopes,
                        value.protocols,
                    ),
                )
            ],
        })

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["service_lags"] = list(self.service_lags)
        payload["metric_lags"] = list(self.metric_lags)
        payload["metric_roles"] = [item.to_dict() for item in self.metric_roles]
        payload["group_penalties"] = dict(sorted(self.group_penalties.items()))
        return payload

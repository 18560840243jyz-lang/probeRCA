"""Dataclass schemas for probeRCA P0 records."""

from __future__ import annotations

import ipaddress
import math
from dataclasses import MISSING, asdict, dataclass, field, fields, is_dataclass
from typing import Any, ClassVar

PROBERCA_SCHEMA_VERSION = "1.0"
METRIC_RECORD_SCHEMA_VERSION = "2.0"
LEGACY_METRIC_RECORD_SCHEMA_VERSION = "1.0"
METRIC_INVALID_REASONS = frozenset({
    "no_exposure",
    "zero_coverage",
    "insufficient_sample_count",
    "excessive_event_loss",
    "missing_component",
})

_LEGACY_COUNT_METRICS = frozenset({
    "request_rate",
    "edge_request_count",
    "dns_query_count",
})


@dataclass
class MetricRecord:
    """Metric observation for one service instance node at one timestamp."""

    timestamp: float
    service: str
    instance: str
    node: str
    metric: str
    value: float
    source: str
    incident_id: str | None = None


@dataclass
class EvidenceRecord:
    """Semantic evidence observation used to describe a root-cause type."""

    timestamp: float
    service: str
    instance: str
    node: str
    evidence_type: str
    metric: str
    value: float
    source: str
    probe_id: str
    sampling_rate: float
    incident_id: str | None = None


@dataclass
class IncidentRecord:
    """Fault injection label for offline P0 validation."""

    incident_id: str
    root_service: str
    root_metric: str
    root_type: str
    symptom_service: str
    start_ts: float
    end_ts: float
    injected_path: list[str]


@dataclass
class RCAResult:
    """Root cause analysis output record for one incident."""

    incident_id: str
    symptom_service: str
    top_services: list[dict]
    top_metrics: list[dict]
    root_type: str
    evidence: list[str]
    path: list[str]
    latency_ms: float | None = None


def to_dict(record: Any) -> dict:
    """Convert a dataclass record or dictionary to a plain dictionary."""

    if isinstance(record, StrictRecord):
        return record.to_dict()
    if is_dataclass(record):
        return asdict(record)
    if isinstance(record, dict):
        return dict(record)
    raise TypeError(f"Unsupported record type for to_dict: {type(record).__name__}")


NODE_METRIC_FAMILIES = frozenset({"request", "cpu", "memory", "io", "net_local", "lock"})
BURST_EVENT_TYPES = frozenset(
    {
        "sched.runqueue_wait",
        "sched.offcpu",
        "block.latency",
        "fs.read_latency",
        "fs.write_latency",
        "futex.wait",
        "futex.wake",
        "block.issue",
        "block.complete",
        "tcp.retransmit",
        "tcp.rto",
        "tcp.rtt",
        "tcp.rst",
        "tcp.connect_fail",
        "dns.latency",
        "dns.timeout",
        "dns.query",
        "dns.response",
        "process.fork",
        "process.exec",
        "process.exit",
        "process.cgroup_migrate",
        "sidecar.queue",
        "proxy.upstream_latency",
        "probe.loss",
    }
)
ALERT_STATES = frozenset({"healthy", "soft", "hard", "recovery", "edge_anomaly"})
EDGE_SUBTYPES = frozenset({"propagated-edge", "exogenous-edge-shock"})
METRIC_KINDS = frozenset({"gauge", "monotonic_counter", "delta_counter", "histogram_bucket", "quantile"})
NODE_METRIC_SCOPES = frozenset({"pod", "service", "node"})
EDGE_METRIC_SCOPES = frozenset({"flow", "pod_pair", "service_pair"})


def _required_string(name: str, value: Any) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _optional_string(name: str, value: Any) -> None:
    if value is not None:
        _required_string(name, value)


def _identity_component(name: str, value: str) -> None:
    _required_string(name, value)
    if "::" in value or "->" in value:
        raise ValueError(f"{name} contains an ambiguous stable-ID separator")


def _integer(name: str, value: Any, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")


def _finite_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _probability(name: str, value: Any) -> float:
    result = _finite_number(name, value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return result


def _fixed_record_type(actual: Any, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"record_type must be fixed as {expected!r}")


def _metric_distribution_semantics(
    metric_kind: Any,
    histogram_upper_bound: Any,
    histogram_is_inf_bucket: Any,
    histogram_is_cumulative: Any,
    quantile: Any,
) -> tuple[float | None, float | None]:
    if metric_kind not in METRIC_KINDS:
        raise ValueError(f"invalid metric_kind {metric_kind!r}")
    bound = None
    quantile_value = None
    if histogram_upper_bound is not None:
        bound = _finite_number("histogram_upper_bound", histogram_upper_bound)
    if quantile is not None:
        quantile_value = _finite_number("quantile", quantile)

    if not isinstance(histogram_is_inf_bucket, bool):
        raise TypeError("histogram_is_inf_bucket must be a boolean")
    if metric_kind == "histogram_bucket":
        if histogram_is_inf_bucket and bound is not None:
            raise ValueError("+Inf histogram buckets must not have a finite upper bound")
        if not histogram_is_inf_bucket and bound is None:
            raise ValueError("finite histogram buckets require histogram_upper_bound")
        if histogram_is_cumulative is None:
            raise ValueError("histogram_is_cumulative is required for histogram_bucket")
        if not isinstance(histogram_is_cumulative, bool):
            raise TypeError("histogram_is_cumulative must be a boolean for histogram_bucket")
        if quantile is not None:
            raise ValueError("quantile must be None for histogram_bucket")
    elif metric_kind == "quantile":
        if quantile_value is None or not 0.0 < quantile_value < 1.0:
            raise ValueError("quantile must be in (0, 1) for quantile metrics")
        if histogram_upper_bound is not None or histogram_is_inf_bucket or histogram_is_cumulative is not None:
            raise ValueError("histogram fields must be None for quantile metrics")
    elif histogram_is_inf_bucket or any(
        value is not None for value in (histogram_upper_bound, histogram_is_cumulative, quantile)
    ):
        raise ValueError("distribution fields must be None for scalar metric kinds")
    return bound, quantile_value


def _string_list(name: str, value: Any) -> None:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    for item in value:
        _required_string(f"{name} item", item)


def _score_map(name: str, value: Any) -> None:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a dictionary")
    for key, score in value.items():
        _required_string(f"{name} key", key)
        _probability(f"{name}[{key!r}]", score)


def _anomaly_score_map(name: str, value: Any) -> None:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a dictionary")
    for key, score in value.items():
        _required_string(f"{name} key", key)
        if _finite_number(f"{name}[{key!r}]", score) < 0:
            raise ValueError(f"{name} anomaly scores must be non-negative")


def _json_value(name: str, value: Any) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        _finite_number(name, value)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _json_value(f"{name}[{index}]", item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _required_string(f"{name} key", key)
            _json_value(f"{name}.{key}", item)
        return
    raise TypeError(f"{name} contains a non-JSON value: {type(value).__name__}")


class StrictRecord:
    """Strict dataclass parsing shared by versioned ProbeRCA records."""

    _nested_fields: ClassVar[dict[str, type["StrictRecord"]]] = {}
    _nested_list_fields: ClassVar[dict[str, type["StrictRecord"]]] = {}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]):
        if not isinstance(payload, dict):
            raise TypeError(f"{cls.__name__} payload must be a dictionary")
        dataclass_fields = fields(cls)
        expected = {dataclass_field.name for dataclass_field in dataclass_fields}
        actual = set(payload)
        unknown = sorted(actual - expected)
        missing = sorted(expected - actual)
        if unknown:
            raise ValueError(f"{cls.__name__} unknown fields: {unknown}")
        if missing:
            raise ValueError(f"{cls.__name__} missing fields: {missing}")
        values = dict(payload)
        for dataclass_field in dataclass_fields:
            if not dataclass_field.init:
                if dataclass_field.default is MISSING:
                    raise TypeError(f"{dataclass_field.name} has no fixed default")
                if values[dataclass_field.name] != dataclass_field.default:
                    raise ValueError(
                        f"{cls.__name__} {dataclass_field.name} conflicts with its fixed value"
                    )
        for name, nested_type in cls._nested_fields.items():
            value = values[name]
            if isinstance(value, dict):
                values[name] = nested_type.from_dict(value)
            elif not isinstance(value, nested_type):
                raise TypeError(f"{name} must be {nested_type.__name__}")
        for name, nested_type in cls._nested_list_fields.items():
            value = values[name]
            if not isinstance(value, list):
                raise TypeError(f"{name} must be a list")
            values[name] = [
                nested_type.from_dict(item) if isinstance(item, dict) else item for item in value
            ]
            if any(not isinstance(item, nested_type) for item in values[name]):
                raise TypeError(f"{name} items must be {nested_type.__name__}")
        return cls(
            **{dataclass_field.name: values[dataclass_field.name]
               for dataclass_field in dataclass_fields if dataclass_field.init}
        )

    def validate(self) -> None:
        self.__post_init__()

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


def _schema_version(value: Any) -> None:
    _required_string("schema_version", value)
    if value != PROBERCA_SCHEMA_VERSION:
        raise ValueError(
            f"incompatible schema_version {value!r}; expected {PROBERCA_SCHEMA_VERSION!r}"
        )


def _metric_record_schema_version(value: Any) -> None:
    _required_string("schema_version", value)
    if value != METRIC_RECORD_SCHEMA_VERSION:
        raise ValueError(
            f"incompatible metric record schema_version {value!r}; "
            f"expected {METRIC_RECORD_SCHEMA_VERSION!r}"
        )


def _metric_validity(
    value: Any,
    valid: Any,
    invalid_reason: Any,
) -> float | None:
    if not isinstance(valid, bool):
        raise TypeError("valid must be a boolean")
    if valid:
        if invalid_reason is not None:
            raise ValueError("valid metric records must not have invalid_reason")
        return _finite_number("value", value)
    if value is not None:
        raise ValueError("invalid metric records must have value=None")
    if not isinstance(invalid_reason, str) or not invalid_reason:
        raise ValueError(
            "invalid metric records require a non-empty invalid_reason"
        )
    if invalid_reason not in METRIC_INVALID_REASONS:
        raise ValueError(
            f"unsupported metric invalid_reason {invalid_reason!r}"
        )
    return None


def _legacy_metric_projection(
    payload: dict[str, Any],
) -> tuple[float | None, bool, str | None]:
    value = _finite_number("value", payload.get("value"))
    coverage = _probability("coverage", payload.get("coverage"))
    sample_count = payload.get("sample_count")
    _integer("sample_count", sample_count)
    metric_name = payload.get("metric_name")
    _identity_component("metric_name", metric_name)
    if metric_name in _LEGACY_COUNT_METRICS:
        return value, True, None
    if coverage > 0.0:
        return value, True, None
    is_no_exposure_role = (
        "latency" in metric_name
        or "failure" in metric_name
        or metric_name.endswith("_ratio")
    )
    if value == 0.0 and sample_count == 0 and is_no_exposure_role:
        return None, False, "no_exposure"
    return None, False, "zero_coverage"


def _metric_record_from_dict(record_type, payload: dict[str, Any]):
    values = dict(payload)
    dataclass_fields = fields(record_type)
    expected = {item.name for item in dataclass_fields}
    legacy_expected = expected - {
        "valid",
        "invalid_reason",
        "mapping_quality",
    }
    if (
        values.get("schema_version") == LEGACY_METRIC_RECORD_SCHEMA_VERSION
        and set(values) == legacy_expected
    ):
        value, valid, invalid_reason = _legacy_metric_projection(values)
        values.update(
            schema_version=METRIC_RECORD_SCHEMA_VERSION,
            value=value,
            valid=valid,
            invalid_reason=invalid_reason,
            mapping_quality=1.0,
        )
    return StrictRecord.from_dict.__func__(record_type, values)


@dataclass(frozen=True)
class NodeMetricRecord(StrictRecord):
    schema_version: str
    timestamp_ns: int
    window_sec: int
    cluster_id: str
    node_name: str | None
    namespace: str
    service_name: str
    pod_uid: str | None
    container_id: str | None
    metric_family: str
    metric_name: str
    value: float | None
    valid: bool
    invalid_reason: str | None
    unit: str
    sample_count: int
    coverage: float
    event_loss_rate: float
    mapping_quality: float
    source: str
    metric_kind: str
    scope: str
    histogram_upper_bound: float | None
    histogram_is_inf_bucket: bool
    histogram_is_cumulative: bool | None
    quantile: float | None
    record_type: str = field(default="node_metric", init=False)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]):
        return _metric_record_from_dict(cls, payload)

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "node_metric")
        _metric_record_schema_version(self.schema_version)
        _integer("timestamp_ns", self.timestamp_ns)
        _integer("window_sec", self.window_sec, 1)
        _identity_component("cluster_id", self.cluster_id)
        _optional_string("node_name", self.node_name)
        _identity_component("namespace", self.namespace)
        _identity_component("service_name", self.service_name)
        _optional_string("pod_uid", self.pod_uid)
        _optional_string("container_id", self.container_id)
        if self.metric_family not in NODE_METRIC_FAMILIES:
            raise ValueError(f"invalid metric_family {self.metric_family!r}")
        _identity_component("metric_name", self.metric_name)
        object.__setattr__(
            self,
            "value",
            _metric_validity(self.value, self.valid, self.invalid_reason),
        )
        _required_string("unit", self.unit)
        _integer("sample_count", self.sample_count)
        object.__setattr__(self, "coverage", _probability("coverage", self.coverage))
        object.__setattr__(
            self, "event_loss_rate", _probability("event_loss_rate", self.event_loss_rate)
        )
        object.__setattr__(
            self, "mapping_quality", _probability(
                "mapping_quality", self.mapping_quality,
            )
        )
        _required_string("source", self.source)
        if self.scope not in NODE_METRIC_SCOPES:
            raise ValueError(f"invalid node metric scope {self.scope!r}")
        if self.scope == "pod" and self.pod_uid is None:
            raise ValueError("pod_uid is required for pod-scoped node metrics")
        if self.scope == "node" and self.node_name is None:
            raise ValueError("node_name is required for node-scoped node metrics")
        bound, quantile_value = _metric_distribution_semantics(
            self.metric_kind,
            self.histogram_upper_bound,
            self.histogram_is_inf_bucket,
            self.histogram_is_cumulative,
            self.quantile,
        )
        if (
            self.valid
            and self.metric_kind == "histogram_bucket"
            and self.value < 0
        ):
            raise ValueError("histogram bucket value is a count and must be non-negative")
        object.__setattr__(self, "histogram_upper_bound", bound)
        object.__setattr__(self, "quantile", quantile_value)

    @property
    def stable_id(self) -> str:
        return f"{self.cluster_id}::{self.namespace}::{self.service_name}::{self.metric_name}"

    @property
    def series_id(self) -> str:
        if self.scope == "pod":
            resource = f"pod={self.pod_uid};container={self.container_id or '-'}"
        elif self.scope == "node":
            resource = f"node={self.node_name}"
        else:
            resource = "service"
        return f"{self.stable_id}::scope={self.scope}::{resource}"


@dataclass(frozen=True)
class EdgeMetricRecord(StrictRecord):
    schema_version: str
    timestamp_ns: int
    window_sec: int
    cluster_id: str
    namespace: str
    src_service: str
    dst_service: str
    src_pod_uid: str | None
    dst_pod_uid: str | None
    src_node: str | None
    dst_node: str | None
    protocol: str
    metric_name: str
    value: float | None
    valid: bool
    invalid_reason: str | None
    unit: str
    sample_count: int
    coverage: float
    event_loss_rate: float
    mapping_quality: float
    source: str
    metric_kind: str
    scope: str
    histogram_upper_bound: float | None
    histogram_is_inf_bucket: bool
    histogram_is_cumulative: bool | None
    quantile: float | None
    record_type: str = field(default="edge_metric", init=False)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]):
        return _metric_record_from_dict(cls, payload)

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "edge_metric")
        _metric_record_schema_version(self.schema_version)
        _integer("timestamp_ns", self.timestamp_ns)
        _integer("window_sec", self.window_sec, 1)
        for name in ("cluster_id", "namespace", "src_service", "dst_service", "protocol", "metric_name"):
            _identity_component(name, getattr(self, name))
        for name in ("src_pod_uid", "dst_pod_uid", "src_node", "dst_node"):
            _optional_string(name, getattr(self, name))
        object.__setattr__(
            self,
            "value",
            _metric_validity(self.value, self.valid, self.invalid_reason),
        )
        _required_string("unit", self.unit)
        _integer("sample_count", self.sample_count)
        object.__setattr__(self, "coverage", _probability("coverage", self.coverage))
        object.__setattr__(
            self, "event_loss_rate", _probability("event_loss_rate", self.event_loss_rate)
        )
        object.__setattr__(
            self, "mapping_quality", _probability(
                "mapping_quality", self.mapping_quality,
            )
        )
        _required_string("source", self.source)
        if self.scope not in EDGE_METRIC_SCOPES:
            raise ValueError(f"invalid edge metric scope {self.scope!r}")
        bound, quantile_value = _metric_distribution_semantics(
            self.metric_kind,
            self.histogram_upper_bound,
            self.histogram_is_inf_bucket,
            self.histogram_is_cumulative,
            self.quantile,
        )
        if (
            self.valid
            and self.metric_kind == "histogram_bucket"
            and self.value < 0
        ):
            raise ValueError("histogram bucket value is a count and must be non-negative")
        object.__setattr__(self, "histogram_upper_bound", bound)
        object.__setattr__(self, "quantile", quantile_value)

    @property
    def stable_id(self) -> str:
        return (
            f"{self.cluster_id}::{self.namespace}::{self.src_service}->{self.dst_service}::"
            f"{self.protocol}::{self.metric_name}"
        )

    @property
    def stable_shock_id(self) -> str:
        return (
            f"{self.cluster_id}::{self.namespace}::{self.src_service}->{self.dst_service}::"
            f"{self.protocol}::shock::{self.metric_name}"
        )

    @property
    def series_id(self) -> str:
        if self.scope in {"flow", "pod_pair"}:
            resource = f"pods={self.src_pod_uid or '-'}->{self.dst_pod_uid or '-'}"
        else:
            resource = "service_pair"
        return f"{self.stable_id}::scope={self.scope}::{resource}"


@dataclass(frozen=True)
class BurstEventRecord(StrictRecord):
    schema_version: str
    event_id: str
    timestamp_ns: int
    event_type: str
    pid: int | None
    tid: int | None
    cgroup_id: int | None
    container_id: str | None
    pod_uid: str | None
    service_name: str | None
    node_name: str | None
    src_service: str | None
    dst_service: str | None
    src_ip: str | None
    dst_ip: str | None
    src_port: int | None
    dst_port: int | None
    protocol: str | None
    value: float
    unit: str
    probe_mode: str
    burst_id: str | None
    lost_events: int
    event_class: str = "unmapped"
    quality: str = "unmapped"
    mapping_status: str = "unmapped"
    process_start_time_ns: int = 0
    container_identity_fingerprint: str | None = None
    pod_identity_fingerprint: str | None = None
    source_cgroup_id: int | None = None
    target_cgroup_id: int | None = None
    direction: str = "unknown"
    metric_family: str | None = None
    probe_name: str | None = None
    attach_epoch: int = 0
    event_sequence: int = 0
    cpu: int = 0
    record_type: str = field(default="burst_event", init=False)

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "burst_event")
        _schema_version(self.schema_version)
        _required_string("event_id", self.event_id)
        _integer("timestamp_ns", self.timestamp_ns)
        if self.event_type not in BURST_EVENT_TYPES:
            raise ValueError(f"invalid event_type {self.event_type!r}")
        for name in ("pid", "tid", "cgroup_id"):
            value = getattr(self, name)
            if value is not None:
                _integer(name, value)
        for name in (
            "container_id",
            "pod_uid",
            "service_name",
            "node_name",
            "src_service",
            "dst_service",
            "protocol",
        ):
            _optional_string(name, getattr(self, name))
        for name in ("src_ip", "dst_ip"):
            value = getattr(self, name)
            if value is not None:
                _required_string(name, value)
                try:
                    ipaddress.ip_address(value)
                except ValueError as exc:
                    raise ValueError(f"{name} must be a valid IP address") from exc
        for name in ("src_port", "dst_port"):
            value = getattr(self, name)
            if value is not None:
                _integer(name, value, 1)
                if value > 65535:
                    raise ValueError(f"{name} must be <= 65535")
        object.__setattr__(self, "value", _finite_number("value", self.value))
        _required_string("unit", self.unit)
        if self.probe_mode not in {"always_on", "burst"}:
            raise ValueError(f"invalid probe_mode {self.probe_mode!r}")
        _optional_string("burst_id", self.burst_id)
        if self.probe_mode == "burst" and self.burst_id is None:
            raise ValueError("burst_id is required in burst mode")
        if self.probe_mode == "always_on" and self.burst_id is not None:
            raise ValueError("burst_id must be None in always_on mode")
        _integer("lost_events", self.lost_events)
        if self.event_class not in {"node", "edge", "unmapped", "control", "loss"}:
            raise ValueError("invalid burst event_class")
        if self.quality not in {"exact", "derived", "partial", "unmapped"}:
            raise ValueError("invalid burst event quality")
        if self.mapping_status not in {"mapped", "partial", "unmapped", "pid_reused"}:
            raise ValueError("invalid burst event mapping_status")
        _integer("process_start_time_ns", self.process_start_time_ns)
        for name in ("container_identity_fingerprint", "pod_identity_fingerprint"):
            value = getattr(self, name)
            if value is not None and (len(value) != 64 or any(
                    character not in "0123456789abcdef" for character in value)):
                raise ValueError(f"{name} must be lowercase SHA-256")
        for name in ("source_cgroup_id", "target_cgroup_id"):
            value = getattr(self, name)
            if value is not None:
                _integer(name, value)
        if self.direction not in {"unknown", "ingress", "egress"}:
            raise ValueError("invalid burst event direction")
        _optional_string("metric_family", self.metric_family)
        _optional_string("probe_name", self.probe_name)
        for name in ("attach_epoch", "event_sequence", "cpu"):
            _integer(name, getattr(self, name))


@dataclass(frozen=True)
class TopologyEdge(StrictRecord):
    src_service: str
    dst_service: str
    relation_type: str
    src_namespace: str | None = None
    dst_namespace: str | None = None
    protocol: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    directed: bool | None = None

    def __post_init__(self) -> None:
        _identity_component("src_service", self.src_service)
        _identity_component("dst_service", self.dst_service)
        if self.relation_type not in {"call", "impact", "host", "resource"}:
            raise ValueError(f"invalid relation_type {self.relation_type!r}")
        for name in ("src_namespace", "dst_namespace", "protocol", "resource_type", "resource_id"):
            _optional_string(name, getattr(self, name))
        if self.directed is not None and not isinstance(self.directed, bool):
            raise TypeError("directed must be a boolean or None")
        effective_direction = self.directed
        if effective_direction is None:
            effective_direction = self.relation_type in {"call", "impact"}
            object.__setattr__(self, "directed", effective_direction)
        if self.relation_type in {"call", "impact"} and not effective_direction:
            raise ValueError("call and impact relations must be directed")


@dataclass(frozen=True)
class ServiceNodePlacement(StrictRecord):
    namespace: str
    service_name: str
    node_name: str
    pod_uid: str | None

    def __post_init__(self) -> None:
        for name in ("namespace", "service_name", "node_name"):
            _identity_component(name, getattr(self, name))
        _optional_string("pod_uid", self.pod_uid)


@dataclass(frozen=True)
class ServiceResourceBinding(StrictRecord):
    namespace: str
    service_name: str
    resource_type: str
    resource_id: str

    def __post_init__(self) -> None:
        for name in ("namespace", "service_name", "resource_type", "resource_id"):
            _identity_component(name, getattr(self, name))


@dataclass(frozen=True)
class TopologySnapshot(StrictRecord):
    schema_version: str
    snapshot_id: str
    valid_from_ns: int
    valid_to_ns: int
    cluster_id: str
    services: list[str]
    call_edges: list[TopologyEdge]
    host_edges: list[TopologyEdge]
    resource_edges: list[TopologyEdge]
    service_nodes: list[ServiceNodePlacement] = field(default_factory=list)
    service_resources: list[ServiceResourceBinding] = field(default_factory=list)
    structure_fingerprint: str | None = None
    inventory_revision_id: str | None = None
    resource_version_vector: dict[str, str] = field(default_factory=dict)
    runtime_identity_fingerprints: list[str] = field(default_factory=list)
    call_edge_provider_fingerprint: str | None = None
    topology_build_issues: list[dict[str, Any]] = field(default_factory=list)
    record_type: str = field(default="topology_snapshot", init=False)

    _nested_list_fields = {
        "call_edges": TopologyEdge,
        "host_edges": TopologyEdge,
        "resource_edges": TopologyEdge,
        "service_nodes": ServiceNodePlacement,
        "service_resources": ServiceResourceBinding,
    }

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "topology_snapshot")
        _schema_version(self.schema_version)
        _required_string("snapshot_id", self.snapshot_id)
        _integer("valid_from_ns", self.valid_from_ns)
        _integer("valid_to_ns", self.valid_to_ns)
        if self.valid_to_ns <= self.valid_from_ns:
            raise ValueError("valid_to_ns must be greater than valid_from_ns")
        _identity_component("cluster_id", self.cluster_id)
        for name in ("structure_fingerprint", "inventory_revision_id",
                     "call_edge_provider_fingerprint"):
            value = getattr(self, name)
            if value is not None:
                _required_string(name, value)
        if not isinstance(self.resource_version_vector, dict) or any(
                not isinstance(key, str) or not key or not isinstance(value, str) or not value
                for key, value in self.resource_version_vector.items()):
            raise ValueError("resource_version_vector must map kinds to opaque strings")
        _string_list("runtime_identity_fingerprints", self.runtime_identity_fingerprints)
        if len(self.runtime_identity_fingerprints) != len(set(self.runtime_identity_fingerprints)):
            raise ValueError("runtime_identity_fingerprints contains duplicates")
        if not isinstance(self.topology_build_issues, list) or any(
                not isinstance(item, dict) or not item.get("reason_code")
                for item in self.topology_build_issues):
            raise ValueError("topology_build_issues must be structured")
        _string_list("services", self.services)
        if len(self.services) != len(set(self.services)):
            raise ValueError("services must not contain duplicates")
        service_ids: set[str] = set()
        service_name_index: dict[str, list[str]] = {}
        for service in self.services:
            parts = service.split("::")
            if len(parts) != 2 or any(not part for part in parts):
                raise ValueError("topology services must use namespace::service_name")
            service_ids.add(service)
            service_name_index.setdefault(parts[1], []).append(service)
        for name in ("call_edges", "host_edges", "resource_edges"):
            edges = getattr(self, name)
            if not isinstance(edges, list) or any(not isinstance(edge, TopologyEdge) for edge in edges):
                raise TypeError(f"{name} must be a list of TopologyEdge records")
        if any(edge.relation_type not in {"call", "impact"} for edge in self.call_edges):
            raise ValueError("call_edges may only contain call or impact relations")
        if any(edge.relation_type != "host" for edge in self.host_edges):
            raise ValueError("host_edges may only contain host relations")
        if any(edge.relation_type != "resource" for edge in self.resource_edges):
            raise ValueError("resource_edges may only contain resource relations")
        all_edges = [*self.call_edges, *self.host_edges, *self.resource_edges]
        identities = [(
            edge.src_namespace, edge.src_service, edge.dst_namespace, edge.dst_service,
            edge.relation_type, edge.protocol, edge.resource_type, edge.resource_id,
        ) for edge in all_edges]
        if len(identities) != len(set(identities)):
            raise ValueError("topology edges must not contain duplicates")
        def endpoint(namespace: str | None, service_name: str) -> str:
            if namespace is not None:
                return f"{namespace}::{service_name}"
            matches = service_name_index.get(service_name, [])
            if len(matches) != 1:
                raise ValueError("topology edge endpoint namespace is ambiguous")
            return matches[0]
        for edge in all_edges:
            src_id = endpoint(edge.src_namespace, edge.src_service)
            dst_id = endpoint(edge.dst_namespace, edge.dst_service)
            if src_id not in service_ids or dst_id not in service_ids:
                raise ValueError("topology edge endpoint is not present in services")
            if src_id == dst_id:
                raise ValueError("topology self-loops are not allowed by this contract")
        for placement in self.service_nodes:
            if f"{placement.namespace}::{placement.service_name}" not in service_ids:
                raise ValueError("service node placement references an unknown service")
        if len(self.service_nodes) != len(set(self.service_nodes)):
            raise ValueError("service node placements must not contain duplicates")
        for binding in self.service_resources:
            if f"{binding.namespace}::{binding.service_name}" not in service_ids:
                raise ValueError("service resource binding references an unknown service")
        if len(self.service_resources) != len(set(self.service_resources)):
            raise ValueError("service resource bindings must not contain duplicates")


@dataclass(frozen=True)
class AlertEvent(StrictRecord):
    schema_version: str
    alert_id: str
    timestamp_ns: int
    state: str
    trigger_services: list[str]
    trigger_edges: list[str]
    service_scores: dict[str, float]
    edge_scores: dict[str, float]
    reason: str
    frozen_baseline: bool
    frozen_service_model: bool
    frozen_metric_model: bool
    record_type: str = field(default="alert_event", init=False)

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "alert_event")
        _schema_version(self.schema_version)
        _required_string("alert_id", self.alert_id)
        _integer("timestamp_ns", self.timestamp_ns)
        if self.state not in ALERT_STATES:
            raise ValueError(f"invalid alert state {self.state!r}")
        _string_list("trigger_services", self.trigger_services)
        _string_list("trigger_edges", self.trigger_edges)
        _anomaly_score_map("service_scores", self.service_scores)
        _anomaly_score_map("edge_scores", self.edge_scores)
        _required_string("reason", self.reason)
        for name in ("frozen_baseline", "frozen_service_model", "frozen_metric_model"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a boolean")


@dataclass(frozen=True)
class IncidentLabel(StrictRecord):
    """Offline-only experiment truth. Online inference must not import this type."""

    schema_version: str
    incident_id: str
    start_ns: int
    end_ns: int
    fault_mode: str
    edge_subtype: str | None
    root_service: str | None
    root_metric: str | None
    root_edge: str | None
    injection_method: str
    seed: int
    record_type: str = field(default="incident_label", init=False)

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "incident_label")
        _schema_version(self.schema_version)
        _required_string("incident_id", self.incident_id)
        _integer("start_ns", self.start_ns)
        _integer("end_ns", self.end_ns)
        if self.end_ns <= self.start_ns:
            raise ValueError("end_ns must be greater than start_ns")
        if self.fault_mode not in {"self", "edge"}:
            raise ValueError(f"invalid fault_mode {self.fault_mode!r}")
        _optional_string("edge_subtype", self.edge_subtype)
        _optional_string("root_service", self.root_service)
        _optional_string("root_metric", self.root_metric)
        _optional_string("root_edge", self.root_edge)
        if self.fault_mode == "self" and self.edge_subtype is not None:
            raise ValueError("self incidents cannot have edge_subtype")
        if self.fault_mode == "edge" and self.edge_subtype not in EDGE_SUBTYPES:
            raise ValueError("edge incidents require a valid edge_subtype")
        if self.fault_mode == "edge" and self.root_edge is None:
            raise ValueError("edge incidents require root_edge")
        _required_string("injection_method", self.injection_method)
        _integer("seed", self.seed)


@dataclass(frozen=True)
class RootCause(StrictRecord):
    kind: str
    service_name: str | None
    metric_name: str | None
    edge_id: str | None
    fault_mode: str
    edge_subtype: str | None
    node_id: str | None = None
    service_id: str | None = None
    edge_kind: str | None = None
    parent_service_id: str | None = None
    target_service_id: str | None = None
    relation_types: list[str] = field(default_factory=list)
    physical_edge_id: str | None = None
    src_service_id: str | None = None
    dst_service_id: str | None = None
    protocol: str | None = None
    dominant_member: str | None = None
    dominant_metric_name: str | None = None
    ambiguity_reasons: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.kind not in {"node", "edge", "ambiguous"}:
            raise ValueError(f"invalid primary root kind {self.kind!r}")
        for name in ("service_name", "metric_name", "edge_id", "edge_subtype", "node_id",
                     "service_id", "edge_kind", "parent_service_id", "target_service_id",
                     "physical_edge_id", "src_service_id", "dst_service_id", "protocol",
                     "dominant_member", "dominant_metric_name"):
            _optional_string(name, getattr(self, name))
        _string_list("relation_types", self.relation_types)
        _string_list("ambiguity_reasons", self.ambiguity_reasons)
        if self.kind == "node":
            if self.service_name is None or self.metric_name is None or self.edge_id is not None:
                raise ValueError("node roots require service_name and metric_name only")
            if self.fault_mode != "self" or self.edge_subtype is not None:
                raise ValueError("node roots must use self fault mode")
        elif self.kind == "edge":
            if self.edge_id is None or self.service_name is not None or self.metric_name is not None:
                raise ValueError("edge roots require edge_id only")
            if self.edge_subtype not in EDGE_SUBTYPES or self.fault_mode not in {"edge", self.edge_subtype}:
                raise ValueError("edge root fault_mode must identify edge semantics")
        else:
            if any(value is not None for value in (self.service_name, self.metric_name, self.edge_id, self.edge_subtype)):
                raise ValueError("ambiguous roots cannot identify a node or edge")
            if self.fault_mode != "ambiguous":
                raise ValueError("ambiguous roots must use ambiguous fault mode")

    @property
    def object_type(self) -> str:
        return self.kind


def _validate_ranked_candidate(candidate: dict[str, Any]) -> None:
    required = {"object_type", "node_id", "edge_id", "root_metric", "edge_subtype", "score", "role"}
    if "candidate_id" in candidate:
        p9_required = {
            "rank", "candidate_id", "object_type", "fault_mode", "edge_subtype",
            "raw_solver_values", "contribution_energy", "counterfactual_status",
            "delta_loss", "relative_delta_loss", "counterfactual_support", "margin",
            "candidate_quality", "coherence", "lag_entropy", "best_path_score",
            "identifiability", "confidence", "status", "member_variables",
            "dominant_member", "provenance",
        }
        if not p9_required <= set(candidate):
            raise ValueError("P9 ranked candidate fields are incomplete")
        if candidate["object_type"] not in {"node", "edge"}:
            raise ValueError("P9 ranked candidate object_type is invalid")
        if candidate["fault_mode"] not in {"self", "edge"}:
            raise ValueError("P9 ranked candidate fault_mode is invalid")
        if candidate["object_type"] == "node" and candidate["edge_subtype"] is not None:
            raise ValueError("P9 node candidate cannot have edge_subtype")
        if candidate["object_type"] == "edge" and candidate["edge_subtype"] not in EDGE_SUBTYPES:
            raise ValueError("P9 edge candidate requires a subtype")
        _json_value("P9 ranked candidate", candidate)
        return
    if set(candidate) != required:
        raise ValueError("ranked candidate fields are invalid")
    object_type = candidate["object_type"]
    if object_type not in {"node", "edge", "ambiguous"}:
        raise ValueError("ranked candidate object_type is invalid")
    for name in ("node_id", "edge_id", "root_metric", "edge_subtype"):
        _optional_string(f"ranked_candidate.{name}", candidate[name])
    _probability("ranked_candidate.score", candidate["score"])
    if candidate["role"] != "root":
        raise ValueError("propagated observations belong in symptoms, not ranked_candidates")
    if object_type == "node":
        if candidate["node_id"] is None or candidate["root_metric"] is None:
            raise ValueError("node candidates require node_id and root_metric")
        if candidate["edge_id"] is not None or candidate["edge_subtype"] is not None:
            raise ValueError("node candidates cannot contain edge fields")
    elif object_type == "edge":
        if candidate["edge_id"] is None or candidate["edge_subtype"] not in EDGE_SUBTYPES:
            raise ValueError("edge candidates require edge_id and edge_subtype")
        if candidate["node_id"] is not None or candidate["root_metric"] is not None:
            raise ValueError("edge candidates cannot contain node fields")
    elif any(candidate[name] is not None for name in ("node_id", "edge_id", "root_metric", "edge_subtype")):
        raise ValueError("ambiguous candidates cannot identify a node or edge")


@dataclass(frozen=True)
class RCAReport(StrictRecord):
    schema_version: str
    incident_id: str
    generated_at_ns: int
    alert: AlertEvent
    primary_root: RootCause
    ranked_candidates: list[dict[str, Any]]
    symptoms: list[dict[str, Any]]
    propagation_paths: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    quality: dict[str, Any]
    runtime: dict[str, Any]
    cluster_id: str | None = None
    namespace: str | None = None
    weighted_problem_id: str | None = None
    solver_result_id: str | None = None
    diagnosis_result_id: str | None = None
    counterfactual_solver_runs: list[str] = field(default_factory=list)
    report_fingerprint: str | None = None
    config_fingerprint: str | None = None
    record_type: str = field(default="rca_report", init=False)

    _nested_fields = {"alert": AlertEvent, "primary_root": RootCause}

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "rca_report")
        _schema_version(self.schema_version)
        _required_string("incident_id", self.incident_id)
        _integer("generated_at_ns", self.generated_at_ns)
        if not isinstance(self.alert, AlertEvent):
            raise TypeError("alert must be an AlertEvent")
        if not isinstance(self.primary_root, RootCause):
            raise TypeError("primary_root must be a RootCause")
        for name in ("ranked_candidates", "symptoms", "propagation_paths", "evidence"):
            value = getattr(self, name)
            if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
                raise TypeError(f"{name} must be a list of dictionaries")
            _json_value(name, value)
        for candidate in self.ranked_candidates:
            _validate_ranked_candidate(candidate)
        for name in ("quality", "runtime"):
            value = getattr(self, name)
            if not isinstance(value, dict):
                raise TypeError(f"{name} must be a dictionary")
            _json_value(name, value)
        for name in ("cluster_id", "namespace", "weighted_problem_id", "solver_result_id",
                     "diagnosis_result_id", "report_fingerprint", "config_fingerprint"):
            _optional_string(name, getattr(self, name))
        _string_list("counterfactual_solver_runs", self.counterfactual_solver_runs)
        for name in ("coverage", "event_loss_rate"):
            if name in self.quality:
                _probability(f"quality.{name}", self.quality[name])


CANDIDATE_OBJECT_TYPES = frozenset({"service", "node_metric", "physical_edge", "edge_metric", "shock"})
CANDIDATE_REASON_CODES = frozenset({
    "trigger_service", "trigger_edge_endpoint", "impact_ancestor", "call_descendant",
    "cohost", "shared_resource", "configured_metric", "observed_edge_metric",
})


@dataclass(frozen=True)
class CandidateProvenance(StrictRecord):
    object_id: str
    object_type: str
    reason_code: str
    source_object_id: str
    hop_count: int
    relation_path: list[str]
    relation_ids: list[str]
    snapshot_id: str
    alert_id: str
    detail: dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("object_id", "source_object_id", "snapshot_id", "alert_id"):
            _required_string(name, getattr(self, name))
        if self.object_type not in CANDIDATE_OBJECT_TYPES:
            raise ValueError("invalid candidate provenance object_type")
        if self.reason_code not in CANDIDATE_REASON_CODES:
            raise ValueError("invalid candidate provenance reason_code")
        _integer("hop_count", self.hop_count)
        _string_list("relation_path", self.relation_path)
        _string_list("relation_ids", self.relation_ids)
        if self.reason_code in {"trigger_service", "trigger_edge_endpoint", "configured_metric", "observed_edge_metric"} and self.hop_count != 0:
            raise ValueError("direct candidate provenance must have hop_count=0")
        if self.reason_code in {"impact_ancestor", "call_descendant"}:
            if len(self.relation_ids) != self.hop_count or len(self.relation_path) != self.hop_count + 1:
                raise ValueError("path provenance length conflicts with hop_count")
        if self.reason_code == "cohost" and not self.detail.get("shared_node"):
            raise ValueError("cohost provenance requires shared_node")
        if self.reason_code == "shared_resource" and (
            not self.detail.get("resource_type") or not self.detail.get("resource_id")
        ):
            raise ValueError("shared_resource provenance requires resource type and ID")
        _json_value("candidate provenance detail", self.detail)


@dataclass(frozen=True)
class CandidateSubgraph(StrictRecord):
    schema_version: str
    candidate_id: str
    cluster_id: str
    namespace_scope: list[str]
    alert_id: str
    alert_state: str
    alert_timestamp_ns: int
    topology_snapshot_id: str
    topology_valid_from_ns: int
    topology_valid_to_ns: int
    seed_services: list[str]
    trigger_edges: list[str]
    candidate_services: list[str]
    candidate_node_ids: list[str]
    candidate_edge_metric_ids: list[str]
    candidate_shock_ids: list[str]
    call_edges: list[dict[str, Any]]
    impact_edges: list[dict[str, Any]]
    host_relations: list[dict[str, Any]]
    resource_relations: list[dict[str, Any]]
    physical_edges: list[dict[str, Any]]
    provenance: list[CandidateProvenance]
    missing_node_metrics: list[str]
    missing_edge_metrics: list[str]
    rca_eligible: bool
    quality_issues: list[dict[str, Any]]
    config_fingerprint: str
    service_count: int
    node_metric_count: int
    physical_edge_count: int
    shock_count: int
    build_latency_ms: float
    record_type: str = field(default="candidate_subgraph", init=False)

    _nested_list_fields = {"provenance": CandidateProvenance}

    def __post_init__(self) -> None:
        _fixed_record_type(self.record_type, "candidate_subgraph")
        _schema_version(self.schema_version)
        for name in ("candidate_id", "cluster_id", "alert_id", "topology_snapshot_id", "config_fingerprint"):
            _required_string(name, getattr(self, name))
        _integer("alert_timestamp_ns", self.alert_timestamp_ns)
        _integer("topology_valid_from_ns", self.topology_valid_from_ns)
        _integer("topology_valid_to_ns", self.topology_valid_to_ns)
        if not self.topology_valid_from_ns <= self.alert_timestamp_ns < self.topology_valid_to_ns:
            raise ValueError("candidate alert timestamp is outside topology validity")
        if self.alert_state not in {"soft", "hard", "edge_anomaly"}:
            raise ValueError("candidate_subgraph requires soft, hard, or edge_anomaly alert")
        if self.rca_eligible != (self.alert_state == "hard"):
            raise ValueError("candidate rca_eligible conflicts with alert_state")
        if not isinstance(self.rca_eligible, bool):
            raise TypeError("rca_eligible must be a boolean")
        list_names = (
            "namespace_scope", "seed_services", "trigger_edges", "candidate_services",
            "candidate_node_ids", "candidate_edge_metric_ids", "candidate_shock_ids",
            "missing_node_metrics", "missing_edge_metrics",
        )
        for name in list_names:
            values = getattr(self, name)
            _string_list(name, values)
            if len(values) != len(set(values)):
                raise ValueError(f"{name} contains duplicates")
            object.__setattr__(self, name, sorted(values))
        if not set(self.seed_services) <= set(self.candidate_services):
            raise ValueError("candidate seed service is absent from candidate_services")
        for name in ("call_edges", "impact_edges", "host_relations", "resource_relations", "physical_edges", "quality_issues"):
            values = getattr(self, name)
            if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
                raise TypeError(f"{name} must be a list of dictionaries")
            _json_value(name, values)
            key = "physical_edge_id" if name == "physical_edges" else "relation_id" if name != "quality_issues" else "reason_code"
            object.__setattr__(self, name, sorted(values, key=lambda item: (str(item.get(key, "")), json_key(item))))
            if name != "quality_issues":
                identifiers = [item.get(key) for item in values]
                if any(not isinstance(item, str) or not item for item in identifiers) or len(identifiers) != len(set(identifiers)):
                    raise ValueError(f"{name} contains missing or duplicate stable IDs")
        service_set = set(self.candidate_services)
        for name in ("call_edges", "impact_edges", "host_relations", "resource_relations", "physical_edges"):
            for relation in getattr(self, name):
                if relation.get("src_service_id") not in service_set or relation.get("dst_service_id") not in service_set:
                    raise ValueError(f"{name} contains an endpoint outside candidate_services")
        for node_id in self.candidate_node_ids:
            if "::".join(node_id.split("::")[:3]) not in service_set:
                raise ValueError("candidate node metric points outside candidate_services")
        physical_ids = {item.get("physical_edge_id") for item in self.physical_edges}
        if not set(self.trigger_edges) <= physical_ids:
            raise ValueError("candidate trigger edge has no physical edge")
        if any(edge_id.rsplit("::", 1)[0] not in physical_ids for edge_id in self.candidate_edge_metric_ids):
            raise ValueError("candidate edge metric has no physical edge")
        if any(shock_id.split("::shock::", 1)[0] not in physical_ids for shock_id in self.candidate_shock_ids):
            raise ValueError("candidate shock has no physical edge")
        if len(self.provenance) != len(set(json_key(item.to_dict()) for item in self.provenance)):
            raise ValueError("candidate provenance contains duplicates")
        object.__setattr__(self, "provenance", sorted(
            self.provenance,
            key=lambda item: (item.object_id, item.hop_count, item.reason_code,
                              tuple(item.relation_ids), item.source_object_id, json_key(item.detail)),
        ))
        required_objects = set(self.candidate_services) | set(self.candidate_node_ids) | physical_ids | set(self.candidate_edge_metric_ids) | set(self.candidate_shock_ids)
        provenance_objects = {item.object_id for item in self.provenance}
        if not required_objects <= provenance_objects:
            raise ValueError("candidate object is missing structured provenance")
        for name, expected in (("service_count", len(self.candidate_services)),
                               ("node_metric_count", len(self.candidate_node_ids)),
                               ("physical_edge_count", len(self.physical_edges)),
                               ("shock_count", len(self.candidate_shock_ids))):
            _integer(name, getattr(self, name))
            if getattr(self, name) != expected:
                raise ValueError(f"{name} does not match candidate content")
        object.__setattr__(self, "build_latency_ms", _finite_number("build_latency_ms", self.build_latency_ms))
        if self.build_latency_ms < 0:
            raise ValueError("build_latency_ms must be non-negative")
        if len(self.config_fingerprint) != 64 or any(character not in "0123456789abcdef" for character in self.config_fingerprint):
            raise ValueError("config_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True)
class ServiceStateRecord(StrictRecord):
    schema_version: str
    timestamp_ns: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    namespace: str
    service_name: str
    service_id: str
    value: float
    baseline_ready: bool
    family_coverage: dict[str, bool]
    missing_families: list[str]
    observation_quality: float
    source_alert_state: str
    config_fingerprint: str
    record_type: str = field(default="service_state", init=False)

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        _fixed_record_type(self.record_type, "service_state")
        for name in ("timestamp_ns", "window_start_ns", "window_end_ns"):
            _integer(name, getattr(self, name))
        if not self.window_start_ns < self.window_end_ns or self.timestamp_ns != self.window_end_ns:
            raise ValueError("service state timestamp must equal the end of a non-empty window")
        for name in ("cluster_id", "namespace", "service_name"):
            _identity_component(name, getattr(self, name))
        expected_id = f"{self.cluster_id}::{self.namespace}::{self.service_name}"
        if self.service_id != expected_id:
            raise ValueError(f"service_id must equal {expected_id!r}")
        object.__setattr__(self, "value", _finite_number("value", self.value))
        if not isinstance(self.baseline_ready, bool):
            raise TypeError("baseline_ready must be boolean")
        if not isinstance(self.family_coverage, dict):
            raise TypeError("family_coverage must be a dictionary")
        for family, covered in self.family_coverage.items():
            _required_string("family_coverage key", family)
            if not isinstance(covered, bool):
                raise TypeError("family_coverage values must be boolean")
        _string_list("missing_families", self.missing_families)
        if len(self.missing_families) != len(set(self.missing_families)):
            raise ValueError("missing_families contains duplicates")
        object.__setattr__(self, "missing_families", sorted(self.missing_families))
        object.__setattr__(self, "observation_quality", _probability("observation_quality", self.observation_quality))
        if self.source_alert_state not in {"healthy", "soft", "hard", "recovery", "edge_anomaly"}:
            raise ValueError("invalid source_alert_state")
        if len(self.config_fingerprint) != 64 or any(character not in "0123456789abcdef" for character in self.config_fingerprint):
            raise ValueError("config_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True)
class NodeAnomalyRecord(StrictRecord):
    schema_version: str
    timestamp_ns: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    namespace: str
    service_name: str
    service_id: str
    node_id: str
    metric_family: str
    metric_name: str
    signed_z: float
    anomaly_score: float
    baseline_ready: bool
    observation_quality: float
    source_alert_state: str
    source_metric_record_id: str
    baseline_config_fingerprint: str
    signal_spec_id: str
    signal_kind: str = "signed_z"
    record_type: str = field(default="node_anomaly", init=False)

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        _fixed_record_type(self.record_type, "node_anomaly")
        for name in ("timestamp_ns", "window_start_ns", "window_end_ns"):
            _integer(name, getattr(self, name))
        if not self.window_start_ns < self.window_end_ns or self.timestamp_ns != self.window_end_ns:
            raise ValueError("node anomaly timestamp must equal a non-empty window end")
        for name in ("cluster_id", "namespace", "service_name"):
            _identity_component(name, getattr(self, name))
        for name in ("metric_family", "metric_name", "source_metric_record_id", "signal_spec_id"):
            _required_string(name, getattr(self, name))
        expected_service = f"{self.cluster_id}::{self.namespace}::{self.service_name}"
        expected_node = f"{expected_service}::{self.metric_name}"
        if self.service_id != expected_service or self.node_id != expected_node:
            raise ValueError("node anomaly stable identities conflict with their components")
        object.__setattr__(self, "signed_z", _finite_number("signed_z", self.signed_z))
        object.__setattr__(self, "anomaly_score", _finite_number("anomaly_score", self.anomaly_score))
        if self.anomaly_score < 0:
            raise ValueError("anomaly_score must be non-negative")
        if not isinstance(self.baseline_ready, bool):
            raise TypeError("baseline_ready must be boolean")
        object.__setattr__(self, "observation_quality", _probability(
            "observation_quality", self.observation_quality
        ))
        if self.source_alert_state not in {"healthy", "soft", "hard", "recovery", "edge_anomaly"}:
            raise ValueError("invalid source_alert_state")
        if self.signal_kind != "signed_z":
            raise ValueError("node anomaly signal_kind must be signed_z")
        if len(self.baseline_config_fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in self.baseline_config_fingerprint
        ):
            raise ValueError("baseline_config_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True)
class EdgeAnomalyRecord(StrictRecord):
    schema_version: str
    timestamp_ns: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    namespace: str
    src_service: str
    dst_service: str
    protocol: str
    edge_metric_id: str
    shock_id: str | None
    metric_name: str
    signed_z: float
    anomaly_score: float
    baseline_ready: bool
    observation_quality: float
    source_metric_record_id: str
    baseline_config_fingerprint: str
    signal_spec_id: str
    source_alert_state: str
    signal_kind: str = "signed_z"
    record_type: str = field(default="edge_anomaly", init=False)

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        _fixed_record_type(self.record_type, "edge_anomaly")
        for name in ("timestamp_ns", "window_start_ns", "window_end_ns"):
            _integer(name, getattr(self, name))
        if not self.window_start_ns < self.window_end_ns or self.timestamp_ns != self.window_end_ns:
            raise ValueError("edge anomaly timestamp must equal a non-empty window end")
        for name in ("cluster_id", "namespace", "src_service", "dst_service", "protocol"):
            _identity_component(name, getattr(self, name))
        for name in ("metric_name", "source_metric_record_id", "signal_spec_id"):
            _required_string(name, getattr(self, name))
        expected = (
            f"{self.cluster_id}::{self.namespace}::{self.src_service}->{self.dst_service}::"
            f"{self.protocol}::{self.metric_name}"
        )
        if self.edge_metric_id != expected or self.source_metric_record_id != expected:
            raise ValueError("edge anomaly stable identity conflicts with its components")
        expected_shock = (
            f"{self.cluster_id}::{self.namespace}::{self.src_service}->{self.dst_service}::"
            f"{self.protocol}::shock::{self.metric_name}"
        )
        if self.shock_id is not None and self.shock_id != expected_shock:
            raise ValueError("edge anomaly shock_id conflicts with its components")
        object.__setattr__(self, "signed_z", _finite_number("signed_z", self.signed_z))
        object.__setattr__(self, "anomaly_score", _finite_number("anomaly_score", self.anomaly_score))
        if self.anomaly_score < 0:
            raise ValueError("anomaly_score must be non-negative")
        if not isinstance(self.baseline_ready, bool):
            raise TypeError("baseline_ready must be boolean")
        object.__setattr__(self, "observation_quality", _probability(
            "observation_quality", self.observation_quality
        ))
        if self.source_alert_state not in {"healthy", "soft", "hard", "recovery", "edge_anomaly"}:
            raise ValueError("invalid source_alert_state")
        if self.signal_kind != "signed_z":
            raise ValueError("edge anomaly signal_kind must be signed_z")
        if len(self.baseline_config_fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in self.baseline_config_fingerprint
        ):
            raise ValueError("baseline_config_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True)
class EvidenceObservationRecord(StrictRecord):
    schema_version: str
    evidence_id: str
    timestamp_ns: int
    evidence_window_start_ns: int
    evidence_window_end_ns: int
    analysis_cutoff_ns: int
    cluster_id: str
    namespace: str
    target_type: str
    target_id: str
    channel_id: str
    source_type: str
    normalized_strength: float
    observation_quality: float
    reliability_weight: float
    source_record_ids: list[str]
    source_object_ids: list[str]
    independent_from_residual: bool
    provenance: dict[str, Any]
    config_fingerprint: str
    record_type: str = field(default="evidence_observation", init=False)

    def __post_init__(self) -> None:
        _schema_version(self.schema_version)
        _fixed_record_type(self.record_type, "evidence_observation")
        for name in ("evidence_id", "target_id", "channel_id"):
            _required_string(name, getattr(self, name))
        for name in ("cluster_id", "namespace"):
            _identity_component(name, getattr(self, name))
        for name in ("timestamp_ns", "evidence_window_start_ns", "evidence_window_end_ns",
                     "analysis_cutoff_ns"):
            _integer(name, getattr(self, name))
        if not self.evidence_window_start_ns <= self.timestamp_ns < self.evidence_window_end_ns:
            raise ValueError("evidence timestamp must be inside its half-open evidence window")
        if self.timestamp_ns > self.analysis_cutoff_ns:
            raise ValueError("evidence timestamp must not exceed analysis_cutoff_ns")
        if self.target_type not in {"node", "shock"}:
            raise ValueError("evidence target_type must be node or shock")
        if self.source_type not in {
            "node_metric", "edge_metric", "burst_event", "profiler", "topology",
            "external_observer",
        }:
            raise ValueError("invalid evidence source_type")
        for name in ("normalized_strength", "observation_quality", "reliability_weight"):
            object.__setattr__(self, name, _probability(name, getattr(self, name)))
        _string_list("source_record_ids", self.source_record_ids)
        if not self.source_record_ids or len(self.source_record_ids) != len(set(self.source_record_ids)):
            raise ValueError("source_record_ids must be non-empty and unique")
        _string_list("source_object_ids", self.source_object_ids)
        if len(self.source_object_ids) != len(set(self.source_object_ids)):
            raise ValueError("source_object_ids must be unique")
        object.__setattr__(self, "source_record_ids", sorted(self.source_record_ids))
        object.__setattr__(self, "source_object_ids", sorted(self.source_object_ids))
        if not isinstance(self.independent_from_residual, bool):
            raise TypeError("independent_from_residual must be boolean")
        if not isinstance(self.provenance, dict) or not self.provenance.get("calibration_id"):
            raise ValueError("evidence provenance requires a calibration_id")
        _json_value("evidence provenance", self.provenance)
        if len(self.config_fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in self.config_fingerprint
        ):
            raise ValueError("evidence config_fingerprint must be lowercase SHA-256")


def json_key(value: Any) -> str:
    import json
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


STRICT_RECORD_TYPES: dict[str, type[StrictRecord]] = {
    "node_metric": NodeMetricRecord,
    "edge_metric": EdgeMetricRecord,
    "burst_event": BurstEventRecord,
    "topology_snapshot": TopologySnapshot,
    "alert_event": AlertEvent,
    "incident_label": IncidentLabel,
    "rca_report": RCAReport,
    "candidate_subgraph": CandidateSubgraph,
    "service_state": ServiceStateRecord,
    "node_anomaly": NodeAnomalyRecord,
    "edge_anomaly": EdgeAnomalyRecord,
    "evidence_observation": EvidenceObservationRecord,
}


@dataclass(frozen=True)
class MetricSemantics:
    metric_kind: str
    scope: str
    histogram_upper_bound: float | None
    histogram_is_inf_bucket: bool
    histogram_is_cumulative: bool | None
    quantile: float | None

    def __post_init__(self) -> None:
        if self.scope not in NODE_METRIC_SCOPES:
            raise ValueError("legacy node metric semantics require a valid node scope")
        _metric_distribution_semantics(
            self.metric_kind,
            self.histogram_upper_bound,
            self.histogram_is_inf_bucket,
            self.histogram_is_cumulative,
            self.quantile,
        )


@dataclass(frozen=True)
class MetricRegistry:
    entries: dict[str, MetricSemantics]

    def __post_init__(self) -> None:
        if not isinstance(self.entries, dict) or not self.entries:
            raise ValueError("MetricRegistry entries must be a non-empty dictionary")
        for metric_id, semantics in self.entries.items():
            _required_string("metric registry ID", metric_id)
            if not isinstance(semantics, MetricSemantics):
                raise TypeError("MetricRegistry values must be MetricSemantics")

    def require(self, metric_id: str) -> MetricSemantics:
        try:
            return self.entries[metric_id]
        except KeyError as exc:
            raise KeyError(f"metric semantics are not registered for {metric_id!r}") from exc


def node_metric_from_legacy(
    record: MetricRecord,
    *,
    schema_version: str,
    window_sec: int,
    cluster_id: str,
    namespace: str,
    metric_family: str,
    unit: str,
    sample_count: int,
    coverage: float,
    event_loss_rate: float,
    metric_kind: str,
    scope: str,
    histogram_upper_bound: float | None,
    histogram_is_inf_bucket: bool,
    histogram_is_cumulative: bool | None,
    quantile: float | None,
) -> NodeMetricRecord:
    """Explicitly convert the legacy P0 MetricRecord into the P1 node contract."""

    if not isinstance(record, MetricRecord):
        raise TypeError("record must be a MetricRecord")
    if schema_version not in {
        LEGACY_METRIC_RECORD_SCHEMA_VERSION,
        METRIC_RECORD_SCHEMA_VERSION,
    }:
        raise ValueError("unsupported legacy metric schema_version")
    timestamp = _finite_number("record.timestamp", record.timestamp)
    value = _finite_number("record.value", record.value)
    return NodeMetricRecord(
        schema_version=METRIC_RECORD_SCHEMA_VERSION,
        timestamp_ns=int(timestamp * 1_000_000_000),
        window_sec=window_sec,
        cluster_id=cluster_id,
        node_name=record.node,
        namespace=namespace,
        service_name=record.service,
        pod_uid=record.instance,
        container_id=None,
        metric_family=metric_family,
        metric_name=record.metric,
        value=value,
        valid=True,
        invalid_reason=None,
        unit=unit,
        sample_count=sample_count,
        coverage=coverage,
        event_loss_rate=event_loss_rate,
        mapping_quality=1.0,
        source=record.source,
        metric_kind=metric_kind,
        scope=scope,
        histogram_upper_bound=histogram_upper_bound,
        histogram_is_inf_bucket=histogram_is_inf_bucket,
        histogram_is_cumulative=histogram_is_cumulative,
        quantile=quantile,
    )


def node_metric_from_registry(
    record: MetricRecord,
    *,
    registry: MetricRegistry,
    metric_id: str,
    schema_version: str,
    window_sec: int,
    cluster_id: str,
    namespace: str,
    metric_family: str,
    unit: str,
    sample_count: int,
    coverage: float,
    event_loss_rate: float,
) -> NodeMetricRecord:
    """Convert legacy data using an exact registry entry, never name heuristics."""
    if not isinstance(registry, MetricRegistry):
        raise TypeError("registry must be a MetricRegistry")
    semantics = registry.require(metric_id)
    return node_metric_from_legacy(
        record,
        schema_version=schema_version,
        window_sec=window_sec,
        cluster_id=cluster_id,
        namespace=namespace,
        metric_family=metric_family,
        unit=unit,
        sample_count=sample_count,
        coverage=coverage,
        event_loss_rate=event_loss_rate,
        metric_kind=semantics.metric_kind,
        scope=semantics.scope,
        histogram_upper_bound=semantics.histogram_upper_bound,
        histogram_is_inf_bucket=semantics.histogram_is_inf_bucket,
        histogram_is_cumulative=semantics.histogram_is_cumulative,
        quantile=semantics.quantile,
    )

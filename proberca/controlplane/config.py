"""Configuration and metric taxonomy for the final ProbeRCA-BPF control plane."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from proberca.dataplane.contracts import fingerprint


ROOT_CATEGORIES = frozenset({
    "CPU", "Memory", "IO", "Lock", "LocalNet", "NIC", "TCP", "DNS",
})
ENTITY_TYPES = frozenset({"service", "host", "edge"})
RECORD_TYPES = frozenset({"node_metric", "edge_metric"})
TRANSFORMS = frozenset({"identity", "log1p"})


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

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MetricRoleSpec":
        expected = set(cls.__dataclass_fields__)
        values = dict(payload)
        values.setdefault("protocols", ())
        values.setdefault("root_category", None)
        values.setdefault("root_eligible", False)
        values.setdefault("transform", "identity")
        values.setdefault("polarity", 1)
        if set(values) != expected:
            raise ValueError("metric role fields mismatch")
        values["scopes"] = tuple(values["scopes"])
        values["protocols"] = tuple(values["protocols"])
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_metric_roles() -> tuple[MetricRoleSpec, ...]:
    service_scopes = ("service",)
    edge_scopes = ("service_pair",)
    return tuple(sorted((
        MetricRoleSpec("node_metric", "request_rate", "service", "request_rate", service_scopes),
        MetricRoleSpec("node_metric", "request_failure_rate", "service", "request_failure", service_scopes),
        MetricRoleSpec("node_metric", "request_latency_p95", "service", "request_latency", service_scopes, transform="log1p"),
        MetricRoleSpec("node_metric", "cpu_usage_rate", "service", "service_cpu_usage", service_scopes, root_category="CPU", root_eligible=True),
        MetricRoleSpec("node_metric", "cpu_throttle_ratio", "service", "service_cpu_throttle", service_scopes, root_category="CPU", root_eligible=True),
        MetricRoleSpec("node_metric", "memory_working_set_ratio", "service", "service_memory", service_scopes, root_category="Memory", root_eligible=True),
        MetricRoleSpec("node_metric", "io_psi", "service", "service_io", service_scopes, root_category="IO", root_eligible=True),
        MetricRoleSpec("node_metric", "futex_wait_time_rate", "service", "service_lock", service_scopes, root_category="Lock", root_eligible=True),
        MetricRoleSpec("node_metric", "local_socket_failure_rate", "service", "service_localnet", service_scopes, root_category="LocalNet", root_eligible=True),
        MetricRoleSpec("node_metric", "cpu_psi", "host", "host_cpu", ("node",), root_category="CPU", root_eligible=True),
        MetricRoleSpec("node_metric", "memory_psi", "host", "host_memory", ("node",), root_category="Memory", root_eligible=True),
        MetricRoleSpec("node_metric", "io_psi", "host", "host_io", ("node",), root_category="IO", root_eligible=True),
        MetricRoleSpec("node_metric", "nic_drop_error_rate", "host", "host_nic", ("node",), root_category="NIC", root_eligible=True),
        MetricRoleSpec("edge_metric", "edge_request_count", "edge", "edge_count", edge_scopes, protocols=("tcp",)),
        MetricRoleSpec("edge_metric", "edge_latency_p95", "edge", "edge_latency", edge_scopes, protocols=("tcp",), root_category="TCP", root_eligible=True, transform="log1p"),
        MetricRoleSpec("edge_metric", "edge_failure_rate", "edge", "edge_failure", edge_scopes, protocols=("tcp",), root_category="TCP", root_eligible=True),
        MetricRoleSpec("edge_metric", "dns_query_count", "edge", "edge_count", edge_scopes, protocols=("dns",)),
        MetricRoleSpec("edge_metric", "dns_latency_p95", "edge", "edge_latency", edge_scopes, protocols=("dns",), root_category="DNS", root_eligible=True, transform="log1p"),
        MetricRoleSpec("edge_metric", "dns_failure_rate", "edge", "edge_failure", edge_scopes, protocols=("dns",), root_category="DNS", root_eligible=True),
    ), key=lambda item: (
        item.record_type, item.metric_name, item.entity_type, item.scopes, item.protocols,
    )))


@dataclass(frozen=True)
class FinalControlConfig:
    window_sec: int = 1
    baseline_min_windows: int = 6
    baseline_min_scale: float = 1.0e-6
    service_lags: tuple[int, ...] = (1, 2)
    metric_lags: tuple[int, ...] = (1, 2)
    rls_forgetting_factor: float = 0.99
    rls_initial_covariance: float = 100.0
    metric_ridge: float = 0.1
    metric_min_training_rows: int = 4
    alpha_latency: float = 0.5
    alpha_failure: float = 0.5
    soft_threshold: float = 2.5
    soft_consecutive_windows: int = 2
    hard_threshold: float = 4.0
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
            "fista_tolerance",
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
        values["metric_roles"] = tuple(
            item if isinstance(item, MetricRoleSpec) else MetricRoleSpec.from_dict(item)
            for item in values["metric_roles"]
        )
        values["group_penalties"] = {
            str(key): float(value) for key, value in values["group_penalties"].items()
        }
        return cls(**values)

    @property
    def config_fingerprint(self) -> str:
        return fingerprint(self.to_dict())

    @property
    def collection_contract(self) -> dict[str, Any]:
        return {
            "schema_version": "probeRCA-final-collection-contract-v1",
            "normal_metric_roles": [
                item.to_dict() for item in sorted(
                    self.metric_roles,
                    key=lambda value: (
                        value.record_type, value.metric_name, value.entity_type,
                        value.scopes, value.protocols,
                    ),
                )
            ],
            "burst_evidence_source_type": "burst_event",
            "burst_evidence_semantics": "normalized_strength_times_quality",
            "window_sec": self.window_sec,
        }

    @property
    def collection_contract_fingerprint(self) -> str:
        return fingerprint(self.collection_contract)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["service_lags"] = list(self.service_lags)
        payload["metric_lags"] = list(self.metric_lags)
        payload["metric_roles"] = [item.to_dict() for item in self.metric_roles]
        payload["group_penalties"] = dict(sorted(self.group_penalties.items()))
        return payload

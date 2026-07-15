"""Strict ProbeRCA-BPF configuration contract using the existing YAML dependency."""

from __future__ import annotations

import math
import hashlib
import json
from dataclasses import MISSING, asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from proberca.data.schema import METRIC_KINDS, NODE_METRIC_FAMILIES


def _strict_dict(payload: Any, required: set[str], context: str) -> dict:
    if not isinstance(payload, dict):
        raise TypeError(f"{context} must be a dictionary")
    unknown = sorted(set(payload) - required)
    missing = sorted(required - set(payload))
    if unknown:
        raise ValueError(f"{context} unknown fields: {unknown}")
    if missing:
        raise ValueError(f"{context} missing fields: {missing}")
    return payload


def _positive_int(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _probability(name: str, value: Any) -> float:
    result = _finite(name, value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return result


@dataclass(frozen=True)
class AlertConfig:
    healthy_threshold: float
    soft_threshold: float
    soft_consecutive_windows: int
    hard_threshold: float
    hard_consecutive_windows: int
    recovery_threshold: float
    recovery_windows: int
    recovery_cooldown_sec: int

    @classmethod
    def from_dict(cls, payload: dict) -> "AlertConfig":
        values = _strict_dict(payload, {field.name for field in cls.__dataclass_fields__.values()}, "alert")
        result = cls(**values)
        for name in ("healthy_threshold", "soft_threshold", "hard_threshold", "recovery_threshold"):
            object.__setattr__(result, name, _probability(f"alert.{name}", getattr(result, name)))
        for name in ("soft_consecutive_windows", "hard_consecutive_windows", "recovery_windows", "recovery_cooldown_sec"):
            _positive_int(f"alert.{name}", getattr(result, name))
        if not (
            result.recovery_threshold
            < result.healthy_threshold
            < result.soft_threshold
            < result.hard_threshold
        ):
            raise ValueError(
                "thresholds must satisfy recovery < healthy < soft < hard"
            )
        return result


@dataclass(frozen=True)
class MetricParentRule:
    rule_id: str
    enabled: bool
    target_family: str
    target_metric_names: list[str] | None
    relation_type: str
    parent_family: str
    parent_metric_names: list[str] | None
    lags: list[int]
    require_signal_spec: bool
    provenance_label: str

    @classmethod
    def from_dict(cls, payload: dict) -> "MetricParentRule":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "MetricParentRule")
        result = cls(**values)
        result.validate()
        return result

    def validate(self, max_lag: int | None = None) -> None:
        for name in ("rule_id", "target_family", "parent_family", "provenance_label"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"MetricParentRule.{name} must be a non-empty string")
        if self.target_family not in NODE_METRIC_FAMILIES or self.parent_family not in NODE_METRIC_FAMILIES:
            raise ValueError("MetricParentRule family must be a configured node metric family")
        if not isinstance(self.enabled, bool) or not isinstance(self.require_signal_spec, bool):
            raise TypeError("MetricParentRule flags must be boolean")
        if self.relation_type not in {"self_history", "same_service", "impact", "host", "resource"}:
            raise ValueError("MetricParentRule relation_type is invalid")
        for name in ("target_metric_names", "parent_metric_names"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, list) or not value or any(
                    not isinstance(item, str) or not item.strip() for item in value
                ):
                    raise ValueError(f"MetricParentRule.{name} must be null or non-empty exact names")
                if value != sorted(set(value)):
                    raise ValueError(f"MetricParentRule.{name} must be sorted and unique")
        if not isinstance(self.lags, list) or not self.lags:
            raise ValueError("MetricParentRule.lags must not be empty")
        if any(isinstance(lag, bool) or not isinstance(lag, int) or lag <= 0 for lag in self.lags):
            raise ValueError("MetricParentRule lags must be positive integers")
        if self.lags != sorted(set(self.lags)):
            raise ValueError("MetricParentRule lags must be sorted and unique")
        if max_lag is not None and any(lag > max_lag for lag in self.lags):
            raise ValueError("MetricParentRule lag exceeds configured metric_lags")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class PropagationConfig:
    service_lags: list[int]
    metric_lags: list[int]
    rls_forgetting_factor: float
    metric_ridge: float
    rls_initial_covariance: float = 100.0
    service_min_updates: int = 30
    service_min_observation_quality: float = 0.0
    service_max_gap_windows: int = 3
    topology_reconfigure_min_updates: int = 1
    include_self_history: bool = True
    include_impact_parents: bool = True
    include_host_parents: bool = True
    include_resource_parents: bool = True
    metric_history_sec: int = 600
    metric_min_training_rows: int = 60
    metric_min_observation_quality: float = 0.8
    metric_max_condition_number: float = 1e8
    metric_max_gap_windows: int = 5
    metric_model_cache_size: int = 8
    metric_include_self_history: bool = True
    metric_parent_rules: list[MetricParentRule] = field(default_factory=list)

    @classmethod
    def from_dict(cls, payload: dict) -> "PropagationConfig":
        required = {"service_lags", "metric_lags", "rls_forgetting_factor", "metric_ridge"}
        allowed = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict):
            raise TypeError("propagation must be a dictionary")
        unknown = sorted(set(payload) - allowed)
        missing = sorted(required - set(payload))
        if unknown or missing:
            raise ValueError(f"propagation invalid fields; unknown={unknown}, missing={missing}")
        values = dict(payload)
        for name, dataclass_field in cls.__dataclass_fields__.items():
            if name not in values and dataclass_field.default is not MISSING:
                values[name] = dataclass_field.default
        raw_rules = values.get("metric_parent_rules", [])
        if not isinstance(raw_rules, list):
            raise TypeError("propagation.metric_parent_rules must be a list")
        values["metric_parent_rules"] = [
            item if isinstance(item, MetricParentRule) else MetricParentRule.from_dict(item)
            for item in raw_rules
        ]
        result = cls(**values)
        for name in ("service_lags", "metric_lags"):
            lags = getattr(result, name)
            if not isinstance(lags, list) or not lags:
                raise ValueError(f"propagation.{name} must be a non-empty list")
            for lag in lags:
                _positive_int(f"propagation.{name} item", lag)
            if len(lags) != len(set(lags)):
                raise ValueError(f"propagation.{name} contains duplicate lags")
        factor = _finite("propagation.rls_forgetting_factor", result.rls_forgetting_factor)
        if not 0.0 < factor <= 1.0:
            raise ValueError("propagation.rls_forgetting_factor must be in (0, 1]")
        object.__setattr__(result, "rls_forgetting_factor", factor)
        ridge = _finite("propagation.metric_ridge", result.metric_ridge)
        if ridge <= 0.0:
            raise ValueError("propagation.metric_ridge must be positive")
        object.__setattr__(result, "metric_ridge", ridge)
        covariance = _finite("propagation.rls_initial_covariance", result.rls_initial_covariance)
        if covariance <= 0.0:
            raise ValueError("propagation.rls_initial_covariance must be positive")
        object.__setattr__(result, "rls_initial_covariance", covariance)
        for name in ("service_min_updates", "service_max_gap_windows", "topology_reconfigure_min_updates"):
            _positive_int(f"propagation.{name}", getattr(result, name))
        object.__setattr__(
            result,
            "service_min_observation_quality",
            _probability("propagation.service_min_observation_quality", result.service_min_observation_quality),
        )
        for name in ("include_self_history", "include_impact_parents", "include_host_parents",
                     "include_resource_parents"):
            if not isinstance(getattr(result, name), bool):
                raise TypeError(f"propagation.{name} must be boolean")
        if not result.include_self_history:
            raise ValueError("propagation.include_self_history must be true")
        object.__setattr__(result, "service_lags", sorted(result.service_lags))
        object.__setattr__(result, "metric_lags", sorted(result.metric_lags))
        for name in ("metric_history_sec", "metric_min_training_rows", "metric_max_gap_windows",
                     "metric_model_cache_size"):
            _positive_int(f"propagation.{name}", getattr(result, name))
        if result.metric_history_sec < max(result.metric_lags):
            raise ValueError("propagation.metric_history_sec must cover metric_lags")
        if result.metric_min_training_rows <= max(result.metric_lags):
            raise ValueError("propagation.metric_min_training_rows must exceed the maximum metric lag")
        object.__setattr__(result, "metric_min_observation_quality", _probability(
            "propagation.metric_min_observation_quality", result.metric_min_observation_quality
        ))
        condition = _finite("propagation.metric_max_condition_number", result.metric_max_condition_number)
        if condition <= 1.0:
            raise ValueError("propagation.metric_max_condition_number must be greater than 1")
        object.__setattr__(result, "metric_max_condition_number", condition)
        if not isinstance(result.metric_include_self_history, bool) or not result.metric_include_self_history:
            raise ValueError("propagation.metric_include_self_history must be true")
        if len({item.rule_id for item in result.metric_parent_rules}) != len(result.metric_parent_rules):
            raise ValueError("propagation.metric_parent_rules contains duplicate rule_id")
        for item in result.metric_parent_rules:
            item.validate(max(result.metric_lags))
        return result


@dataclass(frozen=True)
class CandidateGraphConfig:
    upstream_hops: int
    downstream_hops: int
    include_cohost: bool
    include_shared_resource: bool
    allow_cross_namespace: bool = False
    allowed_namespaces: list[str] = field(default_factory=list)
    max_candidate_services: int = 100
    max_candidate_node_metrics: int = 1000
    max_candidate_physical_edges: int = 500
    fail_on_candidate_overflow: bool = True
    include_trigger_edge_endpoints: bool = True
    include_all_provenance_paths: bool = True
    max_provenance_paths_per_object: int = 20

    @classmethod
    def from_dict(cls, payload: dict) -> "CandidateGraphConfig":
        required = {"upstream_hops", "downstream_hops", "include_cohost", "include_shared_resource"}
        allowed = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict):
            raise TypeError("candidate_graph must be a dictionary")
        unknown = sorted(set(payload) - allowed)
        missing = sorted(required - set(payload))
        if unknown or missing:
            raise ValueError(f"candidate_graph invalid fields; unknown={unknown}, missing={missing}")
        values = dict(payload)
        for name, dataclass_field in cls.__dataclass_fields__.items():
            if name not in values:
                values[name] = dataclass_field.default_factory() if dataclass_field.default_factory is not MISSING else dataclass_field.default
        result = cls(**values)
        _positive_int("candidate_graph.upstream_hops", result.upstream_hops)
        _positive_int("candidate_graph.downstream_hops", result.downstream_hops)
        for name in ("include_cohost", "include_shared_resource", "allow_cross_namespace",
                     "fail_on_candidate_overflow", "include_trigger_edge_endpoints",
                     "include_all_provenance_paths"):
            if not isinstance(getattr(result, name), bool):
                raise TypeError(f"candidate_graph.{name} must be a boolean")
        if not isinstance(result.allowed_namespaces, list) or any(not isinstance(item, str) or not item for item in result.allowed_namespaces):
            raise ValueError("candidate_graph.allowed_namespaces must contain non-empty strings")
        if len(result.allowed_namespaces) != len(set(result.allowed_namespaces)):
            raise ValueError("candidate_graph.allowed_namespaces contains duplicates")
        for name in ("max_candidate_services", "max_candidate_node_metrics",
                     "max_candidate_physical_edges", "max_provenance_paths_per_object"):
            _positive_int(f"candidate_graph.{name}", getattr(result, name))
        return result


@dataclass(frozen=True)
class ImpactDerivationRule:
    rule_id: str
    source_relation_type: str
    protocol: str | None
    direction: str
    enabled: bool
    provenance_label: str

    @classmethod
    def from_dict(cls, payload: dict) -> "ImpactDerivationRule":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "ImpactDerivationRule")
        result = cls(**values)
        for name in ("rule_id", "source_relation_type", "provenance_label"):
            _optional_nonempty_string(name, getattr(result, name))
        _optional_nonempty_string("protocol", result.protocol)
        if result.source_relation_type != "call":
            raise ValueError("impact derivation currently accepts only explicit call relations")
        if result.direction not in {"forward", "reverse", "bidirectional", "none"}:
            raise ValueError("invalid impact derivation direction")
        if not isinstance(result.enabled, bool):
            raise TypeError("impact derivation enabled must be a boolean")
        if "::" in result.rule_id:
            raise ValueError("impact rule_id contains an ambiguous separator")
        return result


@dataclass(frozen=True)
class BurstConfig:
    ttl_sec: int
    max_ttl_sec: int

    @classmethod
    def from_dict(cls, payload: dict) -> "BurstConfig":
        values = _strict_dict(payload, {"ttl_sec", "max_ttl_sec"}, "burst")
        result = cls(**values)
        _positive_int("burst.ttl_sec", result.ttl_sec)
        _positive_int("burst.max_ttl_sec", result.max_ttl_sec)
        if result.max_ttl_sec < result.ttl_sec:
            raise ValueError("burst.max_ttl_sec must be >= burst.ttl_sec")
        return result


@dataclass(frozen=True)
class SolverConfig:
    method: str
    max_iterations: int
    tolerance: float
    objective_tolerance: float = 1e-8
    gradient_mapping_tolerance: float = 1e-6
    backtracking_factor: float = 2.0
    max_backtracking_steps: int = 50
    initial_lipschitz: float = 1.0
    lipschitz_floor: float = 1e-12
    monotone: bool = True
    adaptive_restart: bool = True
    minimum_iterations: int = 2
    convergence_patience: int = 2
    warm_start_enabled: bool = True
    strict_convergence: bool = True
    diagnostic_zero_tolerance: float = 1e-12

    @classmethod
    def from_dict(cls, payload: dict) -> "SolverConfig":
        required = {"method", "max_iterations", "tolerance"}
        allowed = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict):
            raise TypeError("solver must be a mapping")
        unknown = set(payload) - allowed
        missing = required - set(payload)
        if unknown or missing:
            raise ValueError(f"solver fields mismatch; missing={sorted(missing)}, unknown={sorted(unknown)}")
        values = dict(payload)
        result = cls(**values)
        if result.method != "fista":
            raise ValueError("solver.method must be 'fista'; fallback is not allowed")
        _positive_int("solver.max_iterations", result.max_iterations)
        tolerance = _finite("solver.tolerance", result.tolerance)
        if tolerance <= 0.0:
            raise ValueError("solver.tolerance must be positive")
        object.__setattr__(result, "tolerance", tolerance)
        for name in (
            "objective_tolerance", "gradient_mapping_tolerance", "backtracking_factor",
            "initial_lipschitz", "lipschitz_floor",
        ):
            value = _finite(f"solver.{name}", getattr(result, name))
            if value <= 0.0:
                raise ValueError(f"solver.{name} must be positive")
            object.__setattr__(result, name, value)
        if result.backtracking_factor <= 1.0:
            raise ValueError("solver.backtracking_factor must be greater than one")
        for name in (
            "max_backtracking_steps", "minimum_iterations", "convergence_patience",
        ):
            _positive_int(f"solver.{name}", getattr(result, name))
        for name in ("monotone", "adaptive_restart", "warm_start_enabled", "strict_convergence"):
            if type(getattr(result, name)) is not bool:
                raise TypeError(f"solver.{name} must be boolean")
        if not result.monotone or not result.adaptive_restart:
            raise ValueError("solver monotone and adaptive_restart must both be true")
        zero_tolerance = _finite(
            "solver.diagnostic_zero_tolerance", result.diagnostic_zero_tolerance
        )
        if zero_tolerance < 0.0:
            raise ValueError("solver.diagnostic_zero_tolerance must be non-negative")
        object.__setattr__(result, "diagnostic_zero_tolerance", zero_tolerance)
        return result


@dataclass(frozen=True)
class ConfidenceConfig:
    strong: float
    weak: float

    @classmethod
    def from_dict(cls, payload: dict) -> "ConfidenceConfig":
        values = _strict_dict(payload, {"strong", "weak"}, "confidence")
        result = cls(**values)
        object.__setattr__(result, "strong", _probability("confidence.strong", result.strong))
        object.__setattr__(result, "weak", _probability("confidence.weak", result.weak))
        if result.strong <= result.weak:
            raise ValueError("confidence.strong must be greater than confidence.weak")
        return result


@dataclass(frozen=True)
class ShockTemplateConfig:
    source_metric_families: list[str]
    target_metric_families: list[str]

    @classmethod
    def from_dict(cls, payload: dict, context: str) -> "ShockTemplateConfig":
        values = _strict_dict(
            payload,
            {"source_metric_families", "target_metric_families"},
            context,
        )
        result = cls(**values)
        for name in ("source_metric_families", "target_metric_families"):
            families = getattr(result, name)
            if not isinstance(families, list) or not families:
                raise ValueError(f"{context}.{name} must be a non-empty list")
            if any(family not in NODE_METRIC_FAMILIES for family in families):
                raise ValueError(f"{context}.{name} contains an invalid metric family")
            if len(families) != len(set(families)):
                raise ValueError(f"{context}.{name} contains duplicates")
        return result


@dataclass(frozen=True)
class ShockProjection:
    endpoint_role: str
    metric_family: str
    metric_names: list[str] | None
    raw_weight: float

    @classmethod
    def from_dict(cls, payload: dict) -> "ShockProjection":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "ShockProjection")
        result = cls(**values)
        if result.endpoint_role not in {"source", "target"}:
            raise ValueError("shock projection endpoint_role must be source or target")
        if result.metric_family not in NODE_METRIC_FAMILIES:
            raise ValueError("shock projection metric_family is invalid")
        if result.metric_names is not None:
            if not isinstance(result.metric_names, list) or not result.metric_names or any(
                not isinstance(item, str) or not item.strip() for item in result.metric_names
            ):
                raise ValueError("shock projection metric_names must be null or exact names")
            if result.metric_names != sorted(set(result.metric_names)):
                raise ValueError("shock projection metric_names must be sorted and unique")
        weight = _finite("shock projection raw_weight", result.raw_weight)
        if weight <= 0:
            raise ValueError("shock projection raw_weight must be positive")
        object.__setattr__(result, "raw_weight", weight)
        return result


@dataclass(frozen=True)
class ShockProjectionTemplate:
    template_id: str
    enabled: bool
    edge_metric_name: str
    protocol: str | None
    projections: list[ShockProjection]

    @classmethod
    def from_dict(cls, payload: dict) -> "ShockProjectionTemplate":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "ShockProjectionTemplate")
        for name in ("template_id", "edge_metric_name"):
            if not isinstance(values[name], str) or not values[name].strip():
                raise ValueError(f"shock projection template {name} must be non-empty")
        if any(token in values["edge_metric_name"] for token in ("::", "->")):
            raise ValueError("shock projection template requires an exact metric name, not an ID")
        if values["protocol"] is not None and (
            not isinstance(values["protocol"], str) or not values["protocol"].strip()
        ):
            raise ValueError("shock projection protocol must be null or non-empty")
        if not isinstance(values["enabled"], bool):
            raise TypeError("shock projection enabled must be boolean")
        raw = values["projections"]
        if not isinstance(raw, list) or not raw:
            raise ValueError("shock projection template requires projections")
        values["projections"] = [
            item if isinstance(item, ShockProjection) else ShockProjection.from_dict(item)
            for item in raw
        ]
        return cls(**values)


@dataclass(frozen=True)
class ResidualConfig:
    signal_kind: str = "signed_z"
    require_hard_alert: bool = True
    require_rca_eligible: bool = True
    require_global_metric_model_ready: bool = True
    require_complete_node_rows: bool = True
    require_complete_edge_rows: bool = True
    max_joint_rows: int = 10000
    max_propagation_variables: int = 50000
    max_shock_variables: int = 10000
    fail_on_overflow: bool = True

    @classmethod
    def from_dict(cls, payload: dict) -> "ResidualConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "residual")
        result = cls(**values)
        if result.signal_kind != "signed_z":
            raise ValueError("residual.signal_kind must be signed_z")
        for name in (
            "require_hard_alert", "require_rca_eligible",
            "require_global_metric_model_ready", "require_complete_node_rows",
            "require_complete_edge_rows", "fail_on_overflow",
        ):
            if not isinstance(getattr(result, name), bool):
                raise TypeError(f"residual.{name} must be boolean")
        for name in ("max_joint_rows", "max_propagation_variables", "max_shock_variables"):
            _positive_int(f"residual.{name}", getattr(result, name))
        if not result.fail_on_overflow:
            raise ValueError("residual.fail_on_overflow must be true")
        return result


@dataclass(frozen=True)
class PropagationDictionaryConfig:
    allowed_relation_types: list[str] = field(
        default_factory=lambda: ["same_service", "impact", "host", "resource"]
    )
    exclude_self_history: bool = True
    exclude_call: bool = True

    @classmethod
    def from_dict(cls, payload: dict) -> "PropagationDictionaryConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "propagation_dictionary")
        result = cls(**values)
        expected = {"same_service", "impact", "host", "resource"}
        if set(result.allowed_relation_types) != expected or len(result.allowed_relation_types) != len(expected):
            raise ValueError("propagation dictionary relation types must be the canonical four")
        if not result.exclude_self_history or not result.exclude_call:
            raise ValueError("propagation dictionary must exclude self_history and call")
        object.__setattr__(result, "allowed_relation_types", sorted(result.allowed_relation_types))
        return result


@dataclass(frozen=True)
class EvidenceConfig:
    max_age_windows: int = 30
    min_observation_quality: float = 0.0
    require_independent_from_residual: bool = True
    channel_aggregation: str = "max_then_noisy_or"

    @classmethod
    def from_dict(cls, payload: dict) -> "EvidenceConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "evidence")
        result = cls(**values)
        if isinstance(result.max_age_windows, bool) or not isinstance(result.max_age_windows, int) \
                or result.max_age_windows < 0:
            raise ValueError("evidence.max_age_windows must be a non-negative integer")
        object.__setattr__(result, "min_observation_quality", _probability(
            "evidence.min_observation_quality", result.min_observation_quality
        ))
        if result.require_independent_from_residual is not True:
            raise ValueError("circular evidence protection cannot be disabled")
        if result.channel_aggregation != "max_then_noisy_or":
            raise ValueError("evidence.channel_aggregation must be max_then_noisy_or")
        return result


@dataclass(frozen=True)
class QualityConfig:
    quality_weight_floor: float = 0.2

    @classmethod
    def from_dict(cls, payload: dict) -> "QualityConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "quality")
        result = cls(**values)
        floor = _probability("quality.quality_weight_floor", result.quality_weight_floor)
        if floor <= 0:
            raise ValueError("quality_weight_floor must be in (0, 1]")
        object.__setattr__(result, "quality_weight_floor", floor)
        return result


@dataclass(frozen=True)
class PenaltyConfig:
    residual_scale_floor: float = 0.1
    c_u: float = 1.0
    c_delta: float = 1.0
    c_xi: float = 1.0
    eta_v: float = 1.0
    eta_p: float = 1.0
    eta_s: float = 1.0
    rho_v: float = 1.0
    rho_p: float = 1.0
    rho_s: float = 1.0
    rho_m: float = 1.0
    group_ratio_u: float = 0.0
    group_ratio_delta: float = 0.0
    group_ratio_xi: float = 0.0

    @classmethod
    def from_dict(cls, payload: dict) -> "PenaltyConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "penalties")
        result = cls(**values)
        for name in ("residual_scale_floor", "c_u", "c_delta", "c_xi"):
            value = _finite(f"penalties.{name}", getattr(result, name))
            if value <= 0:
                raise ValueError(f"penalties.{name} must be positive")
            object.__setattr__(result, name, value)
        for name in (
            "eta_v", "eta_p", "eta_s", "rho_v", "rho_p", "rho_s", "rho_m",
            "group_ratio_u", "group_ratio_delta", "group_ratio_xi",
        ):
            value = _finite(f"penalties.{name}", getattr(result, name))
            if value < 0:
                raise ValueError(f"penalties.{name} must be non-negative")
            object.__setattr__(result, name, value)
        return result


AGGREGATION_METHODS = frozenset(
    {
        "sum",
        "last_same_series",
        "median_max",
        "ratio_from_components",
        "histogram_merge_quantile",
        "reject_cross_scope_quantile",
    }
)
AGGREGATION_SCOPES = frozenset({"pod", "service", "node", "flow", "pod_pair", "service_pair"})


def _scope_transition_allowed(source: str, target: str) -> bool:
    allowed = {
        "pod": {"pod", "service"},
        "service": {"service"},
        "node": {"node"},
        "flow": {"flow", "pod_pair", "service_pair"},
        "pod_pair": {"pod_pair", "service_pair"},
        "service_pair": {"service_pair"},
    }
    return target in allowed[source]


def _optional_nonempty_string(name: str, value: Any) -> None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"{name} must be None or a non-empty string")


@dataclass(frozen=True)
class HistogramBucketInputSpec:
    metric_id: str
    metric_identity: str
    metric_kind: str
    unit: str
    scope: str
    upper_bound: float | None
    is_inf_bucket: bool
    is_cumulative: bool

    @classmethod
    def from_dict(cls, payload: dict) -> "HistogramBucketInputSpec":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "HistogramBucketInputSpec")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        for name in ("metric_id", "metric_identity", "unit"):
            _optional_nonempty_string(name, getattr(self, name))
        if self.metric_kind != "histogram_bucket":
            raise ValueError("histogram input metric_kind must be histogram_bucket")
        if self.scope not in AGGREGATION_SCOPES:
            raise ValueError("invalid histogram input scope")
        if not isinstance(self.is_inf_bucket, bool) or not isinstance(self.is_cumulative, bool):
            raise TypeError("histogram bucket flags must be booleans")
        if self.is_inf_bucket:
            if self.upper_bound is not None:
                raise ValueError("+Inf bucket upper_bound must be None")
        elif self.upper_bound is None:
            raise ValueError("finite histogram bucket requires upper_bound")
        else:
            _finite("histogram upper_bound", self.upper_bound)

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class MonotonicCounterPolicy:
    delta_before_cross_series_sum: bool
    value_decrease_means_reset: bool
    reset_policy: str

    @classmethod
    def from_dict(cls, payload: dict) -> "MonotonicCounterPolicy":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "MonotonicCounterPolicy")
        result = cls(**values)
        if result.delta_before_cross_series_sum is not True:
            raise ValueError("monotonic counters must be differenced before cross-series sum")
        if result.value_decrease_means_reset is not True:
            raise ValueError("monotonic counter decreases must be treated as resets")
        if result.reset_policy not in {"use_current_value", "mark_missing", "reject_window"}:
            raise ValueError("invalid monotonic counter reset_policy")
        return result

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MetricAggregationSpec:
    """Strict declarative aggregation policy; execution belongs to P2."""

    method: str
    input_metric_kind: str
    source_scope: str
    target_scope: str
    input_metric_ids: list[str] | None
    input_series_ids: list[str] | None
    numerator_metric_id: str | None
    denominator_metric_id: str | None
    numerator_metric_kind: str | None
    denominator_metric_kind: str | None
    numerator_scope: str | None
    denominator_scope: str | None
    output_metric_name: str | None
    output_metric_kind: str | None
    output_unit: str | None
    missing_component_policy: str
    zero_denominator_policy: str | None
    median_weight: float | None
    histogram_inputs: list[HistogramBucketInputSpec] | None
    output_quantiles: list[float] | None

    @classmethod
    def from_dict(cls, payload: dict) -> "MetricAggregationSpec":
        required = {field.name for field in cls.__dataclass_fields__.values()}
        values = _strict_dict(payload, required, "MetricAggregationSpec")
        histogram_inputs = values["histogram_inputs"]
        if histogram_inputs is not None:
            if not isinstance(histogram_inputs, list):
                raise TypeError("histogram_inputs must be a list or None")
            values = dict(values)
            values["histogram_inputs"] = [
                item if isinstance(item, HistogramBucketInputSpec) else HistogramBucketInputSpec.from_dict(item)
                for item in histogram_inputs
            ]
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if self.method not in AGGREGATION_METHODS:
            raise ValueError(f"invalid aggregation method {self.method!r}")
        if self.input_metric_kind not in METRIC_KINDS:
            raise ValueError(f"invalid input_metric_kind {self.input_metric_kind!r}")
        if self.source_scope not in AGGREGATION_SCOPES or self.target_scope not in AGGREGATION_SCOPES:
            raise ValueError("invalid aggregation source_scope or target_scope")
        for name in (
            "numerator_metric_id",
            "denominator_metric_id",
            "output_metric_name",
            "output_unit",
        ):
            _optional_nonempty_string(name, getattr(self, name))
        for name in ("input_metric_ids", "input_series_ids"):
            values = getattr(self, name)
            if values is not None:
                if not isinstance(values, list) or not values:
                    raise ValueError(f"{name} must be None or a non-empty list")
                if any(not isinstance(value, str) or not value.strip() for value in values):
                    raise ValueError(f"{name} must contain non-empty strings")
                if len(values) != len(set(values)):
                    raise ValueError(f"{name} must not contain duplicates")
        if self.output_metric_kind is not None and self.output_metric_kind not in METRIC_KINDS:
            raise ValueError("invalid output_metric_kind")
        if self.missing_component_policy not in {"missing", "invalid"}:
            raise ValueError("missing_component_policy must be missing or invalid")
        if self.zero_denominator_policy not in {None, "missing", "invalid"}:
            raise ValueError("zero_denominator_policy must be missing, invalid, or None")
        ratio_fields = (self.numerator_metric_id, self.denominator_metric_id)
        if self.input_metric_kind == "quantile" and self.source_scope != self.target_scope and self.method != "reject_cross_scope_quantile":
            raise ValueError("cross-scope quantiles require reject_cross_scope_quantile")
        if self.method == "sum":
            if self.input_metric_kind not in {"delta_counter", "histogram_bucket"}:
                raise ValueError("sum only accepts delta_counter or validated histogram buckets")
            self._require_output()
            if self.input_metric_kind == "histogram_bucket":
                self._validate_histogram_inputs(require_quantiles=False)
                if self.output_metric_kind != "histogram_bucket":
                    raise ValueError("histogram bucket sum must preserve histogram_bucket kind")
                if self.input_metric_ids != [item.metric_id for item in self.histogram_inputs]:
                    raise ValueError("histogram sum input IDs must match histogram_inputs")
            elif self.histogram_inputs is not None:
                raise ValueError("delta_counter sum does not accept histogram_inputs")
            elif self.output_metric_kind != "delta_counter":
                raise ValueError("delta_counter sum must preserve delta_counter kind")
            if self.input_metric_ids is None:
                raise ValueError("sum requires input_metric_ids")
            self._reject_unused_ratio_fields()
        elif self.method == "last_same_series":
            self._require_output()
            if self.source_scope != self.target_scope:
                raise ValueError("last_same_series cannot cross scopes")
            if not isinstance(self.input_metric_ids, list) or len(self.input_metric_ids) != 1:
                raise ValueError("last_same_series requires exactly one input metric ID")
            if not isinstance(self.input_series_ids, list) or len(self.input_series_ids) != 1:
                raise ValueError("last_same_series requires exactly one stable series ID")
            if self.output_metric_kind != self.input_metric_kind:
                raise ValueError("last_same_series preserves metric_kind")
            self._reject_unused_ratio_fields()
            if self.histogram_inputs is not None or self.output_quantiles is not None:
                raise ValueError("last_same_series does not accept histogram merge fields")
        elif self.method == "median_max":
            self._require_output()
            if self.input_metric_kind != "gauge" or self.output_metric_kind != "gauge":
                raise ValueError("median_max only accepts gauge metrics")
            if self.input_metric_ids is None:
                raise ValueError("median_max requires input_metric_ids")
            if self.median_weight is None:
                raise ValueError("median_max requires median_weight")
            weight = _finite("median_weight", self.median_weight)
            if not 0.0 <= weight <= 1.0:
                raise ValueError("median_weight must be in [0, 1]")
            self._reject_unused_ratio_fields()
            if self.histogram_inputs is not None or self.output_quantiles is not None:
                raise ValueError("median_max does not accept histogram fields")
        elif self.method == "ratio_from_components":
            if self.input_metric_ids is not None:
                raise ValueError("ratio_from_components uses explicit numerator and denominator IDs")
            self._require_output()
            if any(value is None for value in ratio_fields):
                raise ValueError("ratio_from_components requires numerator and denominator IDs")
            if self.numerator_metric_kind not in METRIC_KINDS or self.denominator_metric_kind not in METRIC_KINDS:
                raise ValueError("ratio component metric kinds are required")
            if self.numerator_metric_kind in {"quantile", "histogram_bucket"} or self.denominator_metric_kind in {"quantile", "histogram_bucket"}:
                raise ValueError("ratio components cannot be quantiles or histograms")
            if (
                self.numerator_scope != self.denominator_scope
                or self.numerator_scope != self.source_scope
                or self.source_scope != self.target_scope
            ):
                raise ValueError("ratio component and output scopes must match")
            if self.zero_denominator_policy not in {"missing", "invalid"}:
                raise ValueError("ratio requires a non-zero-output denominator policy")
            if self.histogram_inputs is not None or self.output_quantiles is not None:
                raise ValueError("ratio_from_components does not accept histogram fields")
        elif self.method == "histogram_merge_quantile":
            if self.input_metric_kind != "histogram_bucket":
                raise ValueError("histogram_merge_quantile requires histogram_bucket input")
            if self.input_metric_ids is not None:
                raise ValueError("histogram_merge_quantile uses explicit histogram_inputs")
            self._require_output()
            if self.output_metric_kind != "quantile":
                raise ValueError("histogram_merge_quantile output must be quantile")
            self._reject_unused_ratio_fields()
            self._validate_histogram_inputs(require_quantiles=True)
        else:
            if self.input_metric_kind != "quantile":
                raise ValueError("reject_cross_scope_quantile requires quantile input")
            if self.source_scope == self.target_scope:
                raise ValueError("reject_cross_scope_quantile requires different scopes")
            if not isinstance(self.input_metric_ids, list) or len(self.input_metric_ids) != 1:
                raise ValueError("reject_cross_scope_quantile requires exactly one input metric ID")
            if any(value is not None for value in (
                *ratio_fields, self.output_metric_name, self.output_metric_kind, self.output_unit,
                self.histogram_inputs, self.output_quantiles, self.input_series_ids,
            )):
                raise ValueError("reject_cross_scope_quantile accepts no output fields")

    def _require_output(self) -> None:
        if self.output_metric_name is None or self.output_metric_kind is None or self.output_unit is None:
            raise ValueError(f"{self.method} requires explicit output name, kind, scope, and unit")

    def _reject_unused_ratio_fields(self) -> None:
        if any(value is not None for value in (
            self.numerator_metric_id, self.denominator_metric_id, self.numerator_metric_kind,
            self.denominator_metric_kind, self.numerator_scope, self.denominator_scope,
            self.zero_denominator_policy,
        )):
            raise ValueError(f"{self.method} does not accept ratio fields")

    def _validate_histogram_inputs(self, *, require_quantiles: bool) -> None:
        if not isinstance(self.histogram_inputs, list) or not self.histogram_inputs:
            raise ValueError("histogram aggregation requires histogram_inputs")
        for item in self.histogram_inputs:
            item.validate()
        identities = {item.metric_identity for item in self.histogram_inputs}
        units = {item.unit for item in self.histogram_inputs}
        scopes = {item.scope for item in self.histogram_inputs}
        cumulative = {item.is_cumulative for item in self.histogram_inputs}
        if len(identities) != 1 or len(units) != 1 or scopes != {self.source_scope} or len(cumulative) != 1:
            raise ValueError("histogram inputs must share identity, unit, scope, and cumulative semantics")
        if not _scope_transition_allowed(self.source_scope, self.target_scope):
            raise ValueError("histogram source and output scopes are incompatible")
        if self.output_unit not in units:
            raise ValueError("histogram output unit must match input unit")
        finite_bounds = [item.upper_bound for item in self.histogram_inputs if not item.is_inf_bucket]
        inf_positions = [index for index, item in enumerate(self.histogram_inputs) if item.is_inf_bucket]
        if finite_bounds != sorted(set(finite_bounds)):
            raise ValueError("finite histogram bounds must be unique and increasing")
        if len(inf_positions) > 1 or (inf_positions and inf_positions[0] != len(self.histogram_inputs) - 1):
            raise ValueError("at most one +Inf bucket is allowed and it must be last")
        if require_quantiles:
            if not isinstance(self.output_quantiles, list) or not self.output_quantiles:
                raise ValueError("histogram_merge_quantile requires output_quantiles")
            normalized = [_finite("output_quantile", value) for value in self.output_quantiles]
            if any(not 0.0 < value < 1.0 for value in normalized) or normalized != sorted(set(normalized)):
                raise ValueError("output_quantiles must be unique, increasing, and in (0, 1)")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class MetricSignalSpec:
    """Exact anomaly semantics for one configured aggregation output."""

    record_type: str
    metric_family: str | None
    metric_name: str
    protocol: str | None
    transform: str
    polarity: str
    rare_event_threshold: float | None
    direct_hard: bool
    z_cap: float
    aggregation_output_id: str

    @classmethod
    def from_dict(cls, payload: dict) -> "MetricSignalSpec":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "MetricSignalSpec")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if self.record_type not in {"node_metric", "edge_metric"}:
            raise ValueError("signal record_type must be node_metric or edge_metric")
        _optional_nonempty_string("metric_name", self.metric_name)
        _optional_nonempty_string("aggregation_output_id", self.aggregation_output_id)
        _optional_nonempty_string("metric_family", self.metric_family)
        _optional_nonempty_string("protocol", self.protocol)
        if self.record_type == "node_metric":
            if self.metric_family not in NODE_METRIC_FAMILIES or self.protocol is not None:
                raise ValueError("node signal requires metric_family and no protocol")
        elif self.metric_family is not None:
            raise ValueError("edge signal must not declare a node metric_family")
        if self.transform not in {"identity", "log1p"}:
            raise ValueError("signal transform must be identity or log1p")
        if self.polarity not in {"increase_bad", "decrease_bad"}:
            raise ValueError("signal polarity must be increase_bad or decrease_bad")
        if self.rare_event_threshold is not None:
            _finite("rare_event_threshold", self.rare_event_threshold)
        if not isinstance(self.direct_hard, bool):
            raise TypeError("direct_hard must be a boolean")
        if _finite("z_cap", self.z_cap) <= 0:
            raise ValueError("z_cap must be positive")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class BaselineConfig:
    healthy_history_sec: int
    min_healthy_windows: int
    min_scale: float
    z_cap: float

    @classmethod
    def from_dict(cls, payload: dict) -> "BaselineConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "BaselineConfig")
        result = cls(**values)
        _positive_int("healthy_history_sec", result.healthy_history_sec)
        _positive_int("min_healthy_windows", result.min_healthy_windows)
        if result.min_healthy_windows > result.healthy_history_sec:
            raise ValueError("min_healthy_windows cannot exceed healthy_history_sec")
        if _finite("min_scale", result.min_scale) <= 0 or _finite("z_cap", result.z_cap) <= 0:
            raise ValueError("min_scale and z_cap must be positive")
        return result


@dataclass(frozen=True)
class WindowConfig:
    window_sec: int
    allowed_lateness_sec: int

    @classmethod
    def from_dict(cls, payload: dict) -> "WindowConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "WindowConfig")
        result = cls(**values)
        _positive_int("window_sec", result.window_sec)
        if isinstance(result.allowed_lateness_sec, bool) or not isinstance(result.allowed_lateness_sec, int):
            raise TypeError("allowed_lateness_sec must be an integer")
        if result.allowed_lateness_sec < 0:
            raise ValueError("allowed_lateness_sec must be non-negative")
        return result


@dataclass(frozen=True)
class ScoreConfig:
    family_weights: dict[str, float]
    allow_partial_families: bool
    edge_weight: float
    edge_business_impact_threshold: float

    @classmethod
    def from_dict(cls, payload: dict) -> "ScoreConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "ScoreConfig")
        result = cls(**values)
        if set(result.family_weights) != set(NODE_METRIC_FAMILIES):
            raise ValueError("family_weights must define every node metric family exactly once")
        weights = {name: _finite(f"family_weights.{name}", value) for name, value in result.family_weights.items()}
        if any(value < 0 for value in weights.values()) or not math.isclose(sum(weights.values()), 1.0, abs_tol=1e-9):
            raise ValueError("family weights must be non-negative and sum to 1")
        if not isinstance(result.allow_partial_families, bool):
            raise TypeError("allow_partial_families must be a boolean")
        if _finite("edge_weight", result.edge_weight) < 0:
            raise ValueError("edge_weight must be non-negative")
        if _finite("edge_business_impact_threshold", result.edge_business_impact_threshold) < 0:
            raise ValueError("edge_business_impact_threshold must be non-negative")
        return result


@dataclass(frozen=True)
class CompositeAlertRule:
    rule_id: str
    target: str
    all_of: list[str]
    any_of: list[str]
    threshold: float
    consecutive_windows: int
    resulting_level: str

    @classmethod
    def from_dict(cls, payload: dict) -> "CompositeAlertRule":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "CompositeAlertRule")
        result = cls(**values)
        _optional_nonempty_string("rule_id", result.rule_id)
        if "::" in result.rule_id:
            raise ValueError("composite rule_id must not contain the internal separator")
        if result.target not in {"same_service", "same_edge"}:
            raise ValueError("composite target must be same_service or same_edge")
        for name in ("all_of", "any_of"):
            selectors = getattr(result, name)
            if not isinstance(selectors, list) or any(not isinstance(item, str) or not item for item in selectors):
                raise ValueError(f"{name} must be a list of non-empty exact metric IDs")
            if len(selectors) != len(set(selectors)):
                raise ValueError(f"{name} contains duplicate selectors")
        if bool(result.all_of) == bool(result.any_of):
            raise ValueError("exactly one of all_of or any_of must be configured")
        if set(result.all_of) & set(result.any_of):
            raise ValueError("composite selectors must not overlap")
        if _finite("composite threshold", result.threshold) < 0:
            raise ValueError("composite threshold must be non-negative")
        _positive_int("composite consecutive_windows", result.consecutive_windows)
        if result.resulting_level not in {"soft", "hard"}:
            raise ValueError("composite resulting_level must be soft or hard")
        return result


@dataclass(frozen=True)
class AlertStateConfig:
    healthy_threshold: float
    soft_threshold: float
    soft_consecutive_windows: int
    hard_threshold: float
    hard_consecutive_windows: int
    recovery_threshold: float
    recovery_windows: int
    recovery_cooldown_sec: int
    edge_business_impact_threshold: float

    @classmethod
    def from_dict(cls, payload: dict) -> "AlertStateConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "AlertStateConfig")
        result = cls(**values)
        thresholds = [
            _finite("recovery_threshold", result.recovery_threshold),
            _finite("healthy_threshold", result.healthy_threshold),
            _finite("soft_threshold", result.soft_threshold),
            _finite("hard_threshold", result.hard_threshold),
        ]
        if thresholds[0] < 0 or not thresholds[0] < thresholds[1] < thresholds[2] < thresholds[3]:
            raise ValueError("alert thresholds must satisfy 0 <= recovery < healthy < soft < hard")
        for name in ("soft_consecutive_windows", "hard_consecutive_windows", "recovery_windows"):
            _positive_int(name, getattr(result, name))
        if isinstance(result.recovery_cooldown_sec, bool) or not isinstance(result.recovery_cooldown_sec, int) or result.recovery_cooldown_sec < 0:
            raise ValueError("recovery_cooldown_sec must be a non-negative integer")
        if _finite("edge_business_impact_threshold", result.edge_business_impact_threshold) < 0:
            raise ValueError("edge_business_impact_threshold must be non-negative")
        return result


@dataclass(frozen=True)
class DiagnosisConfig:
    diagnostic_zero_tolerance: float = 1e-12
    max_active_candidates: int = 100
    fail_on_candidate_overflow: bool = True
    counterfactual_top_k: int = 10
    require_primary_counterfactual: bool = True
    counterfactual_min_relative_delta: float = 0.0
    counterfactual_numerical_tolerance: float = 1e-8
    symptom_anomaly_threshold: float = 1.0
    propagated_explained_ratio_threshold: float = 0.5
    include_root_node_as_symptom: bool = False
    max_path_length: int = 5
    max_paths_per_root: int = 10
    path_length_penalty: float = 0.1
    minimum_path_edge_support: float = 0.01
    allow_service_level_fallback: bool = False
    ident_cf_weight: float = 0.4
    ident_path_weight: float = 0.3
    ident_margin_weight: float = 0.3
    ident_coherence_weight: float = 0.5
    ident_lag_entropy_weight: float = 0.5
    confidence_cf_weight: float = 0.3
    confidence_margin_weight: float = 0.2
    confidence_quality_weight: float = 0.2
    confidence_identifiability_weight: float = 0.3
    strong_identifiability_threshold: float = 0.7
    minimum_identifiability_threshold: float = 0.3
    minimum_relative_counterfactual_delta: float = 0.01
    minimum_margin_for_root: float = 0.05

    @classmethod
    def from_dict(cls, payload: dict) -> "DiagnosisConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "diagnosis")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        for name in ("diagnostic_zero_tolerance", "counterfactual_numerical_tolerance",
                     "path_length_penalty"):
            if _finite(f"diagnosis.{name}", getattr(self, name)) < 0:
                raise ValueError(f"diagnosis.{name} must be non-negative")
        for name in ("max_active_candidates", "counterfactual_top_k", "max_path_length",
                     "max_paths_per_root"):
            _positive_int(f"diagnosis.{name}", getattr(self, name))
        if self.counterfactual_top_k > self.max_active_candidates:
            raise ValueError("diagnosis.counterfactual_top_k cannot exceed max_active_candidates")
        for name in ("fail_on_candidate_overflow", "require_primary_counterfactual",
                     "include_root_node_as_symptom", "allow_service_level_fallback"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"diagnosis.{name} must be boolean")
        probability_fields = (
            "counterfactual_min_relative_delta", "symptom_anomaly_threshold",
            "propagated_explained_ratio_threshold", "minimum_path_edge_support",
            "strong_identifiability_threshold", "minimum_identifiability_threshold",
            "minimum_relative_counterfactual_delta", "minimum_margin_for_root",
        )
        for name in probability_fields:
            _probability(f"diagnosis.{name}", getattr(self, name))
        if self.strong_identifiability_threshold < self.minimum_identifiability_threshold:
            raise ValueError("strong identifiability must be >= minimum identifiability")
        positive = (self.ident_cf_weight, self.ident_path_weight, self.ident_margin_weight)
        uncertainty = (self.ident_coherence_weight, self.ident_lag_entropy_weight)
        confidence = (self.confidence_cf_weight, self.confidence_margin_weight,
                      self.confidence_quality_weight, self.confidence_identifiability_weight)
        if any(_finite("diagnosis ident weight", value) < 0 for value in (*positive, *uncertainty)) \
                or sum(positive) <= 0 or sum(uncertainty) <= 0:
            raise ValueError("diagnosis identifiability weights must be non-negative with positive sums")
        if any(_finite("diagnosis confidence weight", value) < 0 for value in confidence) \
                or not math.isclose(sum(confidence), 1.0, abs_tol=1e-9):
            raise ValueError("diagnosis confidence weights must be non-negative and sum to 1")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class OrchestrationConfig:
    analysis_delay_windows: int = 0
    evidence_window_windows: int = 0
    allow_single_active_incident_only: bool = True
    fail_on_concurrent_incident: bool = True
    retain_intermediates: bool = False
    checkpoint_every_windows: int = 0
    strict_stage_identity: bool = True
    continue_after_incident_failure: bool = True

    @classmethod
    def from_dict(cls, payload: dict) -> "OrchestrationConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "orchestration")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        for name in ("analysis_delay_windows", "evidence_window_windows", "checkpoint_every_windows"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"orchestration.{name} must be a non-negative integer")
        if self.evidence_window_windows > self.analysis_delay_windows:
            raise ValueError("evidence_window_windows cannot exceed analysis_delay_windows")
        for name in (
            "allow_single_active_incident_only", "fail_on_concurrent_incident",
            "retain_intermediates", "strict_stage_identity", "continue_after_incident_failure",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"orchestration.{name} must be boolean")
        if not self.allow_single_active_incident_only or not self.fail_on_concurrent_incident:
            raise ValueError("P10 requires one active incident per cluster with concurrent fail-fast")
        if not self.strict_stage_identity:
            raise ValueError("P10 requires strict stage identity")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class ReplayConfig:
    strict_order: bool = True
    allow_explicit_reorder: bool = False
    parquet_batch_size: int = 1024
    output_overwrite: bool = False
    write_alerts: bool = True
    write_failures: bool = True

    @classmethod
    def from_dict(cls, payload: dict) -> "ReplayConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "replay")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        for name in ("strict_order", "allow_explicit_reorder", "output_overwrite",
                     "write_alerts", "write_failures"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"replay.{name} must be boolean")
        _positive_int("replay.parquet_batch_size", self.parquet_batch_size)
        if self.strict_order == self.allow_explicit_reorder:
            raise ValueError("exactly one of strict_order or allow_explicit_reorder must be enabled")

    def to_dict(self) -> dict:
        self.validate()
        return asdict(self)


def _stable_fingerprint(payload: dict) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PrometheusQuerySpec:
    spec_id: str
    enabled: bool
    record_type: str
    promql: str
    query_mode: str
    metric_name: str
    metric_family: str | None
    metric_kind: str
    counter_semantics: str | None
    signal_spec_id: str | None
    aggregation_spec_id: str | None
    unit: str
    value_field: str
    label_mapping: dict[str, str]
    required_labels: list[str]
    optional_labels: list[str]
    service_resolution: str
    source_resolution: str | None
    destination_resolution: str | None
    protocol_label: str | None
    histogram_le_label: str | None
    quantile_label: str | None
    expected_scope: str
    allow_empty: bool
    quality_policy: str
    query_timeout_sec: float

    @classmethod
    def from_dict(cls, payload: dict) -> "PrometheusQuerySpec":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "prometheus query spec")
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        for name in ("spec_id", "promql", "metric_name", "unit", "value_field",
                     "service_resolution", "expected_scope", "quality_policy"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"query spec {name} must be non-empty")
        if type(self.enabled) is not bool or type(self.allow_empty) is not bool:
            raise TypeError("query spec enabled and allow_empty must be boolean")
        if self.record_type not in {"node_metric", "edge_metric", "call_edge"}:
            raise ValueError("query spec record_type is invalid")
        if self.query_mode not in {"range", "instant_at_end"}:
            raise ValueError("query spec query_mode is invalid")
        if self.metric_kind not in METRIC_KINDS:
            raise ValueError("query spec metric_kind is invalid")
        if self.metric_kind == "monotonic_counter":
            if self.counter_semantics != "raw_cumulative" or "rate(" in self.promql.lower():
                raise ValueError("monotonic_counter requires raw_cumulative semantics")
        elif self.counter_semantics not in {None, "delta"}:
            raise ValueError("counter_semantics is incompatible with metric_kind")
        if self.metric_kind == "histogram_bucket" and not self.histogram_le_label:
            raise ValueError("histogram bucket query requires histogram_le_label")
        if self.metric_kind != "histogram_bucket" and self.histogram_le_label is not None:
            raise ValueError("histogram_le_label is only valid for histogram buckets")
        if self.metric_kind == "quantile" and not self.quantile_label:
            raise ValueError("quantile query requires quantile_label")
        if self.record_type in {"edge_metric", "call_edge"} and (
                not self.source_resolution or not self.destination_resolution):
            raise ValueError("edge/call query requires source and destination resolution")
        if not isinstance(self.label_mapping, dict) or any(
                not isinstance(key, str) or not key or not isinstance(value, str) or not value
                for key, value in self.label_mapping.items()):
            raise ValueError("query spec label_mapping must be exact non-empty strings")
        if len(self.required_labels) != len(set(self.required_labels)) or \
                len(self.optional_labels) != len(set(self.optional_labels)):
            raise ValueError("query spec labels must be unique")
        if set(self.required_labels) & set(self.optional_labels):
            raise ValueError("required and optional labels overlap")
        if any(label not in self.label_mapping.values() for label in self.required_labels):
            raise ValueError("required label has no exact label mapping")
        _finite("query_timeout_sec", self.query_timeout_sec)
        if self.query_timeout_sec <= 0:
            raise ValueError("query_timeout_sec must be positive")

    @property
    def fingerprint(self) -> str:
        self.validate()
        return _stable_fingerprint(asdict(self))


@dataclass(frozen=True)
class KubernetesConfig:
    enabled: bool = False
    cluster_id: str = ""
    in_cluster: bool = False
    kubeconfig_path: str | None = None
    context: str | None = None
    namespaces: tuple[str, ...] = ()
    namespace_label_selector: str | None = None
    field_selectors: dict[str, str] = field(default_factory=dict)
    resync_timeout_sec: float = 60.0
    watch_timeout_sec: float = 30.0
    reconnect_initial_sec: float = 1.0
    reconnect_max_sec: float = 30.0
    allow_watch_bookmarks: bool = True
    endpoint_ready_policy: str = "ready_only"
    include_terminating_endpoints: bool = False
    include_persistent_volumes: bool = True
    include_volume_attachments: bool = False
    include_jobs: bool = True
    include_external_name_services: bool = False
    pod_service_ambiguity_policy: str = "fail"
    inventory_stale_after_sec: float = 120.0
    topology_snapshot_each_window: bool = True
    explicit_resource_annotation_prefix: str = "proberca.io/resource-"

    @classmethod
    def from_dict(cls, payload: dict) -> "KubernetesConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "kubernetes")
        if isinstance(values.get("namespaces"), list):
            values = dict(values)
            values["namespaces"] = tuple(values["namespaces"])
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if type(self.enabled) is not bool or type(self.in_cluster) is not bool:
            raise TypeError("kubernetes enabled/in_cluster must be boolean")
        if not isinstance(self.cluster_id, str) or not self.cluster_id.strip():
            raise ValueError("kubernetes.cluster_id is required")
        if not self.namespaces and not self.namespace_label_selector:
            raise ValueError("explicit namespaces or namespace_label_selector is required")
        if any(not isinstance(item, str) or not item.strip() for item in self.namespaces):
            raise ValueError("kubernetes namespaces must be non-empty strings")
        if len(self.namespaces) != len(set(self.namespaces)):
            raise ValueError("kubernetes namespaces contain duplicates")
        for name in ("resync_timeout_sec", "watch_timeout_sec", "reconnect_initial_sec",
                     "reconnect_max_sec", "inventory_stale_after_sec"):
            value = _finite(f"kubernetes.{name}", getattr(self, name))
            if value <= 0:
                raise ValueError(f"kubernetes.{name} must be positive")
        if self.reconnect_initial_sec > self.reconnect_max_sec:
            raise ValueError("kubernetes reconnect_initial_sec exceeds reconnect_max_sec")
        if self.endpoint_ready_policy not in {"ready_only", "ready_or_serving", "all"}:
            raise ValueError("invalid endpoint_ready_policy")
        if self.pod_service_ambiguity_policy not in {"fail", "explicit_only"}:
            raise ValueError("invalid pod_service_ambiguity_policy")


@dataclass(frozen=True)
class PrometheusConfig:
    enabled: bool = False
    base_url: str | None = None
    token_file: str | None = None
    ca_file: str | None = None
    client_cert_file: str | None = None
    client_key_file: str | None = None
    timeout_sec: float = 10.0
    max_retries: int = 3
    retry_initial_sec: float = 0.5
    retry_max_sec: float = 5.0
    collection_delay_sec: float = 1.0
    query_step_sec: float = 1.0
    query_specs: tuple[PrometheusQuerySpec, ...] = ()
    call_edge_query_specs: tuple[PrometheusQuerySpec, ...] = ()
    reject_partial_response: bool = True
    maximum_sample_lateness_sec: float = 0.0
    allow_insecure_test_endpoint: bool = False

    @classmethod
    def from_dict(cls, payload: dict) -> "PrometheusConfig":
        values = _strict_dict(payload, set(cls.__dataclass_fields__), "prometheus")
        values = dict(values)
        values["query_specs"] = tuple(
            item if isinstance(item, PrometheusQuerySpec) else PrometheusQuerySpec.from_dict(item)
            for item in values["query_specs"])
        values["call_edge_query_specs"] = tuple(
            item if isinstance(item, PrometheusQuerySpec) else PrometheusQuerySpec.from_dict(item)
            for item in values["call_edge_query_specs"])
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if self.enabled and (not isinstance(self.base_url, str) or not self.base_url):
            raise ValueError("prometheus.base_url is required when enabled")
        if self.base_url:
            parsed = urlparse(self.base_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError("prometheus.base_url must be HTTP(S)")
            forbidden = {"token", "access_token", "authorization", "auth"}
            if parsed.username or parsed.password or forbidden & set(parse_qs(parsed.query)):
                raise ValueError("Prometheus URL must not contain credentials")
            if parsed.scheme == "http" and not self.allow_insecure_test_endpoint:
                raise ValueError(
                    "plain HTTP Prometheus requires allow_insecure_test_endpoint=true")
        if type(self.allow_insecure_test_endpoint) is not bool:
            raise TypeError("allow_insecure_test_endpoint must be boolean")
        for name in ("timeout_sec", "retry_initial_sec", "retry_max_sec",
                     "query_step_sec"):
            if _finite(f"prometheus.{name}", getattr(self, name)) <= 0:
                raise ValueError(f"prometheus.{name} must be positive")
        for name in ("collection_delay_sec", "maximum_sample_lateness_sec"):
            if _finite(f"prometheus.{name}", getattr(self, name)) < 0:
                raise ValueError(f"prometheus.{name} must be non-negative")
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise ValueError("prometheus.max_retries must be a non-negative integer")
        if self.retry_initial_sec > self.retry_max_sec:
            raise ValueError("prometheus retry_initial_sec exceeds retry_max_sec")
        specs = [*self.query_specs, *self.call_edge_query_specs]
        if len({item.spec_id for item in specs}) != len(specs):
            raise ValueError("Prometheus query spec IDs must be unique")
        for item in specs:
            item.validate()

    @property
    def fingerprint(self) -> str:
        self.validate()
        payload = asdict(self)
        for name in ("token_file", "ca_file", "client_cert_file", "client_key_file"):
            payload[name] = bool(payload[name])
        return _stable_fingerprint(payload)


@dataclass(frozen=True)
class LeaderElectionConfig:
    enabled: bool = True
    lease_namespace: str = "default"
    lease_name: str = "proberca"
    holder_identity_source: str = "pod_uid"
    lease_duration_sec: float = 15.0
    renew_deadline_sec: float = 10.0
    retry_period_sec: float = 2.0
    run_state_annotation_max_bytes: int = 196608

    @classmethod
    def from_dict(cls, payload: dict) -> "LeaderElectionConfig":
        result = cls(**_strict_dict(payload, set(cls.__dataclass_fields__), "leader_election"))
        result.validate()
        return result

    def validate(self) -> None:
        if not self.lease_namespace or not self.lease_name:
            raise ValueError("Lease namespace and name are required")
        if self.holder_identity_source not in {"pod_uid", "explicit_instance_id"}:
            raise ValueError("holder_identity_source must be unique")
        if not (self.lease_duration_sec > self.renew_deadline_sec > self.retry_period_sec > 0):
            raise ValueError("lease duration must exceed renew deadline and retry period")
        if isinstance(self.run_state_annotation_max_bytes, bool) or not (
            0 < self.run_state_annotation_max_bytes <= 262144
        ):
            raise ValueError("Lease RunState annotation limit is invalid")


@dataclass(frozen=True)
class LiveLivenessConfig:
    freeze_revision_timeout_sec: float = 10.0
    topology_build_timeout_sec: float = 30.0
    call_edge_collection_timeout_sec: float = 60.0
    node_metric_collection_timeout_sec: float = 60.0
    edge_metric_collection_timeout_sec: float = 60.0
    record_adaptation_timeout_sec: float = 30.0
    engine_process_timeout_sec: float = 120.0
    generation_prepare_timeout_sec: float = 120.0
    run_state_commit_timeout_sec: float = 30.0
    output_projection_timeout_sec: float = 60.0
    retention_timeout_sec: float = 30.0
    progress_timeout_sec: float = 180.0
    watchdog_poll_interval_sec: float = 1.0
    watchdog_dump_grace_sec: float = 10.0
    watchdog_exit_grace_sec: float = 30.0
    transient_retry_max_attempts: int = 3
    transient_retry_initial_backoff_sec: float = 1.0
    transient_retry_max_backoff_sec: float = 10.0
    backlog_not_ready_threshold: int = 10
    backlog_fatal_threshold: int = 1000
    maximum_stage_event_history: int = 256
    attempt_audit_max_bytes: int = 1_048_576
    attempt_audit_backup_count: int = 2
    fail_stop_on_unrecoverable_stall: bool = True
    controlled_stage_delay_enabled: bool = False
    controlled_stage_delay_stage: str = ""
    controlled_stage_delay_sec: float = 0.0
    controlled_collection_fault_enabled: bool = False
    controlled_transient_empty_attempts: int = 0

    @classmethod
    def from_dict(cls, payload: dict) -> "LiveLivenessConfig":
        result = cls(**_strict_dict(
            payload,
            set(cls.__dataclass_fields__),
            "live_liveness",
        ))
        result.validate()
        return result

    def validate(self) -> None:
        timeout_names = (
            "freeze_revision_timeout_sec",
            "topology_build_timeout_sec",
            "call_edge_collection_timeout_sec",
            "node_metric_collection_timeout_sec",
            "edge_metric_collection_timeout_sec",
            "record_adaptation_timeout_sec",
            "engine_process_timeout_sec",
            "generation_prepare_timeout_sec",
            "run_state_commit_timeout_sec",
            "output_projection_timeout_sec",
            "retention_timeout_sec",
            "progress_timeout_sec",
            "watchdog_poll_interval_sec",
            "watchdog_dump_grace_sec",
            "watchdog_exit_grace_sec",
            "transient_retry_initial_backoff_sec",
            "transient_retry_max_backoff_sec",
        )
        for name in timeout_names:
            if _finite(f"live_liveness.{name}", getattr(self, name)) <= 0:
                raise ValueError(f"live_liveness.{name} must be positive")
        for name in (
            "transient_retry_max_attempts",
            "backlog_not_ready_threshold",
            "backlog_fatal_threshold",
            "maximum_stage_event_history",
            "attempt_audit_max_bytes",
            "attempt_audit_backup_count",
        ):
            _positive_int(f"live_liveness.{name}", getattr(self, name))
        if self.transient_retry_initial_backoff_sec > self.transient_retry_max_backoff_sec:
            raise ValueError("live liveness retry initial exceeds maximum")
        if self.watchdog_poll_interval_sec >= self.progress_timeout_sec:
            raise ValueError("watchdog poll must be shorter than progress timeout")
        if self.watchdog_dump_grace_sec >= self.watchdog_exit_grace_sec:
            raise ValueError("watchdog dump grace must be shorter than exit grace")
        if self.backlog_fatal_threshold < self.backlog_not_ready_threshold:
            raise ValueError("fatal backlog threshold must not be lower than readiness threshold")
        if self.fail_stop_on_unrecoverable_stall is not True:
            raise ValueError("production live liveness requires fail-stop")
        if type(self.controlled_stage_delay_enabled) is not bool:
            raise TypeError("controlled stage delay enabled must be boolean")
        if self.controlled_stage_delay_enabled:
            allowed = {item.value for item in self.stage_timeouts()}
            if self.controlled_stage_delay_stage not in allowed:
                raise ValueError("controlled stage delay requires a LiveStage")
            if _finite(
                "live_liveness.controlled_stage_delay_sec",
                self.controlled_stage_delay_sec,
            ) <= 0:
                raise ValueError("controlled stage delay must be positive")
        elif (
            self.controlled_stage_delay_stage != ""
            or self.controlled_stage_delay_sec != 0.0
        ):
            raise ValueError("disabled controlled stage delay must be empty")
        if type(self.controlled_collection_fault_enabled) is not bool:
            raise TypeError("controlled collection fault enabled must be boolean")
        if self.controlled_collection_fault_enabled:
            _positive_int(
                "live_liveness.controlled_transient_empty_attempts",
                self.controlled_transient_empty_attempts,
            )
            if (self.controlled_transient_empty_attempts
                    >= self.transient_retry_max_attempts):
                raise ValueError(
                    "controlled transient empty attempts must permit recovery",
                )
        elif self.controlled_transient_empty_attempts != 0:
            raise ValueError(
                "disabled controlled collection fault must have zero attempts",
            )

    def stage_timeouts(self) -> dict:
        from proberca.live.progress import LiveStage

        return {
            LiveStage.BEGIN_WINDOW: self.record_adaptation_timeout_sec,
            LiveStage.FREEZE_REVISION: self.freeze_revision_timeout_sec,
            LiveStage.BUILD_TOPOLOGY: self.topology_build_timeout_sec,
            LiveStage.COLLECT_CALL_EDGES: self.call_edge_collection_timeout_sec,
            LiveStage.COLLECT_NODE_METRICS: self.node_metric_collection_timeout_sec,
            LiveStage.COLLECT_EDGE_METRICS: self.edge_metric_collection_timeout_sec,
            LiveStage.ADAPT_RECORDS: self.record_adaptation_timeout_sec,
            LiveStage.ADAPT_NODE_RECORDS: self.record_adaptation_timeout_sec,
            LiveStage.ADAPT_EDGE_RECORDS: self.record_adaptation_timeout_sec,
            LiveStage.BUILD_ENGINE_INPUT: self.record_adaptation_timeout_sec,
            LiveStage.ENGINE_PROCESS: self.engine_process_timeout_sec,
            LiveStage.PREPARE_GENERATION: self.generation_prepare_timeout_sec,
            LiveStage.COMMIT_RUN_STATE: self.run_state_commit_timeout_sec,
            LiveStage.PROJECT_OUTPUT: self.output_projection_timeout_sec,
            LiveStage.RETENTION: self.retention_timeout_sec,
        }


@dataclass(frozen=True)
class LiveConfig:
    window_sec: int = 1
    start_alignment: str = "utc_epoch"
    collection_delay_sec: float = 1.0
    maximum_catchup_windows: int = 1
    fail_on_missed_window: bool = True
    checkpoint_every_windows: int = 1
    output_flush_every_windows: int = 1
    graceful_shutdown_timeout_sec: float = 30.0
    leader_election: bool = True
    health_bind: str = "127.0.0.1:8080"
    metrics_bind: str = "127.0.0.1:9090"
    topology_required: bool = True
    prometheus_required: bool = True
    call_edge_required: bool = True
    normalized_evidence_required: bool = False
    no_evidence_is_degraded_not_failed: bool = True

    @classmethod
    def from_dict(cls, payload: dict) -> "LiveConfig":
        result = cls(**_strict_dict(payload, set(cls.__dataclass_fields__), "live"))
        result.validate()
        return result

    def validate(self, engine_window_sec: int | None = None) -> None:
        _positive_int("live.window_sec", self.window_sec)
        if engine_window_sec is not None and self.window_sec != engine_window_sec:
            raise ValueError("live.window_sec must match ProbeRCA window_sec")
        if self.start_alignment != "utc_epoch":
            raise ValueError("live start_alignment must be utc_epoch")
        if self.collection_delay_sec < 0 or self.graceful_shutdown_timeout_sec <= 0:
            raise ValueError("live delay/timeout is invalid")
        for name in ("maximum_catchup_windows", "checkpoint_every_windows",
                     "output_flush_every_windows"):
            _positive_int(f"live.{name}", getattr(self, name))
        if not self.topology_required:
            raise ValueError("live cannot skip topology/Engine stages")


@dataclass(frozen=True)
class RetentionConfig:
    checkpoint_generations: int = 2
    checkpoint_min_age_sec: float = 60.0
    report_retention_days: int = 30
    failure_retention_days: int = 30

    @classmethod
    def from_dict(cls, payload: dict) -> "RetentionConfig":
        result = cls(**_strict_dict(payload, set(cls.__dataclass_fields__), "retention"))
        result.validate()
        return result

    def validate(self) -> None:
        if self.checkpoint_generations < 2:
            raise ValueError("retention must preserve current and previous checkpoint")
        if self.checkpoint_min_age_sec < 0:
            raise ValueError("checkpoint_min_age_sec must be non-negative")
        _positive_int("report_retention_days", self.report_retention_days)
        _positive_int("failure_retention_days", self.failure_retention_days)


@dataclass(frozen=True)
class ProbeRCAConfig:
    window_sec: int
    healthy_history_sec: int
    alert: AlertConfig
    propagation: PropagationConfig
    candidate_graph: CandidateGraphConfig
    burst: BurstConfig
    solver: SolverConfig
    confidence: ConfidenceConfig
    shock_templates: dict[str, ShockTemplateConfig]
    impact_derivation_rules: list[ImpactDerivationRule] = field(default_factory=list)
    rca_metric_families: list[str] = field(default_factory=lambda: sorted(NODE_METRIC_FAMILIES))
    residual: ResidualConfig = field(default_factory=ResidualConfig)
    propagation_dictionary: PropagationDictionaryConfig = field(default_factory=PropagationDictionaryConfig)
    shock_projection_templates: list[ShockProjectionTemplate] = field(default_factory=list)
    evidence: EvidenceConfig = field(default_factory=EvidenceConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    penalties: PenaltyConfig = field(default_factory=PenaltyConfig)
    diagnosis: DiagnosisConfig = field(default_factory=DiagnosisConfig)
    orchestration: OrchestrationConfig = field(default_factory=OrchestrationConfig)
    replay: ReplayConfig = field(default_factory=ReplayConfig)
    aggregation_specs: dict[str, MetricAggregationSpec] = field(default_factory=dict)
    metric_signal_specs: list[MetricSignalSpec] = field(default_factory=list)
    baseline: BaselineConfig = field(default_factory=lambda: BaselineConfig(600, 300, 0.001, 6.0))
    score: ScoreConfig = field(default_factory=lambda: ScoreConfig(
        {"request": 0.4, "cpu": 0.12, "memory": 0.12, "io": 0.12,
         "net_local": 0.12, "lock": 0.12}, False, 1.0, 1.0,
    ))
    alert_state: AlertStateConfig | None = None
    composite_alert_rules: list[CompositeAlertRule] = field(default_factory=list)
    kubernetes: KubernetesConfig | None = None
    prometheus: PrometheusConfig | None = None
    live: LiveConfig | None = None
    live_liveness: LiveLivenessConfig = field(default_factory=LiveLivenessConfig)
    leader_election: LeaderElectionConfig | None = None
    retention: RetentionConfig | None = None

    @classmethod
    def from_dict(cls, payload: dict) -> "ProbeRCAConfig":
        required = {
            "window_sec",
            "healthy_history_sec",
            "alert",
            "propagation",
            "candidate_graph",
            "burst",
            "solver",
            "confidence",
            "shock_templates",
        }
        optional = {
            "impact_derivation_rules", "rca_metric_families", "residual",
            "propagation_dictionary", "shock_projection_templates",
            "evidence", "quality", "penalties", "diagnosis", "orchestration", "replay",
            "aggregation_specs", "metric_signal_specs", "baseline", "score",
            "alert_state", "composite_alert_rules",
            "kubernetes", "prometheus", "live", "live_liveness", "leader_election", "retention",
        }
        if not isinstance(payload, dict):
            raise TypeError("ProbeRCAConfig must be a dictionary")
        unknown = sorted(set(payload) - required - optional)
        missing = sorted(required - set(payload))
        if unknown or missing:
            raise ValueError(f"ProbeRCAConfig invalid fields; unknown={unknown}, missing={missing}")
        values = dict(payload)
        values.setdefault("impact_derivation_rules", [])
        values.setdefault("rca_metric_families", sorted(NODE_METRIC_FAMILIES))
        values.setdefault("residual", asdict(ResidualConfig()))
        values.setdefault("propagation_dictionary", asdict(PropagationDictionaryConfig()))
        values.setdefault("shock_projection_templates", [])
        values.setdefault("evidence", asdict(EvidenceConfig()))
        values.setdefault("quality", asdict(QualityConfig()))
        values.setdefault("penalties", asdict(PenaltyConfig()))
        values.setdefault("diagnosis", asdict(DiagnosisConfig()))
        values.setdefault("orchestration", asdict(OrchestrationConfig()))
        values.setdefault("replay", asdict(ReplayConfig()))
        values.setdefault("aggregation_specs", {})
        values.setdefault("metric_signal_specs", [])
        values.setdefault("baseline", asdict(BaselineConfig(600, 300, 0.001, 6.0)))
        values.setdefault("score", asdict(ScoreConfig(
            {"request": 0.4, "cpu": 0.12, "memory": 0.12, "io": 0.12,
             "net_local": 0.12, "lock": 0.12}, False, 1.0, 1.0,
        )))
        values.setdefault("alert_state", None)
        values.setdefault("composite_alert_rules", [])
        values.setdefault("kubernetes", None)
        values.setdefault("prometheus", None)
        values.setdefault("live", None)
        values.setdefault("live_liveness", asdict(LiveLivenessConfig()))
        values.setdefault("leader_election", None)
        values.setdefault("retention", None)
        if not isinstance(values["aggregation_specs"], dict):
            raise TypeError("aggregation_specs must map stable output IDs to specs")
        if not isinstance(values["metric_signal_specs"], list):
            raise TypeError("metric_signal_specs must be a list")
        if not isinstance(values["composite_alert_rules"], list):
            raise TypeError("composite_alert_rules must be a list")
        _positive_int("window_sec", values["window_sec"])
        _positive_int("healthy_history_sec", values["healthy_history_sec"])
        templates_payload = values["shock_templates"]
        if not isinstance(templates_payload, dict) or not templates_payload:
            raise ValueError("shock_templates must be a non-empty dictionary")
        templates: dict[str, ShockTemplateConfig] = {}
        for metric_name, template in templates_payload.items():
            if not isinstance(metric_name, str) or not metric_name.strip():
                raise ValueError("shock template metric names must be non-empty strings")
            if "::" in metric_name or "->" in metric_name:
                raise ValueError("shock template keys must be edge metric types, not service-specific IDs")
            templates[metric_name] = ShockTemplateConfig.from_dict(
                template, f"shock_templates.{metric_name}"
            )
        rules_payload = values["impact_derivation_rules"]
        if not isinstance(rules_payload, list):
            raise TypeError("impact_derivation_rules must be a list")
        rules = [item if isinstance(item, ImpactDerivationRule) else ImpactDerivationRule.from_dict(item)
                 for item in rules_payload]
        if len({item.rule_id for item in rules}) != len(rules):
            raise ValueError("impact_derivation_rules contains duplicate rule_id")
        families = values["rca_metric_families"]
        if not isinstance(families, list) or not families or any(item not in NODE_METRIC_FAMILIES for item in families):
            raise ValueError("rca_metric_families must contain configured node metric families")
        if len(families) != len(set(families)):
            raise ValueError("rca_metric_families contains duplicates")
        projection_payload = values["shock_projection_templates"]
        if not isinstance(projection_payload, list):
            raise TypeError("shock_projection_templates must be a list")
        projection_templates = [
            item if isinstance(item, ShockProjectionTemplate) else ShockProjectionTemplate.from_dict(item)
            for item in projection_payload
        ]
        if len({item.template_id for item in projection_templates}) != len(projection_templates):
            raise ValueError("shock_projection_templates contains duplicate template_id")
        return cls(
            window_sec=values["window_sec"],
            healthy_history_sec=values["healthy_history_sec"],
            alert=AlertConfig.from_dict(values["alert"]),
            propagation=PropagationConfig.from_dict(values["propagation"]),
            candidate_graph=CandidateGraphConfig.from_dict(values["candidate_graph"]),
            burst=BurstConfig.from_dict(values["burst"]),
            solver=SolverConfig.from_dict(values["solver"]),
            confidence=ConfidenceConfig.from_dict(values["confidence"]),
            shock_templates=templates,
            impact_derivation_rules=rules,
            rca_metric_families=list(families),
            residual=(values["residual"] if isinstance(values["residual"], ResidualConfig)
                      else ResidualConfig.from_dict(values["residual"])),
            propagation_dictionary=(
                values["propagation_dictionary"]
                if isinstance(values["propagation_dictionary"], PropagationDictionaryConfig)
                else PropagationDictionaryConfig.from_dict(values["propagation_dictionary"])
            ),
            shock_projection_templates=projection_templates,
            evidence=(values["evidence"] if isinstance(values["evidence"], EvidenceConfig)
                      else EvidenceConfig.from_dict(values["evidence"])),
            quality=(values["quality"] if isinstance(values["quality"], QualityConfig)
                     else QualityConfig.from_dict(values["quality"])),
            penalties=(values["penalties"] if isinstance(values["penalties"], PenaltyConfig)
                       else PenaltyConfig.from_dict(values["penalties"])),
            diagnosis=(values["diagnosis"] if isinstance(values["diagnosis"], DiagnosisConfig)
                       else DiagnosisConfig.from_dict(values["diagnosis"])),
            orchestration=(
                values["orchestration"] if isinstance(values["orchestration"], OrchestrationConfig)
                else OrchestrationConfig.from_dict(values["orchestration"])
            ),
            replay=(values["replay"] if isinstance(values["replay"], ReplayConfig)
                    else ReplayConfig.from_dict(values["replay"])),
            aggregation_specs={
                output_id: (item if isinstance(item, MetricAggregationSpec)
                            else MetricAggregationSpec.from_dict(item))
                for output_id, item in values["aggregation_specs"].items()
            },
            metric_signal_specs=[
                item if isinstance(item, MetricSignalSpec) else MetricSignalSpec.from_dict(item)
                for item in values["metric_signal_specs"]
            ],
            baseline=(values["baseline"] if isinstance(values["baseline"], BaselineConfig)
                      else BaselineConfig.from_dict(values["baseline"])),
            score=(values["score"] if isinstance(values["score"], ScoreConfig)
                   else ScoreConfig.from_dict(values["score"])),
            alert_state=(
                values["alert_state"] if isinstance(values["alert_state"], AlertStateConfig)
                else (AlertStateConfig.from_dict(values["alert_state"])
                      if values["alert_state"] is not None else None)
            ),
            composite_alert_rules=[
                item if isinstance(item, CompositeAlertRule) else CompositeAlertRule.from_dict(item)
                for item in values["composite_alert_rules"]
            ],
            kubernetes=(
                values["kubernetes"] if isinstance(values["kubernetes"], KubernetesConfig)
                else (KubernetesConfig.from_dict(values["kubernetes"])
                      if values["kubernetes"] is not None else None)
            ),
            prometheus=(
                values["prometheus"] if isinstance(values["prometheus"], PrometheusConfig)
                else (PrometheusConfig.from_dict(values["prometheus"])
                      if values["prometheus"] is not None else None)
            ),
            live=(
                values["live"] if isinstance(values["live"], LiveConfig)
                else (LiveConfig.from_dict(values["live"])
                      if values["live"] is not None else None)
            ),
            live_liveness=(
                values["live_liveness"]
                if isinstance(values["live_liveness"], LiveLivenessConfig)
                else LiveLivenessConfig.from_dict(values["live_liveness"])
            ),
            leader_election=(
                values["leader_election"]
                if isinstance(values["leader_election"], LeaderElectionConfig)
                else (LeaderElectionConfig.from_dict(values["leader_election"])
                      if values["leader_election"] is not None else None)
            ),
            retention=(
                values["retention"] if isinstance(values["retention"], RetentionConfig)
                else (RetentionConfig.from_dict(values["retention"])
                      if values["retention"] is not None else None)
            ),
        )

    def to_dict(self) -> dict:
        return asdict(self)


def dump_config_yaml(path: str | Path, config: ProbeRCAConfig) -> None:
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config.to_dict(), handle, sort_keys=False, allow_unicode=True)


def load_config_yaml(path: str | Path) -> ProbeRCAConfig:
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"configuration file not found: {input_path}")
    with input_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if payload is None:
        raise ValueError("configuration file must not be empty")
    return ProbeRCAConfig.from_dict(payload)

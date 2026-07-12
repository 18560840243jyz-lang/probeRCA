"""Strict ProbeRCA-BPF configuration contract using the existing YAML dependency."""

from __future__ import annotations

import math
from dataclasses import MISSING, asdict, dataclass, field
from pathlib import Path
from typing import Any

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
            "evidence", "quality", "penalties", "diagnosis",
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

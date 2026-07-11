"""Strict ProbeRCA-BPF configuration contract using the existing YAML dependency."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
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
class PropagationConfig:
    service_lags: list[int]
    metric_lags: list[int]
    rls_forgetting_factor: float
    metric_ridge: float

    @classmethod
    def from_dict(cls, payload: dict) -> "PropagationConfig":
        values = _strict_dict(payload, {field.name for field in cls.__dataclass_fields__.values()}, "propagation")
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
        return result


@dataclass(frozen=True)
class CandidateGraphConfig:
    upstream_hops: int
    downstream_hops: int
    include_cohost: bool
    include_shared_resource: bool

    @classmethod
    def from_dict(cls, payload: dict) -> "CandidateGraphConfig":
        values = _strict_dict(payload, {field.name for field in cls.__dataclass_fields__.values()}, "candidate_graph")
        result = cls(**values)
        _positive_int("candidate_graph.upstream_hops", result.upstream_hops)
        _positive_int("candidate_graph.downstream_hops", result.downstream_hops)
        for name in ("include_cohost", "include_shared_resource"):
            if not isinstance(getattr(result, name), bool):
                raise TypeError(f"candidate_graph.{name} must be a boolean")
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

    @classmethod
    def from_dict(cls, payload: dict) -> "SolverConfig":
        values = _strict_dict(payload, {"method", "max_iterations", "tolerance"}, "solver")
        result = cls(**values)
        if result.method != "fista":
            raise ValueError("solver.method must be 'fista'; fallback is not allowed")
        _positive_int("solver.max_iterations", result.max_iterations)
        tolerance = _finite("solver.tolerance", result.tolerance)
        if tolerance <= 0.0:
            raise ValueError("solver.tolerance must be positive")
        object.__setattr__(result, "tolerance", tolerance)
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
        values = _strict_dict(payload, required, "ProbeRCAConfig")
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

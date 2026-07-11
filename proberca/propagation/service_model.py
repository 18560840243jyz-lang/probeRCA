"""Strict service-level propagation result contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass


def _required_string(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _finite(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _string_list(name: str, value: object, *, nonempty: bool = False) -> None:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise TypeError(f"{name} must be a list of non-empty strings")
    if nonempty and not value:
        raise ValueError(f"{name} must not be empty")
    if value != sorted(set(value)):
        raise ValueError(f"{name} must be sorted and unique")


@dataclass(frozen=True, order=True)
class ServiceFeatureKey:
    parent_service_id: str
    lag: int

    def __post_init__(self) -> None:
        _required_string("parent_service_id", self.parent_service_id)
        if isinstance(self.lag, bool) or not isinstance(self.lag, int) or self.lag <= 0:
            raise ValueError("lag must be a positive integer")


@dataclass(frozen=True)
class ServicePropagationContribution:
    parent_service_id: str
    target_service_id: str
    lag: int
    coefficient: float
    parent_value: float
    contribution_value: float
    positive_support: float
    relation_ids: list[str]
    relation_types: list[str]

    def __post_init__(self) -> None:
        _required_string("parent_service_id", self.parent_service_id)
        _required_string("target_service_id", self.target_service_id)
        if isinstance(self.lag, bool) or not isinstance(self.lag, int) or self.lag <= 0:
            raise ValueError("lag must be a positive integer")
        coefficient = _finite("coefficient", self.coefficient)
        parent_value = _finite("parent_value", self.parent_value)
        contribution = _finite("contribution_value", self.contribution_value)
        support = _finite("positive_support", self.positive_support)
        if not math.isclose(contribution, coefficient * parent_value, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError("contribution_value must equal coefficient * parent_value")
        if not math.isclose(support, max(coefficient, 0.0), rel_tol=0.0, abs_tol=1e-12):
            raise ValueError("positive_support must equal max(coefficient, 0)")
        _string_list("relation_ids", self.relation_ids, nonempty=True)
        _string_list("relation_types", self.relation_types, nonempty=True)
        if not set(self.relation_types) <= {"self", "impact", "host", "resource"}:
            raise ValueError("relation_types contains a relation outside the service parent mask")


@dataclass(frozen=True)
class ServicePropagationPrediction:
    schema_version: str
    record_type: str
    timestamp_ns: int
    cluster_id: str
    namespace_scope: list[str]
    topology_snapshot_id: str
    model_snapshot_id: str
    target_service_id: str
    predicted_value: float
    actual_value: float | None
    prediction_error: float | None
    ready: bool
    frozen: bool
    updated: bool
    skipped_reason: str | None
    gate_state: str
    observation_quality: float | None
    contributions: list[ServicePropagationContribution]
    config_fingerprint: str

    def __post_init__(self) -> None:
        if self.schema_version != "1.0" or self.record_type != "service_propagation_prediction":
            raise ValueError("invalid service propagation prediction version or record_type")
        if isinstance(self.timestamp_ns, bool) or not isinstance(self.timestamp_ns, int) or self.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be a non-negative integer")
        for name in ("cluster_id", "topology_snapshot_id", "model_snapshot_id", "target_service_id"):
            _required_string(name, getattr(self, name))
        _string_list("namespace_scope", self.namespace_scope, nonempty=True)
        predicted = _finite("predicted_value", self.predicted_value)
        if any(not isinstance(value, bool) for value in (self.ready, self.frozen, self.updated)):
            raise TypeError("ready, frozen, and updated must be boolean")
        if self.frozen and self.updated:
            raise ValueError("a frozen prediction cannot update the model")
        allowed_skips = {
            None, "update_gate_closed", "baseline_not_ready", "insufficient_history",
            "missing_target", "missing_parent_state", "low_observation_quality",
            "topology_unavailable", "model_reconfigured", "numerical_error", "history_gap",
        }
        if self.skipped_reason not in allowed_skips:
            raise ValueError("invalid skipped_reason")
        if self.gate_state not in {"healthy", "soft", "hard", "recovery", "edge_anomaly"}:
            raise ValueError("invalid gate_state")
        if self.observation_quality is not None:
            quality = _finite("observation_quality", self.observation_quality)
            if not 0.0 <= quality <= 1.0:
                raise ValueError("observation_quality must be in [0, 1]")
        if not isinstance(self.contributions, list) or any(
            not isinstance(item, ServicePropagationContribution) for item in self.contributions
        ):
            raise TypeError("contributions must contain ServicePropagationContribution")
        if any(item.target_service_id != self.target_service_id for item in self.contributions):
            raise ValueError("contribution target does not match prediction target")
        if self.contributions != sorted(
            self.contributions, key=lambda item: (item.parent_service_id, item.lag)
        ):
            raise ValueError("contributions must use deterministic parent and lag order")
        total = sum(item.contribution_value for item in self.contributions)
        if not math.isclose(predicted, total, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError("predicted_value must equal the sum of contributions")
        if self.actual_value is None:
            if self.prediction_error is not None:
                raise ValueError("prediction_error requires actual_value")
        else:
            actual = _finite("actual_value", self.actual_value)
            error = _finite("prediction_error", self.prediction_error)
            if not math.isclose(error, actual - predicted, rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError("prediction_error must equal actual_value - predicted_value")
        if len(self.config_fingerprint) != 64 or any(character not in "0123456789abcdef"
                                                     for character in self.config_fingerprint):
            raise ValueError("config_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True)
class ServicePropagationCoefficient:
    target_service_id: str
    parent_service_id: str
    lag: int
    coefficient: float
    positive_support: float
    relation_ids: list[str]
    relation_types: list[str]
    update_count: int
    ready: bool

    def __post_init__(self) -> None:
        contribution = ServicePropagationContribution(
            self.parent_service_id, self.target_service_id, self.lag,
            self.coefficient, 1.0, self.coefficient, self.positive_support,
            self.relation_ids, self.relation_types,
        )
        if isinstance(self.update_count, bool) or not isinstance(self.update_count, int) or self.update_count < 0:
            raise ValueError("update_count must be a non-negative integer")
        if not isinstance(self.ready, bool):
            raise TypeError("ready must be boolean")


@dataclass(frozen=True)
class PropagationIssue:
    target_service_id: str
    reason_code: str
    detail: str

    def __post_init__(self) -> None:
        for name in ("target_service_id", "reason_code", "detail"):
            _required_string(name, getattr(self, name))


@dataclass(frozen=True)
class ServicePropagationWindowResult:
    timestamp_ns: int
    predictions: list[ServicePropagationPrediction]
    issues: list[PropagationIssue]
    topology_reconfigured: bool
    reordered: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.timestamp_ns, bool) or not isinstance(self.timestamp_ns, int) or self.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be a non-negative integer")
        if any(not isinstance(item, ServicePropagationPrediction) for item in self.predictions):
            raise TypeError("predictions contains an invalid item")
        if any(not isinstance(item, PropagationIssue) for item in self.issues):
            raise TypeError("issues contains an invalid item")
        if any(not isinstance(value, bool) for value in (self.topology_reconfigured, self.reordered)):
            raise TypeError("result flags must be boolean")

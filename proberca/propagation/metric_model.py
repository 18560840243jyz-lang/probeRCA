"""Strict metric propagation model and output contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from .metric_rules import MetricFeatureKey


@dataclass(frozen=True)
class MetricTrainingMatrixInfo:
    target_node_id: str
    feature_keys: list[MetricFeatureKey]
    row_timestamps: list[int]
    excluded_row_counts: dict[str, int]
    effective_training_rows: int
    training_start_ns: int | None
    training_end_ns: int | None


@dataclass(frozen=True)
class MetricPropagationContribution:
    target_node_id: str
    parent_node_id: str
    lag: int
    coefficient: float
    parent_value: float
    contribution_value: float
    positive_support: float
    relation_type: str
    relation_types: list[str]
    relation_ids: list[str]
    rule_ids: list[str]

    def __post_init__(self) -> None:
        values = (self.coefficient, self.parent_value, self.contribution_value,
                  self.positive_support)
        if any(not math.isfinite(value) for value in values):
            raise ValueError("metric contribution values must be finite")
        if not math.isclose(self.contribution_value, self.coefficient * self.parent_value,
                            rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError("metric contribution must equal coefficient * parent_value")
        if self.positive_support != max(self.coefficient, 0.0):
            raise ValueError("positive_support must equal max(coefficient, 0)")
        if not self.relation_types or self.relation_type != sorted(self.relation_types)[0]:
            raise ValueError("relation_type must be the deterministic primary relation")


@dataclass(frozen=True)
class MetricPropagationPrediction:
    schema_version: str
    record_type: str
    timestamp_ns: int
    alert_id: str
    candidate_id: str
    topology_snapshot_id: str
    model_snapshot_id: str
    target_node_id: str
    predicted_value: float | None
    actual_value: float | None
    ready: bool
    frozen: bool
    provisional: bool
    available: bool
    unavailable_reason: str | None
    observation_quality: float | None
    contributions: list[MetricPropagationContribution]
    config_fingerprint: str

    def __post_init__(self) -> None:
        if self.schema_version != "1.0" or self.record_type != "metric_propagation_prediction":
            raise ValueError("invalid metric propagation prediction type")
        if self.available:
            if self.predicted_value is None or not math.isfinite(self.predicted_value):
                raise ValueError("available metric prediction requires a finite value")
            total = sum(item.contribution_value for item in self.contributions)
            if not math.isclose(self.predicted_value, total, rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError("metric prediction must equal sum(contributions)")
            if self.unavailable_reason is not None:
                raise ValueError("available prediction cannot have unavailable_reason")
        elif self.predicted_value is not None or self.unavailable_reason != "missing_prediction_feature":
            raise ValueError("unavailable metric prediction must identify missing_prediction_feature")
        if self.actual_value is not None and not math.isfinite(self.actual_value):
            raise ValueError("actual_value must be finite when present")


@dataclass(frozen=True)
class MetricPropagationCoefficient:
    target_node_id: str
    parent_node_id: str
    lag: int
    coefficient: float
    positive_support: float
    relation_types: list[str]
    relation_ids: list[str]
    rule_ids: list[str]
    effective_training_rows: int
    condition_number: float
    ready: bool


@dataclass(frozen=True)
class MetricPropagationModelInfo:
    model_snapshot_id: str
    candidate_id: str
    alert_id: str
    lifecycle_state: str
    global_ready: bool
    frozen: bool
    training_start_ns: int | None
    training_end_ns: int | None
    healthy_history_cutoff_ns: int | None
    target_count: int
    ready_target_count: int
    unready_targets: list[str]
    candidate_fingerprint: str
    topology_fingerprint: str
    rules_fingerprint: str
    config_fingerprint: str
    node_index_fingerprint: str
    fit_duration_ms: float
    quality_issues: list[dict[str, object]]


@dataclass(frozen=True)
class MetricPropagationIssue:
    reason_code: str
    target_node_id: str | None
    detail: str


@dataclass(frozen=True)
class MetricPropagationPrepareResult:
    info: MetricPropagationModelInfo
    issues: list[MetricPropagationIssue]
    cache_hit: bool


@dataclass(frozen=True)
class MetricWindowProcessResult:
    timestamp_ns: int
    predictions: list[MetricPropagationPrediction]
    training_result: object
    runtime_result: object
    lifecycle_result: MetricPropagationPrepareResult | None
    reordered: bool = False


@dataclass
class MetricTargetModel:
    target_node_id: str
    feature_keys: list[MetricFeatureKey]
    coefficients: np.ndarray
    condition_number: float
    matrix_info: MetricTrainingMatrixInfo
    ready: bool


@dataclass
class MetricModelBundle:
    cache_key: str
    info: MetricPropagationModelInfo
    topology_snapshot_id: str
    node_ids: list[str]
    targets: dict[str, MetricTargetModel]

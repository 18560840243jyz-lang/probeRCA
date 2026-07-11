"""Health-gated robust baselines for P1 node and edge metric records."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median

from proberca.aggregation import AggregationIssue
from proberca.config import BaselineConfig, MetricSignalSpec, ScoreConfig
from proberca.data.schema import PROBERCA_SCHEMA_VERSION, EdgeMetricRecord, NodeMetricRecord

Metric = NodeMetricRecord | EdgeMetricRecord
BASELINE_STATE_VERSION = "1"


class AmbiguousSignalSpecError(ValueError):
    """Raised when one metric matches more than one exact signal spec."""


@dataclass(frozen=True)
class BaselineStats:
    center: float
    mad: float
    scale: float
    count: int


@dataclass(frozen=True)
class AnomalyScore:
    stable_id: str
    record_type: str
    service_id: str | None
    edge_id: str | None
    metric_family: str | None
    metric_name: str
    transformed_value: float
    signed_z: float
    anomaly: float
    direct_hard: bool
    coverage: float
    event_loss_rate: float


@dataclass(frozen=True)
class BaselineScoreResult:
    score: AnomalyScore | None
    issues: list[AggregationIssue]


@dataclass(frozen=True)
class ServiceState:
    service_id: str
    score: float | None
    family_scores: dict[str, float]
    family_coverage: dict[str, bool]
    missing_families: list[str]


@dataclass(frozen=True)
class EdgeState:
    edge_id: str
    score: float


@dataclass(frozen=True)
class StateScores:
    services: dict[str, ServiceState]
    edges: dict[str, EdgeState]
    global_anomaly: float
    issues: list[AggregationIssue]


def _transform(value: float, transform: str) -> float:
    if transform == "identity":
        return float(value)
    if value <= -1:
        raise ValueError("log1p signal input must be greater than -1")
    return math.log1p(value)


def _validate_record_spec(record: Metric, spec: MetricSignalSpec) -> None:
    if record.record_type != spec.record_type or record.metric_name != spec.metric_name:
        raise ValueError("metric record conflicts with MetricSignalSpec")
    if isinstance(record, NodeMetricRecord):
        if record.metric_family != spec.metric_family or spec.protocol is not None:
            raise ValueError("node metric signal selector mismatch")
    elif spec.metric_family is not None or (spec.protocol is not None and spec.protocol != record.protocol):
        raise ValueError("edge metric signal selector mismatch")
    if record.stable_id != spec.aggregation_output_id:
        raise ValueError("metric stable ID conflicts with aggregation_output_id")


class MetricSignalRegistry:
    """Resolve signal semantics only by exact configured fields."""

    def __init__(self, specs: list[MetricSignalSpec]):
        if not isinstance(specs, list) or any(not isinstance(spec, MetricSignalSpec) for spec in specs):
            raise TypeError("signal registry requires MetricSignalSpec entries")
        self.specs = list(specs)

    @staticmethod
    def _matches(spec: MetricSignalSpec, record: Metric) -> bool:
        if spec.record_type != record.record_type or spec.metric_name != record.metric_name:
            return False
        if isinstance(record, NodeMetricRecord):
            return spec.metric_family == record.metric_family and spec.protocol is None
        return spec.metric_family is None and (spec.protocol is None or spec.protocol == record.protocol)

    def resolve(self, record: Metric) -> MetricSignalSpec | None:
        matches = [spec for spec in self.specs if self._matches(spec, record)]
        if len(matches) > 1:
            raise AmbiguousSignalSpecError(f"multiple signal specs match {record.stable_id}")
        return matches[0] if matches else None

    def resolve_with_issues(self, record: Metric, start_ns: int, end_ns: int):
        try:
            result = self.resolve(record)
        except AmbiguousSignalSpecError:
            issue = AggregationIssue(record.stable_id, start_ns, end_ns, "ambiguous_signal_spec", {})
            raise AmbiguousSignalSpecError(str(issue))
        if result is None:
            return None, [AggregationIssue(record.stable_id, start_ns, end_ns, "unconfigured_metric", {})]
        return result, []


class RobustBaselineStore:
    """Per-stable-ID healthy-only ring buffers with median/MAD scoring."""

    def __init__(self, config: BaselineConfig, window_sec: int):
        if not isinstance(config, BaselineConfig):
            raise TypeError("config must be BaselineConfig")
        if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
            raise ValueError("window_sec must be a positive integer")
        self.config = config
        self.window_sec = window_sec
        self.capacity = max(1, config.healthy_history_sec // window_sec)
        if config.min_healthy_windows > self.capacity:
            raise ValueError("min_healthy_windows exceeds baseline ring capacity")
        self._buffers: dict[str, list[tuple[int, float]]] = {}

    def update(self, record: Metric, spec: MetricSignalSpec, *, state: str,
               frozen_ids: set[str] | None = None) -> bool:
        _validate_record_spec(record, spec)
        if state not in {"healthy", "edge_anomaly"}:
            return False
        if state == "edge_anomaly" and isinstance(record, NodeMetricRecord):
            allowed = True
        else:
            allowed = record.stable_id not in (frozen_ids or set())
        if not allowed:
            return False
        value = _transform(record.value, spec.transform)
        buffer = self._buffers.setdefault(record.stable_id, [])
        if buffer and record.timestamp_ns <= buffer[-1][0]:
            raise ValueError("healthy baseline timestamps must increase")
        buffer.append((record.timestamp_ns, value))
        if len(buffer) > self.capacity:
            del buffer[:-self.capacity]
        return True

    def is_ready(self, stable_id: str) -> bool:
        return len(self._buffers.get(stable_id, [])) >= self.config.min_healthy_windows

    def stats(self, stable_id: str) -> BaselineStats:
        values = [value for _, value in self._buffers.get(stable_id, [])]
        if not values:
            raise KeyError(f"no baseline for {stable_id!r}")
        center = float(median(values))
        mad = float(median([abs(value - center) for value in values]))
        return BaselineStats(center, mad, max(1.4826 * mad, self.config.min_scale), len(values))

    def score(self, record: Metric, spec: MetricSignalSpec, start_ns: int, end_ns: int) -> BaselineScoreResult:
        _validate_record_spec(record, spec)
        if not self.is_ready(record.stable_id):
            return BaselineScoreResult(None, [AggregationIssue(record.stable_id, start_ns, end_ns,
                                                                 "baseline_not_ready", {"count": len(self._buffers.get(record.stable_id, []))})])
        transformed = _transform(record.value, spec.transform)
        stats = self.stats(record.stable_id)
        signed = ((transformed - stats.center) if spec.polarity == "increase_bad"
                  else (stats.center - transformed)) / stats.scale
        cap = min(spec.z_cap, self.config.z_cap)
        anomaly = max(0.0, min(float(signed), cap))
        rare_triggered = spec.rare_event_threshold is not None and (
            record.value >= spec.rare_event_threshold if spec.polarity == "increase_bad"
            else record.value <= spec.rare_event_threshold
        )
        if rare_triggered:
            anomaly = cap
        if isinstance(record, NodeMetricRecord):
            service_id = f"{record.cluster_id}::{record.namespace}::{record.service_name}"
            edge_id = None
            family = record.metric_family
        else:
            service_id = None
            edge_id = f"{record.cluster_id}::{record.namespace}::{record.src_service}->{record.dst_service}::{record.protocol}"
            family = None
        return BaselineScoreResult(AnomalyScore(
            stable_id=record.stable_id,
            record_type=record.record_type,
            service_id=service_id,
            edge_id=edge_id,
            metric_family=family,
            metric_name=record.metric_name,
            transformed_value=transformed,
            signed_z=float(signed),
            anomaly=anomaly,
            direct_hard=bool(spec.direct_hard and rare_triggered),
            coverage=record.coverage,
            event_loss_rate=record.event_loss_rate,
        ), [])

    def to_dict(self) -> dict:
        return {
            "format_version": BASELINE_STATE_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "window_sec": self.window_sec,
            "config": asdict(self.config),
            "buffers": {key: [[timestamp, value] for timestamp, value in values]
                        for key, values in sorted(self._buffers.items())},
        }

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path, config: BaselineConfig, window_sec: int) -> "RobustBaselineStore":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if set(payload) != {"format_version", "schema_version", "window_sec", "config", "buffers"}:
            raise ValueError("invalid baseline snapshot fields")
        if payload["format_version"] != BASELINE_STATE_VERSION or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise ValueError("incompatible baseline snapshot version")
        if payload["window_sec"] != window_sec:
            raise ValueError("baseline snapshot window_sec mismatch")
        if payload["config"] != asdict(config):
            raise ValueError("baseline snapshot configuration mismatch")
        result = cls(config, window_sec)
        result._buffers = {key: [(int(item[0]), float(item[1])) for item in values]
                           for key, values in payload["buffers"].items()}
        if any(len(values) > result.capacity for values in result._buffers.values()):
            raise ValueError("baseline snapshot exceeds configured capacity")
        return result


class ScoreAggregator:
    """Compute service family maxima, service sums, edge maxima and global A_t."""

    def __init__(self, config: ScoreConfig):
        if not isinstance(config, ScoreConfig):
            raise TypeError("config must be ScoreConfig")
        self.config = config

    def aggregate(self, scores: list[AnomalyScore]) -> StateScores:
        if any(not isinstance(score, AnomalyScore) for score in scores):
            raise TypeError("ScoreAggregator requires AnomalyScore entries")
        service_metrics: dict[str, dict[str, list[float]]] = {}
        edge_metrics: dict[str, list[float]] = {}
        for score in scores:
            if score.record_type == "node_metric":
                service_metrics.setdefault(score.service_id, {}).setdefault(score.metric_family, []).append(score.anomaly)
            else:
                edge_metrics.setdefault(score.edge_id, []).append(score.anomaly)
        services: dict[str, ServiceState] = {}
        issues: list[AggregationIssue] = []
        families = set(self.config.family_weights)
        for service_id, grouped in sorted(service_metrics.items()):
            family_scores = {family: max(values) for family, values in grouped.items()}
            missing = sorted(families - set(family_scores))
            score = sum(self.config.family_weights[family] * value for family, value in family_scores.items())
            if missing and not self.config.allow_partial_families:
                score = None
                issues.append(AggregationIssue(service_id, 0, 0, "missing_family", {"families": missing}))
            services[service_id] = ServiceState(service_id, score, family_scores,
                                                 {family: family in family_scores for family in sorted(families)}, missing)
        edges = {edge_id: EdgeState(edge_id, max(values)) for edge_id, values in sorted(edge_metrics.items())}
        service_max = max((state.score for state in services.values() if state.score is not None), default=0.0)
        edge_max = max((state.score for state in edges.values()), default=0.0)
        return StateScores(services, edges, max(service_max, self.config.edge_weight * edge_max), issues)

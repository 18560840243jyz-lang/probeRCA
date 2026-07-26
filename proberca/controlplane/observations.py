"""Metric identity resolution and robust healthy baselines for the control plane."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict

from proberca.data.schema import EdgeMetricRecord, NodeMetricRecord

from .config import FinalControlConfig, MetricRoleSpec
from .model import MetricNode, NormalizedObservation


class MetricContractError(ValueError):
    """A collected metric cannot be mapped to exactly one final-scheme role."""


class RobustBaselineStore:
    def __init__(self, config: FinalControlConfig):
        self.config = config
        self._values: dict[str, list[float]] = defaultdict(list)

    @staticmethod
    def transform(value: float, spec: MetricRoleSpec) -> float:
        raw = float(value)
        if not math.isfinite(raw):
            raise MetricContractError("metric value must be finite")
        if spec.transform == "log1p":
            if raw < 0:
                raise MetricContractError("log1p metric value must be non-negative")
            return math.log1p(raw)
        return raw

    def ready(self, node_id: str) -> bool:
        return len(self._values.get(node_id, ())) >= self.config.baseline_min_windows

    def update(self, node_id: str, transformed_value: float) -> None:
        if not math.isfinite(transformed_value):
            raise MetricContractError("baseline value must be finite")
        self._values[node_id].append(float(transformed_value))

    def score(self, node_id: str, transformed_value: float, polarity: int) -> float | None:
        values = self._values.get(node_id, ())
        if len(values) < self.config.baseline_min_windows:
            return None
        median = float(statistics.median(values))
        mad = float(statistics.median(abs(value - median) for value in values))
        scale = max(1.4826 * mad, self.config.baseline_min_scale)
        return float(polarity * (transformed_value - median) / scale)

    def snapshot(self) -> dict[str, tuple[float, ...]]:
        return {key: tuple(values) for key, values in sorted(self._values.items())}


class MetricResolver:
    def __init__(self, config: FinalControlConfig):
        self.config = config

    def resolve(self, record) -> tuple[MetricNode, MetricRoleSpec]:
        record_type = getattr(record, "record_type", None)
        matches = []
        for spec in self.config.metric_roles:
            if spec.record_type != record_type or spec.metric_name != record.metric_name:
                continue
            if record.scope not in spec.scopes:
                continue
            if isinstance(record, EdgeMetricRecord) and spec.protocols \
                    and record.protocol not in spec.protocols:
                continue
            matches.append(spec)
        if len(matches) != 1:
            raise MetricContractError(
                f"metric {getattr(record, 'stable_id', '<unknown>')} matched "
                f"{len(matches)} final metric roles"
            )
        spec = matches[0]
        if isinstance(record, NodeMetricRecord):
            if spec.entity_type == "service":
                entity_id = (
                    f"{record.cluster_id}::{record.namespace}::{record.service_name}"
                )
            elif spec.entity_type == "host":
                if record.node_name is None:
                    raise MetricContractError("host metric has no node_name")
                entity_id = f"{record.cluster_id}::host::{record.node_name}"
            else:
                raise MetricContractError("node metric resolved to edge entity")
        elif isinstance(record, EdgeMetricRecord):
            entity_id = (
                f"{record.cluster_id}::{record.namespace}::"
                f"{record.src_service}->{record.dst_service}::{record.protocol}"
            )
        else:
            raise MetricContractError(
                f"unsupported collected metric type {type(record).__name__}"
            )
        metric = MetricNode(
            node_id=f"{entity_id}::{record.metric_name}",
            entity_id=entity_id,
            entity_type=spec.entity_type,
            metric_name=record.metric_name,
            role=spec.role,
            root_category=spec.root_category,
            root_eligible=spec.root_eligible,
        )
        return metric, spec

    def normalize_window(
        self, window, baseline: RobustBaselineStore,
    ) -> tuple[dict[str, NormalizedObservation], dict[str, tuple[float, MetricRoleSpec]]]:
        normalized = {}
        raw = {}
        for record in (*window.node_metrics, *window.edge_metrics):
            try:
                metric, spec = self.resolve(record)
            except MetricContractError:
                if self.config.strict_metric_contract:
                    raise
                continue
            if metric.node_id in raw:
                raise MetricContractError(
                    f"collected window has duplicate metric node {metric.node_id}"
                )
            transformed = baseline.transform(record.value, spec)
            raw[metric.node_id] = (transformed, spec)
            signed_z = baseline.score(metric.node_id, transformed, spec.polarity)
            if signed_z is None:
                continue
            quality = float(record.coverage * (1.0 - record.event_loss_rate))
            normalized[metric.node_id] = NormalizedObservation(
                metric=metric,
                signed_z=signed_z,
                anomaly=max(signed_z, 0.0),
                quality=quality,
                source_record_id=record.stable_id,
            )
        return normalized, raw

    @staticmethod
    def required_alert_nodes(
        observations: dict[str, NormalizedObservation],
    ) -> tuple[str, ...]:
        roles = {"request_latency", "request_failure", "edge_latency", "edge_failure"}
        return tuple(sorted(
            node_id for node_id, item in observations.items()
            if item.metric.role in roles
        ))

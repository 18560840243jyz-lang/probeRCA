"""Metric identity resolution and robust healthy baselines for the control plane."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from proberca.data.schema import EdgeMetricRecord, NodeMetricRecord

from .config import (
    FinalControlConfig,
    MetricRoleSpec,
    experimental_dns_metric_roles,
)
from .model import MetricNode, NormalizedObservation


class MetricContractError(ValueError):
    """A collected metric cannot be mapped to exactly one final-scheme role."""


@dataclass(frozen=True)
class BaselineScale:
    sample_count: int
    center: float
    mad: float
    iqr: float
    mad_scale: float
    iqr_scale: float
    family_floor: float
    final_scale: float
    scale_source: str


class RobustBaselineStore:
    def __init__(self, config: FinalControlConfig):
        self.config = config
        self._values: dict[str, list[float]] = defaultdict(list)
        self._specs: dict[str, MetricRoleSpec] = {}

    def reset(self) -> None:
        """Start a new healthy segment after a topology-version change."""
        self._values = defaultdict(list)
        self._specs = {}

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

    def scale(
        self, node_id: str, spec: MetricRoleSpec | None = None,
    ) -> BaselineScale | None:
        values = self._values.get(node_id, ())
        role = spec or self._specs.get(node_id)
        if len(values) < self.config.baseline_min_windows or role is None:
            return None
        floor = self.config.baseline_family_min_scales.get(
            role.scale_family
        )
        if floor is None:
            return None
        center = float(statistics.median(values))
        mad = float(statistics.median(
            abs(value - center) for value in values
        ))
        mad_scale = 1.4826 * mad
        quartiles = statistics.quantiles(
            values, n=4, method="inclusive",
        )
        iqr = float(quartiles[2] - quartiles[0])
        iqr_scale = float(iqr / 1.349)
        if mad_scale >= floor:
            final_scale, source = mad_scale, "mad"
        elif iqr_scale >= floor:
            final_scale, source = iqr_scale, "iqr"
        else:
            final_scale, source = float(floor), "family_floor"
        final_scale = max(final_scale, self.config.baseline_min_scale)
        return BaselineScale(
            sample_count=len(values),
            center=center,
            mad=mad,
            iqr=iqr,
            mad_scale=mad_scale,
            iqr_scale=iqr_scale,
            family_floor=float(floor),
            final_scale=float(final_scale),
            scale_source=source,
        )

    def ready(
        self, node_id: str, spec: MetricRoleSpec | None = None,
    ) -> bool:
        return self.scale(node_id, spec) is not None

    def status(
        self, node_id: str, spec: MetricRoleSpec,
    ) -> dict[str, Any]:
        values = self._values.get(node_id, ())
        scale = self.scale(node_id, spec)
        reason = None
        if len(values) < self.config.baseline_min_windows:
            reason = "insufficient_valid_history"
        elif self.config.baseline_family_min_scales.get(
            spec.scale_family
        ) is None:
            reason = "family_floor_not_frozen"
        return {
            "target_metric": node_id,
            "scale_family": spec.scale_family,
            "baseline_sample_count": len(values),
            "valid_healthy_samples": len(values),
            "minimum_healthy_samples": self.config.baseline_min_windows,
            "ready": scale is not None,
            "not_ready_reason": reason,
            "scale": (
                None if scale is None else {
                    "median": scale.center,
                    "mad": scale.mad,
                    "iqr": scale.iqr,
                    "center": scale.center,
                    "mad_scale": scale.mad_scale,
                    "iqr_scale": scale.iqr_scale,
                    "family_floor": scale.family_floor,
                    "final_scale": scale.final_scale,
                    "scale_source": scale.scale_source,
                }
            ),
        }

    def update(
        self, node_id: str, transformed_value: float,
        spec: MetricRoleSpec | None = None,
    ) -> None:
        if not math.isfinite(transformed_value):
            raise MetricContractError("baseline value must be finite")
        if spec is not None:
            existing = self._specs.get(node_id)
            if existing is not None and existing != spec:
                raise MetricContractError("metric baseline role changed")
            self._specs[node_id] = spec
        self._values[node_id].append(float(transformed_value))

    def score(
        self, node_id: str, transformed_value: float, polarity: int,
        spec: MetricRoleSpec | None = None,
    ) -> tuple[float, BaselineScale] | None:
        scale = self.scale(node_id, spec)
        if scale is None:
            return None
        score = float(
            polarity
            * (transformed_value - scale.center)
            / scale.final_scale
        )
        if not math.isfinite(score):
            raise MetricContractError("normalized metric score is not finite")
        return score, scale

    def snapshot(self) -> dict[str, tuple[float, ...]]:
        return {key: tuple(values) for key, values in sorted(self._values.items())}

    def scale_snapshot(self) -> dict[str, dict[str, float | int | str]]:
        output = {}
        for node_id in sorted(self._values):
            scale = self.scale(node_id)
            if scale is not None:
                output[node_id] = {
                    "baseline_sample_count": scale.sample_count,
                    "sample_count": scale.sample_count,
                    "median": scale.center,
                    "mad": scale.mad,
                    "iqr": scale.iqr,
                    "center": scale.center,
                    "mad_scale": scale.mad_scale,
                    "iqr_scale": scale.iqr_scale,
                    "family_floor": scale.family_floor,
                    "final_scale": scale.final_scale,
                    "scale_source": scale.scale_source,
                }
        return output


class MetricResolver:
    def __init__(self, config: FinalControlConfig):
        self.config = config
        self._excluded_roles = frozenset(experimental_dns_metric_roles())
        self._resolvable_roles = (
            *config.metric_roles,
            *self._excluded_roles,
        )
        self.last_validity: dict[str, dict[str, Any]] = {}

    def is_excluded_from_formal_rca(self, spec: MetricRoleSpec) -> bool:
        return spec in self._excluded_roles

    def resolve(self, record) -> tuple[MetricNode, MetricRoleSpec]:
        record_type = getattr(record, "record_type", None)
        matches = []
        for spec in self._resolvable_roles:
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
        normalized, raw = {}, {}
        resolved = []
        for record in (*window.node_metrics, *window.edge_metrics):
            try:
                metric, spec = self.resolve(record)
            except MetricContractError:
                if self.config.strict_metric_contract:
                    raise
                continue
            resolved.append((record, metric, spec))
        count_by_entity = {}
        for record, metric, spec in resolved:
            if spec.role == "request_rate" and record.coverage > 0.0:
                count_by_entity[metric.entity_id] = (
                    float(record.value) * record.window_sec
                )
            elif spec.role == "edge_count" and record.coverage > 0.0:
                count_by_entity[metric.entity_id] = float(record.value)
        validity = {}
        seen = set()
        for record, metric, spec in resolved:
            if metric.node_id in seen:
                raise MetricContractError(
                    f"collected window has duplicate metric node {metric.node_id}"
                )
            seen.add(metric.node_id)
            quality = float(record.coverage * (1.0 - record.event_loss_rate))
            if self.is_excluded_from_formal_rca(spec):
                validity[metric.node_id] = {
                    "valid": False,
                    "invalid_reason": "excluded_from_formal_rca",
                    "exclusion_reason": "excluded_from_formal_rca",
                    "raw_value": record.value,
                    "coverage": record.coverage,
                    "sample_count": record.sample_count,
                    "request_count": count_by_entity.get(metric.entity_id),
                    "quality": quality,
                }
                continue
            reason = None
            if quality <= 0.0:
                reason = "zero_coverage"
            elif spec.role in {"request_latency", "edge_latency"} \
                    and record.sample_count < self.config.latency_min_samples:
                reason = "insufficient_sample_count"
            elif spec.role in {"request_failure", "edge_failure"}:
                exposure = count_by_entity.get(metric.entity_id)
                if exposure is None:
                    reason = "missing_request_count"
                elif exposure < self.config.failure_min_requests:
                    reason = "insufficient_request_count"
            validity[metric.node_id] = {
                "valid": reason is None,
                "invalid_reason": reason,
                "raw_value": record.value,
                "coverage": record.coverage,
                "sample_count": record.sample_count,
                "request_count": count_by_entity.get(metric.entity_id),
                "quality": quality,
            }
            if reason is not None:
                continue
            transformed = baseline.transform(record.value, spec)
            raw[metric.node_id] = (transformed, spec)
            scored = baseline.score(
                metric.node_id, transformed, spec.polarity, spec,
            )
            if scored is None:
                continue
            signed_z, scale = scored
            normalized[metric.node_id] = NormalizedObservation(
                metric=metric,
                signed_z=signed_z,
                anomaly=max(signed_z, 0.0),
                quality=quality,
                source_record_id=record.stable_id,
                baseline_center=scale.center,
                baseline_scale=scale.final_scale,
                scale_source=scale.scale_source,
            )
        self.last_validity = validity
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

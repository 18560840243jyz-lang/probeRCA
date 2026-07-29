"""Deterministic P2 window aggregation over the P1 metric contracts.

``timestamp_ns`` is treated as event time. A timestamp exactly on a boundary
belongs to the next half-open window. Aggregated records carry the window end
timestamp and are not fed back into the same aggregator.
"""

from __future__ import annotations

import json
import hashlib
import math
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from statistics import median
from typing import Iterable

from proberca.config import MetricAggregationSpec, MonotonicCounterPolicy
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    STRICT_RECORD_TYPES,
    EdgeMetricRecord,
    NodeMetricRecord,
)

Metric = NodeMetricRecord | EdgeMetricRecord
AGGREGATION_STATE_VERSION = "1"


class LateRecordError(ValueError):
    """Raised when a finalized window receives another record."""


class RejectedWindowError(ValueError):
    """Raised when an explicit counter policy rejects the current window."""


@dataclass(frozen=True)
class AggregationIssue:
    stable_id: str
    window_start_ns: int
    window_end_ns: int
    reason_code: str
    detail: dict[str, object]


@dataclass(frozen=True)
class AggregationBatch:
    window_start_ns: int
    window_end_ns: int
    node_records: list[NodeMetricRecord] = field(default_factory=list)
    edge_records: list[EdgeMetricRecord] = field(default_factory=list)
    missing_outputs: list[AggregationIssue] = field(default_factory=list)
    invalid_outputs: list[AggregationIssue] = field(default_factory=list)
    quality_issues: list[AggregationIssue] = field(default_factory=list)
    counter_resets: list[AggregationIssue] = field(default_factory=list)
    finalized: bool = True

    def sorted(self) -> "AggregationBatch":
        key = lambda record: (
            record.timestamp_ns,
            record.record_type,
            record.stable_id,
            record.quantile if record.quantile is not None else -1.0,
            record.histogram_upper_bound if record.histogram_upper_bound is not None else math.inf,
        )
        issue_key = lambda issue: (issue.stable_id, issue.reason_code, json.dumps(issue.detail, sort_keys=True))
        return replace(
            self,
            node_records=sorted(self.node_records, key=key),
            edge_records=sorted(self.edge_records, key=key),
            missing_outputs=sorted(self.missing_outputs, key=issue_key),
            invalid_outputs=sorted(self.invalid_outputs, key=issue_key),
            quality_issues=sorted(self.quality_issues, key=issue_key),
            counter_resets=sorted(self.counter_resets, key=issue_key),
        )


def _issue(stable_id: str, start: int, end: int, reason: str, **detail: object) -> AggregationIssue:
    return AggregationIssue(stable_id, start, end, reason, detail)


def _quality(records: list[Metric]) -> tuple[int, float, float]:
    if not records:
        raise ValueError("quality requires at least one record")
    weights = [max(record.sample_count, 1) for record in records]
    total_weight = sum(weights)
    coverage = sum(weight * record.coverage for weight, record in zip(weights, records)) / total_weight
    loss = sum(weight * record.event_loss_rate for weight, record in zip(weights, records)) / total_weight
    if not 0.0 <= coverage <= 1.0 or not 0.0 <= loss <= 1.0:
        raise ValueError("aggregated quality is outside [0, 1]")
    return sum(record.sample_count for record in records), coverage, loss


def _output_record(
    template: Metric,
    spec: MetricAggregationSpec,
    end_ns: int,
    value: float,
    sample_count: int,
    coverage: float,
    loss: float,
    *,
    quantile: float | None = None,
) -> Metric:
    common = dict(
        timestamp_ns=end_ns,
        window_sec=max(1, (end_ns - (end_ns - template.window_sec * 1_000_000_000)) // 1_000_000_000),
        metric_name=spec.output_metric_name,
        metric_kind=spec.output_metric_kind,
        scope=spec.target_scope,
        value=float(value),
        unit=spec.output_unit,
        sample_count=int(sample_count),
        coverage=float(coverage),
        event_loss_rate=float(loss),
        source="window_aggregation",
        histogram_upper_bound=None,
        histogram_is_inf_bucket=False,
        histogram_is_cumulative=None,
        quantile=quantile,
    )
    if isinstance(template, NodeMetricRecord):
        if spec.target_scope == "service":
            common.update(pod_uid=None, container_id=None)
        return replace(template, **common)
    if spec.target_scope == "service_pair":
        common.update(src_pod_uid=None, dst_pod_uid=None)
    return replace(template, **common)


class CounterDeltaTracker:
    """Convert monotonic counters to per-series deltas before aggregation."""

    def __init__(self, policy: MonotonicCounterPolicy):
        if not isinstance(policy, MonotonicCounterPolicy):
            raise TypeError("policy must be MonotonicCounterPolicy")
        self.policy = policy
        self._previous: dict[str, tuple[int, float]] = {}

    def process(self, record: Metric) -> tuple[Metric | None, list[AggregationIssue]]:
        if record.metric_kind != "monotonic_counter":
            raise ValueError("CounterDeltaTracker only accepts monotonic_counter records")
        key = record.series_id
        if not record.valid:
            start = (
                record.timestamp_ns
                // (record.window_sec * 1_000_000_000)
            ) * record.window_sec * 1_000_000_000
            end = start + record.window_sec * 1_000_000_000
            return None, [
                _issue(
                    key,
                    start,
                    end,
                    record.invalid_reason,
                    source="data_plane",
                ),
            ]
        previous = self._previous.get(key)
        if previous is not None and record.timestamp_ns <= previous[0]:
            raise ValueError("monotonic counter timestamps must increase within each series")
        self._previous[key] = (record.timestamp_ns, record.value)
        start = (record.timestamp_ns // (record.window_sec * 1_000_000_000)) * record.window_sec * 1_000_000_000
        end = start + record.window_sec * 1_000_000_000
        if previous is None:
            return None, [_issue(key, start, end, "insufficient_history", timestamp_ns=record.timestamp_ns)]
        delta = record.value - previous[1]
        issues: list[AggregationIssue] = []
        if delta < 0:
            issues.append(_issue(key, start, end, "counter_reset", previous=previous[1], current=record.value,
                                 policy=self.policy.reset_policy))
            if self.policy.reset_policy == "mark_missing":
                return None, issues
            if self.policy.reset_policy == "reject_window":
                raise RejectedWindowError(f"counter reset rejected for {key}")
            delta = record.value
        if delta < 0:
            raise ValueError("counter delta must never be negative")
        return replace(record, value=float(delta), metric_kind="delta_counter"), issues

    def to_dict(self) -> dict:
        return {
            "format_version": AGGREGATION_STATE_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "policy": asdict(self.policy),
            "previous": {key: [timestamp, value] for key, (timestamp, value) in sorted(self._previous.items())},
        }

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def from_dict(cls, payload: dict, policy: MonotonicCounterPolicy) -> "CounterDeltaTracker":
        if set(payload) != {"format_version", "schema_version", "policy", "previous"}:
            raise ValueError("invalid counter snapshot fields")
        if payload["format_version"] != AGGREGATION_STATE_VERSION or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise ValueError("incompatible counter snapshot version")
        if payload["policy"] != asdict(policy):
            raise ValueError("counter snapshot policy mismatch")
        result = cls(policy)
        result._previous = {key: (int(value[0]), float(value[1])) for key, value in payload["previous"].items()}
        return result

    @classmethod
    def load_json(cls, path: str | Path, policy: MonotonicCounterPolicy) -> "CounterDeltaTracker":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")), policy)


class AggregationPlan:
    """Validated DAG of output IDs bound to P1 ``MetricAggregationSpec`` objects."""

    def __init__(self, entries: Iterable[tuple[str, MetricAggregationSpec]]):
        self.entries = list(entries)
        output_ids = [output_id for output_id, _ in self.entries]
        if any(not isinstance(output_id, str) or not output_id for output_id in output_ids):
            raise ValueError("aggregation output IDs must be non-empty strings")
        if len(output_ids) != len(set(output_ids)):
            raise ValueError("duplicate aggregation output ID")
        for _, item in self.entries:
            if not isinstance(item, MetricAggregationSpec):
                raise TypeError("aggregation plan values must be MetricAggregationSpec")
            item.validate()
        self.specs = dict(self.entries)
        self.order = self._topological_order()

    @property
    def signature(self) -> str:
        payload = [(output_id, self.specs[output_id].to_dict()) for output_id in sorted(self.specs)]
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _topological_order(self) -> list[str]:
        dependencies = {
            output_id: ({spec.numerator_metric_id, spec.denominator_metric_id} if spec.method == "ratio_from_components" else set())
            for output_id, spec in self.entries
        }
        for output_id, values in dependencies.items():
            missing = values - set(self.specs)
            if missing:
                raise ValueError(f"aggregation output {output_id!r} has missing dependencies: {sorted(missing)}")
        order: list[str] = []
        remaining = {key: set(value) for key, value in dependencies.items()}
        while remaining:
            ready = sorted(key for key, value in remaining.items() if not value)
            if not ready:
                raise ValueError("aggregation dependency cycle detected")
            order.extend(ready)
            for key in ready:
                remaining.pop(key)
            for values in remaining.values():
                values.difference_update(ready)
        return order

    def execute(self, records: Iterable[Metric], start_ns: int, end_ns: int) -> AggregationBatch:
        supplied_records = list(records)
        if any(not isinstance(record, (NodeMetricRecord, EdgeMetricRecord)) for record in supplied_records):
            raise TypeError("aggregation accepts only P1 node or edge metric records")
        configured_inputs: set[str] = set()
        for spec in self.specs.values():
            configured_inputs.update(spec.input_metric_ids or [])
            configured_inputs.update(item.metric_id for item in (spec.histogram_inputs or []))
        unconfigured = sorted(
            {record.stable_id for record in supplied_records}
            - configured_inputs
        )
        if unconfigured:
            raise ValueError(f"missing aggregation spec for input IDs: {unconfigured}")
        batch = AggregationBatch(start_ns, end_ns)
        for record in supplied_records:
            if not record.valid:
                batch.invalid_outputs.append(
                    _issue(
                        record.stable_id,
                        start_ns,
                        end_ns,
                        record.invalid_reason,
                        source="data_plane",
                    )
                )
        records_list = sorted(
            (record for record in supplied_records if record.valid),
            key=lambda record: (
                record.timestamp_ns,
                record.record_type,
                record.stable_id,
                record.series_id,
                record.value,
            ),
        )
        produced: dict[str, list[Metric]] = {}
        for output_id in self.order:
            spec = self.specs[output_id]
            if spec.method == "ratio_from_components":
                outputs = self._ratio(output_id, spec, produced, batch, start_ns, end_ns)
            else:
                inputs = self._inputs(spec, records_list)
                outputs = self._execute_one(output_id, spec, inputs, batch, start_ns, end_ns)
            produced[output_id] = outputs
            for output in outputs:
                (batch.node_records if isinstance(output, NodeMetricRecord) else batch.edge_records).append(output)
        return batch.sorted()

    @staticmethod
    def _inputs(spec: MetricAggregationSpec, records: list[Metric]) -> list[Metric]:
        if spec.method == "histogram_merge_quantile":
            ids = {item.metric_id for item in spec.histogram_inputs or []}
        else:
            ids = set(spec.input_metric_ids or [])
        return [record for record in records if record.stable_id in ids]

    def _execute_one(self, output_id, spec, inputs, batch, start, end) -> list[Metric]:
        if spec.method == "reject_cross_scope_quantile":
            batch.invalid_outputs.append(_issue(output_id, start, end, "incompatible_scope", method=spec.method))
            return []
        if not inputs:
            batch.missing_outputs.append(_issue(output_id, start, end, "missing_component", inputs=spec.input_metric_ids or []))
            return []
        if any(record.metric_kind != spec.input_metric_kind for record in inputs):
            raise ValueError(f"input metric kind conflicts with spec for {output_id}")
        if spec.method == "sum":
            self._validate_compatible(inputs, spec, allow_multiple_series=True)
            count, coverage, loss = _quality(inputs)
            return [_output_record(inputs[0], spec, end, sum(record.value for record in inputs), count, coverage, loss)]
        if spec.method == "median_max":
            self._validate_compatible(inputs, spec, allow_multiple_series=True)
            values = [record.value for record in inputs]
            value = spec.median_weight * median(values) + (1.0 - spec.median_weight) * max(values)
            count, coverage, loss = _quality(inputs)
            return [_output_record(inputs[0], spec, end, value, count, coverage, loss)]
        if spec.method == "last_same_series":
            self._validate_compatible(inputs, spec, allow_multiple_series=False)
            latest_timestamp = max(record.timestamp_ns for record in inputs)
            latest = [record for record in inputs if record.timestamp_ns == latest_timestamp]
            if len({record.value for record in latest}) != 1:
                batch.invalid_outputs.append(_issue(output_id, start, end, "duplicate_series_value",
                                                     timestamp_ns=latest_timestamp))
                return []
            chosen = latest[0]
            return [_output_record(chosen, spec, end, chosen.value, chosen.sample_count,
                                   chosen.coverage, chosen.event_loss_rate, quantile=chosen.quantile)]
        if spec.method == "histogram_merge_quantile":
            return self._histogram(output_id, spec, inputs, batch, start, end)
        raise ValueError(f"unsupported aggregation method {spec.method!r}")

    @staticmethod
    def _validate_compatible(inputs: list[Metric], spec: MetricAggregationSpec, *, allow_multiple_series: bool) -> None:
        if {record.unit for record in inputs} != {spec.output_unit}:
            raise ValueError("aggregation input units are incompatible")
        if {record.scope for record in inputs} != {spec.source_scope}:
            raise ValueError("aggregation input scopes are incompatible")
        identities = {(record.record_type, record.stable_id, record.metric_kind) for record in inputs}
        if len({identity[1] for identity in identities}) != 1:
            raise ValueError("aggregation input metric identities are incompatible")
        if not allow_multiple_series:
            series = {record.series_id for record in inputs}
            if len(series) != 1 or series != set(spec.input_series_ids or []):
                raise ValueError("last_same_series inputs must have one configured series_id")

    def _ratio(self, output_id, spec, produced, batch, start, end) -> list[Metric]:
        numerator = produced.get(spec.numerator_metric_id, [])
        denominator = produced.get(spec.denominator_metric_id, [])
        if len(numerator) != 1 or len(denominator) != 1:
            batch.missing_outputs.append(_issue(output_id, start, end, "missing_component",
                                                 numerator_count=len(numerator), denominator_count=len(denominator)))
            return []
        num, den = numerator[0], denominator[0]
        if den.value == 0:
            batch.missing_outputs.append(_issue(output_id, start, end, "zero_denominator",
                                                 denominator_metric_id=spec.denominator_metric_id))
            return []
        value = num.value / den.value
        if not math.isfinite(value):
            raise ValueError("ratio result must be finite")
        return [_output_record(num, spec, end, value, min(num.sample_count, den.sample_count),
                               min(num.coverage, den.coverage), max(num.event_loss_rate, den.event_loss_rate))]

    def _histogram(self, output_id, spec, inputs, batch, start, end) -> list[Metric]:
        descriptors = spec.histogram_inputs or []
        expected = [(item.upper_bound, item.is_inf_bucket) for item in descriptors]
        if any(record.unit != spec.output_unit or record.scope != spec.source_scope for record in inputs):
            batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram", detail="unit_or_scope"))
            return []
        groups: dict[str, list[Metric]] = {}
        for record in inputs:
            groups.setdefault(record.series_id, []).append(record)
        merged = [0.0] * len(descriptors)
        all_records: list[Metric] = []
        for series_id, series_records in sorted(groups.items()):
            keyed = {(record.histogram_upper_bound, record.histogram_is_inf_bucket): record for record in series_records}
            if len(keyed) != len(series_records) or set(keyed) != set(expected):
                batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram",
                                                     series_id=series_id, detail="missing_or_duplicate_bucket"))
                return []
            ordered = [keyed[key] for key in expected]
            if any(record.histogram_is_cumulative != descriptors[index].is_cumulative for index, record in enumerate(ordered)):
                batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram",
                                                     series_id=series_id, detail="cumulative_mismatch"))
                return []
            values = [record.value for record in ordered]
            if descriptors[0].is_cumulative and values != sorted(values):
                batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram",
                                                     series_id=series_id, detail="non_monotonic_cumulative"))
                return []
            merged = [left + right for left, right in zip(merged, values)]
            all_records.extend(ordered)
        if not descriptors or not descriptors[-1].is_inf_bucket:
            batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram", detail="missing_total"))
            return []
        cumulative = merged if descriptors[0].is_cumulative else list(_running_sum(merged))
        total = cumulative[-1]
        if total <= 0:
            batch.invalid_outputs.append(_issue(output_id, start, end, "invalid_histogram", detail="non_positive_total"))
            return []
        finite_bounds = [item.upper_bound for item in descriptors if not item.is_inf_bucket]
        sample_count, coverage, loss = _quality(all_records)
        sample_count = int(total)
        outputs: list[Metric] = []
        for quantile in spec.output_quantiles or []:
            threshold = quantile * total
            index = next(index for index, count in enumerate(cumulative) if count >= threshold)
            if descriptors[index].is_inf_bucket:
                value = finite_bounds[-1]
                batch.quality_issues.append(_issue(output_id, start, end, "quantile_in_inf_bucket", quantile=quantile))
            else:
                value = descriptors[index].upper_bound
            outputs.append(_output_record(inputs[0], spec, end, value, sample_count, coverage, loss, quantile=quantile))
        return outputs


def _running_sum(values: Iterable[float]) -> Iterable[float]:
    total = 0.0
    for value in values:
        total += value
        yield total


class WindowAggregator:
    """Collect records into deterministic half-open event-time windows."""

    def __init__(self, window_sec: int, allowed_lateness_sec: int, plan: AggregationPlan,
                 counter_tracker: CounterDeltaTracker | None = None):
        if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
            raise ValueError("window_sec must be a positive integer")
        if isinstance(allowed_lateness_sec, bool) or not isinstance(allowed_lateness_sec, int) or allowed_lateness_sec < 0:
            raise ValueError("allowed_lateness_sec must be a non-negative integer")
        self.window_sec = window_sec
        self.allowed_lateness_sec = allowed_lateness_sec
        self.plan = plan
        self.counter_tracker = counter_tracker
        self._windows: dict[int, list[Metric]] = {}
        self._finalized: set[int] = set()
        self._last_finalized_start: int | None = None
        self._counter_issues: dict[int, list[AggregationIssue]] = {}

    @property
    def window_ns(self) -> int:
        return self.window_sec * 1_000_000_000

    def add(self, record: Metric) -> None:
        if not isinstance(record, (NodeMetricRecord, EdgeMetricRecord)):
            raise TypeError("WindowAggregator accepts P1 metric records")
        if record.timestamp_ns < 0:
            raise ValueError("timestamp_ns must be non-negative")
        if record.window_sec != self.window_sec:
            raise ValueError("record window_sec conflicts with WindowAggregator")
        start = (record.timestamp_ns // self.window_ns) * self.window_ns
        if start in self._finalized:
            raise LateRecordError(f"window {start} is already finalized")
        self._windows.setdefault(start, []).append(record)

    def finalize(self, window_start_ns: int, *, watermark_ns: int | None = None) -> AggregationBatch:
        if window_start_ns < 0 or window_start_ns % self.window_ns:
            raise ValueError("window_start_ns must be a non-negative aligned boundary")
        if window_start_ns in self._finalized:
            raise ValueError("window is already finalized")
        if self._last_finalized_start is not None and window_start_ns <= self._last_finalized_start:
            raise ValueError("windows must be finalized in chronological order")
        end = window_start_ns + self.window_ns
        if self.allowed_lateness_sec > 0:
            required_watermark = end + self.allowed_lateness_sec * 1_000_000_000
            if watermark_ns is None or watermark_ns < required_watermark:
                raise ValueError("watermark has not passed the configured allowed lateness")
        records: list[Metric] = []
        counter_issues: list[AggregationIssue] = []
        for record in sorted(self._windows.pop(window_start_ns, []), key=lambda item: (item.timestamp_ns, item.series_id)):
            if record.metric_kind == "monotonic_counter":
                if self.counter_tracker is None:
                    raise ValueError("monotonic_counter requires CounterDeltaTracker")
                converted, issues = self.counter_tracker.process(record)
                counter_issues.extend(issues)
                if converted is not None:
                    records.append(converted)
            else:
                records.append(record)
        batch = self.plan.execute(records, window_start_ns, end)
        batch.counter_resets.extend(issue for issue in counter_issues if issue.reason_code == "counter_reset")
        batch.missing_outputs.extend(issue for issue in counter_issues if issue.reason_code == "insufficient_history")
        self._finalized.add(window_start_ns)
        self._last_finalized_start = window_start_ns
        return batch.sorted()

    def to_dict(self) -> dict:
        return {
            "format_version": AGGREGATION_STATE_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "window_sec": self.window_sec,
            "allowed_lateness_sec": self.allowed_lateness_sec,
            "plan_signature": self.plan.signature,
            "counter_tracker": self.counter_tracker.to_dict() if self.counter_tracker is not None else None,
            "windows": {str(start): [record.to_dict() for record in records]
                        for start, records in sorted(self._windows.items())},
            "finalized": sorted(self._finalized),
            "last_finalized_start": self._last_finalized_start,
        }

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path, plan: AggregationPlan,
                  counter_tracker: CounterDeltaTracker | None = None) -> "WindowAggregator":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        required = {"format_version", "schema_version", "window_sec", "allowed_lateness_sec",
                    "plan_signature", "counter_tracker", "windows", "finalized", "last_finalized_start"}
        if set(payload) != required:
            raise ValueError("invalid window snapshot fields")
        if payload["format_version"] != AGGREGATION_STATE_VERSION or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise ValueError("incompatible window snapshot version")
        if payload["plan_signature"] != plan.signature:
            raise ValueError("window snapshot aggregation plan mismatch")
        if payload["counter_tracker"] is not None:
            if counter_tracker is None:
                policy = MonotonicCounterPolicy.from_dict(payload["counter_tracker"]["policy"])
            else:
                policy = counter_tracker.policy
            counter_tracker = CounterDeltaTracker.from_dict(payload["counter_tracker"], policy)
        result = cls(payload["window_sec"], payload["allowed_lateness_sec"], plan, counter_tracker)
        for start, records in payload["windows"].items():
            restored = []
            for record in records:
                record_type = record.get("record_type")
                if record_type not in {"node_metric", "edge_metric"}:
                    raise ValueError("window snapshot contains a non-metric record")
                restored.append(STRICT_RECORD_TYPES[record_type].from_dict(record))
            result._windows[int(start)] = restored
        result._finalized = set(payload["finalized"])
        result._last_finalized_start = payload["last_finalized_start"]
        return result

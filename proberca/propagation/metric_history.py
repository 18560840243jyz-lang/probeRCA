"""Health-gated history for P2 node anomaly records."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, replace
from dataclasses import asdict
from pathlib import Path

import numpy as np

from proberca.alerting import UpdateGate
from proberca.baseline import AnomalyScore
from proberca.config import BaselineConfig, MetricSignalSpec, PropagationConfig
from proberca.data.schema import NodeAnomalyRecord, NodeMetricRecord, PROBERCA_SCHEMA_VERSION


HISTORY_FORMAT_VERSION = "1"


def node_anomaly_from_p2(source_metric_record, anomaly_score: AnomalyScore,
                         update_gate: UpdateGate, signal_spec: MetricSignalSpec,
                         alert_state: str, baseline_config: BaselineConfig,
                         baseline_window_sec: int) -> NodeAnomalyRecord:
    """Adapt a real P2 score without recomputing any anomaly mathematics."""
    if not isinstance(source_metric_record, NodeMetricRecord):
        raise TypeError("P2 to P5 handoff requires NodeMetricRecord")
    if not isinstance(anomaly_score, AnomalyScore):
        raise TypeError("anomaly_score must be P2 AnomalyScore")
    if not isinstance(update_gate, UpdateGate) or not isinstance(signal_spec, MetricSignalSpec):
        raise TypeError("handoff requires UpdateGate and MetricSignalSpec")
    if not isinstance(baseline_config, BaselineConfig):
        raise TypeError("handoff requires BaselineConfig metadata")
    if isinstance(baseline_window_sec, bool) or not isinstance(baseline_window_sec, int) \
            or baseline_window_sec <= 0:
        raise ValueError("baseline_window_sec metadata must be positive")
    if alert_state not in {"healthy", "soft", "hard", "recovery", "edge_anomaly"}:
        raise ValueError("invalid source alert state")
    if anomaly_score.coverage is None or anomaly_score.event_loss_rate is None:
        raise ValueError("P2 anomaly score is missing required handoff metadata")
    observation_quality = float(anomaly_score.coverage * (1.0 - anomaly_score.event_loss_rate))
    if not 0.0 <= observation_quality <= 1.0:
        raise ValueError("P2 observation quality metadata is outside [0, 1]")
    expected_spec_id = hashlib.sha256(
        json.dumps(asdict(signal_spec), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    baseline_fingerprint = hashlib.sha256(
        json.dumps({"config": asdict(baseline_config), "window_sec": baseline_window_sec},
                   sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    expected_service = (
        f"{source_metric_record.cluster_id}::{source_metric_record.namespace}::"
        f"{source_metric_record.service_name}"
    )
    if (
        anomaly_score.record_type != "node_metric"
        or anomaly_score.stable_id != source_metric_record.stable_id
        or anomaly_score.service_id != expected_service
        or anomaly_score.metric_family != source_metric_record.metric_family
        or anomaly_score.metric_name != source_metric_record.metric_name
        or signal_spec.aggregation_output_id != source_metric_record.stable_id
        or signal_spec.metric_family != source_metric_record.metric_family
        or signal_spec.metric_name != source_metric_record.metric_name
        or signal_spec.record_type != "node_metric"
    ):
        raise ValueError("P2 handoff fields conflict; semantic inference is forbidden")
    return NodeAnomalyRecord(
        schema_version=PROBERCA_SCHEMA_VERSION,
        timestamp_ns=source_metric_record.timestamp_ns,
        window_start_ns=(source_metric_record.timestamp_ns
                         - source_metric_record.window_sec * 1_000_000_000),
        window_end_ns=source_metric_record.timestamp_ns,
        cluster_id=source_metric_record.cluster_id,
        namespace=source_metric_record.namespace,
        service_name=source_metric_record.service_name,
        service_id=expected_service,
        node_id=source_metric_record.stable_id,
        metric_family=source_metric_record.metric_family,
        metric_name=source_metric_record.metric_name,
        signed_z=anomaly_score.signed_z,
        anomaly_score=anomaly_score.anomaly,
        baseline_ready=True,
        observation_quality=observation_quality,
        source_alert_state=alert_state,
        source_metric_record_id=source_metric_record.stable_id,
        baseline_config_fingerprint=baseline_fingerprint,
        signal_spec_id=expected_spec_id,
    )


class MetricHistoryConflictError(ValueError):
    """A node and timestamp have two different observations."""


class MetricHistoryOrderError(ValueError):
    """Online history windows are out of order."""


@dataclass(frozen=True)
class MetricHistoryIssue:
    node_id: str
    timestamp_ns: int
    reason_code: str


@dataclass(frozen=True)
class MetricHistoryIngestResult:
    timestamp_ns: int
    inserted_count: int
    issues: list[MetricHistoryIssue]
    reordered: bool = False


class MetricHealthyHistoryStore:
    """Store only healthy, baseline-ready, quality-qualified anomaly windows."""

    def __init__(self, config: PropagationConfig, window_sec: int):
        if not isinstance(config, PropagationConfig):
            raise TypeError("config must be PropagationConfig")
        if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
            raise ValueError("window_sec must be positive")
        if config.metric_history_sec < max(config.metric_lags) * window_sec:
            raise ValueError("metric history duration must cover all configured lag windows")
        self.config = config
        self.window_sec = window_sec
        self.window_ns = window_sec * 1_000_000_000
        self._records: dict[str, dict[int, NodeAnomalyRecord]] = {}
        self._last_seen_timestamp: int | None = None

    def _validate_window(self, records: list[NodeAnomalyRecord]) -> int:
        if not records or any(not isinstance(item, NodeAnomalyRecord) for item in records):
            raise TypeError("node_anomalies must be a non-empty list of NodeAnomalyRecord")
        timestamps = {item.timestamp_ns for item in records}
        if len(timestamps) != 1:
            raise MetricHistoryOrderError("one ingest call must contain exactly one window")
        timestamp = next(iter(timestamps))
        if any(item.window_end_ns - item.window_start_ns != self.window_ns for item in records):
            raise MetricHistoryOrderError("node anomaly window differs from configured window_sec")
        dedup: dict[str, NodeAnomalyRecord] = {}
        for item in records:
            existing = dedup.get(item.node_id)
            if existing is not None and existing != item:
                raise MetricHistoryConflictError(f"node={item.node_id} timestamp={timestamp} conflicts")
            dedup[item.node_id] = item
        if self._last_seen_timestamp is not None and timestamp < self._last_seen_timestamp:
            raise MetricHistoryOrderError(f"timestamp={timestamp} is older than {self._last_seen_timestamp}")
        return timestamp

    def ingest_healthy_window(self, node_anomalies, update_gate: UpdateGate) -> MetricHistoryIngestResult:
        records = list(node_anomalies)
        timestamp = self._validate_window(records)
        if not isinstance(update_gate, UpdateGate):
            raise TypeError("update_gate must be UpdateGate")
        issues: list[MetricHistoryIssue] = []
        inserted = 0
        gate_open = update_gate.update_service_model and update_gate.baseline_ready
        for item in sorted(records, key=lambda record: record.node_id):
            reason = None
            if item.node_id in update_gate.frozen_node_ids:
                reason = "frozen_series"
            elif not gate_open or item.source_alert_state in {"soft", "hard", "recovery"}:
                reason = "non_healthy_window"
            elif not item.baseline_ready:
                reason = "baseline_not_ready"
            elif item.observation_quality < self.config.metric_min_observation_quality:
                reason = "low_quality"
            if reason is not None:
                issues.append(MetricHistoryIssue(item.node_id, timestamp, reason))
                continue
            series = self._records.setdefault(item.node_id, {})
            existing = series.get(timestamp)
            if existing is not None:
                if existing != item:
                    raise MetricHistoryConflictError(f"node={item.node_id} timestamp={timestamp} conflicts")
                continue
            series[timestamp] = item
            inserted += 1
        if inserted:
            self._last_seen_timestamp = timestamp if self._last_seen_timestamp is None else max(
                timestamp, self._last_seen_timestamp
            )
            cutoff = timestamp - self.config.metric_history_sec * 1_000_000_000
            self._records = {
                node_id: {ts: record for ts, record in values.items() if ts >= cutoff}
                for node_id, values in self._records.items()
            }
            self._records = {node_id: values for node_id, values in self._records.items() if values}
        return MetricHistoryIngestResult(timestamp, inserted, issues)

    def ingest_replay(self, batches) -> list[MetricHistoryIngestResult]:
        prepared = list(batches)
        timestamps = [self._validate_window(list(records)) for records, _ in prepared]
        reordered = timestamps != sorted(timestamps)
        ordered = [item for _, item in sorted(zip(timestamps, prepared), key=lambda pair: pair[0])]
        return [replace(self.ingest_healthy_window(records, gate), reordered=reordered)
                for records, gate in ordered]

    def get(self, node_id: str, timestamp_ns: int) -> NodeAnomalyRecord | None:
        return self._records.get(node_id, {}).get(timestamp_ns)

    def series(self, node_id: str) -> list[NodeAnomalyRecord]:
        return [record for _, record in sorted(self._records.get(node_id, {}).items())]

    def node_ids(self) -> list[str]:
        return sorted(self._records)

    @property
    def cutoff_timestamp_ns(self) -> int | None:
        timestamps = [timestamp for values in self._records.values() for timestamp in values]
        return max(timestamps) if timestamps else None

    def to_dict(self) -> dict:
        return {
            "format_version": HISTORY_FORMAT_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "window_sec": self.window_sec,
            "config": self.config.__dict__ | {
                "metric_parent_rules": [item.to_dict() for item in self.config.metric_parent_rules]
            },
            "last_seen_timestamp": self._last_seen_timestamp,
            "records": [record.to_dict() for node_id in sorted(self._records)
                        for record in self.series(node_id)],
        }

    def snapshot(self, directory) -> None:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        payload = self.to_dict()
        (path / "metadata.json").write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        timestamps = np.asarray([item["timestamp_ns"] for item in payload["records"]], dtype=np.int64)
        np.savez_compressed(path / "arrays.npz", timestamps=timestamps)

    @classmethod
    def restore(cls, directory, config: PropagationConfig, window_sec: int):
        path = Path(directory)
        payload = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        expected = {"format_version", "schema_version", "window_sec", "config",
                    "last_seen_timestamp", "records"}
        if set(payload) != expected or payload["format_version"] != HISTORY_FORMAT_VERSION \
                or payload["schema_version"] != PROBERCA_SCHEMA_VERSION:
            raise ValueError("incompatible metric history snapshot")
        result = cls(config, window_sec)
        if payload["window_sec"] != window_sec or payload["config"] != result.to_dict()["config"]:
            raise ValueError("metric history snapshot configuration mismatch")
        arrays = np.load(path / "arrays.npz", allow_pickle=False)
        records = [NodeAnomalyRecord.from_dict(item) for item in payload["records"]]
        if arrays["timestamps"].tolist() != [item.timestamp_ns for item in records]:
            raise ValueError("metric history snapshot array mismatch")
        for item in records:
            result._records.setdefault(item.node_id, {})[item.timestamp_ns] = item
        result._last_seen_timestamp = payload["last_seen_timestamp"]
        return result


MetricTrainingHistoryStore = MetricHealthyHistoryStore


class MetricRuntimeHistoryStore(MetricHealthyHistoryStore):
    """Prediction-only history that never participates in Ridge fitting."""

    def ingest_runtime_window(self, node_anomalies) -> MetricHistoryIngestResult:
        records = list(node_anomalies)
        timestamp = self._validate_window(records)
        issues: list[MetricHistoryIssue] = []
        inserted = 0
        for item in sorted(records, key=lambda record: record.node_id):
            if item.observation_quality < self.config.metric_min_observation_quality:
                issues.append(MetricHistoryIssue(item.node_id, timestamp, "low_quality"))
                continue
            series = self._records.setdefault(item.node_id, {})
            existing = series.get(timestamp)
            if existing is not None:
                if existing != item:
                    raise MetricHistoryConflictError(
                        f"runtime node={item.node_id} timestamp={timestamp} conflicts"
                    )
                continue
            if series:
                previous = max(series)
                if (timestamp - previous) // self.window_ns > self.config.metric_max_gap_windows:
                    series.clear()
                    issues.append(MetricHistoryIssue(item.node_id, timestamp, "runtime_gap"))
            series[timestamp] = item
            inserted += 1
        self._last_seen_timestamp = timestamp if self._last_seen_timestamp is None else max(
            timestamp, self._last_seen_timestamp
        )
        cutoff = timestamp - self.config.metric_history_sec * 1_000_000_000
        self._records = {
            node_id: {ts: record for ts, record in values.items() if ts >= cutoff}
            for node_id, values in self._records.items()
        }
        self._records = {node_id: values for node_id, values in self._records.items() if values}
        return MetricHistoryIngestResult(timestamp, inserted, issues)

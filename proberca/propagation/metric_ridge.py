"""Canonical candidate-masked multi-lag metric Ridge learner."""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import OrderedDict
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np

from proberca.alerting import UpdateGate
from proberca.config import PropagationConfig
from proberca.data.schema import AlertEvent, CandidateSubgraph, NodeAnomalyRecord

from .metric_history import MetricHealthyHistoryStore
from .metric_history import MetricRuntimeHistoryStore, MetricTrainingHistoryStore
from .metric_model import (
    MetricModelBundle,
    MetricPropagationCoefficient,
    MetricPropagationContribution,
    MetricPropagationIssue,
    MetricPropagationModelInfo,
    MetricPropagationPrediction,
    MetricPropagationPrepareResult,
    MetricTargetModel,
    MetricTrainingMatrixInfo,
    MetricWindowProcessResult,
)
from .metric_rules import MetricFeatureKey, MetricParentRuleRegistry


class MetricPropagationNumericalError(ArithmeticError):
    """Masked Ridge system is invalid or cannot be solved."""


MODEL_FORMAT_VERSION = "1"


def _fingerprint(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class MetricPropagationLearner:
    """Unique online and replay production entry for P5 metric propagation."""

    def __init__(self, config: PropagationConfig, window_sec: int):
        if not isinstance(config, PropagationConfig):
            raise TypeError("config must be PropagationConfig")
        if not config.metric_parent_rules:
            raise ValueError("production metric_parent_rules must not be empty")
        self.config = config
        self.window_sec = window_sec
        self.window_ns = window_sec * 1_000_000_000
        self.training_history = MetricTrainingHistoryStore(config, window_sec)
        self.runtime_history = MetricRuntimeHistoryStore(config, window_sec)
        self.history = self.training_history
        self.rules = MetricParentRuleRegistry(config)
        metric_config = {
            key: value for key, value in asdict(config).items()
            if key.startswith("metric_") and key != "metric_parent_rules"
        }
        self.config_fingerprint = _fingerprint(metric_config)
        self.rules_fingerprint = _fingerprint([item.to_dict() for item in config.metric_parent_rules])
        self._active: MetricModelBundle | None = None
        self._cache: OrderedDict[str, MetricModelBundle] = OrderedDict()
        self._last_processed_timestamp: int | None = None

    def ingest_healthy_window(self, node_anomalies, update_gate: UpdateGate):
        records = list(node_anomalies)
        training_result = self.training_history.ingest_healthy_window(records, update_gate)
        self.runtime_history.ingest_runtime_window(records)
        return training_result

    def ingest_replay(self, batches):
        prepared = list(batches)
        timestamps = [next(iter({item.timestamp_ns for item in records}))
                      for records, _ in prepared]
        ordered = [item for _, item in sorted(zip(timestamps, prepared), key=lambda pair: pair[0])]
        reordered = timestamps != sorted(timestamps)
        return [replace(self.ingest_healthy_window(records, update_gate), reordered=reordered)
                for records, update_gate in ordered]

    @staticmethod
    def _candidate_content(candidate: CandidateSubgraph) -> dict:
        return {
            "candidate_id": candidate.candidate_id,
            "config_fingerprint": candidate.config_fingerprint,
            "topology_snapshot_id": candidate.topology_snapshot_id,
            "candidate_node_ids": candidate.candidate_node_ids,
            "impact_edges": candidate.impact_edges,
            "host_relations": candidate.host_relations,
            "resource_relations": candidate.resource_relations,
        }

    def _cache_identity(self, alert: AlertEvent, candidate: CandidateSubgraph) -> tuple[str, dict]:
        candidate_fingerprint = _fingerprint(self._candidate_content(candidate))
        topology_fingerprint = _fingerprint({
            "snapshot": candidate.topology_snapshot_id,
            "impact": candidate.impact_edges,
            "host": candidate.host_relations,
            "resource": candidate.resource_relations,
        })
        node_fingerprint = _fingerprint(sorted(candidate.candidate_node_ids))
        payload = {
            "candidate_id": candidate.candidate_id,
            "candidate_fingerprint": candidate_fingerprint,
            "topology_snapshot_id": candidate.topology_snapshot_id,
            "topology_fingerprint": topology_fingerprint,
            "rules_fingerprint": self.rules_fingerprint,
            "config_fingerprint": self.config_fingerprint,
            "history_cutoff": self.training_history.cutoff_timestamp_ns,
            "node_index_fingerprint": node_fingerprint,
            "trigger_signature": [sorted(alert.trigger_services), sorted(alert.trigger_edges)],
        }
        return _fingerprint(payload), payload

    def _latest_index(self, candidate: CandidateSubgraph, cutoff: int) -> dict[str, NodeAnomalyRecord]:
        output = {}
        for node_id in candidate.candidate_node_ids:
            records = [item for item in self.training_history.series(node_id) if item.timestamp_ns < cutoff]
            if records:
                output[node_id] = records[-1]
        return output

    def _training_matrix(self, target_id: str, features: list[MetricFeatureKey],
                         alert_timestamp_ns: int):
        reasons = {key: 0 for key in ("missing_target", "missing_parent_lag", "low_quality",
                                      "non_healthy_window", "history_gap")}
        rows: list[list[float]] = []
        targets: list[float] = []
        timestamps: list[int] = []
        target_records = [item for item in self.training_history.series(target_id)
                          if item.timestamp_ns < alert_timestamp_ns]
        candidate_timestamps = {item.timestamp_ns for item in target_records}
        for feature in features:
            candidate_timestamps.update(
                item.timestamp_ns + feature.lag * self.window_ns
                for item in self.training_history.series(feature.parent_node_id)
                if item.timestamp_ns + feature.lag * self.window_ns < alert_timestamp_ns
            )
        previous_target_timestamp = None
        for timestamp in sorted(candidate_timestamps):
            target = self.training_history.get(target_id, timestamp)
            if target is None:
                reasons["missing_target"] += 1
                continue
            if previous_target_timestamp is not None and (
                target.timestamp_ns - previous_target_timestamp
            ) // self.window_ns > self.config.metric_max_gap_windows:
                reasons["history_gap"] += 1
                previous_target_timestamp = target.timestamp_ns
                continue
            previous_target_timestamp = target.timestamp_ns
            if not target.baseline_ready or target.source_alert_state not in {"healthy", "edge_anomaly"}:
                reasons["non_healthy_window"] += 1
                continue
            if target.observation_quality < self.config.metric_min_observation_quality:
                reasons["low_quality"] += 1
                continue
            values = []
            missing = False
            for feature in features:
                record = self.training_history.get(feature.parent_node_id,
                                          target.timestamp_ns - feature.lag * self.window_ns)
                if record is None:
                    reasons["missing_parent_lag"] += 1
                    missing = True
                    break
                if not record.baseline_ready or record.observation_quality < self.config.metric_min_observation_quality:
                    reasons["low_quality"] += 1
                    missing = True
                    break
                values.append(record.signed_z)
            if missing:
                continue
            rows.append(values)
            targets.append(target.signed_z)
            timestamps.append(target.timestamp_ns)
        matrix = np.asarray(rows, dtype=float) if rows else np.empty((0, len(features)), dtype=float)
        vector = np.asarray(targets, dtype=float)
        info = MetricTrainingMatrixInfo(
            target_id, list(features), timestamps, reasons, len(timestamps),
            min(timestamps) if timestamps else None, max(timestamps) if timestamps else None,
        )
        return matrix, vector, info

    def _solve(self, matrix: np.ndarray, target: np.ndarray, target_id: str):
        if not np.all(np.isfinite(matrix)) or not np.all(np.isfinite(target)):
            raise MetricPropagationNumericalError(f"target={target_id} training matrix is non-finite")
        if matrix.shape[1] == 0 or matrix.shape[0] == 0:
            return np.zeros(matrix.shape[1], dtype=float), math.inf
        gram = matrix.T @ matrix + self.config.metric_ridge * np.eye(matrix.shape[1])
        rhs = matrix.T @ target
        if not np.all(np.isfinite(gram)) or not np.all(np.isfinite(rhs)):
            raise MetricPropagationNumericalError(f"target={target_id} Ridge system is non-finite")
        condition = float(np.linalg.cond(gram))
        if not math.isfinite(condition):
            return np.zeros(matrix.shape[1], dtype=float), math.inf
        try:
            coefficients = np.linalg.solve(gram, rhs)
        except np.linalg.LinAlgError as exc:
            raise MetricPropagationNumericalError(f"target={target_id} Ridge solve failed: {exc}") from exc
        if not np.all(np.isfinite(coefficients)):
            raise MetricPropagationNumericalError(f"target={target_id} coefficients are non-finite")
        return coefficients, condition

    def _fit(self, alert: AlertEvent, candidate: CandidateSubgraph, lifecycle: str,
             rebuild_issue: bool = False) -> MetricPropagationPrepareResult:
        started = time.perf_counter()
        if alert.timestamp_ns != candidate.alert_timestamp_ns or alert.alert_id != candidate.alert_id:
            raise ValueError("alert and CandidateSubgraph identity/timestamp mismatch")
        cache_key, identity = self._cache_identity(alert, candidate)
        latest = self._latest_index(candidate, alert.timestamp_ns)
        issues: list[MetricPropagationIssue] = []
        missing = sorted(set(candidate.candidate_node_ids) - set(latest))
        for node_id in missing:
            issues.append(MetricPropagationIssue("missing_candidate_history", node_id, "no healthy history"))
        feature_map = self.rules.build(candidate, latest)
        targets: dict[str, MetricTargetModel] = {}
        for target_id in sorted(candidate.candidate_node_ids):
            features = feature_map.get(target_id, [])
            matrix, vector, matrix_info = self._training_matrix(target_id, features, alert.timestamp_ns)
            coefficients, condition = self._solve(matrix, vector, target_id)
            ready = bool(
                features and matrix_info.effective_training_rows >= self.config.metric_min_training_rows
                and condition <= self.config.metric_max_condition_number
                and matrix_info.training_end_ns is not None
                and matrix_info.training_end_ns < alert.timestamp_ns
            )
            if condition > self.config.metric_max_condition_number:
                issues.append(MetricPropagationIssue("ill_conditioned", target_id,
                                                     f"condition_number={condition}"))
            if not ready:
                issues.append(MetricPropagationIssue("target_not_ready", target_id,
                                                     f"effective_rows={matrix_info.effective_training_rows}"))
            targets[target_id] = MetricTargetModel(target_id, features, coefficients,
                                                    condition, matrix_info, ready)
        unready = sorted(target for target, model in targets.items() if not model.ready)
        global_ready = not unready and len(targets) == len(candidate.candidate_node_ids)
        actual_lifecycle = lifecycle if global_ready else "NOT_READY"
        model_snapshot_id = _fingerprint({
            "cache_key": cache_key,
            "coefficients": {target: model.coefficients.tolist() for target, model in targets.items()},
        })
        training_starts = [model.matrix_info.training_start_ns for model in targets.values()
                           if model.matrix_info.training_start_ns is not None]
        training_ends = [model.matrix_info.training_end_ns for model in targets.values()
                         if model.matrix_info.training_end_ns is not None]
        if rebuild_issue:
            issues.append(MetricPropagationIssue("model_rebuilt_at_hard", None,
                                                 "candidate or model cache identity changed"))
        info = MetricPropagationModelInfo(
            model_snapshot_id, candidate.candidate_id, alert.alert_id, actual_lifecycle,
            global_ready, lifecycle == "FROZEN", min(training_starts) if training_starts else None,
            max(training_ends) if training_ends else None, self.training_history.cutoff_timestamp_ns,
            len(targets), len(targets) - len(unready), unready,
            identity["candidate_fingerprint"], identity["topology_fingerprint"],
            self.rules_fingerprint, self.config_fingerprint, identity["node_index_fingerprint"],
            (time.perf_counter() - started) * 1000.0,
            [{"reason_code": item.reason_code, "target_node_id": item.target_node_id,
              "detail": item.detail} for item in issues],
        )
        bundle = MetricModelBundle(cache_key, info, candidate.topology_snapshot_id,
                                   sorted(candidate.candidate_node_ids), targets)
        self._active = bundle
        self._cache[cache_key] = bundle
        self._cache.move_to_end(cache_key)
        if len(self._cache) > self.config.metric_model_cache_size:
            evicted_key, _ = self._cache.popitem(last=False)
            eviction = MetricPropagationIssue("model_cache_eviction", None, evicted_key)
            issues.append(eviction)
            info = replace(info, quality_issues=[*info.quality_issues, asdict(eviction)])
            bundle.info = info
        return MetricPropagationPrepareResult(info, issues, False)

    def prepare_for_alert(self, alert: AlertEvent, candidate: CandidateSubgraph):
        if alert.state != "soft" or candidate.alert_state != "soft":
            raise ValueError("prepare_for_alert requires soft alert and candidate")
        if self._active is not None and self._active.info.frozen:
            raise RuntimeError("frozen metric model cannot be refit")
        cache_key, _ = self._cache_identity(alert, candidate)
        if cache_key in self._cache:
            self._active = self._cache[cache_key]
            self._cache.move_to_end(cache_key)
            return MetricPropagationPrepareResult(self._active.info, [], True)
        return self._fit(alert, candidate, "PREPARED")

    def freeze_for_hard(self, alert: AlertEvent, candidate: CandidateSubgraph):
        if alert.state != "hard" or candidate.alert_state != "hard":
            raise ValueError("freeze_for_hard requires hard alert and candidate")
        cache_key, _ = self._cache_identity(alert, candidate)
        if self._active is not None and self._active.cache_key == cache_key:
            state = "FROZEN" if self._active.info.global_ready else "NOT_READY"
            info = replace(self._active.info, lifecycle_state=state, frozen=True,
                           alert_id=alert.alert_id)
            self._active.info = info
            return MetricPropagationPrepareResult(info, [], True)
        return self._fit(alert, candidate, "FROZEN", rebuild_issue=self._active is not None)

    def archive_soft_model(self):
        if self._active is None or self._active.info.frozen \
                or self._active.info.lifecycle_state not in {"PREPARED", "NOT_READY"}:
            raise RuntimeError("no soft metric model can be archived")
        self._active.info = replace(self._active.info, lifecycle_state="ARCHIVED", frozen=False)
        return self._active.info

    def handle_recovery(self):
        if self._active is None or not self._active.info.frozen:
            raise RuntimeError("recovery requires a frozen metric model")
        return self._active.info

    def _require_active(self) -> MetricModelBundle:
        if self._active is None:
            raise RuntimeError("metric propagation model is NOT_PREPARED")
        return self._active

    def cached_model_infos(self) -> list[MetricPropagationModelInfo]:
        return [bundle.info for bundle in self._cache.values()]

    def training_matrix_info(self, target_node_id: str) -> MetricTrainingMatrixInfo:
        return self._require_active().targets[target_node_id].matrix_info

    def export_sparse_coefficients(self) -> list[MetricPropagationCoefficient]:
        bundle = self._require_active()
        output = []
        for target_id in bundle.node_ids:
            model = bundle.targets[target_id]
            for feature, coefficient in zip(model.feature_keys, model.coefficients):
                output.append(MetricPropagationCoefficient(
                    target_id, feature.parent_node_id, feature.lag, float(coefficient),
                    max(float(coefficient), 0.0), list(feature.relation_types),
                    list(feature.relation_ids), list(feature.rule_ids),
                    model.matrix_info.effective_training_rows, model.condition_number, model.ready,
                ))
        return output

    def export_dense_matrices(self):
        bundle = self._require_active()
        nodes = list(bundle.node_ids)
        index = {node_id: position for position, node_id in enumerate(nodes)}
        shape = (max(self.config.metric_lags), len(nodes), len(nodes))
        dense = np.zeros(shape, dtype=float)
        mask = np.zeros(shape, dtype=bool)
        readiness = np.asarray([bundle.targets[node_id].ready for node_id in nodes], dtype=bool)
        for item in self.export_sparse_coefficients():
            position = (item.lag - 1, index[item.target_node_id], index[item.parent_node_id])
            dense[position] = item.coefficient
            mask[position] = True
        return dense, mask, readiness, nodes

    def predict_window(self, timestamp_ns: int, current_anomalies=None):
        bundle = self._require_active()
        current = {item.node_id: item for item in (current_anomalies or [])}
        output = []
        for target_id in bundle.node_ids:
            model = bundle.targets[target_id]
            contributions = []
            missing = not model.feature_keys
            for feature, coefficient in zip(model.feature_keys, model.coefficients):
                parent = self.runtime_history.get(feature.parent_node_id,
                                          timestamp_ns - feature.lag * self.window_ns)
                if parent is None:
                    missing = True
                    break
                contributions.append(MetricPropagationContribution(
                    target_id, feature.parent_node_id, feature.lag, float(coefficient),
                    parent.signed_z, float(coefficient * parent.signed_z),
                    max(float(coefficient), 0.0), sorted(feature.relation_types)[0],
                    list(feature.relation_types),
                    list(feature.relation_ids), list(feature.rule_ids),
                ))
            actual = current.get(target_id)
            output.append(MetricPropagationPrediction(
                "1.0", "metric_propagation_prediction", timestamp_ns,
                bundle.info.alert_id, bundle.info.candidate_id,
                bundle.topology_snapshot_id, bundle.info.model_snapshot_id, target_id,
                None if missing else float(sum(item.contribution_value for item in contributions)),
                actual.signed_z if actual is not None else None, model.ready,
                bundle.info.frozen, not model.ready, not missing,
                "missing_prediction_feature" if missing else None,
                actual.observation_quality if actual is not None else None,
                [] if missing else contributions, self.config_fingerprint,
            ))
        return output

    def process_window(self, node_anomalies, update_gate: UpdateGate,
                       optional_alert: AlertEvent | None,
                       optional_candidate: CandidateSubgraph | None) -> MetricWindowProcessResult:
        records = list(node_anomalies)
        if not records or any(not isinstance(item, NodeAnomalyRecord) for item in records):
            raise TypeError("process_window requires NodeAnomalyRecord values")
        timestamps = {item.timestamp_ns for item in records}
        if len(timestamps) != 1:
            raise ValueError("process_window requires one timestamp")
        timestamp = next(iter(timestamps))
        if self._last_processed_timestamp is not None and timestamp <= self._last_processed_timestamp:
            raise ValueError("metric process windows must be strictly increasing")
        if (optional_alert is None) != (optional_candidate is None):
            raise ValueError("alert and candidate must be supplied together")
        lifecycle_result = None
        if optional_alert is not None:
            if optional_alert.state == "soft":
                lifecycle_result = self.prepare_for_alert(optional_alert, optional_candidate)
            elif optional_alert.state == "hard":
                lifecycle_result = self.freeze_for_hard(optional_alert, optional_candidate)
            else:
                raise ValueError("process_window lifecycle alert must be soft or hard")
        predictions = self.predict_window(timestamp, records) if self._active is not None else []
        training_result = self.training_history.ingest_healthy_window(records, update_gate)
        runtime_result = self.runtime_history.ingest_runtime_window(records)
        self._last_processed_timestamp = timestamp
        return MetricWindowProcessResult(
            timestamp, predictions, training_result, runtime_result, lifecycle_result
        )

    def process_replay(self, batches) -> list[MetricWindowProcessResult]:
        prepared = list(batches)
        timestamps = [next(iter({item.timestamp_ns for item in batch[0]})) for batch in prepared]
        reordered = timestamps != sorted(timestamps)
        ordered = [item for _, item in sorted(zip(timestamps, prepared), key=lambda pair: pair[0])]
        return [replace(self.process_window(*batch), reordered=reordered) for batch in ordered]

    def snapshot(self, directory) -> None:
        bundle = self._require_active()
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        arrays: dict[str, np.ndarray] = {}
        targets = []
        for index, target_id in enumerate(bundle.node_ids):
            model = bundle.targets[target_id]
            arrays[f"coefficients_{index}"] = model.coefficients
            targets.append({
                "target_node_id": target_id,
                "feature_keys": [asdict(item) for item in model.feature_keys],
                "condition_number": model.condition_number,
                "matrix_info": asdict(model.matrix_info),
                "ready": model.ready,
            })
        metadata = {
            "format_version": MODEL_FORMAT_VERSION,
            "schema_version": "1.0",
            "window_sec": self.window_sec,
            "config_fingerprint": self.config_fingerprint,
            "rules_fingerprint": self.rules_fingerprint,
            "cache_key": bundle.cache_key,
            "cache_keys": list(self._cache),
            "info": asdict(bundle.info),
            "topology_snapshot_id": bundle.topology_snapshot_id,
            "node_ids": bundle.node_ids,
            "targets": targets,
            "last_processed_timestamp": self._last_processed_timestamp,
        }
        (path / "metadata.json").write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        np.savez_compressed(path / "arrays.npz", **arrays)
        self.training_history.snapshot(path / "training_history")
        self.runtime_history.snapshot(path / "runtime_history")

    @classmethod
    def restore(cls, directory, config: PropagationConfig, window_sec: int,
                expected_candidate: CandidateSubgraph):
        path = Path(directory)
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        expected_fields = {
            "format_version", "schema_version", "window_sec", "config_fingerprint",
            "rules_fingerprint", "cache_key", "cache_keys", "info",
            "topology_snapshot_id", "node_ids", "targets", "last_processed_timestamp",
        }
        if set(metadata) != expected_fields or metadata["format_version"] != MODEL_FORMAT_VERSION \
                or metadata["schema_version"] != "1.0":
            raise ValueError("incompatible metric model snapshot version")
        result = cls(config, window_sec)
        if metadata["window_sec"] != window_sec or metadata["config_fingerprint"] != result.config_fingerprint:
            raise ValueError("metric model snapshot config mismatch")
        if metadata["rules_fingerprint"] != result.rules_fingerprint:
            raise ValueError("metric model snapshot rules mismatch")
        info = MetricPropagationModelInfo(**metadata["info"])
        if expected_candidate.candidate_id != info.candidate_id:
            raise ValueError("metric model snapshot candidate mismatch")
        candidate_fingerprint = _fingerprint(result._candidate_content(expected_candidate))
        if candidate_fingerprint != info.candidate_fingerprint:
            if expected_candidate.topology_snapshot_id != metadata["topology_snapshot_id"]:
                raise ValueError("metric model snapshot topology mismatch")
            raise ValueError("metric model snapshot candidate mismatch")
        topology_fingerprint = _fingerprint({
            "snapshot": expected_candidate.topology_snapshot_id,
            "impact": expected_candidate.impact_edges,
            "host": expected_candidate.host_relations,
            "resource": expected_candidate.resource_relations,
        })
        if topology_fingerprint != info.topology_fingerprint:
            raise ValueError("metric model snapshot topology mismatch")
        node_fingerprint = _fingerprint(sorted(expected_candidate.candidate_node_ids))
        if node_fingerprint != info.node_index_fingerprint or metadata["node_ids"] != sorted(
            expected_candidate.candidate_node_ids
        ):
            raise ValueError("metric model snapshot node index mismatch")
        arrays = np.load(path / "arrays.npz", allow_pickle=False)
        targets: dict[str, MetricTargetModel] = {}
        for index, item in enumerate(metadata["targets"]):
            features = [MetricFeatureKey(**feature) for feature in item["feature_keys"]]
            matrix_payload = dict(item["matrix_info"])
            matrix_payload["feature_keys"] = [MetricFeatureKey(**feature)
                                               for feature in matrix_payload["feature_keys"]]
            matrix_info = MetricTrainingMatrixInfo(**matrix_payload)
            coefficients = np.asarray(arrays[f"coefficients_{index}"], dtype=float)
            if coefficients.shape != (len(features),) or not np.all(np.isfinite(coefficients)):
                raise ValueError("metric model snapshot coefficient array mismatch")
            target_id = item["target_node_id"]
            targets[target_id] = MetricTargetModel(
                target_id, features, coefficients.copy(), float(item["condition_number"]),
                matrix_info, bool(item["ready"]),
            )
        bundle = MetricModelBundle(metadata["cache_key"], info,
                                   metadata["topology_snapshot_id"], list(metadata["node_ids"]), targets)
        result.training_history = MetricTrainingHistoryStore.restore(
            path / "training_history", config, window_sec
        )
        result.runtime_history = MetricRuntimeHistoryStore.restore(
            path / "runtime_history", config, window_sec
        )
        result.history = result.training_history
        result._last_processed_timestamp = metadata["last_processed_timestamp"]
        result._active = bundle
        result._cache[bundle.cache_key] = bundle
        return result

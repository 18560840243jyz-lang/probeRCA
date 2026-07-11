"""Canonical topology-constrained multi-lag service RLS learner."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Iterable

import numpy as np

from proberca.alerting import UpdateGate
from proberca.config import ImpactDerivationRule, PropagationConfig
from proberca.data.schema import PROBERCA_SCHEMA_VERSION, ServiceStateRecord, TopologySnapshot
from proberca.topology import TopologyGraph, build_topology_graph

from .service_model import (
    PropagationIssue,
    ServiceFeatureKey,
    ServicePropagationCoefficient,
    ServicePropagationContribution,
    ServicePropagationPrediction,
    ServicePropagationWindowResult,
)


MODEL_FORMAT_VERSION = "1"
NUMERICAL_EPSILON = 1e-12


class NumericalPropagationError(ArithmeticError):
    """A service RLS update cannot be represented by finite numbers."""


class PropagationTimeError(ValueError):
    """Service-state windows are duplicated, out of order, or discontinuous."""


class TopologyModelMismatchError(ValueError):
    """A restored model requires explicit reconciliation with another topology."""


@dataclass
class TargetModelState:
    target_service_id: str
    feature_keys: list[ServiceFeatureKey]
    relation_ids: dict[str, list[str]]
    relation_types: dict[str, list[str]]
    theta: np.ndarray
    covariance: np.ndarray
    update_count: int = 0
    prediction_count: int = 0
    last_update_timestamp: int | None = None
    ready: bool = False
    reconfigure_update_count: int = 0
    frozen: bool = False


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ServicePropagationLearner:
    """Online and replay entry point for service-level propagation learning."""

    def __init__(self, config: PropagationConfig, window_sec: int,
                 impact_derivation_rules: list[ImpactDerivationRule],
                 allow_cross_namespace: bool):
        if not isinstance(config, PropagationConfig):
            raise TypeError("config must be PropagationConfig")
        if isinstance(window_sec, bool) or not isinstance(window_sec, int) or window_sec <= 0:
            raise ValueError("window_sec must be a positive integer")
        if any(not isinstance(rule, ImpactDerivationRule) for rule in impact_derivation_rules):
            raise TypeError("impact_derivation_rules must contain ImpactDerivationRule")
        if not isinstance(allow_cross_namespace, bool):
            raise TypeError("allow_cross_namespace must be boolean")
        self.config = config
        self.window_sec = window_sec
        self.window_ns = window_sec * 1_000_000_000
        self.impact_derivation_rules = list(impact_derivation_rules)
        self.allow_cross_namespace = allow_cross_namespace
        self.config_fingerprint = _fingerprint({
            "config": asdict(config),
            "window_sec": window_sec,
            "impact_derivation_rules": [asdict(item) for item in impact_derivation_rules],
            "allow_cross_namespace": allow_cross_namespace,
        })
        self._models: dict[str, TargetModelState] = {}
        self._history: dict[int, dict[str, ServiceStateRecord]] = {}
        self._last_timestamp: int | None = None
        self._topology_fingerprint: str | None = None
        self._topology_snapshot_id: str | None = None
        self._graph: TopologyGraph | None = None
        self._model_snapshot_id = _fingerprint({"empty": True, "config": self.config_fingerprint})
        self._archived_services: list[str] = []

    def _graph_fingerprint(self, graph: TopologyGraph) -> str:
        relations = [
            (item.relation_id, item.relation_type, item.src_service_id, item.dst_service_id,
             item.symmetric, item.detail)
            for group in (graph.impact_edges, graph.host_relations, graph.resource_relations)
            for item in group
        ]
        return _fingerprint({"services": graph.service_ids, "relations": relations,
                             "lags": self.config.service_lags})

    def _parent_metadata(self, graph: TopologyGraph, target: str):
        parents: dict[str, dict[str, set[str]]] = {
            target: {"ids": {f"{target}::self"}, "types": {"self"}}
        }

        def add(parent: str, relation_id: str, relation_type: str) -> None:
            entry = parents.setdefault(parent, {"ids": set(), "types": set()})
            entry["ids"].add(relation_id)
            entry["types"].add(relation_type)

        if self.config.include_impact_parents:
            for relation in graph.impact_edges:
                if relation.dst_service_id == target:
                    add(relation.src_service_id, relation.relation_id, "impact")
        for enabled, relations, relation_type in (
            (self.config.include_host_parents, graph.host_relations, "host"),
            (self.config.include_resource_parents, graph.resource_relations, "resource"),
        ):
            if not enabled:
                continue
            for relation in relations:
                if relation.src_service_id == target:
                    add(relation.dst_service_id, relation.relation_id, relation_type)
                elif relation.symmetric and relation.dst_service_id == target:
                    add(relation.src_service_id, relation.relation_id, relation_type)
                elif not relation.symmetric and relation.dst_service_id == target:
                    add(relation.src_service_id, relation.relation_id, relation_type)
        keys = [ServiceFeatureKey(parent, lag) for parent in sorted(parents)
                for lag in self.config.service_lags]
        relation_ids = {parent: sorted(value["ids"]) for parent, value in parents.items()}
        relation_types = {parent: sorted(value["types"]) for parent, value in parents.items()}
        return keys, relation_ids, relation_types

    def reconcile_topology(self, snapshot: TopologySnapshot) -> bool:
        graph = build_topology_graph(snapshot, self.impact_derivation_rules,
                                     self.allow_cross_namespace)
        fingerprint = self._graph_fingerprint(graph)
        if fingerprint == self._topology_fingerprint:
            self._topology_snapshot_id = snapshot.snapshot_id
            self._graph = graph
            return False
        old_models = self._models
        models: dict[str, TargetModelState] = {}
        gamma = self.config.rls_initial_covariance
        for target in graph.service_ids:
            keys, relation_ids, relation_types = self._parent_metadata(graph, target)
            dimension = len(keys)
            theta = np.zeros(dimension, dtype=float)
            covariance = np.eye(dimension, dtype=float) * gamma
            update_count = prediction_count = 0
            last_update = None
            if target in old_models:
                old = old_models[target]
                old_index = {key: index for index, key in enumerate(old.feature_keys)}
                new_index = {key: index for index, key in enumerate(keys)}
                shared = sorted(set(old_index) & set(new_index))
                for key in shared:
                    theta[new_index[key]] = old.theta[old_index[key]]
                for left in shared:
                    for right in shared:
                        covariance[new_index[left], new_index[right]] = old.covariance[
                            old_index[left], old_index[right]
                        ]
                update_count = old.update_count
                prediction_count = old.prediction_count
                last_update = old.last_update_timestamp
            models[target] = TargetModelState(
                target, keys, relation_ids, relation_types, theta, covariance,
                update_count, prediction_count, last_update, False, 0, False,
            )
        self._archived_services = sorted(set(old_models) - set(models))
        self._models = models
        self._graph = graph
        self._topology_fingerprint = fingerprint
        self._topology_snapshot_id = snapshot.snapshot_id
        self._model_snapshot_id = _fingerprint({"topology": fingerprint, "config": self.config_fingerprint})
        return True

    def feature_keys(self, target_service_id: str) -> list[ServiceFeatureKey]:
        return list(self._models[target_service_id].feature_keys)

    def active_service_ids(self) -> list[str]:
        return sorted(self._models)

    def archived_service_ids(self) -> list[str]:
        return list(self._archived_services)

    def model_covariance(self, target_service_id: str) -> np.ndarray:
        return self._models[target_service_id].covariance.copy()

    def model_state(self, target_service_id: str) -> TargetModelState:
        state = self._models[target_service_id]
        return TargetModelState(
            state.target_service_id, list(state.feature_keys),
            {key: list(value) for key, value in state.relation_ids.items()},
            {key: list(value) for key, value in state.relation_types.items()},
            state.theta.copy(), state.covariance.copy(), state.update_count,
            state.prediction_count, state.last_update_timestamp, state.ready,
            state.reconfigure_update_count, state.frozen,
        )

    def _validate_window(self, states: list[ServiceStateRecord]) -> tuple[int, dict[str, ServiceStateRecord]]:
        if not states:
            raise ValueError("service_states must not be empty")
        if any(not isinstance(item, ServiceStateRecord) for item in states):
            raise TypeError("service_states must contain ServiceStateRecord")
        timestamps = {item.timestamp_ns for item in states}
        windows = {(item.window_start_ns, item.window_end_ns) for item in states}
        if len(timestamps) != 1 or len(windows) != 1:
            raise PropagationTimeError("all service states must describe one window")
        timestamp = next(iter(timestamps))
        start, end = next(iter(windows))
        if end - start != self.window_ns or timestamp != end:
            raise PropagationTimeError("service state window does not match learner window_sec")
        indexed: dict[str, ServiceStateRecord] = {}
        for item in states:
            existing = indexed.get(item.service_id)
            if existing is not None and existing != item:
                raise PropagationTimeError(f"conflicting duplicate state for {item.service_id}")
            indexed[item.service_id] = item
        return timestamp, indexed

    def _feature_vector(self, model: TargetModelState, timestamp: int):
        values: list[float] = []
        for key in model.feature_keys:
            history_timestamp = timestamp - key.lag * self.window_ns
            if history_timestamp not in self._history:
                return None, "insufficient_history"
            record = self._history[history_timestamp].get(key.parent_service_id)
            if record is None:
                return None, "missing_parent_state"
            if not record.baseline_ready:
                return None, "baseline_not_ready"
            if record.observation_quality < self.config.service_min_observation_quality:
                return None, "low_observation_quality"
            values.append(record.value)
        return np.asarray(values, dtype=float), None

    def _update(self, model: TargetModelState, phi: np.ndarray, actual: float, timestamp: int) -> None:
        covariance_phi = model.covariance @ phi
        denominator = self.config.rls_forgetting_factor + float(phi @ covariance_phi)
        if not math.isfinite(denominator) or denominator <= NUMERICAL_EPSILON:
            raise NumericalPropagationError(
                f"target={model.target_service_id} timestamp={timestamp} invalid denominator={denominator}"
            )
        gain = covariance_phi / denominator
        error = actual - float(phi @ model.theta)
        theta = model.theta + gain * error
        covariance = ((np.eye(len(phi)) - np.outer(gain, phi)) @ model.covariance
                      / self.config.rls_forgetting_factor)
        covariance = 0.5 * (covariance + covariance.T)
        if not (np.all(np.isfinite(gain)) and math.isfinite(error)
                and np.all(np.isfinite(theta)) and np.all(np.isfinite(covariance))):
            raise NumericalPropagationError(
                f"target={model.target_service_id} timestamp={timestamp} non-finite RLS state"
            )
        model.theta = theta
        model.covariance = covariance
        model.update_count += 1
        model.reconfigure_update_count += 1
        model.last_update_timestamp = timestamp

    def process_window(self, service_states: Iterable[ServiceStateRecord], update_gate: UpdateGate,
                       topology_snapshot: TopologySnapshot) -> ServicePropagationWindowResult:
        states = list(service_states)
        timestamp, current = self._validate_window(states)
        if not isinstance(update_gate, UpdateGate):
            raise TypeError("update_gate must be UpdateGate")
        if not isinstance(topology_snapshot, TopologySnapshot):
            raise TypeError("topology_snapshot must be TopologySnapshot")
        if not topology_snapshot.valid_from_ns <= timestamp < topology_snapshot.valid_to_ns:
            raise ValueError("topology snapshot is not valid at service-state timestamp")
        if self._last_timestamp is not None and timestamp <= self._last_timestamp:
            raise PropagationTimeError("online service-state windows must be strictly increasing")
        issues: list[PropagationIssue] = []
        if self._last_timestamp is not None:
            gap = (timestamp - self._last_timestamp) // self.window_ns
            if gap > self.config.service_max_gap_windows:
                self._history.clear()
                for model in self._models.values():
                    model.ready = False
                issues.append(PropagationIssue("*", "history_gap", f"gap_windows={gap}"))
        reconfigured = self.reconcile_topology(topology_snapshot)
        if reconfigured:
            issues.append(PropagationIssue("*", "topology_reconfigured", topology_snapshot.snapshot_id))
        unexpected = sorted(set(current) - set(self._models))
        if unexpected:
            raise ValueError(
                f"service states are absent from topology={topology_snapshot.snapshot_id}: {unexpected}"
            )
        predictions: list[ServicePropagationPrediction] = []
        for target in sorted(self._models):
            model = self._models[target]
            if not np.all(np.isfinite(model.theta)) or not np.all(np.isfinite(model.covariance)):
                raise NumericalPropagationError(
                    f"target={target} timestamp={timestamp} contains non-finite model state"
                )
            target_record = current.get(target)
            if target_record is None:
                model.ready = False
                issues.append(PropagationIssue(target, "missing_target", "current target state is absent"))
                continue
            phi, feature_reason = self._feature_vector(model, timestamp)
            if phi is None:
                model.ready = False
                issues.append(PropagationIssue(target, feature_reason or "insufficient_history", "lag feature is absent"))
                continue
            contributions = [
                ServicePropagationContribution(
                    key.parent_service_id, target, key.lag, float(coefficient), float(parent_value),
                    float(coefficient * parent_value), max(float(coefficient), 0.0),
                    list(model.relation_ids[key.parent_service_id]),
                    list(model.relation_types[key.parent_service_id]),
                )
                for key, coefficient, parent_value in zip(model.feature_keys, model.theta, phi)
            ]
            predicted = float(sum(item.contribution_value for item in contributions))
            if not math.isfinite(predicted):
                raise NumericalPropagationError(f"target={target} timestamp={timestamp} non-finite prediction")
            model.prediction_count += 1
            can_update = True
            reason = None
            if not update_gate.update_service_model:
                can_update, reason = False, "update_gate_closed"
            elif not update_gate.baseline_ready or not target_record.baseline_ready:
                can_update, reason = False, "baseline_not_ready"
            elif target_record.observation_quality < self.config.service_min_observation_quality:
                can_update, reason = False, "low_observation_quality"
            model.frozen = not update_gate.update_service_model
            if can_update:
                self._update(model, phi, target_record.value, timestamp)
            model.ready = bool(
                model.update_count >= self.config.service_min_updates
                and model.reconfigure_update_count >= self.config.topology_reconfigure_min_updates
                and np.all(np.isfinite(model.theta)) and np.all(np.isfinite(model.covariance))
            )
            predictions.append(ServicePropagationPrediction(
                PROBERCA_SCHEMA_VERSION, "service_propagation_prediction", timestamp,
                target_record.cluster_id, sorted({item.namespace for item in states}),
                topology_snapshot.snapshot_id, self._model_snapshot_id, target,
                predicted, target_record.value, target_record.value - predicted,
                model.ready, model.frozen, can_update, reason,
                target_record.source_alert_state, target_record.observation_quality,
                contributions, self.config_fingerprint,
            ))
        self._history[timestamp] = current
        keep_after = timestamp - max(self.config.service_lags) * self.window_ns
        self._history = {key: value for key, value in self._history.items() if key >= keep_after}
        self._last_timestamp = timestamp
        return ServicePropagationWindowResult(timestamp, predictions, issues, reconfigured)

    def predict_window(self, service_states: Iterable[ServiceStateRecord], update_gate: UpdateGate,
                       topology_snapshot: TopologySnapshot) -> ServicePropagationWindowResult:
        if update_gate.update_service_model:
            raise ValueError("predict_window requires a closed service-model update gate")
        return self.process_window(service_states, update_gate, topology_snapshot)

    def process_replay(self, batches):
        """Replay through the online entry after one explicit deterministic reorder."""
        prepared = list(batches)
        if any(not isinstance(item, tuple) or len(item) != 3 for item in prepared):
            raise TypeError("replay batches must be (service_states, update_gate, topology_snapshot) tuples")
        timestamps = [self._validate_window(list(item[0]))[0] for item in prepared]
        ordered = [item for _, item in sorted(zip(timestamps, prepared), key=lambda pair: pair[0])]
        reordered = timestamps != sorted(timestamps)
        return [dataclass_replace(self.process_window(*item), reordered=reordered) for item in ordered]

    def export_sparse_coefficients(self) -> list[ServicePropagationCoefficient]:
        output: list[ServicePropagationCoefficient] = []
        for target in sorted(self._models):
            model = self._models[target]
            for key, coefficient in zip(model.feature_keys, model.theta):
                output.append(ServicePropagationCoefficient(
                    target, key.parent_service_id, key.lag, float(coefficient),
                    max(float(coefficient), 0.0), list(model.relation_ids[key.parent_service_id]),
                    list(model.relation_types[key.parent_service_id]), model.update_count, model.ready,
                ))
        return output

    def export_dense_matrices(self):
        services = sorted(self._models)
        service_index = {service: index for index, service in enumerate(services)}
        lag_index = {lag: index for index, lag in enumerate(self.config.service_lags)}
        dense = np.zeros((len(self.config.service_lags), len(services), len(services)), dtype=float)
        for item in self.export_sparse_coefficients():
            dense[lag_index[item.lag], service_index[item.target_service_id],
                  service_index[item.parent_service_id]] = item.coefficient
        return dense, services, list(self.config.service_lags)

    def snapshot(self, directory) -> None:
        """Save strict JSON metadata and NPZ numeric arrays without object serialization."""
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        services = sorted(self._models)
        model_metadata = []
        arrays: dict[str, np.ndarray] = {}
        for index, service_id in enumerate(services):
            model = self._models[service_id]
            arrays[f"theta_{index}"] = model.theta
            arrays[f"covariance_{index}"] = model.covariance
            model_metadata.append({
                "target_service_id": service_id,
                "feature_keys": [asdict(item) for item in model.feature_keys],
                "relation_ids": model.relation_ids,
                "relation_types": model.relation_types,
                "update_count": model.update_count,
                "prediction_count": model.prediction_count,
                "last_update_timestamp": model.last_update_timestamp,
                "ready": model.ready,
                "reconfigure_update_count": model.reconfigure_update_count,
                "frozen": model.frozen,
            })
        history = [
            {"timestamp_ns": timestamp,
             "records": [record.to_dict() for _, record in sorted(records.items())]}
            for timestamp, records in sorted(self._history.items())
        ]
        metadata = {
            "format_version": MODEL_FORMAT_VERSION,
            "schema_version": PROBERCA_SCHEMA_VERSION,
            "config_fingerprint": self.config_fingerprint,
            "topology_fingerprint": self._topology_fingerprint,
            "topology_snapshot_id": self._topology_snapshot_id,
            "model_snapshot_id": self._model_snapshot_id,
            "last_timestamp": self._last_timestamp,
            "archived_services": self._archived_services,
            "models": model_metadata,
            "history": history,
        }
        (path / "metadata.json").write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        np.savez_compressed(path / "arrays.npz", **arrays)

    @classmethod
    def restore(cls, directory, config: PropagationConfig, window_sec: int,
                impact_derivation_rules: list[ImpactDerivationRule],
                allow_cross_namespace: bool,
                topology_snapshot: TopologySnapshot | None = None):
        path = Path(directory)
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        required = {
            "format_version", "schema_version", "config_fingerprint", "topology_fingerprint",
            "topology_snapshot_id", "model_snapshot_id", "last_timestamp", "archived_services",
            "models", "history",
        }
        if set(metadata) != required:
            raise ValueError("invalid service propagation snapshot fields")
        if (metadata["format_version"] != MODEL_FORMAT_VERSION
                or metadata["schema_version"] != PROBERCA_SCHEMA_VERSION):
            raise ValueError("incompatible service propagation snapshot version")
        result = cls(config, window_sec, impact_derivation_rules, allow_cross_namespace)
        if metadata["config_fingerprint"] != result.config_fingerprint:
            raise ValueError("service propagation snapshot configuration mismatch")
        if topology_snapshot is not None:
            graph = build_topology_graph(topology_snapshot, impact_derivation_rules,
                                         allow_cross_namespace)
            if result._graph_fingerprint(graph) != metadata["topology_fingerprint"]:
                raise TopologyModelMismatchError(
                    f"snapshot={topology_snapshot.snapshot_id} requires explicit topology reconciliation"
                )
            result._graph = graph
        arrays = np.load(path / "arrays.npz", allow_pickle=False)
        models: dict[str, TargetModelState] = {}
        for index, item in enumerate(metadata["models"]):
            expected = {
                "target_service_id", "feature_keys", "relation_ids", "relation_types",
                "update_count", "prediction_count", "last_update_timestamp", "ready",
                "reconfigure_update_count", "frozen",
            }
            if set(item) != expected:
                raise ValueError("invalid target model snapshot fields")
            theta = np.asarray(arrays[f"theta_{index}"], dtype=float)
            covariance = np.asarray(arrays[f"covariance_{index}"], dtype=float)
            keys = [ServiceFeatureKey(**key) for key in item["feature_keys"]]
            if theta.shape != (len(keys),) or covariance.shape != (len(keys), len(keys)):
                raise ValueError("service propagation snapshot array shape mismatch")
            if not np.all(np.isfinite(theta)) or not np.all(np.isfinite(covariance)):
                raise ValueError("service propagation snapshot contains non-finite arrays")
            service_id = item["target_service_id"]
            models[service_id] = TargetModelState(
                service_id, keys,
                {key: list(value) for key, value in item["relation_ids"].items()},
                {key: list(value) for key, value in item["relation_types"].items()},
                theta.copy(), covariance.copy(), int(item["update_count"]),
                int(item["prediction_count"]), item["last_update_timestamp"], bool(item["ready"]),
                int(item["reconfigure_update_count"]), bool(item["frozen"]),
            )
        history: dict[int, dict[str, ServiceStateRecord]] = {}
        for entry in metadata["history"]:
            if set(entry) != {"timestamp_ns", "records"}:
                raise ValueError("invalid service propagation history fields")
            records = [ServiceStateRecord.from_dict(item) for item in entry["records"]]
            history[int(entry["timestamp_ns"])] = {item.service_id: item for item in records}
        result._models = models
        result._history = history
        result._last_timestamp = metadata["last_timestamp"]
        result._topology_fingerprint = metadata["topology_fingerprint"]
        result._topology_snapshot_id = metadata["topology_snapshot_id"]
        result._model_snapshot_id = metadata["model_snapshot_id"]
        result._archived_services = list(metadata["archived_services"])
        return result

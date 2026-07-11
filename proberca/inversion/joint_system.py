"""Canonical construction and strict persistence of P6 joint systems."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np
from scipy import sparse

from proberca.config import ProbeRCAConfig
from proberca.data.schema import AlertEvent, CandidateSubgraph, EdgeAnomalyRecord, NodeAnomalyRecord
from proberca.propagation.metric_model import MetricPropagationModelInfo, MetricPropagationPrediction

from .contracts import (
    CandidateModelMismatchError,
    DictionaryOverflowError,
    JointInversionSystem,
    JointSystemSerializationError,
    NodeVariableRef,
    PropagationVariableRef,
    ResidualNotReadyError,
    ResidualRowRef,
    ShockVariableRef,
    SignalKindMismatchError,
    P5_METRIC_SIGNAL_KIND,
)
from .node_dictionary import build_node_dictionary
from .propagation_dictionary import build_propagation_dictionary
from .residuals import build_residuals
from .shock_dictionary import build_shock_dictionary


JOINT_FORMAT_VERSION = "1"


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _fingerprint(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _matrix_payload(matrix) -> dict:
    canonical = matrix.tocsc()
    return {
        "shape": list(canonical.shape),
        "indices": canonical.indices.tolist(),
        "indptr": canonical.indptr.tolist(),
        "data": canonical.data.tolist(),
    }


def _structure_fingerprint(candidate_id, model_snapshot_id, config_fingerprint, signal_kind,
                           node_rows, edge_rows, node_variables, propagation_variables,
                           shock_variables, U, X_prop, X_shock):
    return _fingerprint({
        "candidate_id": candidate_id,
        "model_snapshot_id": model_snapshot_id,
        "config_fingerprint": config_fingerprint,
        "signal_kind": signal_kind,
        "row_refs": [asdict(item) for item in [*node_rows, *edge_rows]],
        "node_variables": [asdict(item) for item in node_variables],
        "propagation_variables": [asdict(item) for item in propagation_variables],
        "shock_variables": [asdict(item) for item in shock_variables],
        "U": _matrix_payload(U),
        "X_prop": _matrix_payload(X_prop),
        "X_shock": _matrix_payload(X_shock),
    })


def _validate_inputs(alert, candidate, model_info, predictions, node_anomalies, edge_anomalies, config):
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    if not isinstance(alert, AlertEvent) or not isinstance(candidate, CandidateSubgraph):
        raise TypeError("joint builder requires AlertEvent and CandidateSubgraph")
    if alert.state != "hard" or candidate.alert_state != "hard":
        raise ResidualNotReadyError(f"alert={alert.alert_id} formal P6 requires hard state")
    if candidate.alert_id != alert.alert_id or candidate.alert_timestamp_ns != alert.timestamp_ns:
        raise CandidateModelMismatchError(
            f"alert={alert.alert_id} candidate={candidate.candidate_id} alert alignment mismatch"
        )
    if not candidate.rca_eligible:
        raise ResidualNotReadyError(f"candidate={candidate.candidate_id} is not RCA eligible")
    if not isinstance(model_info, MetricPropagationModelInfo):
        raise TypeError("metric_model_info must be MetricPropagationModelInfo")
    if model_info.candidate_id != candidate.candidate_id:
        raise CandidateModelMismatchError(
            f"candidate={candidate.candidate_id} model_candidate={model_info.candidate_id}"
        )
    if model_info.lifecycle_state != "FROZEN" or not model_info.frozen or not model_info.global_ready:
        raise ResidualNotReadyError(
            f"alert={alert.alert_id} candidate={candidate.candidate_id} metric model is not frozen and ready"
        )
    if P5_METRIC_SIGNAL_KIND != config.residual.signal_kind:
        raise SignalKindMismatchError(
            f"model={model_info.model_snapshot_id} signal={P5_METRIC_SIGNAL_KIND}"
        )
    if any(not isinstance(item, MetricPropagationPrediction) for item in predictions):
        raise TypeError("metric_predictions must contain MetricPropagationPrediction")
    if any(not isinstance(item, NodeAnomalyRecord) for item in node_anomalies):
        raise TypeError("current_node_anomalies must contain NodeAnomalyRecord")
    if any(not isinstance(item, EdgeAnomalyRecord) for item in edge_anomalies):
        raise TypeError("current_edge_anomalies must contain EdgeAnomalyRecord")
    for item in predictions:
        if (
            item.candidate_id != candidate.candidate_id
            or item.topology_snapshot_id != candidate.topology_snapshot_id
            or item.model_snapshot_id != model_info.model_snapshot_id
            or item.alert_id != alert.alert_id
        ):
            raise CandidateModelMismatchError(
                f"prediction target={item.target_node_id} conflicts with alert/candidate/model/topology"
            )
    all_records = [*node_anomalies, *edge_anomalies]
    if any(item.timestamp_ns != alert.timestamp_ns for item in all_records):
        raise CandidateModelMismatchError(f"alert={alert.alert_id} current anomaly timestamp mismatch")
    if any(item.cluster_id != candidate.cluster_id or item.namespace not in candidate.namespace_scope
           for item in all_records):
        raise CandidateModelMismatchError(f"candidate={candidate.candidate_id} cluster/namespace mismatch")
    if any(item.signal_kind != P5_METRIC_SIGNAL_KIND for item in all_records):
        raise SignalKindMismatchError(f"candidate={candidate.candidate_id} current anomaly signal mismatch")


def build_joint_inversion_system(*, alert_event, candidate_subgraph, metric_model_info,
                                 metric_predictions, current_node_anomalies,
                                 current_edge_anomalies, config) -> JointInversionSystem:
    started = time.perf_counter()
    predictions = list(metric_predictions)
    node_anomalies = list(current_node_anomalies)
    edge_anomalies = list(current_edge_anomalies)
    _validate_inputs(alert_event, candidate_subgraph, metric_model_info, predictions,
                     node_anomalies, edge_anomalies, config)
    residual = build_residuals(
        candidate_subgraph, predictions, node_anomalies, edge_anomalies,
        alert_event.timestamp_ns,
    )
    row_count = len(residual["joint_residual"])
    if row_count > config.residual.max_joint_rows:
        raise DictionaryOverflowError(
            f"alert={alert_event.alert_id} joint rows={row_count} limit={config.residual.max_joint_rows}"
        )
    U, node_variables = build_node_dictionary(residual["node_ids"], len(residual["edge_ids"]))
    X_prop, propagation_variables = build_propagation_dictionary(
        residual["node_ids"], len(residual["edge_ids"]), residual["prediction_index"],
        metric_model_info.model_snapshot_id, config.propagation_dictionary,
    )
    if len(propagation_variables) > config.residual.max_propagation_variables:
        raise DictionaryOverflowError(
            f"alert={alert_event.alert_id} propagation variables={len(propagation_variables)} "
            f"limit={config.residual.max_propagation_variables}"
        )
    node_index = {item.node_id: item for item in node_anomalies}
    X_shock, shock_variables = build_shock_dictionary(
        candidate_subgraph, residual["node_ids"], residual["edge_ids"],
        node_index, residual["edge_index"], config.shock_projection_templates,
    )
    if len(shock_variables) > config.residual.max_shock_variables:
        raise DictionaryOverflowError(
            f"alert={alert_event.alert_id} shock variables={len(shock_variables)} "
            f"limit={config.residual.max_shock_variables}"
        )
    config_fingerprint = _fingerprint({
        "residual": asdict(config.residual),
        "propagation_dictionary": asdict(config.propagation_dictionary),
        "shock_projection_templates": [asdict(item) for item in config.shock_projection_templates],
    })
    structure_fingerprint = _structure_fingerprint(
        candidate_subgraph.candidate_id, metric_model_info.model_snapshot_id,
        config_fingerprint, P5_METRIC_SIGNAL_KIND,
        residual["node_rows"], residual["edge_rows"], node_variables,
        propagation_variables, shock_variables, U, X_prop, X_shock,
    )
    system_id = _fingerprint({
        "alert_id": alert_event.alert_id, "timestamp_ns": alert_event.timestamp_ns,
        "structure_fingerprint": structure_fingerprint,
    })
    return JointInversionSystem(
        "1.0", "joint_inversion_system", system_id, alert_event.alert_id,
        candidate_subgraph.candidate_id, candidate_subgraph.topology_snapshot_id,
        metric_model_info.model_snapshot_id, alert_event.timestamp_ns,
        P5_METRIC_SIGNAL_KIND, residual["node_rows"], residual["edge_rows"],
        node_variables, propagation_variables, shock_variables,
        residual["actual"], residual["predicted"], residual["node_residual"],
        residual["edge_residual"], residual["joint_residual"],
        residual["node_quality"], residual["edge_quality"],
        residual["source_prediction_ids"], residual["source_anomaly_record_ids"],
        U, X_prop, X_shock,
        list(U.shape), list(X_prop.shape), list(X_shock.shape), U.nnz, X_prop.nnz,
        X_shock.nnz, config_fingerprint, structure_fingerprint, True, [],
        (time.perf_counter() - started) * 1000.0,
    )


def save_joint_inversion_system(path, system: JointInversionSystem) -> None:
    if not isinstance(system, JointInversionSystem):
        raise TypeError("system must be JointInversionSystem")
    output = Path(path)
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "format_version": JOINT_FORMAT_VERSION,
        "schema_version": system.schema_version,
        "record_type": system.record_type,
        "system_id": system.system_id,
        "alert_id": system.alert_id,
        "candidate_id": system.candidate_id,
        "topology_snapshot_id": system.topology_snapshot_id,
        "metric_model_snapshot_id": system.metric_model_snapshot_id,
        "timestamp_ns": system.timestamp_ns,
        "signal_kind": system.signal_kind,
        "node_row_refs": [asdict(item) for item in system.node_row_refs],
        "edge_row_refs": [asdict(item) for item in system.edge_row_refs],
        "node_variable_refs": [asdict(item) for item in system.node_variable_refs],
        "propagation_variable_refs": [asdict(item) for item in system.propagation_variable_refs],
        "shock_variable_refs": [asdict(item) for item in system.shock_variable_refs],
        "source_prediction_ids": system.source_prediction_ids,
        "source_anomaly_record_ids": system.source_anomaly_record_ids,
        "U_shape": system.U_shape, "X_prop_shape": system.X_prop_shape,
        "X_shock_shape": system.X_shock_shape, "U_nnz": system.U_nnz,
        "X_prop_nnz": system.X_prop_nnz, "X_shock_nnz": system.X_shock_nnz,
        "config_fingerprint": system.config_fingerprint,
        "structure_fingerprint": system.structure_fingerprint,
        "solver_eligible": system.solver_eligible,
        "quality_issues": system.quality_issues,
        "build_duration_ms": system.build_duration_ms,
    }
    (output / "metadata.json").write_bytes(_canonical(metadata))
    np.savez(
        output / "vectors.npz", actual_node_values=system.actual_node_values,
        predicted_node_values=system.predicted_node_values, node_residual=system.node_residual,
        edge_residual=system.edge_residual, joint_residual=system.joint_residual,
        node_observation_quality=system.node_observation_quality,
        edge_observation_quality=system.edge_observation_quality,
    )
    sparse.save_npz(output / "U.npz", system.U)
    sparse.save_npz(output / "X_prop.npz", system.X_prop)
    sparse.save_npz(output / "X_shock.npz", system.X_shock)


def load_joint_inversion_system(path, *, expected_alert_id=None, expected_candidate_id=None,
                                expected_model_snapshot_id=None,
                                expected_topology_snapshot_id=None,
                                expected_config_fingerprint=None) -> JointInversionSystem:
    source = Path(path)
    required = ["metadata.json", "vectors.npz", "U.npz", "X_prop.npz", "X_shock.npz"]
    if any(not (source / name).is_file() for name in required):
        raise JointSystemSerializationError(f"joint system is missing files at {source}")
    try:
        metadata = json.loads((source / "metadata.json").read_text(encoding="utf-8"))
        if metadata.get("format_version") != JOINT_FORMAT_VERSION:
            raise JointSystemSerializationError("joint system format version mismatch")
        checks = {
            "alert_id": expected_alert_id,
            "candidate_id": expected_candidate_id,
            "metric_model_snapshot_id": expected_model_snapshot_id,
            "topology_snapshot_id": expected_topology_snapshot_id,
            "config_fingerprint": expected_config_fingerprint,
        }
        for name, expected in checks.items():
            if expected is not None and metadata.get(name) != expected:
                raise JointSystemSerializationError(f"joint system {name} mismatch")
        with np.load(source / "vectors.npz", allow_pickle=False) as vectors:
            arrays = {name: vectors[name].copy() for name in vectors.files}
        U = sparse.load_npz(source / "U.npz").tocsr()
        X_prop = sparse.load_npz(source / "X_prop.npz").tocsc()
        X_shock = sparse.load_npz(source / "X_shock.npz").tocsc()
        system = JointInversionSystem(
            metadata["schema_version"], metadata["record_type"], metadata["system_id"],
            metadata["alert_id"], metadata["candidate_id"], metadata["topology_snapshot_id"],
            metadata["metric_model_snapshot_id"], metadata["timestamp_ns"], metadata["signal_kind"],
            [ResidualRowRef(**item) for item in metadata["node_row_refs"]],
            [ResidualRowRef(**item) for item in metadata["edge_row_refs"]],
            [NodeVariableRef(**item) for item in metadata["node_variable_refs"]],
            [PropagationVariableRef(**item) for item in metadata["propagation_variable_refs"]],
            [ShockVariableRef(**item) for item in metadata["shock_variable_refs"]],
            arrays["actual_node_values"], arrays["predicted_node_values"],
            arrays["node_residual"], arrays["edge_residual"], arrays["joint_residual"],
            arrays["node_observation_quality"], arrays["edge_observation_quality"],
            metadata["source_prediction_ids"], metadata["source_anomaly_record_ids"],
            U, X_prop, X_shock, metadata["U_shape"], metadata["X_prop_shape"],
            metadata["X_shock_shape"], metadata["U_nnz"], metadata["X_prop_nnz"],
            metadata["X_shock_nnz"], metadata["config_fingerprint"],
            metadata["structure_fingerprint"], metadata["solver_eligible"],
            metadata["quality_issues"], metadata["build_duration_ms"],
        )
        fingerprint = _structure_fingerprint(
            system.candidate_id, system.metric_model_snapshot_id, system.config_fingerprint,
            system.signal_kind, system.node_row_refs, system.edge_row_refs,
            system.node_variable_refs, system.propagation_variable_refs,
            system.shock_variable_refs, system.U, system.X_prop, system.X_shock,
        )
        if fingerprint != system.structure_fingerprint:
            raise JointSystemSerializationError("joint system structure fingerprint mismatch")
        return system
    except JointSystemSerializationError:
        raise
    except Exception as error:
        raise JointSystemSerializationError(f"failed to load joint system: {error}") from error

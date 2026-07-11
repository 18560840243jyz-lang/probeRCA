"""P2 anomaly handoff and exact P6 residual alignment."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict

import numpy as np

from proberca.alerting import UpdateGate
from proberca.baseline import AnomalyScore
from proberca.config import BaselineConfig, MetricSignalSpec
from proberca.data.schema import (
    EdgeAnomalyRecord,
    EdgeMetricRecord,
    PROBERCA_SCHEMA_VERSION,
)

from .contracts import (
    MissingEdgeResidualError,
    MissingNodeResidualError,
    ResidualAlignmentError,
    ResidualRowRef,
    SignalKindMismatchError,
)


def _fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def edge_anomaly_from_p2(source_metric_record, anomaly_score: AnomalyScore,
                         update_gate: UpdateGate, signal_spec: MetricSignalSpec,
                         baseline_config: BaselineConfig, baseline_window_sec: int,
                         alert_state: str) -> EdgeAnomalyRecord:
    if not isinstance(source_metric_record, EdgeMetricRecord):
        raise TypeError("edge anomaly source must be EdgeMetricRecord")
    if not isinstance(anomaly_score, AnomalyScore):
        raise TypeError("anomaly_score must be P2 AnomalyScore")
    if not isinstance(update_gate, UpdateGate) or not isinstance(signal_spec, MetricSignalSpec):
        raise TypeError("edge anomaly handoff requires P2 gate and exact signal spec")
    if not isinstance(baseline_config, BaselineConfig) or isinstance(baseline_window_sec, bool) \
            or not isinstance(baseline_window_sec, int) or baseline_window_sec <= 0:
        raise TypeError("edge anomaly handoff requires baseline metadata")
    if anomaly_score.coverage is None or anomaly_score.event_loss_rate is None:
        raise ValueError("P2 anomaly score is missing quality metadata")
    if (
        anomaly_score.record_type != "edge_metric"
        or anomaly_score.stable_id != source_metric_record.stable_id
        or anomaly_score.edge_id != source_metric_record.stable_id.rsplit("::", 1)[0]
        or anomaly_score.metric_name != source_metric_record.metric_name
    ):
        raise ValueError("P2 edge anomaly score conflicts with source record")
    if (
        signal_spec.record_type != "edge_metric"
        or signal_spec.metric_name != source_metric_record.metric_name
        or signal_spec.protocol not in {None, source_metric_record.protocol}
        or signal_spec.aggregation_output_id != source_metric_record.stable_id
    ):
        raise ValueError("edge signal spec conflicts with source record")
    quality = anomaly_score.coverage * (1.0 - anomaly_score.event_loss_rate)
    shock_id = (
        f"{source_metric_record.cluster_id}::{source_metric_record.namespace}::"
        f"{source_metric_record.src_service}->{source_metric_record.dst_service}::"
        f"{source_metric_record.protocol}::shock::{source_metric_record.metric_name}"
    )
    return EdgeAnomalyRecord(
        schema_version=PROBERCA_SCHEMA_VERSION,
        timestamp_ns=source_metric_record.timestamp_ns,
        window_start_ns=source_metric_record.timestamp_ns - source_metric_record.window_sec * 1_000_000_000,
        window_end_ns=source_metric_record.timestamp_ns,
        cluster_id=source_metric_record.cluster_id,
        namespace=source_metric_record.namespace,
        src_service=source_metric_record.src_service,
        dst_service=source_metric_record.dst_service,
        protocol=source_metric_record.protocol,
        edge_metric_id=source_metric_record.stable_id,
        shock_id=shock_id,
        metric_name=source_metric_record.metric_name,
        signed_z=anomaly_score.signed_z,
        anomaly_score=anomaly_score.anomaly,
        baseline_ready=bool(update_gate.baseline_ready),
        observation_quality=quality,
        source_metric_record_id=source_metric_record.stable_id,
        baseline_config_fingerprint=_fingerprint({
            "config": asdict(baseline_config), "window_sec": baseline_window_sec,
        }),
        signal_spec_id=_fingerprint(asdict(signal_spec)),
        source_alert_state=alert_state,
    )


def build_residuals(candidate, predictions, node_anomalies, edge_anomalies, timestamp_ns):
    node_ids = sorted(candidate.candidate_node_ids)
    edge_ids = sorted(candidate.candidate_edge_metric_ids)
    prediction_index = {item.target_node_id: item for item in predictions}
    node_index = {item.node_id: item for item in node_anomalies}
    edge_index = {item.edge_metric_id: item for item in edge_anomalies}
    if len(prediction_index) != len(predictions) or len(node_index) != len(node_anomalies):
        raise ResidualAlignmentError("duplicate node prediction or anomaly")
    if len(edge_index) != len(edge_anomalies):
        raise ResidualAlignmentError("duplicate edge anomaly")
    missing_nodes = sorted(set(node_ids) - set(node_index))
    missing_predictions = sorted(set(node_ids) - set(prediction_index))
    if missing_nodes or missing_predictions:
        raise MissingNodeResidualError(
            f"candidate={candidate.candidate_id} timestamp={timestamp_ns} "
            f"missing_actual={missing_nodes} missing_prediction={missing_predictions}"
        )
    missing_edges = sorted(set(edge_ids) - set(edge_index))
    if missing_edges:
        raise MissingEdgeResidualError(
            f"candidate={candidate.candidate_id} timestamp={timestamp_ns} missing_edges={missing_edges}"
        )
    actual, predicted, node_quality, node_rows = [], [], [], []
    for row, node_id in enumerate(node_ids):
        anomaly = node_index[node_id]
        prediction = prediction_index[node_id]
        if anomaly.timestamp_ns != timestamp_ns or prediction.timestamp_ns != timestamp_ns:
            raise ResidualAlignmentError(f"timestamp mismatch for node={node_id}")
        if anomaly.signal_kind != "signed_z":
            raise SignalKindMismatchError(f"signal mismatch for node={node_id}")
        if not anomaly.baseline_ready:
            raise MissingNodeResidualError(f"node baseline is not ready for node={node_id}")
        if not prediction.available or not prediction.ready or not prediction.frozen or prediction.provisional:
            raise MissingNodeResidualError(f"formal prediction unavailable for node={node_id}")
        if abs(prediction.predicted_value - sum(item.contribution_value for item in prediction.contributions)) > 1e-10:
            raise ResidualAlignmentError(f"prediction contribution mismatch for node={node_id}")
        actual.append(anomaly.signed_z)
        predicted.append(prediction.predicted_value)
        node_quality.append(anomaly.observation_quality)
        node_rows.append(ResidualRowRef(
            row, "node", node_id, anomaly.observation_quality,
            anomaly.source_metric_record_id, timestamp_ns,
        ))
    edge_values, edge_quality, edge_rows = [], [], []
    for offset, edge_id in enumerate(edge_ids, start=len(node_ids)):
        anomaly = edge_index[edge_id]
        if anomaly.timestamp_ns != timestamp_ns or anomaly.signal_kind != "signed_z":
            raise ResidualAlignmentError(f"edge alignment mismatch for edge={edge_id}")
        if not anomaly.baseline_ready:
            raise MissingEdgeResidualError(f"edge baseline is not ready for edge={edge_id}")
        edge_values.append(anomaly.signed_z)
        edge_quality.append(anomaly.observation_quality)
        edge_rows.append(ResidualRowRef(
            offset, "edge", edge_id, anomaly.observation_quality,
            anomaly.source_metric_record_id, timestamp_ns,
        ))
    actual_array = np.asarray(actual, dtype=float)
    predicted_array = np.asarray(predicted, dtype=float)
    node_residual = actual_array - predicted_array
    edge_residual = np.asarray(edge_values, dtype=float)
    return {
        "node_ids": node_ids, "edge_ids": edge_ids,
        "node_rows": node_rows, "edge_rows": edge_rows,
        "actual": actual_array, "predicted": predicted_array,
        "node_residual": node_residual, "edge_residual": edge_residual,
        "joint_residual": np.concatenate((node_residual, edge_residual)),
        "node_quality": np.asarray(node_quality, dtype=float),
        "edge_quality": np.asarray(edge_quality, dtype=float),
        "prediction_index": prediction_index, "edge_index": edge_index,
        "source_prediction_ids": node_ids,
        "source_anomaly_record_ids": [node_index[node_id].source_metric_record_id for node_id in node_ids],
    }

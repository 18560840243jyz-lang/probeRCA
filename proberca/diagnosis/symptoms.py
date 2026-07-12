"""Metric-level propagated symptom identification from current P5 predictions."""

from __future__ import annotations

from .contracts import PropagatedSymptom, SymptomAlignmentError


def identify_propagated_symptoms(node_anomalies, predictions, primary_node_id, config):
    anomalies = {item.node_id: item for item in node_anomalies}
    if len(anomalies) != len(node_anomalies):
        raise SymptomAlignmentError("duplicate current node anomaly")
    output = []
    for prediction in sorted(predictions, key=lambda item: item.target_node_id):
        anomaly = anomalies.get(prediction.target_node_id)
        if anomaly is None:
            continue
        if anomaly.timestamp_ns != prediction.timestamp_ns:
            raise SymptomAlignmentError("symptom anomaly and prediction timestamp mismatch")
        if not prediction.available or not prediction.ready or not prediction.frozen \
                or prediction.provisional or prediction.predicted_value is None:
            continue
        actual_bad = max(anomaly.signed_z, 0.0)
        predicted_bad = max(prediction.predicted_value, 0.0)
        ratio = min(actual_bad, predicted_bad) / max(actual_bad, 1e-12) if actual_bad > 0 else 0.0
        if actual_bad < config.symptom_anomaly_threshold \
                or ratio < config.propagated_explained_ratio_threshold:
            continue
        if anomaly.node_id == primary_node_id and not config.include_root_node_as_symptom:
            continue
        output.append(PropagatedSymptom(
            anomaly.node_id, anomaly.service_id, anomaly.metric_name, anomaly.signed_z,
            prediction.predicted_value, anomaly.signed_z - prediction.predicted_value,
            anomaly.anomaly_score, ratio,
        ))
    return output

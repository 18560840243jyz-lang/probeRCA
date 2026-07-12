"""Offline-only label evaluator; this is the sole P10 IncidentLabel importer."""
from __future__ import annotations
import hashlib
from proberca.data.io import iter_records_jsonl
from proberca.data.schema import IncidentLabel

class ReplayEvaluationError(ValueError):
    """Labels or report alignment are invalid."""

def _f1(confusion):
    classes = sorted(set(confusion) | {key for row in confusion.values() for key in row})
    scores = []
    correct = 0
    for name in classes:
        tp = confusion.get(name, {}).get(name, 0)
        fp = sum(row.get(name, 0) for truth, row in confusion.items() if truth != name)
        fn = sum(value for pred, value in confusion.get(name, {}).items() if pred != name)
        scores.append(2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0)
        correct += tp
    total = sum(sum(row.values()) for row in confusion.values())
    return (sum(scores) / len(scores) if scores else 0.0,
            correct / total if total else 0.0)

class ReplayEvaluator:
    def load_labels(self, path, expected_sha256=None):
        if expected_sha256 is not None:
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != expected_sha256:
                raise ReplayEvaluationError("labels_file SHA-256 mismatch")
        labels = list(iter_records_jsonl(path))
        if any(not isinstance(item, IncidentLabel) for item in labels):
            raise ReplayEvaluationError("labels_file contains a non-IncidentLabel record")
        return labels

    def evaluate(self, reports, failures, labels):
        by_incident = {item.incident_id: item for item in reports}
        if len(by_incident) != len(reports):
            raise ReplayEvaluationError("duplicate report incident_id")
        counts = {"incident_count": len(labels), "evaluated_count": 0,
                  "failed_pipeline_count": len(failures), "ambiguous_count": 0,
                  "service_hit_1": 0, "service_hit_3": 0, "service_hit_5": 0,
                  "metric_hit_1": 0, "metric_hit_3": 0,
                  "edge_hit_1": 0, "edge_hit_3": 0, "unmatched_incidents": []}
        mode_confusion, subtype_confusion = {}, {}
        distribution = {"strong": 0, "weak": 0, "ambiguous": 0}
        latencies = []
        matched_report_ids = set()
        for label in labels:
            report = by_incident.get(label.incident_id)
            if report is None:
                temporal = [
                    item for item in reports
                    if label.start_ns <= item.alert.timestamp_ns < label.end_ns
                    and (item.report_fingerprint or item.incident_id) not in matched_report_ids
                ]
                if len(temporal) > 1:
                    raise ReplayEvaluationError(
                        f"label {label.incident_id} matches multiple reports by time")
                report = temporal[0] if temporal else None
            if report is None:
                counts["unmatched_incidents"].append(label.incident_id); continue
            matched_report_ids.add(report.report_fingerprint or report.incident_id)
            counts["evaluated_count"] += 1
            counts["ambiguous_count"] += report.primary_root.kind == "ambiguous"
            status = report.quality.get("diagnosis_status", "ambiguous")
            if status in distribution:
                distribution[status] += 1
            predicted_mode = ("ambiguous" if report.primary_root.kind == "ambiguous"
                              else report.primary_root.fault_mode)
            mode_confusion.setdefault(label.fault_mode, {}).setdefault(predicted_mode, 0)
            mode_confusion[label.fault_mode][predicted_mode] += 1
            truth_subtype, predicted_subtype = label.edge_subtype or "none", \
                report.primary_root.edge_subtype or "none"
            subtype_confusion.setdefault(truth_subtype, {}).setdefault(predicted_subtype, 0)
            subtype_confusion[truth_subtype][predicted_subtype] += 1
            latencies.append(report.generated_at_ns - report.alert.timestamp_ns)
            for k in (1, 3, 5):
                if label.root_service and any(
                    item.get("service_name") == label.root_service or
                    item.get("service_id") == label.root_service
                    for item in report.ranked_candidates[:k]):
                    counts[f"service_hit_{k}"] += 1
            for k in (1, 3):
                if label.root_metric and any(item.get("metric_name") == label.root_metric
                                             for item in report.ranked_candidates[:k]):
                    counts[f"metric_hit_{k}"] += 1
                if label.root_edge and any(item.get("edge_id") == label.root_edge
                                           for item in report.ranked_candidates[:k]):
                    counts[f"edge_hit_{k}"] += 1
        counts["denominators"] = {
            "service": sum(item.root_service is not None for item in labels),
            "metric": sum(item.root_metric is not None for item in labels),
            "edge": sum(item.root_edge is not None for item in labels)}
        mode_macro, mode_micro = _f1(mode_confusion)
        subtype_macro, subtype_micro = _f1(subtype_confusion)
        counts.update({"fault_mode_macro_f1": mode_macro,
                       "fault_mode_micro_f1": mode_micro,
                       "edge_subtype_macro_f1": subtype_macro,
                       "edge_subtype_micro_f1": subtype_micro,
                       "fault_mode_confusion": mode_confusion,
                       "edge_subtype_confusion": subtype_confusion,
                       "alert_to_rca_latency_ns": latencies,
                       "alert_to_rca_latency_mean_ns":
                           sum(latencies) / len(latencies) if latencies else None,
                       "confidence_distribution": distribution})
        return counts

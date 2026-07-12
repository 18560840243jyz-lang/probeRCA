"""Strict P10 window and orchestration state contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field

from proberca.data.schema import (
    AlertEvent, CandidateSubgraph, EdgeAnomalyRecord, EdgeMetricRecord,
    EvidenceObservationRecord, NodeAnomalyRecord, NodeMetricRecord, RCAReport,
    TopologySnapshot,
)


class EngineStateError(RuntimeError):
    """The orchestration lifecycle cannot accept the requested transition."""


class EngineWindowAlignmentError(ValueError):
    """Records in one engine input do not share a valid window identity."""


@dataclass(frozen=True)
class ReplayIncidentFailure:
    failure_id: str
    pending_incident_id: str
    stage: str
    timestamp_ns: int
    error_type: str
    reason_code: str
    message: str
    context_ids: list[str]
    retryable: bool
    config_fingerprint: str
    failure_fingerprint: str

    @classmethod
    def create(cls, *, pending_incident_id: str, stage: str, timestamp_ns: int,
               error: Exception, reason_code: str, context_ids: list[str],
               retryable: bool, config_fingerprint: str) -> "ReplayIncidentFailure":
        if stage not in {"input", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}:
            raise ValueError("invalid incident failure stage")
        payload = {
            "pending_incident_id": pending_incident_id, "stage": stage,
            "timestamp_ns": timestamp_ns, "error_type": type(error).__name__,
            "reason_code": reason_code, "message": str(error),
            "context_ids": sorted(context_ids), "retryable": retryable,
            "config_fingerprint": config_fingerprint,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        fingerprint = hashlib.sha256(encoded).hexdigest()
        failure_id = hashlib.sha256(f"{pending_incident_id}:{fingerprint}".encode()).hexdigest()
        return cls(failure_id=failure_id, failure_fingerprint=fingerprint, **payload)

    def to_dict(self) -> dict:
        return asdict(self)


def _canonical_payload(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _payload_entry(object_id: str, payload: dict) -> dict:
    if not isinstance(object_id, str) or not object_id:
        raise ValueError("output ledger object ID must be non-empty")
    if not isinstance(payload, dict):
        raise TypeError("output ledger payload must be a dictionary")
    return {
        "object_id": object_id,
        "payload": payload,
        "payload_sha256": hashlib.sha256(_canonical_payload(payload).encode()).hexdigest(),
    }


@dataclass(frozen=True)
class OutputLedger:
    schema_version: str
    run_id: str
    dataset_fingerprint: str
    config_fingerprint: str
    alert_entries: list[dict]
    report_entries: list[dict]
    failure_entries: list[dict]
    processed_window_count: int
    last_processed_timestamp: int | None
    completed_incident_ids: list[str]
    pending_incident_ids: list[str]
    run_manifest_payload: dict | None
    ledger_fingerprint: str

    @classmethod
    def create(cls, *, alerts, reports, failures, processed_window_count,
               last_processed_timestamp, pending_incident, dataset_fingerprint,
               config_fingerprint, run_manifest_payload=None):
        alert_entries = [_payload_entry(item.alert_id, item.to_dict()) for item in alerts]
        report_entries = [_payload_entry(
            item.report_fingerprint or item.incident_id, item.to_dict()) for item in reports]
        failure_entries = [_payload_entry(item.failure_id, item.to_dict()) for item in failures]
        for name, entries in (("alert", alert_entries), ("report", report_entries),
                              ("failure", failure_entries)):
            ids = [item["object_id"] for item in entries]
            if len(ids) != len(set(ids)):
                raise ValueError(f"output ledger contains duplicate {name} IDs")
        pending_ids = ([pending_incident.pending_incident_id]
                       if pending_incident is not None and
                       pending_incident.lifecycle not in {"diagnosed", "failed", "recovered"}
                       else [])
        completed_ids = sorted({item.incident_id for item in reports})
        run_id = hashlib.sha256(_canonical_payload({
            "dataset": dataset_fingerprint, "config": config_fingerprint,
        }).encode()).hexdigest()
        payload = {
            "schema_version": "1.0", "run_id": run_id,
            "dataset_fingerprint": dataset_fingerprint,
            "config_fingerprint": config_fingerprint,
            "alert_entries": alert_entries, "report_entries": report_entries,
            "failure_entries": failure_entries,
            "processed_window_count": processed_window_count,
            "last_processed_timestamp": last_processed_timestamp,
            "completed_incident_ids": completed_ids,
            "pending_incident_ids": pending_ids,
            "run_manifest_payload": run_manifest_payload,
        }
        fingerprint = hashlib.sha256(_canonical_payload(payload).encode()).hexdigest()
        return cls(**payload, ledger_fingerprint=fingerprint)

    @classmethod
    def from_dict(cls, payload: dict) -> "OutputLedger":
        if not isinstance(payload, dict) or set(payload) != set(cls.__dataclass_fields__):
            raise ValueError("invalid OutputLedger fields")
        result = cls(**payload)
        expected = asdict(result)
        fingerprint = expected.pop("ledger_fingerprint")
        if hashlib.sha256(_canonical_payload(expected).encode()).hexdigest() != fingerprint:
            raise ValueError("OutputLedger fingerprint mismatch")
        for entries in (result.alert_entries, result.report_entries, result.failure_entries):
            for entry in entries:
                if set(entry) != {"object_id", "payload", "payload_sha256"} or \
                        hashlib.sha256(_canonical_payload(entry["payload"]).encode()).hexdigest() \
                        != entry["payload_sha256"]:
                    raise ValueError("OutputLedger payload hash mismatch")
        return result

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class PendingIncident:
    pending_incident_id: str
    alert_id: str
    hard_anchor_ns: int
    analysis_cutoff_ns: int
    cluster_id: str
    namespace_scope: list[str]
    candidate_subgraph: CandidateSubgraph
    hard_node_anomalies: list[NodeAnomalyRecord]
    hard_edge_anomalies: list[EdgeAnomalyRecord]
    hard_metric_predictions: list[object]
    service_model_identity: str
    metric_model_identity: str
    normalized_evidence: list[EvidenceObservationRecord]
    lifecycle: str
    failure_reason: str | None
    report_id: str | None
    config_fingerprint: str
    state_fingerprint: str


@dataclass(frozen=True)
class EngineWindowResult:
    timestamp_ns: int
    state: str
    alerts: list[AlertEvent]
    gate: object
    aggregation_batch: object
    node_anomalies: list[NodeAnomalyRecord]
    edge_anomalies: list[EdgeAnomalyRecord]
    service_propagation_result: object | None
    metric_propagation_result: object | None
    candidate_subgraph: CandidateSubgraph | None
    pending_incident: PendingIncident | None
    reports: list[RCAReport]
    failures: list[ReplayIncidentFailure]
    stage_trace: list[str]
    result_fingerprint: str


@dataclass(frozen=True)
class EngineWindowInput:
    timestamp_ns: int
    window_start_ns: int
    window_end_ns: int
    node_metric_records: list[NodeMetricRecord]
    edge_metric_records: list[EdgeMetricRecord]
    topology_snapshot_events: list[TopologySnapshot]
    evidence_observations_available_by_cutoff: list[EvidenceObservationRecord]
    source_record_ids: list[str]
    replay_sequence_number: int
    reorder_issues: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in (self.timestamp_ns, self.window_start_ns, self.window_end_ns,
                             self.replay_sequence_number)):
            raise EngineWindowAlignmentError("window timestamps and sequence must be non-negative integers")
        if self.timestamp_ns != self.window_end_ns or self.window_start_ns >= self.window_end_ns:
            raise EngineWindowAlignmentError("timestamp must equal the end of a non-empty window")
        records = [*self.node_metric_records, *self.edge_metric_records]
        if not records:
            raise EngineWindowAlignmentError("engine metric window must not be empty")
        if any(not self.window_start_ns <= record.timestamp_ns < self.window_end_ns for record in records):
            raise EngineWindowAlignmentError("metric record timestamp is outside engine window")
        if any(record.window_sec * 1_000_000_000 != self.window_end_ns - self.window_start_ns
               for record in records):
            raise EngineWindowAlignmentError("metric record window_sec conflicts with engine window")
        clusters = {record.cluster_id for record in records}
        clusters.update(item.cluster_id for item in self.topology_snapshot_events)
        clusters.update(item.cluster_id for item in self.evidence_observations_available_by_cutoff)
        if len(clusters) != 1:
            raise EngineWindowAlignmentError("engine window crosses clusters")
        if len(self.source_record_ids) != len(set(self.source_record_ids)):
            raise EngineWindowAlignmentError("source_record_ids contains duplicates")
        if any(not isinstance(item, dict) or not item.get("reason_code") for item in self.reorder_issues):
            raise EngineWindowAlignmentError("reorder issues must be structured")

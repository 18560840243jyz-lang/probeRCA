"""Unique online and Replay event-window engine for canonical P2-P9 execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace

from proberca.aggregation import AggregationPlan, WindowAggregator
from proberca.alerting import AlertStateMachine
from proberca.baseline import MetricSignalRegistry, RobustBaselineStore, ScoreAggregator
from proberca.candidates import CandidateSubgraphBuilder
from proberca.config import (
    AlertStateConfig, BaselineConfig, CompositeAlertRule, MetricSignalSpec,
    ProbeRCAConfig, ScoreConfig,
)
from proberca.data.schema import EdgeMetricRecord, NodeMetricRecord
from proberca.diagnosis import build_rca_report, diagnose_weighted_solution
from proberca.inversion import build_joint_inversion_system, edge_anomaly_from_p2
from proberca.inversion.solver import solve_weighted_joint_problem
from proberca.inversion.weighted_problem import build_weighted_joint_problem
from proberca.propagation.metric_history import node_anomaly_from_p2
from proberca.propagation.metric_ridge import MetricPropagationLearner
from proberca.propagation.service_rls import ServicePropagationLearner
from proberca.topology import TopologyStore

from .adapters import service_state_records_from_p2
from .state import (
    EngineStateError, EngineWindowInput, EngineWindowResult, PendingIncident,
    ReplayIncidentFailure,
)


class ReplayConcurrentIncidentError(EngineStateError):
    """A cluster attempted to start a second global Hard incident."""


class ReplayStageIdentityError(EngineStateError):
    """Canonical stage IDs or fingerprints do not align."""


class ReplayIncidentExecutionError(EngineStateError):
    """A formal incident stage failed and no report may be emitted."""

    def __init__(self, stage, error):
        super().__init__(f"{stage} failed: {error}")
        self.stage = stage
        self.original_error = error


def _sha(value) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()


class ProbeRCAEngine:
    """Stateful canonical window processor shared by future online and Replay sources."""

    def __init__(self, config: ProbeRCAConfig, *, aggregation_plan: AggregationPlan,
                 signal_specs: list[MetricSignalSpec], baseline_config: BaselineConfig,
                 score_config: ScoreConfig, alert_state_config: AlertStateConfig,
                 composite_rules: list[CompositeAlertRule] | None = None):
        if not isinstance(config, ProbeRCAConfig) or not isinstance(aggregation_plan, AggregationPlan):
            raise TypeError("engine requires ProbeRCAConfig and AggregationPlan")
        if any(not isinstance(item, MetricSignalSpec) for item in signal_specs):
            raise TypeError("signal_specs must contain MetricSignalSpec")
        self.config = config
        self.aggregation_plan = aggregation_plan
        self.signal_specs = sorted(signal_specs, key=lambda item: item.aggregation_output_id)
        self.registry = MetricSignalRegistry(self.signal_specs)
        self.baseline_config = baseline_config
        self.baseline = RobustBaselineStore(baseline_config, config.window_sec)
        self.score_aggregator = ScoreAggregator(score_config)
        self.alert_machine = AlertStateMachine(
            alert_state_config, config.window_sec, composite_rules or [],
        )
        self.aggregator = WindowAggregator(config.window_sec, 0, aggregation_plan)
        self.topology_store = TopologyStore()
        self.candidate_builder = CandidateSubgraphBuilder(config, self.signal_specs)
        self.service_learner = ServicePropagationLearner(
            config.propagation, config.window_sec, config.impact_derivation_rules,
            config.candidate_graph.allow_cross_namespace,
        )
        self.metric_learner = MetricPropagationLearner(config.propagation, config.window_sec)
        self.config_fingerprint = _sha({
            "config": config.to_dict(), "aggregation_plan": aggregation_plan.signature,
            "signal_specs": [item.to_dict() for item in self.signal_specs],
            "baseline": asdict(baseline_config), "score": asdict(score_config),
            "alert_state": asdict(alert_state_config),
            "composite_rules": [asdict(item) for item in (composite_rules or [])],
        })
        self.pending_incident: PendingIncident | None = None
        self._latest_candidate = None
        self._last_timestamp: int | None = None
        self._alerts = []
        self._reports = []
        self._failures = []

    def _resolve_signal(self, record):
        matches = [item for item in self.signal_specs
                   if item.aggregation_output_id == record.stable_id]
        if len(matches) != 1:
            raise ValueError(
                f"metric {record.stable_id} must match exactly one configured output ID")
        return matches[0]

    @classmethod
    def from_config(cls, config: ProbeRCAConfig) -> "ProbeRCAEngine":
        if not config.aggregation_specs or not config.metric_signal_specs:
            raise ValueError("Replay config requires aggregation_specs and metric_signal_specs")
        state_config = config.alert_state or AlertStateConfig(
            config.alert.healthy_threshold, config.alert.soft_threshold,
            config.alert.soft_consecutive_windows, config.alert.hard_threshold,
            config.alert.hard_consecutive_windows, config.alert.recovery_threshold,
            config.alert.recovery_windows, config.alert.recovery_cooldown_sec,
            config.score.edge_business_impact_threshold,
        )
        return cls(
            config, aggregation_plan=AggregationPlan(config.aggregation_specs.items()),
            signal_specs=config.metric_signal_specs, baseline_config=config.baseline,
            score_config=config.score, alert_state_config=state_config,
            composite_rules=config.composite_alert_rules,
        )

    def _failure(self, stage, timestamp, error, reason, context, pending_id="unstarted"):
        failure = ReplayIncidentFailure.create(
            pending_incident_id=pending_id, stage=stage, timestamp_ns=timestamp,
            error=error, reason_code=reason, context_ids=context, retryable=False,
            config_fingerprint=self.config_fingerprint,
        )
        self._failures.append(failure)
        if self.pending_incident is not None and self.pending_incident.pending_incident_id == pending_id:
            self.pending_incident = replace(
                self.pending_incident, lifecycle="failed", failure_reason=reason,
                state_fingerprint=_sha([self.pending_incident.state_fingerprint, failure.failure_fingerprint]),
            )
        return failure

    def _score(self, batch):
        records = [*batch.node_records, *batch.edge_records]
        scores = []
        for record in records:
            spec = self._resolve_signal(record)
            result = self.baseline.score(record, spec, batch.window_start_ns, batch.window_end_ns)
            if result.score is not None:
                scores.append(result.score)
        baseline_ready = bool(records) and all(self.baseline.is_ready(item.stable_id) for item in records)
        return records, scores, baseline_ready

    def _update_baseline(self, records, state_result):
        frozen = set(state_result.gate.frozen_node_ids) | set(state_result.gate.frozen_edge_ids)
        for record in records:
            spec = self._resolve_signal(record)
            self.baseline.update(record, spec, state=state_result.state, frozen_ids=frozen)

    def _anomalies(self, records, scores, gate, state):
        by_id = {item.stable_id: item for item in scores}
        nodes, edges = [], []
        for record in records:
            score = by_id.get(record.stable_id)
            if score is None:
                continue
            spec = self._resolve_signal(record)
            if isinstance(record, NodeMetricRecord):
                nodes.append(node_anomaly_from_p2(
                    record, score, gate, spec, state, self.baseline_config, self.config.window_sec,
                ))
            elif isinstance(record, EdgeMetricRecord):
                edges.append(edge_anomaly_from_p2(
                    record, score, gate, spec, self.baseline_config, self.config.window_sec, state,
                ))
        return nodes, edges

    def _pending(self, alert, candidate, node_anomalies, edge_anomalies, predictions, info):
        if self.pending_incident is not None and self.pending_incident.lifecycle in {
            "collecting_evidence", "ready_for_diagnosis"
        }:
            if self.pending_incident.candidate_subgraph.candidate_id != candidate.candidate_id:
                raise ReplayConcurrentIncidentError(
                    "active Hard incident candidate changed before diagnosis")
            return self.pending_incident
        delay = self.config.orchestration.analysis_delay_windows * self.config.window_sec * 1_000_000_000
        pending_id = _sha([alert.alert_id, alert.timestamp_ns, candidate.candidate_id, self.config_fingerprint])
        service_identity = self.service_learner.export_sparse_coefficients()
        service_id = _sha([asdict(item) for item in service_identity])
        provisional = PendingIncident(
            pending_id, alert.alert_id, alert.timestamp_ns, alert.timestamp_ns + delay,
            candidate.cluster_id, candidate.namespace_scope, candidate,
            list(node_anomalies), list(edge_anomalies), list(predictions), service_id,
            info.model_snapshot_id, [], "collecting_evidence", None, None,
            self.config_fingerprint, "pending",
        )
        return replace(provisional, state_fingerprint=_sha({
            "pending_incident_id": pending_id, "alert_id": alert.alert_id,
            "hard_anchor_ns": alert.timestamp_ns, "cutoff": alert.timestamp_ns + delay,
            "candidate_id": candidate.candidate_id, "model_id": info.model_snapshot_id,
        }))

    def _diagnose(self, pending):
        matches = [
            item for item in self.metric_learner.cached_model_infos()
            if item.model_snapshot_id == pending.metric_model_identity
        ]
        if len(matches) != 1:
            raise ReplayStageIdentityError("pending metric model identity changed")
        info = matches[0]
        def stage(name, function):
            try:
                return function()
            except Exception as error:
                raise ReplayIncidentExecutionError(name, error) from error

        joint = stage("p6", lambda: build_joint_inversion_system(
            alert_event=self._hard_alert, candidate_subgraph=pending.candidate_subgraph,
            metric_model_info=info, metric_predictions=pending.hard_metric_predictions,
            current_node_anomalies=pending.hard_node_anomalies,
            current_edge_anomalies=pending.hard_edge_anomalies, config=self.config))
        training_timestamps = {
            node_id: list(self.metric_learner.training_matrix_info(node_id).row_timestamps)
            for node_id in pending.candidate_subgraph.candidate_node_ids
        }
        weighted = stage("p7", lambda: build_weighted_joint_problem(
            joint, pending.normalized_evidence, self.topology_store,
            training_timestamps, self.config, pending.analysis_cutoff_ns))
        solved = stage("p8", lambda: solve_weighted_joint_problem(weighted, self.config))
        if not solved.converged or not solved.solver_usable:
            raise ReplayIncidentExecutionError("p8", "solver result is not usable")
        metric_view = {
            "info": info,
            "coefficients": self.metric_learner.export_sparse_coefficients(),
            "predictions": pending.hard_metric_predictions,
        }
        diagnosis = stage("p9", lambda: diagnose_weighted_solution(
            weighted, solved, joint, metric_view,
            self.service_learner.export_sparse_coefficients(),
            pending.hard_node_anomalies, self._hard_alert,
            pending.candidate_subgraph, self.config))
        report = stage("p9", lambda: build_rca_report(
            diagnosis, weighted, solved, self._hard_alert, self.config))
        evidence_status = ("normalized_evidence_used" if pending.normalized_evidence
                           else "no_normalized_evidence")
        report = replace(
            report, quality={**report.quality, "normalized_evidence": evidence_status},
            report_fingerprint=None)
        fingerprint_payload = report.to_dict()
        fingerprint_payload.pop("runtime", None)
        fingerprint_payload["report_fingerprint"] = None
        return replace(report, report_fingerprint=_sha(fingerprint_payload))

    def process_window(self, window: EngineWindowInput) -> EngineWindowResult:
        if not isinstance(window, EngineWindowInput):
            raise TypeError("process_window requires EngineWindowInput")
        if self._last_timestamp is not None and window.timestamp_ns <= self._last_timestamp:
            raise EngineStateError("engine windows must be strictly increasing")
        trace = ["topology"]
        for snapshot in window.topology_snapshot_events:
            self.topology_store.add(snapshot)
        for record in [*window.node_metric_records, *window.edge_metric_records]:
            self.aggregator.add(record)
        trace.append("p2")
        batch = self.aggregator.finalize(window.window_start_ns)
        records, metric_scores, baseline_ready = self._score(batch)
        state_scores = self.score_aggregator.aggregate(metric_scores)
        state_result = self.alert_machine.step(
            window.timestamp_ns, state_scores, metric_scores, baseline_ready,
        )
        self._update_baseline(records, state_result)
        nodes, edges = self._anomalies(
            records, metric_scores, state_result.gate, state_result.state,
        )
        service_records = service_state_records_from_p2(
            state_scores, metric_scores, timestamp_ns=window.timestamp_ns,
            window_sec=self.config.window_sec, baseline_ready=baseline_ready,
            alert_state=state_result.state, config_fingerprint=self.config_fingerprint,
        )
        snapshot = self.topology_store.query(
            next(iter({item.cluster_id for item in records})), window.timestamp_ns,
            self.config.candidate_graph.allowed_namespaces or None,
        )
        service_result = None
        trace.append("p4")
        if service_records:
            service_result = self.service_learner.process_window(
                service_records, state_result.gate, snapshot,
            )
        metric_result = None
        trace.append("p5")
        if nodes:
            metric_result = self.metric_learner.process_window(nodes, state_result.gate, None, None)
        candidate = None
        reports = []
        failures = []
        global_events = [item for item in state_result.events if item.state in {"soft", "hard"}]
        lifecycle_events = {item.state for item in state_result.events}
        cached_infos = self.metric_learner.cached_model_infos()
        active_info = cached_infos[-1] if cached_infos else None
        if "healthy" in lifecycle_events and active_info is not None \
                and not active_info.frozen \
                and active_info.lifecycle_state in {"PREPARED", "NOT_READY"}:
            self.metric_learner.archive_soft_model()
        if "recovery" in lifecycle_events and active_info is not None and active_info.frozen:
            self.metric_learner.handle_recovery()
        if global_events:
            event = global_events[-1]
            candidate = self.candidate_builder.prepare(event, self.topology_store,
                                                       batch.node_records, batch.edge_records)
            self._latest_candidate = candidate
            if event.state == "soft":
                trace.append("p3_soft")
                self.metric_learner.prepare_for_alert(event, candidate)
            else:
                trace.append("p3_hard")
                self._hard_alert = event
                frozen = self.metric_learner.freeze_for_hard(event, candidate)
                if not frozen.info.global_ready:
                    failures.append(self._failure(
                        "p5", window.timestamp_ns, RuntimeError("metric model not ready"),
                        "metric_model_not_ready", [candidate.candidate_id],
                    ))
                else:
                    predictions = self.metric_learner.predict_window(window.timestamp_ns, nodes)
                    self.pending_incident = self._pending(
                        event, candidate, nodes, edges, predictions, frozen.info,
                    )
        if self.pending_incident is not None and self.pending_incident.lifecycle == "collecting_evidence":
            accepted = [item for item in window.evidence_observations_available_by_cutoff
                        if item.timestamp_ns <= self.pending_incident.analysis_cutoff_ns]
            if accepted:
                self.pending_incident = replace(
                    self.pending_incident,
                    normalized_evidence=sorted(
                        {item.evidence_id: item for item in [
                            *self.pending_incident.normalized_evidence, *accepted
                        ]}.values(), key=lambda item: item.evidence_id,
                    ),
                )
            if window.timestamp_ns >= self.pending_incident.analysis_cutoff_ns:
                pending_id = self.pending_incident.pending_incident_id
                try:
                    trace.extend(["p6", "p7", "p8", "p9", "report"])
                    report = self._diagnose(self.pending_incident)
                    reports.append(report); self._reports.append(report)
                    self.pending_incident = replace(
                        self.pending_incident, lifecycle="diagnosed", report_id=report.report_fingerprint,
                        state_fingerprint=_sha([
                            self.pending_incident.state_fingerprint, report.report_fingerprint,
                        ]),
                    )
                except Exception as error:
                    failed_stage = error.stage if isinstance(
                        error, ReplayIncidentExecutionError) else "p9"
                    failures.append(self._failure(
                        failed_stage, window.timestamp_ns, error, "incident_execution_failed",
                        [self.pending_incident.candidate_subgraph.candidate_id], pending_id,
                    ))
        self._alerts.extend(state_result.events)
        self._last_timestamp = window.timestamp_ns
        fingerprint = _sha({
            "timestamp": window.timestamp_ns, "state": state_result.state,
            "alerts": [item.alert_id for item in state_result.events],
            "candidate": candidate.candidate_id if candidate else None,
            "pending": self.pending_incident.state_fingerprint if self.pending_incident else None,
            "reports": [item.report_fingerprint for item in reports],
            "failures": [item.failure_fingerprint for item in failures], "trace": trace,
        })
        return EngineWindowResult(
            window.timestamp_ns, state_result.state, state_result.events, state_result.gate,
            batch, nodes, edges, service_result, metric_result, candidate,
            self.pending_incident, reports, failures, trace, fingerprint,
        )

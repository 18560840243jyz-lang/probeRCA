"""Offline-only final ProbeRCA-BPF control plane over a sealed data-plane archive."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np

from proberca.dataplane import CollectionArchive
from proberca.dataplane.contracts import canonical_json, fingerprint

from .config import FinalControlConfig
from .evidence import aggregate_burst_evidence
from .metric_model import candidate_metric_nodes, fit_metric_propagation
from .model import (
    ControlPlaneRun,
    FinalRCAResult,
    RootCandidateScore,
)
from .observations import MetricResolver, RobustBaselineStore
from .service_model import (
    AllowedServiceGraph,
    ServiceRLS,
    allowed_service_graph,
    build_candidate_graph,
)
from .solver import solve_nonnegative_sparse_group


class ControlPlaneError(RuntimeError):
    """The sealed dataset cannot be analyzed under the final scheme."""


class CollectionContractMismatchError(ControlPlaneError):
    """The collection and control configurations describe different metrics."""


class IncompleteIncidentError(ControlPlaneError):
    """Collection ended before the Hard/Burst analysis cutoff."""


@dataclass
class _FrozenSoftContext:
    soft_sequence: int
    soft_timestamp_ns: int
    service_graph: AllowedServiceGraph
    candidate_graph: object
    metrics: dict
    metric_model: object
    seed_services: set[str]
    seed_edges: set[str]


@dataclass
class _PendingHardContext:
    sequence: int
    timestamp_ns: int
    analysis_sequence: int
    analysis_cutoff_ns: int
    observations: dict


class FinalControlPlane:
    """The only algorithmic process; it never invokes a collector or mutates the archive."""

    def __init__(self, config: FinalControlConfig):
        if not isinstance(config, FinalControlConfig):
            raise TypeError("FinalControlPlane requires FinalControlConfig")
        self.config = config
        self.baseline = RobustBaselineStore(config)
        self.resolver = MetricResolver(config)
        self.service_rls = ServiceRLS(config)
        self.state = "starting"
        self._soft_counts: dict[tuple[str, str], int] = {}
        self._hard_counts: dict[tuple[str, str], int] = {}
        self._recovery_count = 0
        self._topology = []
        self._healthy_topology_fingerprint: str | None = None
        self._healthy_runtime_identity_fingerprint: str | None = None
        self._topology_epoch = 0
        self._topology_reset_count = 0
        self._runtime_identity_reset_count = 0
        self._baseline_reset_count = 0
        self._metric_history_reset_count = 0
        self._last_calibration_reset_reason = "not_initialized"
        self._healthy_history: dict[int, dict[str, float]] = {}
        self._signed_history: dict[int, dict[str, float]] = {}
        self._soft: _FrozenSoftContext | None = None
        self._hard: _PendingHardContext | None = None
        self._evidence = []
        self._timeline = []
        self._results = []
        self._metric_catalog = {}
        self._metric_specs = {}
        self._calibration_validation_count = 0
        self._calibration_report = {}
        self._invalid_reason_counts = Counter()
        self._rca_not_ready_events = []
        self._diagnosed_hard_sequences: set[int] = set()
        self._has_run = False
        self._dataset_id = ""
        self._dataset_fingerprint = ""

    def _active_topology(self, window_start_ns: int, window_end_ns: int):
        matches = [
            item for item in self._topology
            if item.valid_from_ns <= window_start_ns
            and window_end_ns <= item.valid_to_ns
        ]
        if len(matches) != 1:
            raise ControlPlaneError(
                f"window [{window_start_ns},{window_end_ns}) has "
                f"{len(matches)} covering topology snapshots"
            )
        return matches[0]

    @staticmethod
    def _topology_provenance(graph: AllowedServiceGraph) -> dict[str, Any]:
        return {
            "snapshot_id": graph.snapshot_id,
            "topology_snapshot_id": graph.snapshot_id,
            "topology_fingerprint": graph.topology_fingerprint,
            "runtime_identity_fingerprint": (
                graph.runtime_identity_fingerprint
            ),
            "topology_epoch": graph.topology_epoch,
        }

    def _initialize_healthy_segment(self, graph: AllowedServiceGraph) -> None:
        if self._topology_epoch != 0:
            raise ControlPlaneError("topology epoch is already initialized")
        self._topology_epoch = 1
        self._healthy_topology_fingerprint = graph.topology_fingerprint
        self._healthy_runtime_identity_fingerprint = (
            graph.runtime_identity_fingerprint
        )
        self._last_calibration_reset_reason = "initial_topology_epoch"
        self.state = "calibrating"

    def _reset_healthy_segment(
        self, graph: AllowedServiceGraph, *, reason: str,
    ) -> None:
        if reason == "topology_fingerprint_changed":
            self._topology_epoch += 1
            self._topology_reset_count += 1
        elif reason == "runtime_identity_fingerprint_changed":
            self._runtime_identity_reset_count += 1
        else:
            raise ControlPlaneError("invalid calibration reset reason")
        self.baseline.reset()
        self._baseline_reset_count += 1
        self.service_rls.reset()
        self._healthy_history.clear()
        self._metric_history_reset_count += 1
        self._signed_history.clear()
        self._soft_counts.clear()
        self._hard_counts.clear()
        self._metric_catalog.clear()
        self._metric_specs.clear()
        self._calibration_validation_count = 0
        self._calibration_report = {}
        self.state = "calibrating"
        self._healthy_topology_fingerprint = graph.topology_fingerprint
        self._healthy_runtime_identity_fingerprint = (
            graph.runtime_identity_fingerprint
        )
        self._last_calibration_reset_reason = reason

    def _catalog_window(self, window) -> None:
        for record in (*window.node_metrics, *window.edge_metrics):
            metric, spec = self.resolver.resolve(record)
            existing = self._metric_catalog.get(metric.node_id)
            if existing is not None and existing != metric:
                raise ControlPlaneError("metric identity changed during calibration")
            self._metric_catalog[metric.node_id] = metric
            self._metric_specs[metric.node_id] = spec

    def _full_candidate(self, graph: AllowedServiceGraph):
        return build_candidate_graph(
            graph=graph,
            service_strengths=self.service_rls.relation_strengths(),
            seed_services=set(graph.services),
            seed_edges={
                edge_id
                for edge_id, _source, _target, _protocol
                in graph.physical_edges
            },
            config=self.config,
        )

    def _build_calibration_report(
        self, *, graph: AllowedServiceGraph, timestamp_ns: int,
        maximum: float,
    ) -> dict[str, Any]:
        required_types = set(
            self.config.calibration_required_entity_types
        )
        candidate = self._full_candidate(graph)
        metrics = candidate_metric_nodes(self._metric_catalog, candidate)
        model = fit_metric_propagation(
            metrics=metrics,
            healthy_history=self._healthy_history,
            candidate=candidate,
            service_graph=graph,
            healthy_cutoff_ns=timestamp_ns,
            config=self.config,
        )
        configured_roots = set(
            self.config.calibration_required_root_coordinates
        )
        scope_status = {}
        for node_id in sorted(configured_roots):
            metric = metrics.get(node_id)
            reason = None
            if metric is None:
                reason = "metric_not_observed"
            elif not metric.root_eligible:
                reason = "not_root_eligible"
            elif metric.entity_type not in required_types:
                reason = "entity_type_not_enabled"
            scope_status[node_id] = {
                "target_metric": node_id,
                "ready": reason is None,
                "not_ready_reason": reason,
            }
        scope_ready = (
            bool(scope_status)
            and all(item["ready"] for item in scope_status.values())
        )
        required_roots = {
            node_id for node_id, item in scope_status.items()
            if item["ready"]
        }
        required_metric_ids = set(required_roots)
        required_metric_ids.update(
            parent_id
            for target_id, parent_id in model.semantic_mask
            if target_id in required_roots
        )
        required_metrics = {
            node_id: metrics[node_id]
            for node_id in sorted(required_metric_ids)
            if node_id in metrics
        }
        baseline_status = {
            node_id: self.baseline.status(
                node_id, self._metric_specs[node_id],
            )
            for node_id in sorted(required_metrics)
        }
        all_baseline_status = {
            node_id: self.baseline.status(
                node_id, self._metric_specs[node_id],
            )
            for node_id in sorted(metrics)
        }
        all_av_status = {
            node_id: asdict(status)
            for node_id, status in sorted(
                model.target_readiness.items()
            )
            if status.root_eligible
        }
        av_status = {
            node_id: asdict(model.target_readiness[node_id])
            for node_id in sorted(required_roots)
            if node_id in model.target_readiness
        }
        missing_av = sorted(required_roots - set(av_status))
        for node_id in missing_av:
            av_status[node_id] = {
                "target_metric": node_id,
                "root_eligible": True,
                "allowed_feature_count": 0,
                "valid_training_rows": 0,
                "minimum_training_rows": (
                    self.config.metric_min_training_rows
                ),
                "effective_rank": 0,
                "condition_number": None,
                "ready": False,
                "not_ready_reason": "metric_not_observed",
            }
        relevant_services = set()
        physical_edges = {
            edge_id: (source, target)
            for edge_id, source, target, _protocol
            in graph.physical_edges
        }
        for node_id in required_roots:
            metric = metrics[node_id]
            if metric.entity_type == "service":
                relevant_services.add(metric.entity_id)
            elif metric.entity_type == "edge":
                relevant_services.update(
                    physical_edges.get(metric.entity_id, ())
                )
            elif metric.entity_type == "host":
                relevant_services.update(
                    service
                    for service, host in graph.placements
                    if host == metric.entity_id
                )
        all_service_status = self.service_rls.readiness()
        service_status = {
            service: all_service_status[service]
            for service in sorted(relevant_services)
            if service in all_service_status
        }
        missing_services = sorted(
            relevant_services - set(service_status)
        )
        for service in missing_services:
            service_status[service] = {
                "allowed_feature_count": 0,
                "valid_training_rows": 0,
                "minimum_training_rows": (
                    self.config.service_min_training_updates
                ),
                "ready": False,
                "not_ready_reason": "service_state_not_observed",
            }
        baseline_ready = (
            bool(baseline_status)
            and all(item["ready"] for item in baseline_status.values())
        )
        av_ready = (
            bool(av_status)
            and all(item["ready"] for item in av_status.values())
        )
        as_ready = (
            bool(service_status)
            and all(item["ready"] for item in service_status.values())
        )
        core_ready = (
            scope_ready and baseline_ready and av_ready and as_ready
        )
        payload = {
            "schema_version": "probeRCA-calibration-readiness-v1",
            "timestamp_ns": timestamp_ns,
            "state": self.state,
            "ready": False,
            "core_ready": core_ready,
            "baseline_ready": baseline_ready,
            "service_model_ready": as_ready,
            "metric_model_ready": av_ready,
            "healthy_validation_windows": (
                self._calibration_validation_count
            ),
            "required_healthy_validation_windows": (
                self.config.calibration_validation_windows
            ),
            "maximum_symptom_score": maximum,
            "required_entity_types": sorted(required_types),
            "required_root_coordinates": sorted(configured_roots),
            "available_root_coordinates": sorted(all_av_status),
            "planned_scope_ready": scope_ready,
            "planned_scope_status": scope_status,
            "required_scope_fingerprint": fingerprint({
                "entity_types": sorted(required_types),
                "root_coordinates": sorted(configured_roots),
            }),
            **self._topology_provenance(graph),
            "topology_reset_count": self._topology_reset_count,
            "runtime_identity_reset_count": (
                self._runtime_identity_reset_count
            ),
            "baseline_reset_count": self._baseline_reset_count,
            "service_model_reset_count": self.service_rls.reset_count,
            "metric_history_reset_count": (
                self._metric_history_reset_count
            ),
            "last_calibration_reset_reason": (
                self._last_calibration_reset_reason
            ),
            "control_config_fingerprint": self.config.config_fingerprint,
            "baseline_status": baseline_status,
            "all_baseline_status": all_baseline_status,
            "service_model_status": service_status,
            "all_service_model_status": all_service_status,
            "metric_model_status": av_status,
            "all_metric_model_status": all_av_status,
            "invalid_observation_counts": dict(sorted(
                self._invalid_reason_counts.items()
            )),
            "latest_observation_validity": dict(sorted(
                self.resolver.last_validity.items()
            )),
            "scale_snapshot": self.baseline.scale_snapshot(),
            "report_fingerprint": "",
        }
        return payload | {
            "report_fingerprint": fingerprint(payload),
        }

    def _advance_calibration(
        self, *, graph: AllowedServiceGraph, timestamp_ns: int,
        maximum: float, service_scores, edge_scores, observations,
    ) -> None:
        previous = self.state
        report = self._build_calibration_report(
            graph=graph, timestamp_ns=timestamp_ns, maximum=maximum,
        )
        healthy_validation = (
            report["core_ready"]
            and maximum < self.config.soft_threshold
        )
        self._calibration_validation_count = (
            self._calibration_validation_count + 1
            if healthy_validation else 0
        )
        report["healthy_validation_windows"] = (
            self._calibration_validation_count
        )
        report["ready"] = (
            report["core_ready"]
            and self._calibration_validation_count
            >= self.config.calibration_validation_windows
        )
        self.state = "ready" if report["ready"] else "calibrating"
        report["state"] = self.state
        report["report_fingerprint"] = ""
        report["report_fingerprint"] = fingerprint(report)
        self._calibration_report = report
        self._timeline.append({
            "timestamp_ns": timestamp_ns,
            "previous_state": previous,
            "state": self.state,
            "maximum_symptom_score": maximum,
            "service_scores": dict(sorted(service_scores.items())),
            "edge_scores": dict(sorted(edge_scores.items())),
            "scale_sources": {
                node_id: item.scale_source
                for node_id, item in sorted(observations.items())
            },
            "metric_scores": {
                node_id: {
                    "signed_z": item.signed_z,
                    "anomaly": item.anomaly,
                    "quality": item.quality,
                    "baseline_center": item.baseline_center,
                    "baseline_scale": item.baseline_scale,
                    "scale_source": item.scale_source,
                }
                for node_id, item in sorted(observations.items())
            },
            "reason": (
                "calibration_ready"
                if report["ready"]
                else (
                    "calibration_anomaly"
                    if report["core_ready"] and not healthy_validation
                    else "calibration_not_ready"
                )
            ),
            "baseline_frozen": False,
            "service_model_frozen": False,
            **self._topology_provenance(graph),
            "calibration_report_fingerprint": (
                report["report_fingerprint"]
            ),
        })

    def _scores(self, observations, graph: AllowedServiceGraph):
        by_entity: dict[str, dict[str, float]] = {}
        for item in observations.values():
            by_entity.setdefault(item.metric.entity_id, {})[item.metric.role] = item.anomaly
        service_scores = {}
        for service in graph.services:
            roles = by_entity.get(service, {})
            if not (
                {"request_latency", "request_failure"} & set(roles)
            ):
                continue
            service_scores[service] = (
                self.config.alpha_latency * roles.get("request_latency", 0.0)
                + self.config.alpha_failure * roles.get("request_failure", 0.0)
            )
        edge_scores = {}
        for edge_id, _source, _target, _protocol in graph.physical_edges:
            roles = by_entity.get(edge_id, {})
            edge_scores[edge_id] = max(
                roles.get("edge_latency", 0.0),
                roles.get("edge_failure", 0.0),
            )
        return service_scores, edge_scores

    def _baseline_ready(self, raw) -> bool:
        alert_nodes = [
            node_id for node_id, (_value, spec) in raw.items()
            if spec.role in {
                "request_latency", "request_failure", "edge_latency", "edge_failure",
            }
        ]
        return bool(alert_nodes) and all(
            self.baseline.ready(node_id, raw[node_id][1])
            for node_id in alert_nodes
        )

    def _update_healthy_models(
        self, *, sequence: int, raw, observations, service_scores,
        graph: AllowedServiceGraph, safe_healthy: bool,
    ) -> None:
        if not safe_healthy:
            return
        for node_id, (value, spec) in raw.items():
            self.baseline.update(node_id, value, spec)
        if observations:
            signed = {
                node_id: item.signed_z for node_id, item in observations.items()
            }
            self._healthy_history[sequence] = signed
            self.service_rls.update(sequence, service_scores, graph)

    def _advance_alert_counters(
        self, service_scores, edge_scores,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        scores = {
            **{("service", key): value for key, value in service_scores.items()},
            **{("edge", key): value for key, value in edge_scores.items()},
        }
        keys = set(scores) | set(self._soft_counts) | set(self._hard_counts)
        for key in keys:
            value = scores.get(key, 0.0)
            self._soft_counts[key] = (
                self._soft_counts.get(key, 0) + 1
                if value >= self.config.soft_threshold else 0
            )
            self._hard_counts[key] = (
                self._hard_counts.get(key, 0) + 1
                if value >= self.config.hard_threshold else 0
            )
        soft = {
            key for key, count in self._soft_counts.items()
            if count >= self.config.soft_consecutive_windows
        }
        hard = {
            key for key, count in self._hard_counts.items()
            if count >= self.config.hard_consecutive_windows
        }
        return soft, hard

    def _freeze_soft_context(
        self, *, sequence: int, timestamp_ns: int,
        graph: AllowedServiceGraph, observations,
        alert_entities: set[tuple[str, str]],
    ) -> None:
        seeds = {entity_id for kind, entity_id in alert_entities if kind == "service"}
        edge_seeds = {entity_id for kind, entity_id in alert_entities if kind == "edge"}
        candidate = build_candidate_graph(
            graph=graph,
            service_strengths=self.service_rls.relation_strengths(),
            seed_services=seeds,
            seed_edges=edge_seeds,
            config=self.config,
        )
        metrics = candidate_metric_nodes(self._metric_catalog, candidate)
        model = fit_metric_propagation(
            metrics=metrics,
            healthy_history=self._healthy_history,
            candidate=candidate,
            service_graph=graph,
            healthy_cutoff_ns=timestamp_ns,
            config=self.config,
        )
        self._soft = _FrozenSoftContext(
            soft_sequence=sequence,
            soft_timestamp_ns=timestamp_ns,
            service_graph=graph,
            candidate_graph=candidate,
            metrics=metrics,
            metric_model=model,
            seed_services=seeds,
            seed_edges=edge_seeds,
        )

    def _enter_hard(
        self, *, sequence: int, timestamp_ns: int, observations,
    ) -> None:
        if self._soft is None:
            raise ControlPlaneError("Hard transition has no frozen context")
        self.state = "hard"
        self._hard = _PendingHardContext(
            sequence=sequence,
            timestamp_ns=timestamp_ns,
            analysis_sequence=sequence + self.config.burst_window_count,
            analysis_cutoff_ns=(
                timestamp_ns
                + self.config.burst_window_count
                * self.config.window_sec * 1_000_000_000
            ),
            observations=dict(observations),
        )
        self._recovery_count = 0

    def _transition(
        self, *, sequence: int, timestamp_ns: int, service_scores, edge_scores,
        graph: AllowedServiceGraph, observations,
    ) -> None:
        maximum = max((*service_scores.values(), *edge_scores.values()), default=0.0)
        previous = self.state
        soft_entities: set[tuple[str, str]] = set()
        hard_entities: set[tuple[str, str]] = set()
        if self.state in {"healthy", "soft"}:
            soft_entities, hard_entities = self._advance_alert_counters(
                service_scores, edge_scores,
            )
        if self.state in {"healthy", "soft"} and hard_entities:
            if self._soft is None:
                # Hard has its own per-entity 5x2 detector. It may freeze the
                # healthy context before the slower Soft 3x3 detector fires.
                self._freeze_soft_context(
                    sequence=sequence,
                    timestamp_ns=timestamp_ns,
                    graph=graph,
                    observations=observations,
                    alert_entities=hard_entities | soft_entities,
                )
            self._enter_hard(
                sequence=sequence,
                timestamp_ns=timestamp_ns,
                observations=observations,
            )
        elif self.state == "healthy" and soft_entities:
            self._freeze_soft_context(
                sequence=sequence,
                timestamp_ns=timestamp_ns,
                graph=graph,
                observations=observations,
                alert_entities=soft_entities,
            )
            self.state = "soft"
        elif self.state == "soft":
            if maximum <= self.config.recovery_threshold:
                self._recovery_count += 1
                if self._recovery_count >= self.config.recovery_windows:
                    self.state = "recovery"
                    self._recovery_count = 0
            else:
                self._recovery_count = 0
        elif self.state == "hard":
            self._recovery_count = (
                self._recovery_count + 1
                if maximum <= self.config.recovery_threshold else 0
            )
            if self._recovery_count >= self.config.recovery_windows:
                self.state = "recovery"
                self._recovery_count = 0
        elif self.state == "recovery":
            self._recovery_count = (
                self._recovery_count + 1
                if maximum <= self.config.recovery_threshold else 0
            )
            incident_complete = (
                self._hard is None
                or self._hard.sequence in self._diagnosed_hard_sequences
            )
            if self._recovery_count >= self.config.recovery_windows and incident_complete:
                self.state = "healthy"
                self._soft_counts.clear()
                self._hard_counts.clear()
                self._recovery_count = 0
                self._soft = None
                self._hard = None
        models_updated = previous == "healthy" and maximum < self.config.soft_threshold
        self._timeline.append({
            "timestamp_ns": timestamp_ns,
            "previous_state": previous,
            "state": self.state,
            "maximum_symptom_score": maximum,
            "service_scores": dict(sorted(service_scores.items())),
            "edge_scores": dict(sorted(edge_scores.items())),
            "scale_sources": {
                node_id: item.scale_source
                for node_id, item in sorted(observations.items())
            },
            "metric_scores": {
                node_id: {
                    "signed_z": item.signed_z,
                    "anomaly": item.anomaly,
                    "quality": item.quality,
                    "baseline_center": item.baseline_center,
                    "baseline_scale": item.baseline_scale,
                    "scale_source": item.scale_source,
                }
                for node_id, item in sorted(observations.items())
            },
            "soft_consecutive_counts": {
                f"{key[0]}|{key[1]}": value
                for key, value in sorted(self._soft_counts.items())
            },
            "hard_consecutive_counts": {
                f"{key[0]}|{key[1]}": value
                for key, value in sorted(self._hard_counts.items())
            },
            **self._topology_provenance(graph),
            "baseline_frozen": not models_updated,
            "service_model_frozen": not models_updated,
        })

    def _diagnose(self) -> FinalRCAResult:
        if self._soft is None or self._hard is None:
            raise ControlPlaneError("diagnosis requires frozen Soft and Hard contexts")
        model = self._soft.metric_model
        if not model.ready:
            raise ControlPlaneError(
                "RCA_NOT_READY: " + ",".join(
                    model.not_ready_root_coordinates
                )
            )
        observations = self._hard.observations
        root_metrics = [
            self._soft.metrics[node_id]
            for node_id in model.node_ids
            if (
                node_id in observations
                and self._soft.metrics[node_id].root_eligible
                and model.target_readiness[node_id].ready
            )
        ]
        if not root_metrics:
            raise ControlPlaneError("candidate graph has no observed root coordinates")
        root_metrics.sort(key=lambda item: item.node_id)
        residual = np.asarray([
            observations[item.node_id].signed_z
            - model.cross_prediction(
                item.node_id, self._signed_history, self._hard.sequence,
            )
            for item in root_metrics
        ], dtype=float)
        quality = np.asarray([
            observations[item.node_id].quality for item in root_metrics
        ], dtype=float)
        grouped_metrics = {}
        for metric in root_metrics:
            grouped_metrics.setdefault(
                (metric.entity_id, metric.root_category), [],
            ).append(metric)
        groups = {
            key: tuple(sorted(values, key=lambda item: item.node_id))
            for key, values in sorted(grouped_metrics.items())
        }
        evidence = [
            item for item in self._evidence
            if self._hard.timestamp_ns <= item.evidence_window_start_ns
            and item.evidence_window_end_ns <= self._hard.analysis_cutoff_ns
            and self._hard.timestamp_ns <= item.timestamp_ns
            < self._hard.analysis_cutoff_ns
        ]
        burst_strength, evidence_ids = aggregate_burst_evidence(evidence, groups)
        index = {metric.node_id: position for position, metric in enumerate(root_metrics)}
        fista_groups = []
        penalties = {}
        for key, metrics in groups.items():
            base = self.config.group_penalties[key[1]]
            effective = base / (1.0 + self.config.burst_eta * burst_strength[key])
            penalties[key] = (base, effective)
            fista_groups.append((tuple(index[item.node_id] for item in metrics), effective))
        solved = solve_nonnegative_sparse_group(
            residual,
            quality,
            l1_penalties=np.full(len(root_metrics), self.config.l1_penalty),
            groups=fista_groups,
            max_iterations=self.config.fista_max_iterations,
            tolerance=self.config.fista_tolerance,
        )
        if not solved.converged:
            raise ControlPlaneError(
                f"final Sparse-Group FISTA did not converge in {solved.iterations} iterations"
            )
        candidates = []
        theta = np.asarray(solved.theta)
        for key, metrics in groups.items():
            indices = [index[item.node_id] for item in metrics]
            score = float(np.linalg.norm(theta[indices]))
            base, effective = penalties[key]
            candidates.append(RootCandidateScore(
                candidate_id=fingerprint({
                    "entity_id": key[0], "root_category": key[1],
                    "metric_node_ids": [item.node_id for item in metrics],
                }),
                entity_id=key[0],
                entity_type=metrics[0].entity_type,
                root_category=key[1],
                score=score,
                metric_node_ids=tuple(item.node_id for item in metrics),
                metric_contributions={
                    item.node_id: float(theta[index[item.node_id]]) for item in metrics
                },
                signed_residuals={
                    item.node_id: float(residual[index[item.node_id]]) for item in metrics
                },
                observation_quality={
                    item.node_id: float(quality[index[item.node_id]]) for item in metrics
                },
                burst_evidence_strength=burst_strength[key],
                burst_evidence_ids=evidence_ids[key],
                burst_evidence=tuple({
                    "evidence_id": item.evidence_id,
                    "channel_id": item.channel_id,
                    "target_id": item.target_id,
                    "normalized_strength": item.normalized_strength,
                    "observation_quality": item.observation_quality,
                    "reliability_weight": item.reliability_weight,
                    "effective_strength": (
                        item.normalized_strength
                        * item.observation_quality
                        * item.reliability_weight
                    ),
                } for item in sorted(
                    evidence, key=lambda value: value.evidence_id,
                ) if item.evidence_id in evidence_ids[key]),
                base_group_penalty=base,
                effective_group_penalty=effective,
            ))
        ranked = tuple(sorted(
            candidates,
            key=lambda item: (-item.score, item.entity_id, item.root_category),
        ))
        incident_id = fingerprint({
            "dataset": self._dataset_fingerprint,
            "hard_alert_timestamp_ns": self._hard.timestamp_ns,
            "candidate_graph": self._soft.candidate_graph.topology_snapshot_id,
        })
        return FinalRCAResult.create(
            incident_id=incident_id,
            hard_alert_timestamp_ns=self._hard.timestamp_ns,
            analysis_cutoff_ns=self._hard.analysis_cutoff_ns,
            symptom_services=tuple(sorted(self._soft.seed_services)),
            symptom_edges=tuple(sorted(self._soft.seed_edges)),
            candidates=ranked,
            top_k=ranked[:self.config.top_k],
            candidate_graph=self._soft.candidate_graph,
            residual_signal="signed_z_minus_frozen_healthy_cross_metric_Av_only",
            solver=solved,
            model_metadata={
                "service_model": "healthy_only_masked_RLS_As",
                "service_relation_strength": "l2_norm_across_lags",
                "metric_model": "healthy_only_masked_Ridge_Av",
                "self_history_learned": True,
                "self_history_subtracted_from_residual": False,
                "root_coordinates_only": True,
                "burst_role": "candidate_group_penalty_only",
                "counterfactual_resolve": False,
                "metric_training_rows": model.training_rows,
                "metric_target_readiness": {
                    node_id: asdict(status)
                    for node_id, status
                    in sorted(model.target_readiness.items())
                },
                "excluded_not_ready_root_coordinates": list(
                    model.not_ready_root_coordinates
                ),
                "baseline_scales": self.baseline.scale_snapshot(),
                "burst_evidence_ids": {
                    f"{key[0]}|{key[1]}": list(values)
                    for key, values in evidence_ids.items()
                },
            },
        )

    def run(self, archive: CollectionArchive) -> ControlPlaneRun:
        if self._has_run:
            raise ControlPlaneError("FinalControlPlane instances are single-use")
        self._has_run = True
        if not isinstance(archive, CollectionArchive):
            raise TypeError("control plane requires a loaded CollectionArchive")
        archive.validate()
        if archive.collection_contract_fingerprint \
                != self.config.collection_contract_fingerprint:
            raise CollectionContractMismatchError(
                "sealed collection contract does not match final control config"
            )
        if archive.window_sec != self.config.window_sec:
            raise CollectionContractMismatchError(
                "sealed collection window_sec does not match final control config"
            )
        self._dataset_id = archive.dataset_id
        self._dataset_fingerprint = archive.manifest_fingerprint
        processed = 0
        for window in archive.iter_windows():
            processed += 1
            self._topology.extend(window.topology_events)
            self._evidence.extend(window.burst_evidence)
            snapshot = self._active_topology(
                window.window_start_ns, window.window_end_ns,
            )
            live_graph = allowed_service_graph(snapshot)
            topology_reset = False
            runtime_identity_reset = False
            if self.state in {
                "starting", "calibrating", "ready", "healthy",
            }:
                if self._healthy_topology_fingerprint is None:
                    self._initialize_healthy_segment(live_graph)
                elif (
                    self._healthy_topology_fingerprint
                    != live_graph.topology_fingerprint
                ):
                    topology_reset = True
                    self._reset_healthy_segment(
                        live_graph,
                        reason="topology_fingerprint_changed",
                    )
                elif (
                    self._healthy_runtime_identity_fingerprint
                    != live_graph.runtime_identity_fingerprint
                ):
                    runtime_identity_reset = True
                    self._reset_healthy_segment(
                        live_graph,
                        reason="runtime_identity_fingerprint_changed",
                    )
                elif self.state == "ready":
                    self.state = "healthy"
            live_graph = replace(
                live_graph, topology_epoch=self._topology_epoch,
            )
            graph = (
                self._soft.service_graph
                if self.state != "healthy" and self._soft is not None
                else live_graph
            )
            self._catalog_window(window)
            observations, raw = self.resolver.normalize_window(window, self.baseline)
            self._invalid_reason_counts.update(
                item["invalid_reason"]
                for item in self.resolver.last_validity.values()
                if item["invalid_reason"] is not None
            )
            signed = {
                node_id: item.signed_z for node_id, item in observations.items()
            }
            self._signed_history[window.sequence] = signed
            service_scores, edge_scores = self._scores(observations, graph)
            baseline_ready = self._baseline_ready(raw)
            maximum = max(
                (*service_scores.values(), *edge_scores.values()), default=0.0,
            )
            safe_healthy = (
                self.state in {"starting", "calibrating", "healthy"}
                and (not baseline_ready or maximum < self.config.soft_threshold)
            )
            self._update_healthy_models(
                sequence=window.sequence,
                raw=raw,
                observations=observations,
                service_scores=service_scores,
                graph=graph,
                safe_healthy=safe_healthy,
            )
            if self.state in {"starting", "calibrating"}:
                self._advance_calibration(
                    graph=graph,
                    timestamp_ns=window.window_end_ns,
                    maximum=maximum,
                    service_scores=service_scores,
                    edge_scores=edge_scores,
                    observations=observations,
                )
                continue
            if baseline_ready:
                self._transition(
                    sequence=window.sequence,
                    timestamp_ns=window.window_end_ns,
                    service_scores=service_scores,
                    edge_scores=edge_scores,
                    graph=graph,
                    observations=observations,
                )
            else:
                self._timeline.append({
                    "timestamp_ns": window.window_end_ns,
                    "previous_state": self.state,
                    "state": self.state,
                    "maximum_symptom_score": 0.0,
                    "service_scores": {},
                    "edge_scores": {},
                    "baseline_frozen": False,
                    "service_model_frozen": False,
                    "reason": (
                        "topology_changed_baseline_reset"
                        if topology_reset else "baseline_not_ready"
                        if not runtime_identity_reset
                        else "runtime_identity_changed_baseline_reset"
                    ),
                    **self._topology_provenance(graph),
                })
            if self._hard is not None \
                    and window.sequence >= self._hard.analysis_sequence \
                    and self._hard.sequence not in self._diagnosed_hard_sequences:
                model = (
                    self._soft.metric_model
                    if self._soft is not None else None
                )
                if model is None or not model.ready:
                    statuses = (
                        {} if model is None else {
                            node_id: asdict(status)
                            for node_id, status
                            in sorted(model.target_readiness.items())
                            if status.root_eligible and not status.ready
                        }
                    )
                    reasons = sorted({
                        item["not_ready_reason"]
                        for item in statuses.values()
                        if item["not_ready_reason"] is not None
                    })
                    self._rca_not_ready_events.append({
                        "status": "RCA_NOT_READY",
                        "reason": (
                            reasons[0]
                            if len(reasons) == 1
                            else "metric_model_not_ready"
                        ),
                        "not_ready_reasons": reasons,
                        "hard_alert_timestamp_ns": (
                            self._hard.timestamp_ns
                        ),
                        "analysis_cutoff_ns": (
                            self._hard.analysis_cutoff_ns
                        ),
                        "metric_target_readiness": statuses,
                    })
                else:
                    self._results.append(self._diagnose())
                self._diagnosed_hard_sequences.add(self._hard.sequence)
        if self._hard is not None \
                and self._hard.sequence not in self._diagnosed_hard_sequences:
            raise IncompleteIncidentError(
                "collection ended before the configured Burst analysis cutoff"
            )
        return ControlPlaneRun.create(
            dataset_id=archive.dataset_id,
            dataset_fingerprint=archive.manifest_fingerprint,
            collection_contract_fingerprint=archive.collection_contract_fingerprint,
            control_config_fingerprint=self.config.config_fingerprint,
            processed_window_count=processed,
            state_timeline=tuple(self._timeline),
            results=tuple(self._results),
            calibration_readiness=dict(self._calibration_report),
            rca_not_ready_events=tuple(self._rca_not_ready_events),
        )


def save_control_run(output: str | Path, run: ControlPlaneRun) -> None:
    directory = Path(output)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "control-run.json").write_text(
        canonical_json(run.to_dict()) + "\n", encoding="utf-8",
    )
    (directory / "calibration-readiness.json").write_text(
        canonical_json(run.calibration_readiness) + "\n",
        encoding="utf-8",
    )
    with (directory / "rca-results.jsonl").open("x", encoding="utf-8") as handle:
        for result in run.results:
            handle.write(canonical_json(result.to_dict()) + "\n")

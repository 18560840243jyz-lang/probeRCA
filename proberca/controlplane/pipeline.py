"""Offline-only final ProbeRCA-BPF control plane over a sealed data-plane archive."""

from __future__ import annotations

from dataclasses import dataclass
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
        self.state = "healthy"
        self._soft_count = 0
        self._hard_count = 0
        self._recovery_count = 0
        self._topology = []
        self._healthy_history: dict[int, dict[str, float]] = {}
        self._signed_history: dict[int, dict[str, float]] = {}
        self._soft: _FrozenSoftContext | None = None
        self._hard: _PendingHardContext | None = None
        self._evidence = []
        self._timeline = []
        self._results = []
        self._diagnosed_hard_sequences: set[int] = set()
        self._has_run = False
        self._dataset_id = ""
        self._dataset_fingerprint = ""

    def _active_topology(self, timestamp_ns: int):
        matches = [
            item for item in self._topology
            if item.valid_from_ns <= timestamp_ns < item.valid_to_ns
        ]
        if len(matches) != 1:
            raise ControlPlaneError(
                f"timestamp {timestamp_ns} has {len(matches)} active topology snapshots"
            )
        return matches[0]

    def _scores(self, observations, graph: AllowedServiceGraph):
        by_entity: dict[str, dict[str, float]] = {}
        for item in observations.values():
            by_entity.setdefault(item.metric.entity_id, {})[item.metric.role] = item.anomaly
        service_scores = {}
        for service in graph.services:
            roles = by_entity.get(service, {})
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
        return bool(alert_nodes) and all(self.baseline.ready(node_id) for node_id in alert_nodes)

    def _update_healthy_models(
        self, *, sequence: int, raw, observations, service_scores,
        graph: AllowedServiceGraph, safe_healthy: bool,
    ) -> None:
        if not safe_healthy:
            return
        for node_id, (value, _spec) in raw.items():
            self.baseline.update(node_id, value)
        if observations:
            signed = {
                node_id: item.signed_z for node_id, item in observations.items()
            }
            self._healthy_history[sequence] = signed
            self.service_rls.update(sequence, service_scores, graph)

    def _transition(
        self, *, timestamp_ns: int, service_scores, edge_scores,
        graph: AllowedServiceGraph, observations,
    ) -> None:
        maximum = max((*service_scores.values(), *edge_scores.values()), default=0.0)
        previous = self.state
        if self.state == "healthy":
            self._soft_count = self._soft_count + 1 if maximum >= self.config.soft_threshold else 0
            if self._soft_count >= self.config.soft_consecutive_windows:
                self.state = "soft"
                seeds = {
                    key for key, value in service_scores.items()
                    if value >= self.config.soft_threshold
                }
                edge_seeds = {
                    key for key, value in edge_scores.items()
                    if value >= self.config.soft_threshold
                }
                candidate = build_candidate_graph(
                    graph=graph,
                    service_strengths=self.service_rls.relation_strengths(),
                    seed_services=seeds,
                    seed_edges=edge_seeds,
                    config=self.config,
                )
                metrics = candidate_metric_nodes(observations, candidate)
                model = fit_metric_propagation(
                    metrics=metrics,
                    healthy_history=self._healthy_history,
                    candidate=candidate,
                    service_graph=graph,
                    healthy_cutoff_ns=timestamp_ns,
                    config=self.config,
                )
                self._soft = _FrozenSoftContext(
                    soft_sequence=max(self._signed_history),
                    soft_timestamp_ns=timestamp_ns,
                    service_graph=graph,
                    candidate_graph=candidate,
                    metrics=metrics,
                    metric_model=model,
                    seed_services=seeds,
                    seed_edges=edge_seeds,
                )
        elif self.state == "soft":
            self._hard_count = self._hard_count + 1 if maximum >= self.config.hard_threshold else 0
            if self._hard_count >= self.config.hard_consecutive_windows:
                if self._soft is None:
                    raise ControlPlaneError("Hard transition has no frozen Soft context")
                self.state = "hard"
                sequence = max(self._signed_history)
                analysis_sequence = sequence + self.config.burst_window_count
                self._hard = _PendingHardContext(
                    sequence=sequence,
                    timestamp_ns=timestamp_ns,
                    analysis_sequence=analysis_sequence,
                    analysis_cutoff_ns=(
                        timestamp_ns
                        + self.config.burst_window_count
                        * self.config.window_sec * 1_000_000_000
                    ),
                    observations=dict(observations),
                )
                self._recovery_count = 0
            elif maximum <= self.config.recovery_threshold:
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
                self._soft_count = 0
                self._hard_count = 0
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
            "baseline_frozen": not models_updated,
            "service_model_frozen": not models_updated,
        })

    def _diagnose(self) -> FinalRCAResult:
        if self._soft is None or self._hard is None:
            raise ControlPlaneError("diagnosis requires frozen Soft and Hard contexts")
        model = self._soft.metric_model
        observations = self._hard.observations
        root_metrics = [
            self._soft.metrics[node_id]
            for node_id in model.node_ids
            if node_id in observations and self._soft.metrics[node_id].root_eligible
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
            if self._hard.timestamp_ns <= item.timestamp_ns <= self._hard.analysis_cutoff_ns
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
            snapshot = self._active_topology(window.window_end_ns)
            graph = allowed_service_graph(snapshot)
            observations, raw = self.resolver.normalize_window(window, self.baseline)
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
                self.state == "healthy"
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
            if baseline_ready:
                self._transition(
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
                    "reason": "baseline_not_ready",
                })
            if self._hard is not None \
                    and window.sequence >= self._hard.analysis_sequence \
                    and self._hard.sequence not in self._diagnosed_hard_sequences:
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
        )


def save_control_run(output: str | Path, run: ControlPlaneRun) -> None:
    directory = Path(output)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "control-run.json").write_text(
        canonical_json(run.to_dict()) + "\n", encoding="utf-8",
    )
    with (directory / "rca-results.jsonl").open("x", encoding="utf-8") as handle:
        for result in run.results:
            handle.write(canonical_json(result.to_dict()) + "\n")

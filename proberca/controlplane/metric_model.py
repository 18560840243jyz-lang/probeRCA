"""Masked healthy metric Ridge model A_v for the final control plane."""

from __future__ import annotations

import math

import numpy as np

from .config import FinalControlConfig
from .model import (
    CandidateEntityGraph,
    MetricNode,
    MetricPropagationModel,
    MetricTargetReadiness,
)
from .service_model import AllowedServiceGraph


_SAME_ENTITY_ROLE_PAIRS = frozenset({
    ("request_rate", "service_cpu_usage"),
    ("service_cpu_throttle", "request_latency"),
    ("service_cpu_throttle", "request_failure"),
    ("service_memory", "request_latency"),
    ("service_memory", "request_failure"),
    ("service_io", "request_latency"),
    ("service_lock", "request_latency"),
    ("service_localnet", "request_failure"),
    ("edge_count", "edge_latency"),
    ("edge_count", "edge_failure"),
})


def _edge_lookup(graph: AllowedServiceGraph):
    return {
        edge_id: (source, target, protocol)
        for edge_id, source, target, protocol in graph.physical_edges
    }


def _semantic_allowed(
    target: MetricNode,
    parent: MetricNode,
    candidate: CandidateEntityGraph,
    graph: AllowedServiceGraph,
) -> bool:
    if target.node_id == parent.node_id:
        return True
    if target.entity_id == parent.entity_id:
        return (parent.role, target.role) in _SAME_ENTITY_ROLE_PAIRS
    strong = {
        (source, target_id)
        for source, target_id, _strength in candidate.strong_service_relations
    }
    if parent.entity_type == target.entity_type == "service":
        return (
            (parent.entity_id, target.entity_id) in strong
            and parent.role in {
                "request_rate", "request_latency", "request_failure",
                "service_cpu_usage", "service_cpu_throttle", "service_memory",
                "service_io", "service_lock", "service_localnet",
            }
            and target.role in {"request_latency", "request_failure"}
        )
    placements = set(graph.placements)
    if parent.entity_type == "host" and target.entity_type == "service":
        if (target.entity_id, parent.entity_id) not in placements:
            return False
        allowed = {
            "host_cpu": {"service_cpu_usage", "request_latency"},
            "host_memory": {"service_memory", "request_latency", "request_failure"},
            "host_io": {"service_io", "request_latency"},
        }
        return target.role in allowed.get(parent.role, set())
    edges = _edge_lookup(graph)
    if parent.entity_type == "edge" and target.entity_type == "service":
        endpoints = edges.get(parent.entity_id)
        if endpoints is None:
            return False
        caller, _callee, _protocol = endpoints
        return (
            target.entity_id == caller
            and parent.role in {"edge_latency", "edge_failure"}
            and target.role in {"request_latency", "request_failure"}
        )
    if parent.entity_type == "host" and target.entity_type == "edge":
        endpoints = edges.get(target.entity_id)
        if endpoints is None or parent.role != "host_nic" \
                or target.role not in {"edge_latency", "edge_failure"}:
            return False
        hosted_services = {
            service for service, host in graph.placements if host == parent.entity_id
        }
        return bool(hosted_services & set(endpoints[:2]))
    if parent.entity_type == "service" and target.entity_type == "edge":
        endpoints = edges.get(target.entity_id)
        return bool(
            endpoints is not None
            and parent.entity_id in endpoints[:2]
            and parent.role == "request_rate"
            and target.role in {"edge_count", "edge_latency"}
        )
    return False


def candidate_metric_nodes(
    observations: dict[str, object], candidate: CandidateEntityGraph,
) -> dict[str, MetricNode]:
    entities = set(candidate.services) | set(candidate.hosts) | set(candidate.edges)
    return {
        node_id: (
            item if isinstance(item, MetricNode) else item.metric
        )
        for node_id, item in observations.items()
        if (
            item if isinstance(item, MetricNode) else item.metric
        ).entity_id in entities
    }


def fit_metric_propagation(
    *,
    metrics: dict[str, MetricNode],
    healthy_history: dict[int, dict[str, float]],
    candidate: CandidateEntityGraph,
    service_graph: AllowedServiceGraph,
    healthy_cutoff_ns: int,
    config: FinalControlConfig,
) -> MetricPropagationModel:
    node_ids = tuple(sorted(metrics))
    semantic_mask = tuple(sorted(
        (target_id, parent_id)
        for target_id, target in metrics.items()
        for parent_id, parent in metrics.items()
        if _semantic_allowed(target, parent, candidate, service_graph)
    ))
    coefficients: dict[tuple[str, str, int], float] = {}
    row_counts = []
    target_readiness = {}
    sequences = sorted(healthy_history)
    for target_id in node_ids:
        parent_ids = sorted(
            parent_id for target, parent_id in semantic_mask if target == target_id
        )
        features = [
            (parent_id, lag)
            for parent_id in parent_ids
            for lag in config.metric_lags
        ]
        rows = []
        targets = []
        for sequence in sequences:
            current = healthy_history[sequence]
            if target_id not in current:
                continue
            values = []
            complete = True
            for parent_id, lag in features:
                prior = healthy_history.get(sequence - lag)
                if prior is None or parent_id not in prior:
                    complete = False
                    break
                values.append(prior[parent_id])
            if complete and features:
                rows.append(values)
                targets.append(current[target_id])
        feature_count = len(features)
        minimum_rows = max(
            config.metric_min_training_rows,
            int(math.ceil(config.metric_rows_per_feature * feature_count)),
        )
        valid_rows = len(rows)
        row_counts.append(valid_rows)
        matrix = (
            np.asarray(rows, dtype=float)
            if rows and features
            else np.empty((0, feature_count), dtype=float)
        )
        effective_rank = (
            int(np.linalg.matrix_rank(
                matrix, tol=config.metric_rank_tolerance,
            ))
            if matrix.size else 0
        )
        gram = (
            matrix.T @ matrix
            + config.metric_ridge * np.eye(matrix.shape[1])
            if matrix.size else None
        )
        raw_condition = (
            float(np.linalg.cond(matrix))
            if matrix.size else None
        )
        condition = (
            raw_condition
            if raw_condition is None or math.isfinite(raw_condition)
            else None
        )
        reason = None
        if not features:
            reason = "no_allowed_features"
        elif valid_rows < minimum_rows:
            reason = "insufficient_valid_history"
        elif effective_rank < feature_count:
            reason = "rank_deficient"
        elif condition is None:
            reason = "non_finite_condition_number"
        elif condition > config.metric_max_condition_number:
            reason = "ill_conditioned"
        if reason is not None:
            target_readiness[target_id] = MetricTargetReadiness(
                target_metric=target_id,
                root_eligible=metrics[target_id].root_eligible,
                allowed_feature_count=feature_count,
                valid_training_rows=valid_rows,
                minimum_training_rows=minimum_rows,
                effective_rank=effective_rank,
                condition_number=condition,
                ready=False,
                not_ready_reason=reason,
            )
            continue
        vector = np.asarray(targets, dtype=float)
        try:
            solved = np.linalg.solve(gram, matrix.T @ vector)
        except np.linalg.LinAlgError:
            target_readiness[target_id] = MetricTargetReadiness(
                target_metric=target_id,
                root_eligible=metrics[target_id].root_eligible,
                allowed_feature_count=feature_count,
                valid_training_rows=valid_rows,
                minimum_training_rows=minimum_rows,
                effective_rank=effective_rank,
                condition_number=condition,
                ready=False,
                not_ready_reason="ridge_solve_failed",
            )
            continue
        if not np.isfinite(solved).all():
            target_readiness[target_id] = MetricTargetReadiness(
                target_metric=target_id,
                root_eligible=metrics[target_id].root_eligible,
                allowed_feature_count=feature_count,
                valid_training_rows=valid_rows,
                minimum_training_rows=minimum_rows,
                effective_rank=effective_rank,
                condition_number=condition,
                ready=False,
                not_ready_reason="non_finite_coefficients",
            )
            continue
        for (parent_id, lag), value in zip(features, solved):
            coefficients[(target_id, parent_id, lag)] = float(value)
        target_readiness[target_id] = MetricTargetReadiness(
            target_metric=target_id,
            root_eligible=metrics[target_id].root_eligible,
            allowed_feature_count=feature_count,
            valid_training_rows=valid_rows,
            minimum_training_rows=minimum_rows,
            effective_rank=effective_rank,
            condition_number=condition,
            ready=True,
            not_ready_reason=None,
        )
    return MetricPropagationModel(
        node_ids=node_ids,
        lags=config.metric_lags,
        coefficients=coefficients,
        semantic_mask=semantic_mask,
        training_rows=min(row_counts, default=0),
        healthy_cutoff_ns=healthy_cutoff_ns,
        target_readiness=target_readiness,
    )

"""Masked healthy metric Ridge model A_v for the final control plane."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from .config import FinalControlConfig
from .model import CandidateEntityGraph, MetricNode, MetricPropagationModel
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
        node_id: item.metric
        for node_id, item in observations.items()
        if item.metric.entity_id in entities
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
    missing_required = []
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
        row_counts.append(len(rows))
        if not features or len(rows) < config.metric_min_training_rows:
            if metrics[target_id].root_eligible:
                missing_required.append(target_id)
            continue
        matrix = np.asarray(rows, dtype=float)
        vector = np.asarray(targets, dtype=float)
        gram = matrix.T @ matrix + config.metric_ridge * np.eye(matrix.shape[1])
        try:
            solved = np.linalg.solve(gram, matrix.T @ vector)
        except np.linalg.LinAlgError as error:
            raise ValueError(f"metric Ridge failed for {target_id}") from error
        if not np.isfinite(solved).all():
            raise ValueError(f"metric Ridge produced non-finite coefficients for {target_id}")
        for (parent_id, lag), value in zip(features, solved):
            coefficients[(target_id, parent_id, lag)] = float(value)
    if missing_required:
        raise ValueError(
            "metric model is not ready for root coordinates: "
            + ",".join(sorted(missing_required))
        )
    return MetricPropagationModel(
        node_ids=node_ids,
        lags=config.metric_lags,
        coefficients=coefficients,
        semantic_mask=semantic_mask,
        training_rows=min(row_counts, default=0),
        healthy_cutoff_ns=healthy_cutoff_ns,
    )

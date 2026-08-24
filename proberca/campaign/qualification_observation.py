"""Derive load-qualification model coverage from sealed campaign archives."""

from __future__ import annotations

import math
from typing import Any

from proberca.controlplane.config import FinalControlConfig
from proberca.controlplane.metric_model import (
    candidate_metric_nodes,
    fit_metric_propagation,
)
from proberca.controlplane.observations import MetricResolver, RobustBaselineStore
from proberca.controlplane.service_model import (
    build_candidate_graph,
    formal_service_graph,
)
from proberca.dataplane.archive import CollectionArchive
from proberca.dataplane.burst_archive import BurstArchive


class QualificationObservationError(RuntimeError):
    pass


_TELEMETRY_FIELDS = frozenset({
    "measured_rps", "business_error_rate", "worker_cpu_p95",
    "worker_memory_p95", "pod_restart_delta",
})
_SOURCE_GAP_REASONS = frozenset({
    "zero_coverage", "missing_component", "excessive_event_loss",
    "inconsistent_histogram",
})


def _projected_rows(
    statuses: dict[str, dict[str, Any]], *,
    observed_windows: int,
    target_windows: int,
    observed_field: str,
    minimum_field: str,
) -> tuple[dict[str, int], dict[str, int]]:
    projected, required = {}, {}
    for node_id, status in sorted(statuses.items()):
        observed = int(status[observed_field])
        minimum = int(status[minimum_field])
        projected[node_id] = int(math.floor(
            observed / observed_windows * target_windows
        ))
        # The Qualification gate keeps a 2x history safety margin.  This is
        # coordinate-specific and never a fixed per-edge coverage threshold.
        required[node_id] = 2 * minimum
    return projected, required


def build_archive_qualification_observation(
    *,
    normal_root,
    burst_root,
    campaign_config: dict[str, Any],
    control_config: FinalControlConfig,
    profile_id: str,
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    """Build the exact objective observation consumed by qualification.py.

    Soft, Hard, READY, FISTA, and RCA are deliberately neither accepted as
    telemetry nor consulted while building Baseline/A_v coverage projections.
    """

    if set(telemetry) != _TELEMETRY_FIELDS:
        raise QualificationObservationError(
            "qualification telemetry fields are incomplete or contain control output"
        )
    profiles = {
        item["profile_id"]: item
        for item in campaign_config["load_qualification"]["profiles"]
    }
    if profile_id not in profiles:
        raise QualificationObservationError("unknown qualification profile")
    if control_config.load_profile_id != profile_id:
        raise QualificationObservationError(
            "qualification control config belongs to another load profile"
        )
    normal = CollectionArchive.load(normal_root)
    burst = BurstArchive.load(burst_root)
    normal.validate()
    burst.validate()
    expected_windows = int(
        campaign_config["load_qualification"]["duration_seconds_per_profile"]
    )
    if normal.window_count != expected_windows or burst.window_count != expected_windows:
        raise QualificationObservationError(
            "qualification archives must contain the exact 300-window profile"
        )
    normal_windows = tuple(normal.iter_windows())
    burst_windows = tuple(burst.iter_windows())
    aligned = normal.dataset_id == burst.dataset_id and all(
        (
            left.sequence, left.window_start_ns, left.window_end_ns
        ) == (
            right.sequence, right.window_start_ns, right.window_end_ns
        )
        for left, right in zip(normal_windows, burst_windows)
    )

    resolver = MetricResolver(control_config)
    baseline = RobustBaselineStore(control_config)
    metric_catalog = {}
    metric_specs = {}
    healthy_history: dict[int, dict[str, float]] = {}
    topology_fingerprints = set()
    runtime_fingerprints = set()
    observed_edges = set()
    source_gap_windows = 0
    graph = None
    for window, burst_window in zip(normal_windows, burst_windows):
        if len(window.topology_events) != 1:
            raise QualificationObservationError(
                "qualification requires one topology snapshot per window"
            )
        graph = formal_service_graph(window.topology_events[0], control_config)
        topology_fingerprints.add(graph.topology_fingerprint)
        runtime_fingerprints.add(graph.runtime_identity_fingerprint)
        for record in (*window.node_metrics, *window.edge_metrics):
            metric, spec = resolver.resolve(record)
            if not control_config.entity_is_in_formal_scope(metric.entity_id):
                continue
            metric_catalog[metric.node_id] = metric
            metric_specs[metric.node_id] = spec
            if (
                spec.role == "edge_count" and record.valid
                and record.value is not None and float(record.value) > 0.0
            ):
                observed_edges.add(metric.entity_id)
        observations, raw = resolver.normalize_window(window, baseline)
        gap = burst_window.event_loss_rate > 0.0 or any(
            item.get("invalid_reason") in _SOURCE_GAP_REASONS
            for item in resolver.last_validity.values()
        )
        source_gap_windows += int(gap)
        signed = {
            node_id: item.signed_z for node_id, item in observations.items()
        }
        if signed:
            healthy_history[window.sequence] = signed
        for node_id, (value, spec) in raw.items():
            baseline.update(node_id, value, spec)
    if graph is None:
        raise QualificationObservationError("qualification archive is empty")

    strengths = {
        (source, target): 1.0
        for source, target, _relation in graph.relations
    }
    candidate = build_candidate_graph(
        graph=graph, service_strengths=strengths,
        seed_services=set(graph.services),
        seed_edges={item[0] for item in graph.physical_edges},
        config=control_config,
    )
    metrics = candidate_metric_nodes(metric_catalog, candidate)
    model = fit_metric_propagation(
        metrics=metrics, healthy_history=healthy_history,
        candidate=candidate, service_graph=graph,
        healthy_cutoff_ns=normal.end_ns, config=control_config,
    )
    roots = set(control_config.calibration_required_root_coordinates)
    missing_roots = sorted(roots - set(model.target_readiness))
    if missing_roots:
        raise QualificationObservationError(
            f"qualification archive omits formal roots: {missing_roots}"
        )
    required_baseline_ids = set(roots)
    required_baseline_ids.update(
        parent for target, parent in model.semantic_mask if target in roots
    )
    baseline_status = {
        node_id: baseline.status(node_id, metric_specs[node_id])
        for node_id in sorted(required_baseline_ids)
    }
    av_status = {
        node_id: {
            "valid_training_rows": model.target_readiness[node_id].valid_training_rows,
            "minimum_training_rows": model.target_readiness[node_id].minimum_training_rows,
        }
        for node_id in sorted(roots)
    }
    target_windows = int(campaign_config["timing"]["healthy_seconds"])
    projected_baseline, required_baseline = _projected_rows(
        baseline_status, observed_windows=expected_windows,
        target_windows=target_windows,
        observed_field="baseline_sample_count",
        minimum_field="minimum_healthy_samples",
    )
    projected_av, required_av = _projected_rows(
        av_status, observed_windows=expected_windows,
        target_windows=target_windows,
        observed_field="valid_training_rows",
        minimum_field="minimum_training_rows",
    )
    required_edges = tuple(sorted(
        f"{item['src']}->{item['dst']}"
        for item in campaign_config["formal_tcp_edges"]
    ))
    cluster_prefix = (
        f"{campaign_config['formal_scope']['cluster_id']}::"
        f"{campaign_config['formal_scope']['namespace']}::"
    )
    observed_edge_names = tuple(sorted(
        entity_id[len(cluster_prefix):].removesuffix("::tcp")
        for entity_id in observed_edges
    ))
    return {
        "profile_id": profile_id,
        "target_arrival_rate_rps": float(
            profiles[profile_id]["target_arrival_rate_rps"]
        ),
        "measured_rps": float(telemetry["measured_rps"]),
        "normal_burst_aligned": aligned,
        "source_gap_count": source_gap_windows,
        "pod_restart_delta": int(telemetry["pod_restart_delta"]),
        "topology_change_count": max(0, len(topology_fingerprints) - 1),
        "runtime_identity_change_count": max(0, len(runtime_fingerprints) - 1),
        "business_error_rate": float(telemetry["business_error_rate"]),
        "worker_cpu_p95": float(telemetry["worker_cpu_p95"]),
        "worker_memory_p95": float(telemetry["worker_memory_p95"]),
        "observed_tcp_edges": observed_edge_names,
        "required_tcp_edges": required_edges,
        "projected_baseline_rows": projected_baseline,
        "projected_av_rows": projected_av,
        "required_baseline_rows": required_baseline,
        "required_av_rows": required_av,
    }

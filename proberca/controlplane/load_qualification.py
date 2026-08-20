"""Profile-local diagnostics for qualifying a stable Healthy workload."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

import numpy as np

from proberca.dataplane import CollectionArchive

from .config import FinalControlConfig
from .observations import MetricResolver, RobustBaselineStore
from .service_model import formal_service_graph


def _percentiles(values: Iterable[float]) -> dict[str, float | int | None]:
    samples = np.asarray(tuple(values), dtype=float)
    if not samples.size:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    return {
        "count": int(samples.size),
        "p50": float(np.quantile(samples, 0.50)),
        "p95": float(np.quantile(samples, 0.95)),
        "max": float(np.max(samples)),
    }


def project_baseline_coverage(
    statuses: dict[str, dict[str, Any]], *,
    qualification_windows: int, calibration_windows: int,
) -> dict[str, dict[str, Any]]:
    """Project observed model-valid rates using each coordinate's own gate."""
    projected = {}
    for node_id, status in sorted(statuses.items()):
        observed = int(status["baseline_sample_count"])
        minimum = int(status["minimum_healthy_samples"])
        value = observed / qualification_windows * calibration_windows
        projected[node_id] = {
            "observed_model_valid_windows": observed,
            "observed_valid_rate": observed / qualification_windows,
            "projected_valid_windows": value,
            "minimum_required_windows": minimum,
            "qualification_required_windows": 2 * minimum,
            "qualified": value >= 2 * minimum,
        }
    return projected


def project_av_coverage(
    statuses: dict[str, dict[str, Any]], *,
    qualification_windows: int, calibration_windows: int,
) -> dict[str, dict[str, Any]]:
    """Project A_v rows using its computed feature-dependent minimum."""
    projected = {}
    for node_id, status in sorted(statuses.items()):
        observed = int(status["valid_training_rows"])
        minimum = int(status["minimum_training_rows"])
        value = observed / qualification_windows * calibration_windows
        projected[node_id] = {
            "allowed_feature_count": int(status["allowed_feature_count"]),
            "observed_valid_training_rows": observed,
            "training_row_rate": observed / qualification_windows,
            "projected_training_rows": value,
            "minimum_training_rows": minimum,
            "qualification_required_rows": 2 * minimum,
            "qualified": value >= 2 * minimum,
        }
    return projected


def summarize_score_episodes(
    score_windows: Iterable[
        tuple[int, dict[tuple[str, str], float]]
        | tuple[
            int,
            dict[tuple[str, str], float],
            dict[tuple[str, str], dict[str, float]],
        ]
    ],
    *,
    threshold: float,
    consecutive_windows: int,
) -> list[dict[str, Any]]:
    """Collapse consecutive qualifying scores into independent episodes."""
    counts: dict[tuple[str, str], int] = {}
    run_start: dict[tuple[str, str], int] = {}
    run_max: dict[tuple[str, str], float] = {}
    run_role_max: dict[tuple[str, str], dict[str, float]] = {}
    active: dict[tuple[str, str], int] = {}
    episodes: list[dict[str, Any]] = []
    for item in score_windows:
        timestamp_ns, scores = item[:2]
        role_scores = item[2] if len(item) == 3 else {}
        keys = set(scores) | set(counts) | set(active)
        for key in sorted(keys):
            value = float(scores.get(key, 0.0))
            if value >= threshold:
                counts[key] = counts.get(key, 0) + 1
                run_start.setdefault(key, timestamp_ns)
                run_max[key] = max(run_max.get(key, value), value)
                role_maximums = run_role_max.setdefault(key, {})
                for role, score in role_scores.get(key, {}).items():
                    role_maximums[role] = max(
                        role_maximums.get(role, float("-inf")), float(score),
                    )
            else:
                counts[key] = 0
                run_start.pop(key, None)
                run_max.pop(key, None)
                run_role_max.pop(key, None)
                active.pop(key, None)
                continue
            if counts[key] < consecutive_windows:
                continue
            index = active.get(key)
            if index is None:
                index = len(episodes)
                active[key] = index
                episodes.append({
                    "entity_type": key[0],
                    "entity_id": key[1],
                    "start_timestamp_ns": run_start[key],
                    "end_timestamp_ns": timestamp_ns,
                    "duration_windows": counts[key],
                    "maximum_score": run_max[key],
                    "latency_maximum_score": max(
                        (
                            score for role, score
                            in run_role_max.get(key, {}).items()
                            if role.endswith("latency")
                        ),
                        default=0.0,
                    ),
                    "failure_maximum_score": max(
                        (
                            score for role, score
                            in run_role_max.get(key, {}).items()
                            if role.endswith("failure")
                        ),
                        default=0.0,
                    ),
                })
            else:
                episodes[index]["end_timestamp_ns"] = timestamp_ns
                episodes[index]["duration_windows"] = counts[key]
                episodes[index]["maximum_score"] = max(
                    float(episodes[index]["maximum_score"]),
                    run_max[key],
                )
                episodes[index]["latency_maximum_score"] = max(
                    float(episodes[index]["latency_maximum_score"]),
                    max(
                        (
                            score for role, score
                            in run_role_max.get(key, {}).items()
                            if role.endswith("latency")
                        ),
                        default=0.0,
                    ),
                )
                episodes[index]["failure_maximum_score"] = max(
                    float(episodes[index]["failure_maximum_score"]),
                    max(
                        (
                            score for role, score
                            in run_role_max.get(key, {}).items()
                            if role.endswith("failure")
                        ),
                        default=0.0,
                    ),
                )
    return episodes


def summarize_load_qualification(
    archive: CollectionArchive,
    *,
    config: FinalControlConfig,
    profile_id: str,
    profile_fingerprint: str,
    qualification: dict[str, Any],
    pod_restart_delta: int = 0,
) -> dict[str, Any]:
    """Build one bounded, profile-local qualification summary."""
    archive.validate()
    windows = tuple(archive.iter_windows())
    if len(windows) != qualification["duration_windows"]:
        raise ValueError("qualification archive window count mismatch")
    baseline_windows = qualification["provisional_baseline_windows"]
    baseline = RobustBaselineStore(config)
    resolver = MetricResolver(config)
    score_windows: list[tuple[
        int,
        dict[tuple[str, str], float],
        dict[tuple[str, str], dict[str, float]],
    ]] = []
    host_cpu_psi: list[float] = []
    service_latency: dict[str, list[float]] = defaultdict(list)
    edge_request_count: dict[str, list[float]] = defaultdict(list)
    edge_model_valid: dict[str, int] = defaultdict(int)
    edge_alert_eligible: dict[str, int] = defaultdict(int)
    topology_fingerprints = set()
    runtime_fingerprints = set()

    for index, window in enumerate(windows):
        if len(window.topology_events) != 1:
            raise ValueError(
                "qualification requires one complete topology snapshot per window"
            )
        graph = formal_service_graph(window.topology_events[0], config)
        topology_fingerprints.add(graph.topology_fingerprint)
        runtime_fingerprints.add(graph.runtime_identity_fingerprint)
        observations, raw = resolver.normalize_window(window, baseline)
        validity = resolver.last_validity
        for record in (*window.node_metrics, *window.edge_metrics):
            metric, spec = resolver.resolve(record)
            if not config.entity_is_in_formal_scope(metric.entity_id):
                continue
            if spec.entity_type == "host" and spec.metric_name == "cpu_psi" \
                    and record.valid and record.value is not None:
                host_cpu_psi.append(float(record.value))
            if spec.entity_type == "service" \
                    and spec.metric_name == "request_latency_p95" \
                    and record.valid and record.value is not None:
                service_latency[metric.entity_id].append(float(record.value))
            if spec.entity_type == "edge" \
                    and spec.metric_name == "edge_request_count" \
                    and record.valid and record.value is not None:
                edge_request_count[metric.entity_id].append(float(record.value))
            if spec.entity_type == "edge" \
                    and spec.metric_name == "edge_latency_p95":
                status = validity[metric.node_id]
                edge_model_valid[metric.entity_id] += int(
                    status["model_valid"]
                )
                edge_alert_eligible[metric.entity_id] += int(
                    status["alert_eligible"]
                )
        if index < baseline_windows:
            for node_id, (value, spec) in raw.items():
                baseline.update(node_id, value, spec)
            continue
        by_entity: dict[str, dict[str, float]] = defaultdict(dict)
        for observation in observations.values():
            if observation.alert_eligible:
                by_entity[observation.metric.entity_id][
                    observation.metric.role
                ] = observation.anomaly
        scores: dict[tuple[str, str], float] = {}
        for service in graph.services:
            roles = by_entity.get(service, {})
            if {"request_latency", "request_failure"} & set(roles):
                scores[("service", service)] = (
                    config.alpha_latency
                    * roles.get("request_latency", 0.0)
                    + config.alpha_failure
                    * roles.get("request_failure", 0.0)
                )
        for edge_id, _source, _target, _protocol in graph.physical_edges:
            roles = by_entity.get(edge_id, {})
            scores[("edge", edge_id)] = max(
                roles.get("edge_latency", 0.0),
                roles.get("edge_failure", 0.0),
            )
        score_windows.append((
            window.window_end_ns,
            scores,
            {
                (("service", service)): dict(by_entity.get(service, {}))
                for service in graph.services
            } | {
                ("edge", edge_id): dict(by_entity.get(edge_id, {}))
                for edge_id, _source, _target, _protocol
                in graph.physical_edges
            },
        ))

    soft = summarize_score_episodes(
        score_windows,
        threshold=config.soft_threshold,
        consecutive_windows=config.soft_consecutive_windows,
    )
    hard_candidates = summarize_score_episodes(
        score_windows,
        threshold=config.hard_threshold,
        consecutive_windows=config.hard_candidate_windows,
    )
    confirmed_hard = summarize_score_episodes(
        score_windows,
        threshold=config.hard_threshold,
        consecutive_windows=config.hard_consecutive_windows,
    )
    formal_edges = sorted(config.formal_tcp_edge_entity_ids)
    edge_stats = {
        edge_id: {
            "request_count": _percentiles(edge_request_count[edge_id]),
            "model_valid_windows": edge_model_valid[edge_id],
            "alert_eligible_windows": edge_alert_eligible[edge_id],
        }
        for edge_id in formal_edges
    }
    cpu_stats = _percentiles(host_cpu_psi)
    # Run the production control plane on the bounded qualification archive to
    # obtain its exact per-coordinate Baseline and A_v row requirements.  The
    # 300-window rates are then projected to the frozen 600-window learning
    # duration; no fixed per-edge coverage count is used as a proxy.
    from .pipeline import FinalControlPlane
    projection_report = FinalControlPlane(config).run(
        archive
    ).calibration_readiness
    baseline_projection = project_baseline_coverage(
        projection_report["baseline_status"],
        qualification_windows=len(windows),
        calibration_windows=config.calibration_learning_windows,
    )
    av_projection = project_av_coverage(
        projection_report["metric_model_status"],
        qualification_windows=len(windows),
        calibration_windows=config.calibration_learning_windows,
    )
    failed_reasons = []
    if confirmed_hard:
        failed_reasons.append("confirmed_hard_episode")
    if any(not item["qualified"] for item in baseline_projection.values()):
        failed_reasons.append("projected_baseline_coverage")
    if any(not item["qualified"] for item in av_projection.values()):
        failed_reasons.append("projected_Av_training_rows")
    if len(topology_fingerprints) != 1:
        failed_reasons.append("topology_changed")
    if len(runtime_fingerprints) != 1:
        failed_reasons.append("runtime_identity_changed")
    if pod_restart_delta:
        failed_reasons.append("pod_restart")
    return {
        "schema_version": "probeRCA-load-qualification-result-v2",
        "profile_id": profile_id,
        "profile_fingerprint": profile_fingerprint,
        "dataset_id": archive.dataset_id,
        "normal_manifest_fingerprint": archive.manifest_fingerprint,
        "window_count": len(windows),
        "provisional_baseline_windows": baseline_windows,
        "host_cpu_psi": cpu_stats,
        "service_latency": {
            key: _percentiles(values)
            for key, values in sorted(service_latency.items())
        },
        "tcp_edges": edge_stats,
        "soft_episode_count": len(soft),
        "soft_episodes": soft,
        "hard_candidate_episode_count": len(hard_candidates),
        "hard_candidate_episodes": hard_candidates,
        "confirmed_hard_episode_count": len(confirmed_hard),
        "confirmed_hard_episodes": confirmed_hard,
        "baseline_projection": baseline_projection,
        "Av_projection": av_projection,
        "pod_restart_delta": pod_restart_delta,
        "topology_fingerprint_count": len(topology_fingerprints),
        "runtime_identity_fingerprint_count": len(runtime_fingerprints),
        "qualified": not failed_reasons,
        "failed_reasons": failed_reasons,
    }

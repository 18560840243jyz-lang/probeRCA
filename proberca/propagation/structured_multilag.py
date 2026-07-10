"""Structured multi-lag stable propagation support for B2P.

This module is label-free. It consumes raw metrics, service graph, alert windows,
candidate nodes, and probe policy outputs. It never reads root labels, target
labels, injected paths, or incident start/end times for propagation learning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.service_metric_identity import (
    assert_or_repair_node_ownership,
    make_node_id,
    metric_family,
    split_node_id,
)


@dataclass
class StructuredPropagationConfig:
    lags: list[int] = field(default_factory=lambda: [1, 2, 3])
    baseline_strategy: str = "prefix_before_alert"
    baseline_ratio: float = 0.3
    min_points: int = 4
    ridge_lambda: float = 0.1
    max_parents_per_target: int = 48
    max_lagged_features: int = 96
    use_sampling_probability: bool = True
    min_sampling_probability: float = 0.05
    max_ipw_weight: float = 20.0
    resource_to_request_boost: float = 1.2
    cross_service_request_weight: float = 1.0
    same_service_weight: float = 0.8
    self_lag_weight: float = 0.6
    max_path_hops: int = 4


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def load_metrics_panel(metrics_path: str) -> dict[str, Any]:
    rows = load_jsonl(metrics_path)
    grouped: dict[str, list[tuple[float, float]]] = defaultdict(list)
    node_to_service: dict[str, str] = {}
    node_to_metric: dict[str, str] = {}
    timestamps: set[float] = set()
    for row in rows:
        service = row.get("service") or row.get("service_name") or row.get("pod_service")
        metric = row.get("metric") or row.get("metric_name") or row.get("name")
        ts = row.get("timestamp", row.get("ts", row.get("time")))
        value = _as_float(row.get("value"))
        if not service or not metric or ts is None or value is None:
            continue
        try:
            t = float(ts)
        except (TypeError, ValueError):
            continue
        node_id = make_node_id(str(service), str(metric))
        fixed = assert_or_repair_node_ownership({"node_id": node_id, "service": service, "metric": metric})
        node_id = fixed["node_id"]
        grouped[node_id].append((t, value))
        node_to_service[node_id] = fixed["service"]
        node_to_metric[node_id] = fixed["metric"]
        timestamps.add(t)
    node_ids = sorted(grouped)
    ts_sorted = sorted(timestamps)
    t_index = {t: i for i, t in enumerate(ts_sorted)}
    z = np.zeros((len(ts_sorted), len(node_ids)), dtype=float)
    mask = np.zeros((len(ts_sorted), len(node_ids)), dtype=bool)
    for j, node in enumerate(node_ids):
        for t, value in grouped[node]:
            i = t_index[t]
            z[i, j] = value
            mask[i, j] = True
    service_to_nodes: dict[str, list[str]] = defaultdict(list)
    for node in node_ids:
        service_to_nodes[node_to_service[node]].append(node)
    return {
        "node_ids": node_ids,
        "service_to_nodes": {k: sorted(v) for k, v in service_to_nodes.items()},
        "node_to_service": node_to_service,
        "node_to_metric": node_to_metric,
        "timestamps": ts_sorted,
        "raw_value_matrix": z,
        "value_available_mask": mask,
    }


def robust_normalize_panel(panel: dict[str, Any], alert_windows_path: str | None, config: StructuredPropagationConfig) -> dict[str, Any]:
    values = np.asarray(panel["raw_value_matrix"], dtype=float)
    mask = np.asarray(panel["value_available_mask"], dtype=bool)
    timestamps = list(panel["timestamps"])
    baseline_end: float | None = None
    if alert_windows_path:
        windows = load_jsonl(alert_windows_path)
        starts = [_as_float(row.get("start_ts")) for row in windows]
        starts = [s for s in starts if s is not None]
        if starts:
            baseline_end = min(starts)
    z = np.zeros_like(values, dtype=float)
    metadata = {"baseline_strategy": "alert_window_pre_start" if baseline_end is not None else "prefix_baseline", "baseline_end": baseline_end}
    prefix_n = max(1, int(math.ceil(len(timestamps) * config.baseline_ratio))) if timestamps else 0
    for j in range(values.shape[1]):
        if baseline_end is not None:
            idx = [i for i, t in enumerate(timestamps) if t < baseline_end and mask[i, j]]
        else:
            idx = [i for i in range(min(prefix_n, len(timestamps))) if mask[i, j]]
        if len(idx) < config.min_points:
            idx = [i for i in range(min(prefix_n, len(timestamps))) if mask[i, j]]
        if not idx:
            idx = [i for i in range(len(timestamps)) if mask[i, j]]
        base = values[idx, j] if idx else np.asarray([0.0])
        median = float(np.median(base))
        mad = float(np.median(np.abs(base - median)))
        scale = 1.4826 * mad + 1e-6
        z[:, j] = (values[:, j] - median) / scale
    return {"z_matrix": z, "normalization_metadata": metadata}


def parse_service_graph(service_graph_path: str) -> dict[str, Any]:
    rows = load_jsonl(service_graph_path)
    services: set[str] = set()
    caller_to_callee: dict[str, set[str]] = defaultdict(set)
    impact_children: dict[str, set[str]] = defaultdict(set)
    impact_parents: dict[str, set[str]] = defaultdict(set)
    edges: list[dict[str, str]] = []
    for row in rows:
        src = str(row.get("src") or row.get("source") or row.get("caller") or "")
        dst = str(row.get("dst") or row.get("target") or row.get("callee") or "")
        if not src or not dst:
            continue
        services.update([src, dst])
        caller_to_callee[src].add(dst)
        # Current path explanation traverses callee -> caller for impact paths.
        impact_children[dst].add(src)
        impact_parents[src].add(dst)
        edges.append({"src": src, "dst": dst})
    for svc in list(services):
        impact_children.setdefault(svc, set())
        impact_parents.setdefault(svc, set())
    return {
        "services": sorted(services),
        "parents": {k: sorted(v) for k, v in impact_parents.items()},
        "children": {k: sorted(v) for k, v in impact_children.items()},
        "edges": edges,
        "direction_assumption": "service_graph src->dst is caller->callee; propagation impact traverses callee->caller, matching path explanation",
    }


def _candidate_node_ids(candidate_nodes_path: str, panel: dict[str, Any]) -> set[str]:
    rows = load_jsonl(candidate_nodes_path)
    out: set[str] = set()
    for row in rows:
        fixed = assert_or_repair_node_ownership(row)
        if fixed.get("node_id") and fixed["node_id"] != "unknown.unknown":
            out.add(str(fixed["node_id"]))
    return out or set(panel["node_ids"])


def _relation(parent_service: str, parent_metric: str, target_service: str, target_metric: str, graph: dict[str, Any]) -> tuple[str | None, float]:
    pf = metric_family(parent_metric)
    tf = metric_family(target_metric)
    if parent_service == target_service and parent_metric == target_metric:
        return "self_lag", 0.6
    if parent_service == target_service and tf == "load" and pf != "load":
        return "same_service_resource_to_request", 0.8
    if parent_service == target_service and tf == "load" and pf == "load":
        return "same_service_request", 0.8
    if target_service in graph.get("children", {}).get(parent_service, []):
        if tf == "load" and pf != "load":
            return "cross_service_resource_to_request", 1.2
        if tf == "load" and pf == "load":
            return "cross_service_request_to_request", 1.0
    return None, 0.0


def build_structured_parent_sets(panel: dict[str, Any], service_graph: dict[str, Any], candidate_nodes: set[str], config: StructuredPropagationConfig) -> list[dict[str, Any]]:
    node_ids = [node for node in panel["node_ids"] if node in candidate_nodes]
    rows: list[dict[str, Any]] = []
    for target in node_ids:
        target_service = panel["node_to_service"].get(target, split_node_id(target)[0])
        target_metric = panel["node_to_metric"].get(target, split_node_id(target)[1])
        candidates: list[dict[str, Any]] = []
        for parent in node_ids:
            parent_service = panel["node_to_service"].get(parent, split_node_id(parent)[0])
            parent_metric = panel["node_to_metric"].get(parent, split_node_id(parent)[1])
            rel, weight = _relation(parent_service, parent_metric, target_service, target_metric, service_graph)
            if not rel:
                continue
            if rel == "same_service_resource_to_request":
                weight *= config.resource_to_request_boost
            candidates.append({
                "target_node": target,
                "parent_node": parent,
                "parent_service": parent_service,
                "parent_metric": parent_metric,
                "target_service": target_service,
                "target_metric": target_metric,
                "relation_type": rel,
                "allowed_lags": list(config.lags),
                "structural_weight": weight,
                "uses_labels": False,
            })
        candidates.sort(key=lambda r: (-float(r["structural_weight"]), r["relation_type"], r["parent_node"]))
        rows.extend(candidates[: config.max_parents_per_target])
    return rows


def _load_sampling_probability(probe_policy_dir: str, panel: dict[str, Any], config: StructuredPropagationConfig) -> dict[str, float]:
    rows = load_jsonl(Path(probe_policy_dir) / "sampling_log.jsonl") + load_jsonl(Path(probe_policy_dir) / "observation_mask.jsonl")
    probs: dict[str, float] = {}
    for row in rows:
        fixed = assert_or_repair_node_ownership(row)
        node = str(fixed.get("node_id") or "")
        p = _as_float(row.get("sampling_probability", row.get("observed_probability")))
        if node and p is not None:
            probs[node] = max(probs.get(node, 0.0), p)
    for node in panel["node_ids"]:
        if node not in probs:
            metric = panel["node_to_metric"].get(node, split_node_id(node)[1])
            probs[node] = 1.0 if metric_family(metric) == "load" else config.min_sampling_probability
    return probs


def fit_multilag_ridge_for_target(target_node: str, parent_set: list[dict[str, Any]], panel: dict[str, Any], z_matrix: np.ndarray, mask: np.ndarray, sampling_probability: dict[str, float], config: StructuredPropagationConfig) -> dict[str, Any]:
    node_ids = panel["node_ids"]
    idx = {node: i for i, node in enumerate(node_ids)}
    if target_node not in idx or not parent_set:
        return {"edges": [], "predictions": [], "residuals": [], "fit_points": 0, "explained_variance": 0.0}
    features: list[tuple[str, int, dict[str, Any]]] = []
    for parent in parent_set:
        for lag in config.lags:
            features.append((str(parent["parent_node"]), int(lag), parent))
    features = features[: config.max_lagged_features]
    max_lag = max(config.lags) if config.lags else 1
    t_col = idx[target_node]
    X: list[list[float]] = []
    y: list[float] = []
    weights: list[float] = []
    times: list[int] = []
    for t in range(max_lag, z_matrix.shape[0]):
        if not mask[t, t_col]:
            continue
        row: list[float] = []
        ok = True
        for parent_node, lag, _ in features:
            p_col = idx.get(parent_node)
            if p_col is None or not mask[t - lag, p_col]:
                row.append(0.0)
            else:
                p = max(sampling_probability.get(parent_node, config.min_sampling_probability), config.min_sampling_probability)
                ipw = min(1.0 / p, config.max_ipw_weight) if config.use_sampling_probability else 1.0
                row.append(float(z_matrix[t - lag, p_col]) * ipw)
        if ok:
            X.append(row)
            y.append(float(z_matrix[t, t_col]))
            p_t = max(sampling_probability.get(target_node, config.min_sampling_probability), config.min_sampling_probability)
            weights.append(min(1.0 / p_t, config.max_ipw_weight) if config.use_sampling_probability else 1.0)
            times.append(t)
    if len(X) < config.min_points:
        return {"edges": [], "predictions": [], "residuals": [], "fit_points": len(X), "explained_variance": 0.0}
    Xa = np.asarray(X, dtype=float)
    ya = np.asarray(y, dtype=float)
    wa = np.sqrt(np.asarray(weights, dtype=float))[:, None]
    Xw = Xa * wa
    yw = ya * wa[:, 0]
    reg = config.ridge_lambda * np.eye(Xa.shape[1])
    try:
        theta = np.linalg.solve(Xw.T @ Xw + reg, Xw.T @ yw)
    except np.linalg.LinAlgError:
        theta = np.linalg.pinv(Xw.T @ Xw + reg) @ (Xw.T @ yw)
    pred = Xa @ theta
    resid = ya - pred
    denom = float(np.var(ya)) + 1e-9
    explained = max(0.0, min(1.0, 1.0 - float(np.var(resid)) / denom))
    edges: list[dict[str, Any]] = []
    for coef, (parent_node, lag, parent_row) in zip(theta, features):
        coef = float(coef)
        if not math.isfinite(coef):
            continue
        edges.append({
            "target_node": target_node,
            "parent_node": parent_node,
            "lag": lag,
            "coefficient": coef,
            "abs_coefficient": abs(coef),
            "relation_type": parent_row["relation_type"],
            "structural_weight": parent_row["structural_weight"],
            "effective_weight": abs(coef) * float(parent_row["structural_weight"]),
            "fit_points": len(X),
            "explained_variance": explained,
            "uses_labels": False,
        })
    predictions = [{"target_node": target_node, "t_index": int(t), "predicted_z": float(p), "actual_z": float(a)} for t, p, a in zip(times, pred, ya)]
    residuals = [{"target_node": target_node, "t_index": int(t), "residual": float(r)} for t, r in zip(times, resid)]
    return {"edges": edges, "predictions": predictions, "residuals": residuals, "fit_points": len(X), "explained_variance": explained}


def fit_structured_multilag_propagation(raw_input_dir: str, candidate_dir: str, probe_policy_dir: str, alert_dir: str, output_dir: str, config: StructuredPropagationConfig | None = None) -> dict[str, Any]:
    cfg = config or StructuredPropagationConfig()
    raw = Path(raw_input_dir)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    panel = load_metrics_panel(str(raw / "metrics.jsonl"))
    norm = robust_normalize_panel(panel, str(Path(alert_dir) / "alert_windows.jsonl"), cfg)
    graph = parse_service_graph(str(raw / "service_graph.jsonl"))
    candidate_nodes = _candidate_node_ids(str(Path(candidate_dir) / "candidate_metric_nodes.jsonl"), panel)
    parent_sets = build_structured_parent_sets(panel, graph, candidate_nodes, cfg)
    sampling = _load_sampling_probability(probe_policy_dir, panel, cfg)
    by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in parent_sets:
        by_target[str(row["target_node"])].append(row)
    z = np.asarray(norm["z_matrix"], dtype=float)
    mask = np.asarray(panel["value_available_mask"], dtype=bool)
    edges: list[dict[str, Any]] = []
    predictions: list[dict[str, Any]] = []
    residuals: list[dict[str, Any]] = []
    for target, parents in sorted(by_target.items()):
        fit = fit_multilag_ridge_for_target(target, parents, panel, z, mask, sampling, cfg)
        edges.extend(fit["edges"])
        predictions.extend(fit["predictions"])
        residuals.extend(fit["residuals"])
    relation_counts = defaultdict(int)
    for row in parent_sets:
        relation_counts[row["relation_type"]] += 1
    metadata = {
        "lags": list(cfg.lags),
        "node_count": len(panel["node_ids"]),
        "parent_edge_count": len(parent_sets),
        "learned_edge_count": len(edges),
        "same_service_edge_count": relation_counts["same_service_resource_to_request"] + relation_counts["same_service_request"] + relation_counts["self_lag"],
        "cross_service_edge_count": relation_counts["cross_service_resource_to_request"] + relation_counts["cross_service_request_to_request"],
        "resource_to_request_edge_count": relation_counts["same_service_resource_to_request"] + relation_counts["cross_service_resource_to_request"],
        "request_to_request_edge_count": relation_counts["same_service_request"] + relation_counts["cross_service_request_to_request"],
        "self_lag_edge_count": relation_counts["self_lag"],
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "uses_alert_windows": True,
        "uses_sampling_probability": bool(cfg.use_sampling_probability),
        "structured_propagation_model": "structured_multilag_ridge",
        "propagation_model": "structured_multilag_ridge",
        "stable_only": True,
        "propagation_drift_used": False,
        "direction_assumption": graph["direction_assumption"],
        **norm["normalization_metadata"],
    }
    write_jsonl(output / "structured_parent_sets.jsonl", parent_sets)
    write_jsonl(output / "structured_propagation_edges.jsonl", sorted(edges, key=lambda r: (r["target_node"], r["parent_node"], r["lag"])))
    write_jsonl(output / "structured_propagation_predictions.jsonl", predictions)
    write_jsonl(output / "structured_propagation_residuals.jsonl", residuals)
    write_json(output / "structured_propagation_metadata.json", metadata)
    return {"metadata": metadata, "parent_sets": parent_sets, "edges": edges, "predictions": predictions, "residuals": residuals}


def _bfs_path(children: dict[str, list[str]], start: str, goal: str, max_hops: int) -> list[str]:
    if start == goal:
        return [start]
    q: deque[list[str]] = deque([[start]])
    seen = {start}
    while q:
        path = q.popleft()
        if len(path) - 1 >= max_hops:
            continue
        for nxt in children.get(path[-1], []):
            if nxt in seen:
                continue
            new = path + [nxt]
            if nxt == goal:
                return new
            seen.add(nxt)
            q.append(new)
    return []


def compute_service_to_symptom_propagation_support(service: str, symptom_service: str, propagation_edges: list[dict[str, Any]], service_graph: dict[str, Any], calibrated_residual_support: dict[str, Any], config: StructuredPropagationConfig | None = None) -> dict[str, Any]:
    cfg = config or StructuredPropagationConfig()
    children = service_graph.get("children", {})
    path = _bfs_path(children, service, symptom_service, cfg.max_path_hops)
    edge_weights: list[float] = []
    lag_values: list[int] = []
    relation_count = 0
    if path:
        hop_pairs = set(zip(path[:-1], path[1:]))
        for edge in propagation_edges:
            parent_service = split_node_id(str(edge.get("parent_node", "")))[0]
            target_service = split_node_id(str(edge.get("target_node", "")))[0]
            relation = str(edge.get("relation_type", ""))
            if (parent_service, target_service) in hop_pairs and relation in {"cross_service_resource_to_request", "cross_service_request_to_request"}:
                edge_weights.append(float(edge.get("effective_weight", 0.0) or 0.0))
                lag_values.append(int(edge.get("lag", 0) or 0))
                relation_count += 1
    path_edge_support = max(edge_weights) if edge_weights else 0.0
    if path_edge_support > 0:
        path_edge_support = min(1.0, path_edge_support / (path_edge_support + 1.0))
    request_support = calibrated_residual_support.get("service_request_support", {}) if calibrated_residual_support else {}
    downstream_values = [float(request_support.get(svc, 0.0)) for svc in path[1:]] if path else []
    downstream_response_support = max(downstream_values) if downstream_values else 0.0
    lag_support = 1.0 / (1.0 + min(lag_values)) if lag_values else 0.0
    support = min(1.0, 0.55 * path_edge_support + 0.30 * downstream_response_support + 0.15 * lag_support)
    return {
        "has_path_to_symptom": bool(path),
        "best_path": path,
        "path_edge_support": path_edge_support,
        "downstream_response_support": downstream_response_support,
        "lag_support": lag_support,
        "propagation_relation_count": relation_count,
        "structured_propagation_support": support,
        "propagation_support_score": support,
        "support_components": {"path_edge_support": path_edge_support, "downstream_response_support": downstream_response_support, "lag_support": lag_support},
        "uses_injected_path": False,
        "uses_labels": False,
    }

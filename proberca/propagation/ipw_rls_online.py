"""Online IPW-masked Recursive Least Squares propagation preview.

Model:
    z_{i,t} ~= phi_{i,t}^T theta_i

where z_{i,t} is the robust anomaly score for target node i at time t and
phi_{i,t} is built from parent nodes at t-1:

    phi_{i,t} = M_{Pa(i),t-1} * Omega_{Pa(i),t-1} * z_{Pa(i),t-1}

M is the observation mask, Omega is inverse probability weighting, and Pa(i) is
the parent set. For target sample weight w_t, weighted RLS updates are:

    e_t = y_t - phi_t^T theta_{t-1}
    den_t = gamma + w_t phi_t^T P_{t-1} phi_t
    K_t = w_t P_{t-1} phi_t / den_t
    theta_t = theta_{t-1} + K_t e_t
    P_t = gamma^{-1} (P_{t-1} - K_t phi_t^T P_{t-1})

A6 is a preview learner only. It does not use root labels or run RCA.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass
class RLSConfig:
    forgetting_factor: float = 0.98
    ridge_init: float = 100.0
    min_sampling_probability: float = 0.05
    max_ipw_weight: float = 20.0
    max_parents: int = 32
    min_parent_observed: int = 1
    robust_eps: float = 1e-6
    use_expected_mask_preview: bool = True


def load_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    file_path = Path(path)
    if not file_path.exists():
        return rows
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            item = json.loads(text)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def metric_family(metric: str) -> str:
    if metric.startswith("cpu."):
        return "CPU"
    if metric.startswith("net."):
        return "network"
    if metric.startswith("io."):
        return "storage I/O"
    if metric.startswith("lock."):
        return "lock contention"
    if metric.startswith("memory."):
        return "memory"
    if metric.startswith("request."):
        return "load"
    return "unknown"


def parse_metrics(metrics_path: str) -> dict[str, dict[str, Any]]:
    parsed: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(metrics_path):
        service = _first(row, ("service", "service_name", "pod_service", "svc"))
        metric = _first(row, ("metric", "metric_name", "name"))
        timestamp = _as_float(_first(row, ("timestamp", "ts", "time")))
        value = _as_float(row.get("value", row.get("metric_value")))
        if service is None or metric is None or timestamp is None or value is None:
            continue
        node_id = f"{service}.{metric}"
        parsed.setdefault(node_id, {"service": str(service), "metric": str(metric), "points": []})["points"].append((timestamp, value))
    for node in parsed.values():
        node["points"] = sorted(node["points"], key=lambda item: item[0])
    return parsed


def build_candidate_node_index(candidate_metric_nodes_path: str) -> dict[str, Any]:
    rows = load_jsonl(candidate_metric_nodes_path)
    node_ids: list[str] = []
    service_to_nodes: dict[str, list[str]] = defaultdict(list)
    node_to_service: dict[str, str] = {}
    node_to_metric: dict[str, str] = {}
    families: dict[str, str] = {}
    for row in rows:
        service = row.get("service")
        metric = row.get("metric")
        node_id = row.get("node_id") or (f"{service}.{metric}" if service and metric else None)
        if not node_id or not service or not metric:
            continue
        node_s = str(node_id)
        if node_s in node_to_service:
            continue
        node_ids.append(node_s)
        node_to_service[node_s] = str(service)
        node_to_metric[node_s] = str(metric)
        service_to_nodes[str(service)].append(node_s)
        families[node_s] = metric_family(str(metric))
    return {
        "node_ids": sorted(node_ids),
        "service_to_nodes": {service: sorted(nodes) for service, nodes in service_to_nodes.items()},
        "node_to_service": node_to_service,
        "node_to_metric": node_to_metric,
        "metric_family": families,
    }


def _load_candidate_nodes_from_dir(candidate_dir: str) -> dict[str, Any]:
    base = Path(candidate_dir)
    paths: list[Path]
    if (base / "repeat_candidate_summary.json").exists():
        summary = json.loads((base / "repeat_candidate_summary.json").read_text(encoding="utf-8"))
        paths = [Path(item["window_output_dir"]) / "candidate_metric_nodes.jsonl" for item in summary.get("window_summaries", [])]
    elif (base / "candidate_metric_nodes.jsonl").exists():
        paths = [base / "candidate_metric_nodes.jsonl"]
    else:
        paths = sorted(base.glob("window_*/candidate_metric_nodes.jsonl"))
    rows: list[dict[str, Any]] = []
    for path in paths:
        rows.extend(load_jsonl(str(path)))
    tmp = base / ".a6_candidate_metric_nodes_merged.jsonl"
    _write_jsonl(tmp, rows)
    index = build_candidate_node_index(str(tmp))
    try:
        tmp.unlink()
    except FileNotFoundError:
        pass
    return index


def _load_candidate_edges_from_dir(candidate_dir: str) -> list[dict[str, Any]]:
    base = Path(candidate_dir)
    if (base / "repeat_candidate_summary.json").exists():
        summary = json.loads((base / "repeat_candidate_summary.json").read_text(encoding="utf-8"))
        paths = [Path(item["window_output_dir"]) / "candidate_edges.jsonl" for item in summary.get("window_summaries", [])]
    elif (base / "candidate_edges.jsonl").exists():
        paths = [base / "candidate_edges.jsonl"]
    else:
        paths = sorted(base.glob("window_*/candidate_edges.jsonl"))
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for path in paths:
        for row in load_jsonl(str(path)):
            src = row.get("src")
            dst = row.get("dst")
            if src and dst and (str(src), str(dst)) not in seen:
                rows.append({"src": str(src), "dst": str(dst), "edge_type": row.get("edge_type", "call")})
                seen.add((str(src), str(dst)))
    return rows


def build_parent_sets(candidate_edges_path: str, candidate_nodes: dict[str, Any], max_parents: int) -> dict[str, Any]:
    edge_path = Path(candidate_edges_path)
    edges = _load_candidate_edges_from_dir(str(edge_path)) if edge_path.is_dir() else load_jsonl(str(edge_path))
    incoming_services: dict[str, set[str]] = defaultdict(set)
    adjacent_services: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        src = edge.get("src")
        dst = edge.get("dst")
        if src and dst:
            incoming_services[str(dst)].add(str(src))
            adjacent_services[str(dst)].add(str(src))
            adjacent_services[str(src)].add(str(dst))
    service_to_nodes = candidate_nodes["service_to_nodes"]
    node_to_service = candidate_nodes["node_to_service"]
    node_to_metric = candidate_nodes["node_to_metric"]
    families = candidate_nodes["metric_family"]
    parent_sets: dict[str, list[str]] = {}
    reasons: dict[str, dict[str, str]] = {}
    for node in candidate_nodes["node_ids"]:
        service = node_to_service[node]
        family = families.get(node, "unknown")
        scored: list[tuple[int, str, str]] = [(0, node, "self_lag")]
        for other in service_to_nodes.get(service, []):
            if other != node:
                scored.append((1 if families.get(other) in {family, "load"} else 2, other, "same_service_metric"))
        graph_parents = set(incoming_services.get(service, set())) or set(adjacent_services.get(service, set()))
        for parent_service in sorted(graph_parents):
            for parent_node in service_to_nodes.get(parent_service, []):
                score = 2 if families.get(parent_node) in {family, "load"} else 3
                scored.append((score, parent_node, "service_graph_parent"))
        dedup: dict[str, tuple[int, str]] = {}
        for score, parent, reason in scored:
            if parent not in dedup or score < dedup[parent][0]:
                dedup[parent] = (score, reason)
        selected = sorted(dedup, key=lambda parent: (dedup[parent][0], parent))[: int(max_parents)]
        parent_sets[node] = selected
        reasons[node] = {parent: dedup[parent][1] for parent in selected}
    return {"parent_sets": parent_sets, "parent_selection_reason": reasons}


def load_sampling_probabilities(sampling_log_path: str, observation_mask_path: str) -> dict[str, Any]:
    sampling = load_jsonl(sampling_log_path)
    masks = load_jsonl(observation_mask_path)
    probabilities: dict[str, float] = {}
    observed: dict[str, bool] = {}
    probe_by_node: dict[str, str | None] = {}
    for row in masks:
        service = row.get("service")
        metric = row.get("metric")
        if not service or not metric:
            continue
        node_id = f"{service}.{metric}"
        prob = _as_float(row.get("observed_probability"))
        if prob is not None:
            probabilities[node_id] = max(probabilities.get(node_id, 0.0), prob)
            observed[node_id] = prob > 0.0
            probe_by_node[node_id] = row.get("observed_by_probe")
    for row in sampling:
        service = row.get("service")
        metric = row.get("metric")
        if not service or not metric:
            continue
        node_id = f"{service}.{metric}"
        prob = _as_float(row.get("sampling_probability"))
        if prob is not None:
            probabilities[node_id] = max(probabilities.get(node_id, 0.0), prob)
            observed[node_id] = bool(row.get("selected", True))
            probe_by_node[node_id] = str(row.get("probe_name"))
    return {"sampling_probability_by_node": probabilities, "observed_by_node": observed, "probe_by_node": probe_by_node, "policy_metadata": {"sampling_log_count": len(sampling), "observation_mask_count": len(masks)}}


def robust_normalize_timeseries(parsed_metrics: dict[str, dict[str, Any]], baseline_ratio: float = 0.3, eps: float = 1e-6) -> dict[str, Any]:
    z_by_node_time: dict[str, dict[float, float]] = {}
    metadata: dict[str, dict[str, float]] = {}
    timestamps: set[float] = set()
    for node_id, payload in parsed_metrics.items():
        points = payload.get("points", [])
        if not points:
            continue
        values = np.asarray([value for _ts, value in points], dtype=float)
        count = max(1, int(math.ceil(len(values) * baseline_ratio)))
        baseline = values[:count]
        median = float(np.median(baseline))
        mad = float(np.median(np.abs(baseline - median)))
        scale = 1.4826 * mad + eps
        z_rows: dict[float, float] = {}
        for ts, value in points:
            z = float((float(value) - median) / scale)
            if math.isfinite(z):
                z_rows[float(ts)] = z
                timestamps.add(float(ts))
        z_by_node_time[node_id] = z_rows
        metadata[node_id] = {"baseline_count": float(count), "median": median, "mad": mad, "scale": scale}
    return {"z_by_node_time": z_by_node_time, "timestamps": sorted(timestamps), "normalization_metadata": metadata}


def align_panel(z_by_node_time: dict[str, dict[float, float]], candidate_node_ids: list[str]) -> dict[str, Any]:
    timestamps = sorted({ts for node in candidate_node_ids for ts in z_by_node_time.get(node, {})})
    node_ids = sorted(candidate_node_ids)
    z_matrix = np.zeros((len(timestamps), len(node_ids)), dtype=float)
    mask = np.zeros((len(timestamps), len(node_ids)), dtype=bool)
    ts_index = {ts: idx for idx, ts in enumerate(timestamps)}
    node_index = {node: idx for idx, node in enumerate(node_ids)}
    for node in node_ids:
        for ts, value in z_by_node_time.get(node, {}).items():
            if ts in ts_index:
                z_matrix[ts_index[ts], node_index[node]] = float(value)
                mask[ts_index[ts], node_index[node]] = True
    return {"timestamps": timestamps, "node_ids": node_ids, "z_matrix": z_matrix, "value_available_mask": mask}


class OnlineIPWMaskedRLS:
    def __init__(self, node_ids: list[str], parent_sets: dict[str, list[str]], sampling_probabilities: dict[str, Any], config: RLSConfig | None = None):
        self.node_ids = sorted(node_ids)
        self.node_index = {node: idx for idx, node in enumerate(self.node_ids)}
        self.parent_sets = {node: [p for p in parent_sets.get(node, []) if p in self.node_index] for node in self.node_ids}
        self.config = config or RLSConfig()
        self.sampling_probability_by_node = sampling_probabilities.get("sampling_probability_by_node", {})
        self.observed_by_node = sampling_probabilities.get("observed_by_node", {})
        self.theta: dict[str, np.ndarray] = {}
        self.P: dict[str, np.ndarray] = {}
        for node in self.node_ids:
            dim = len(self.parent_sets[node])
            self.theta[node] = np.zeros(dim, dtype=float)
            self.P[node] = np.eye(dim, dtype=float) * float(self.config.ridge_init)
        self.predictions: list[dict[str, Any]] = []
        self.residuals: list[dict[str, Any]] = []
        self.update_log: list[dict[str, Any]] = []

    def _prob(self, node: str) -> float:
        metric = node.split(".", 1)[1] if "." in node else node
        default = 1.0 if metric.startswith("request.") else float(self.config.min_sampling_probability)
        prob = float(self.sampling_probability_by_node.get(node, default))
        return max(prob, float(self.config.min_sampling_probability))

    def _ipw(self, node: str) -> float:
        return min(1.0 / self._prob(node), float(self.config.max_ipw_weight))

    def _feature(self, target_node: str, t_index: int, z_matrix: np.ndarray, value_available_mask: np.ndarray) -> tuple[np.ndarray, int, list[str]]:
        parents = self.parent_sets.get(target_node, [])
        values: list[float] = []
        used: list[str] = []
        observed = 0
        for parent in parents:
            idx = self.node_index[parent]
            is_available = bool(value_available_mask[t_index - 1, idx])
            policy_observed = bool(self.observed_by_node.get(parent, True)) or self.config.use_expected_mask_preview
            if is_available and policy_observed:
                observed += 1
                values.append(float(z_matrix[t_index - 1, idx]) * self._ipw(parent))
                used.append(parent)
            else:
                values.append(0.0)
        return np.asarray(values, dtype=float), observed, used

    def predict(self, target_node: str, t_index: int, z_matrix: np.ndarray, value_available_mask: np.ndarray) -> dict[str, Any]:
        if t_index < 1:
            return {"predicted_z": 0.0, "observed_parent_count": 0, "parent_count": len(self.parent_sets.get(target_node, [])), "used_parents": []}
        phi, observed, used = self._feature(target_node, t_index, z_matrix, value_available_mask)
        theta = self.theta[target_node]
        pred = float(phi @ theta) if len(phi) else 0.0
        return {"predicted_z": pred, "observed_parent_count": observed, "parent_count": len(phi), "used_parents": used}

    def update(self, target_node: str, t_index: int, z_matrix: np.ndarray, value_available_mask: np.ndarray, timestamp: float | None = None) -> dict[str, Any]:
        row = {"timestamp": timestamp, "target_node": target_node, "updated": False, "skip_reason": None}
        target_idx = self.node_index[target_node]
        if t_index < 1:
            row["skip_reason"] = "t_index_lt_1"
            self.update_log.append(row)
            return row
        if not bool(value_available_mask[t_index, target_idx]):
            row["skip_reason"] = "target_missing"
            self.update_log.append(row)
            return row
        phi, observed, used = self._feature(target_node, t_index, z_matrix, value_available_mask)
        pred = float(phi @ self.theta[target_node]) if len(phi) else 0.0
        actual = float(z_matrix[t_index, target_idx])
        residual = actual - pred
        row.update({"predicted_z": pred, "actual_z": actual, "residual": residual, "parent_count": len(phi), "observed_parent_count": observed, "used_parents": used, "sample_weight": self._ipw(target_node)})
        self.predictions.append({k: row.get(k) for k in ["timestamp", "target_node", "predicted_z", "actual_z"]})
        self.residuals.append({"timestamp": timestamp, "target_node": target_node, "residual": residual, "abs_residual": abs(residual), "actual_z": actual, "predicted_z": pred})
        if observed < int(self.config.min_parent_observed):
            row["skip_reason"] = "too_few_observed_parents"
            self.update_log.append(row)
            return row
        if not np.all(np.isfinite(phi)) or not math.isfinite(actual) or not math.isfinite(pred):
            row["skip_reason"] = "non_finite_value"
            self.update_log.append(row)
            return row
        P = self.P[target_node]
        theta = self.theta[target_node]
        w = float(row["sample_weight"])
        gamma = float(self.config.forgetting_factor)
        den = gamma + w * float(phi.T @ P @ phi)
        if not math.isfinite(den) or den <= 0:
            row["skip_reason"] = "invalid_denominator"
            self.update_log.append(row)
            return row
        K = (w * P @ phi) / den
        theta_new = theta + K * residual
        P_new = (P - np.outer(K, phi.T @ P)) / gamma
        if not np.all(np.isfinite(theta_new)) or not np.all(np.isfinite(P_new)):
            row["skip_reason"] = "non_finite_update"
            self.update_log.append(row)
            return row
        self.theta[target_node] = theta_new
        self.P[target_node] = P_new
        row.update({"updated": True, "rls_denominator": den, "gain_norm": float(np.linalg.norm(K))})
        self.update_log.append(row)
        return row

    def run(self, z_matrix: np.ndarray, value_available_mask: np.ndarray, timestamps: list[float]) -> dict[str, Any]:
        for t_index, timestamp in enumerate(timestamps):
            for node in self.node_ids:
                self.update(node, t_index, z_matrix, value_available_mask, timestamp)
        return self.export_state()

    def export_state(self) -> dict[str, Any]:
        return {
            "node_count": len(self.node_ids),
            "total_updates": sum(1 for row in self.update_log if row.get("updated")),
            "skipped_updates": sum(1 for row in self.update_log if not row.get("updated")),
            "update_mode": "online_rls",
            "batch_ridge_used": False,
            "theta_by_node": {node: self.theta[node].tolist() for node in self.node_ids},
            "parent_sets": self.parent_sets,
            "config": asdict(self.config),
        }

    def export_edges(self) -> list[dict[str, Any]]:
        edges: list[dict[str, Any]] = []
        for target in self.node_ids:
            for parent, weight in zip(self.parent_sets[target], self.theta[target].tolist()):
                edges.append({"src": parent, "dst": target, "weight": float(weight), "source": "a6_true_ipw_masked_rls"})
        return edges

    def export_residuals(self) -> list[dict[str, Any]]:
        return self.residuals

    def export_predictions(self) -> list[dict[str, Any]]:
        return self.predictions


def run_ipw_rls_preview(raw_input_dir: str, candidate_dir: str, probe_policy_dir: str, output_dir: str, config: RLSConfig | None = None) -> dict[str, Any]:
    cfg = config or RLSConfig()
    raw = Path(raw_input_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    parsed_metrics = parse_metrics(str(raw / "metrics.jsonl"))
    candidate_nodes = _load_candidate_nodes_from_dir(candidate_dir)
    parent_result = build_parent_sets(candidate_dir, candidate_nodes, cfg.max_parents)
    sampling = load_sampling_probabilities(str(Path(probe_policy_dir) / "sampling_log.jsonl"), str(Path(probe_policy_dir) / "observation_mask.jsonl"))
    normalized = robust_normalize_timeseries(parsed_metrics, eps=cfg.robust_eps)
    available_nodes = [node for node in candidate_nodes["node_ids"] if node in normalized["z_by_node_time"]]
    panel = align_panel(normalized["z_by_node_time"], available_nodes)
    parent_sets = {node: [parent for parent in parent_result["parent_sets"].get(node, []) if parent in available_nodes] for node in available_nodes}
    learner = OnlineIPWMaskedRLS(available_nodes, parent_sets, sampling, cfg)
    learner.run(panel["z_matrix"], panel["value_available_mask"], panel["timestamps"])
    state = learner.export_state()
    edges = learner.export_edges()
    residuals = learner.export_residuals()
    predictions = learner.export_predictions()
    abs_values = [float(row["abs_residual"]) for row in residuals if math.isfinite(float(row.get("abs_residual", 0.0)))]
    metadata = {
        "raw_input_dir": str(raw),
        "candidate_dir": candidate_dir,
        "probe_policy_dir": probe_policy_dir,
        "output_dir": str(out),
        "node_count": len(available_nodes),
        "total_updates": state["total_updates"],
        "skipped_updates": state["skipped_updates"],
        "average_abs_residual": float(np.mean(np.asarray(abs_values, dtype=float))) if abs_values else 0.0,
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "consumes_sampling_probability": True,
        "consumes_observation_mask": True,
        "update_mode": "online_rls",
        "batch_ridge_used": False,
        "source": "a6_true_ipw_masked_rls",
        "normalization_metadata_count": len(normalized["normalization_metadata"]),
        "parent_selection_reason": {node: parent_result["parent_selection_reason"].get(node, {}) for node in available_nodes},
    }
    _write_json(out / "ipw_rls_state.json", state)
    _write_jsonl(out / "ipw_rls_edges.jsonl", edges)
    _write_jsonl(out / "ipw_rls_residuals.jsonl", residuals)
    _write_jsonl(out / "ipw_rls_predictions.jsonl", predictions)
    _write_json(out / "ipw_rls_metadata.json", metadata)
    return {"state": state, "edges": edges, "residuals": residuals, "predictions": predictions, "metadata": metadata}


def evaluate_ipw_rls_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    residuals = load_jsonl(str(Path(output_dir) / "ipw_rls_residuals.jsonl"))
    incidents = load_jsonl(incidents_path)
    mean_abs: dict[str, float] = {}
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in residuals:
        node = row.get("target_node")
        value = _as_float(row.get("abs_residual"))
        if node and value is not None:
            buckets[str(node)].append(value)
    for node, values in buckets.items():
        mean_abs[node] = float(np.mean(np.asarray(values, dtype=float)))
    ranked_nodes = sorted(mean_abs, key=lambda node: (-mean_abs[node], node))
    ranks = {node: idx + 1 for idx, node in enumerate(ranked_nodes)}
    metric_ranks: list[float] = []
    service_ranks: list[float] = []
    for incident in incidents:
        service = incident.get("root_service")
        metric = incident.get("root_metric")
        if service and metric:
            node = f"{service}.{metric}"
            metric_ranks.append(float(ranks.get(node, len(ranked_nodes) + 1)))
        if service:
            service_best = min((rank for node, rank in ranks.items() if node.startswith(f"{service}.")), default=len(ranked_nodes) + 1)
            service_ranks.append(float(service_best))
    return {
        "debug_only": True,
        "root_metric_residual_rank_debug": metric_ranks,
        "root_service_residual_rank_debug": service_ranks,
        "root_metric_residual_rank_mean": float(np.mean(np.asarray(metric_ranks, dtype=float))) if metric_ranks else None,
        "root_service_residual_rank_mean": float(np.mean(np.asarray(service_ranks, dtype=float))) if service_ranks else None,
        "debug_notes": "Incidents are read after RLS learning only for residual rank diagnostics.",
    }

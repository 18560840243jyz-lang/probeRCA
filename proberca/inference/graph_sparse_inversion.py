"""A8/A8R graph-constrained sparse inversion preview.

Solves a graph sparse intervention problem over A7 calibrated residuals only:

    min_u 1/2 ||r - u||_2^2
          + lambda_l1 ||u||_1
          + lambda_graph_tv sum_(i,j) w_ij |u_i - u_j|
          + lambda_group sum_s ||u_Ms||_2

A8R repairs sparsity, graph density, and convergence without using root labels,
target labels, injected paths, or incident windows for inversion.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import math
from pathlib import Path
from typing import Any

import numpy as np


@dataclass
class GraphSparseConfig:
    lambda_l1: float = 0.15
    lambda_graph_tv: float = 0.08
    lambda_group: float = 0.05
    rho: float = 1.0
    max_iter: int = 1000
    abs_tol: float = 1e-4
    rel_tol: float = 1e-4
    residual_aggregation: str = "positive_topk_mean"
    min_signal: float = 0.01
    max_signal: float = 10.0
    evidence_support_weight: float = 0.2
    use_evidence_support_for_tiebreak: bool = True
    max_graph_degree: int = 12
    topk_fraction: float = 0.2
    min_topk: int = 2
    symptom_family_penalty: float = 0.5
    unknown_family_penalty: float = 0.7
    evidence_signal_boost: float = 0.5
    auto_lambda: bool = True
    target_nonzero_ratio: float = 0.25
    min_lambda_l1: float = 0.15
    max_lambda_l1: float = 3.0
    lambda_quantile: float = 0.75
    adaptive_group_lambda: bool = True
    min_lambda_group: float = 0.05
    max_lambda_group: float = 1.5
    post_sparsify: bool = True
    max_nonzero_ratio: float = 0.35
    min_keep_nodes: int = 3
    service_top_metric_keep: int = 2
    adaptive_rho: bool = True
    rho_increase: float = 2.0
    rho_decrease: float = 2.0
    over_relaxation: float = 1.5


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    rows: list[dict[str, Any]] = []
    if not p.exists():
        return rows
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


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


def _is_request_family(family: str) -> bool:
    return family == "load"


def _is_resource_family(family: str) -> bool:
    return family in {"CPU", "network", "storage I/O", "lock contention", "memory"}


def _split_node(node_id: str) -> tuple[str, str]:
    for prefix in ("cpu", "net", "io", "lock", "memory", "request"):
        marker = f".{prefix}."
        if marker in node_id:
            service, tail = node_id.split(marker, 1)
            return service, f"{prefix}.{tail}"
    service, _, metric = node_id.partition(".")
    return service, metric


def _resolve_candidate_window(candidate_dir: str | Path) -> Path:
    base = Path(candidate_dir)
    if (base / "candidate_metric_nodes.jsonl").exists():
        return base
    windows = sorted(p for p in base.glob("window_*") if (p / "candidate_metric_nodes.jsonl").exists())
    if not windows:
        raise FileNotFoundError(f"candidate_metric_nodes.jsonl not found in {candidate_dir}")
    return windows[0]


def load_candidate_graph(candidate_dir: str, config: GraphSparseConfig | None = None) -> dict[str, Any]:
    cfg = config or GraphSparseConfig()
    window_dir = _resolve_candidate_window(candidate_dir)
    node_rows = load_jsonl(window_dir / "candidate_metric_nodes.jsonl")
    service_edge_rows = load_jsonl(window_dir / "candidate_edges.jsonl")
    metadata = _read_json(window_dir / "candidate_subgraph_metadata.json")
    node_ids: list[str] = []
    node_to_service: dict[str, str] = {}
    node_to_metric: dict[str, str] = {}
    node_to_family: dict[str, str] = {}
    service_to_nodes: dict[str, list[str]] = {}
    for row in node_rows:
        node = str(row.get("node_id") or "")
        if not node or node in node_to_service:
            continue
        service = str(row.get("service") or _split_node(node)[0])
        metric = str(row.get("metric") or _split_node(node)[1])
        family = str(row.get("metric_family") or metric_family(metric))
        node_ids.append(node)
        node_to_service[node] = service
        node_to_metric[node] = metric
        node_to_family[node] = family
        service_to_nodes.setdefault(service, []).append(node)
    raw_edges = _expand_metric_edges(node_ids, service_to_nodes, node_to_family, service_edge_rows)
    capped_edges = _cap_edges_by_degree(raw_edges, cfg.max_graph_degree)
    metadata = {
        **metadata,
        "raw_metric_edge_count": len(raw_edges),
        "capped_metric_edge_count": len(capped_edges),
        "max_graph_degree": cfg.max_graph_degree,
        "degree_cap_applied": len(capped_edges) < len(raw_edges),
    }
    return {
        "candidate_dir": str(window_dir),
        "node_ids": node_ids,
        "node_to_service": node_to_service,
        "node_to_metric": node_to_metric,
        "node_to_family": node_to_family,
        "service_to_nodes": service_to_nodes,
        "service_edges": service_edge_rows,
        "graph_edges": capped_edges,
        "metadata": metadata,
    }


def _expand_metric_edges(node_ids: list[str], service_to_nodes: dict[str, list[str]], node_to_family: dict[str, str], service_edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    edges: list[dict[str, Any]] = []

    def add(a: str, b: str, weight: float, edge_type: str) -> None:
        if a == b:
            return
        u, v = sorted([a, b])
        key = (u, v, edge_type)
        if key in seen:
            return
        seen.add(key)
        edges.append({"src": u, "dst": v, "weight": weight, "edge_type": edge_type})

    for _service, nodes in service_to_nodes.items():
        for i, a in enumerate(nodes):
            fa = node_to_family.get(a, "unknown")
            for b in nodes[i + 1 :]:
                fb = node_to_family.get(b, "unknown")
                if fa == fb:
                    add(a, b, 1.0, "same_service_same_family")
                elif (_is_resource_family(fa) and _is_request_family(fb)) or (_is_request_family(fa) and _is_resource_family(fb)):
                    add(a, b, 0.7, "same_service_resource_request")
                elif _is_request_family(fa) and _is_request_family(fb):
                    add(a, b, 1.0, "same_service_request_request")

    for row in service_edges:
        src_service = str(row.get("src") or row.get("source") or row.get("from") or "")
        dst_service = str(row.get("dst") or row.get("target") or row.get("to") or "")
        src_nodes = service_to_nodes.get(src_service, [])
        dst_nodes = service_to_nodes.get(dst_service, [])
        for a in src_nodes:
            fa = node_to_family.get(a, "unknown")
            for b in dst_nodes:
                fb = node_to_family.get(b, "unknown")
                if _is_request_family(fa) and _is_request_family(fb):
                    add(a, b, 0.5, "service_call_request_request")
                elif _is_request_family(fa) and _is_resource_family(fb):
                    add(a, b, 0.1, "service_call_request_downstream_resource")
                elif _is_resource_family(fa) and fa == fb:
                    add(a, b, 0.25, "service_call_same_resource_family")

    by_family: dict[str, list[str]] = {}
    for node in node_ids:
        by_family.setdefault(node_to_family.get(node, "unknown"), []).append(node)
    for family, nodes in by_family.items():
        if family == "unknown":
            continue
        for i, a in enumerate(nodes):
            for b in nodes[i + 1 :]:
                if a.split(".", 1)[0] != b.split(".", 1)[0]:
                    add(a, b, 0.25, "same_family_cross_service_weak")
    return edges


def _cap_edges_by_degree(edges: list[dict[str, Any]], max_degree: int) -> list[dict[str, Any]]:
    if max_degree <= 0:
        return edges
    degree: dict[str, int] = {}
    selected: list[dict[str, Any]] = []
    ordered = sorted(edges, key=lambda e: (-float(e.get("weight", 1.0)), str(e.get("src")), str(e.get("dst")), str(e.get("edge_type"))))
    for edge in ordered:
        src = str(edge["src"])
        dst = str(edge["dst"])
        if degree.get(src, 0) >= max_degree or degree.get(dst, 0) >= max_degree:
            continue
        selected.append(edge)
        degree[src] = degree.get(src, 0) + 1
        degree[dst] = degree.get(dst, 0) + 1
    return selected


def load_calibrated_residuals(evidence_channel_dir: str) -> list[dict[str, Any]]:
    base = Path(evidence_channel_dir)
    metadata = _read_json(base / "evidence_channel_metadata.json")
    if metadata.get("produces_calibrated_residuals") is not True:
        raise ValueError("A7 metadata does not confirm calibrated residual production")
    if metadata.get("raw_residual_directly_used_for_sparse_inversion") is not False:
        raise ValueError("A7 metadata does not forbid raw residual sparse inversion use")
    rows = load_jsonl(base / "calibrated_residuals.jsonl")
    if not rows:
        raise ValueError("calibrated_residuals.jsonl is empty or missing")
    for idx, row in enumerate(rows):
        if "calibrated_residual" not in row:
            raise ValueError(f"calibrated_residual missing at row {idx}")
        if "raw_residual" not in row or "evidence_effect" not in row:
            raise ValueError(f"raw_residual/evidence_effect missing at row {idx}")
        if not (row.get("node_id") or (row.get("service") and row.get("metric"))):
            raise ValueError(f"node_id or service+metric missing at row {idx}")
    return rows


def load_evidence_support(evidence_channel_dir: str) -> dict[str, Any]:
    base = Path(evidence_channel_dir)
    vectors = load_jsonl(base / "evidence_vectors.jsonl")
    effects = load_jsonl(base / "evidence_effects.jsonl")
    support_by_node: dict[str, float] = {}
    service_family_support: dict[tuple[str, str], float] = {}
    for row in vectors:
        node = str(row.get("node_id") or "")
        service = str(row.get("service") or "")
        family = str(row.get("metric_family") or metric_family(str(row.get("metric") or "")))
        score = float(row.get("h_value", 0.0) or 0.0)
        if node:
            support_by_node[node] = max(score, support_by_node.get(node, 0.0))
        if service:
            service_family_support[(service, family)] = max(score, service_family_support.get((service, family), 0.0))
    for row in effects:
        node = str(row.get("node_id") or "")
        score = abs(float(row.get("evidence_effect", 0.0) or 0.0))
        if node:
            support_by_node[node] = max(score, support_by_node.get(node, 0.0))
    return {"evidence_support_by_node": support_by_node, "service_family_support": service_family_support}


def aggregate_residual_signal(calibrated_rows: list[dict[str, Any]], node_ids: list[str], config: GraphSparseConfig, evidence_support: dict[str, Any] | None = None, node_to_family: dict[str, str] | None = None) -> dict[str, Any]:
    evidence_support = evidence_support or {"evidence_support_by_node": {}}
    node_to_family = node_to_family or {}
    values_by_node: dict[str, list[float]] = {node: [] for node in node_ids}
    for row in calibrated_rows:
        node = str(row.get("node_id") or f"{row.get('service')}.{row.get('metric')}")
        if node in values_by_node:
            values_by_node[node].append(float(row["calibrated_residual"]))
    support = evidence_support.get("evidence_support_by_node", {})
    signal: dict[str, float] = {}
    details: dict[str, dict[str, Any]] = {}
    for node in node_ids:
        vals = np.asarray(values_by_node.get(node, []), dtype=float)
        family = node_to_family.get(node) or metric_family(_split_node(node)[1])
        if vals.size == 0:
            signal[node] = 0.0
            details[node] = {"residual_count": 0, "mask": False, "signal_source": "a7_calibrated_residual"}
            continue
        positive = np.maximum(vals, 0.0)
        k = max(int(config.min_topk), int(math.ceil(float(len(positive)) * float(config.topk_fraction))))
        k = min(k, len(positive))
        topk = np.sort(positive)[-k:] if k else np.asarray([], dtype=float)
        raw_topk = float(np.mean(topk)) if topk.size else 0.0
        family_penalty = 1.0
        if family == "load":
            family_penalty = config.symptom_family_penalty
        elif family == "unknown":
            family_penalty = config.unknown_family_penalty
        ev = float(support.get(node, 0.0) or 0.0)
        value = raw_topk * family_penalty * (1.0 + config.evidence_signal_boost * ev)
        value = max(0.0, min(float(config.max_signal), value))
        if value < config.min_signal:
            value = 0.0
        signal[node] = float(value)
        details[node] = {
            "residual_count": int(vals.size),
            "raw_positive_topk_mean": raw_topk,
            "family_penalty": family_penalty,
            "evidence_support": ev,
            "final_signal": float(value),
            "mean_abs_calibrated_residual": float(np.mean(np.abs(vals))),
            "signal_source": "a7_calibrated_residual",
            "mask": True,
        }
    return {"signal_by_node": signal, "details_by_node": details}


def build_graph_incidence(node_ids: list[str], metric_edges: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]]]:
    idx = {node: i for i, node in enumerate(node_ids)}
    valid = [edge for edge in metric_edges if edge.get("src") in idx and edge.get("dst") in idx]
    d = np.zeros((len(valid), len(node_ids)), dtype=float)
    weights = np.zeros(len(valid), dtype=float)
    for row_i, edge in enumerate(valid):
        d[row_i, idx[str(edge["src"])] ] = 1.0
        d[row_i, idx[str(edge["dst"])] ] = -1.0
        weights[row_i] = float(edge.get("weight", 1.0) or 1.0)
    return d, weights, valid


def soft_threshold(x: np.ndarray, threshold: float | np.ndarray) -> np.ndarray:
    return np.sign(x) * np.maximum(np.abs(x) - threshold, 0.0)


def group_shrink(x_group: np.ndarray, threshold: float) -> np.ndarray:
    norm = float(np.linalg.norm(x_group))
    if norm <= threshold or norm <= 0:
        return np.zeros_like(x_group)
    return (1.0 - threshold / norm) * x_group


def _service_groups(node_ids: list[str], node_to_service: dict[str, str]) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = {}
    for idx, node in enumerate(node_ids):
        groups.setdefault(node_to_service[node], []).append(idx)
    return groups


def _effective_config(r: np.ndarray, service_groups: dict[str, list[int]], config: GraphSparseConfig) -> tuple[GraphSparseConfig, dict[str, Any]]:
    cfg = config
    lambda_l1 = cfg.lambda_l1
    lambda_group = cfg.lambda_group
    abs_signal = np.abs(r[np.isfinite(r)])
    positive_abs = abs_signal[abs_signal > cfg.min_signal]
    if cfg.auto_lambda and positive_abs.size:
        lambda_l1 = float(np.clip(np.quantile(positive_abs, cfg.lambda_quantile), cfg.min_lambda_l1, cfg.max_lambda_l1))
    if cfg.adaptive_group_lambda:
        norms = [float(np.linalg.norm(r[indices])) for indices in service_groups.values() if len(indices)]
        positive_norms = np.asarray([v for v in norms if v > cfg.min_signal], dtype=float)
        if positive_norms.size:
            lambda_group = float(np.clip(np.quantile(positive_norms, cfg.lambda_quantile) * 0.5, cfg.min_lambda_group, cfg.max_lambda_group))
    effective = replace(cfg, lambda_l1=lambda_l1, lambda_group=lambda_group)
    return effective, {
        "effective_lambda_l1": lambda_l1,
        "effective_lambda_graph_tv": cfg.lambda_graph_tv,
        "effective_lambda_group": lambda_group,
        "auto_lambda": cfg.auto_lambda,
        "adaptive_group_lambda": cfg.adaptive_group_lambda,
    }


def _incidence_from_index_edges(n: int, edges: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]]]:
    valid = []
    for edge in edges:
        src = int(edge.get("src_index", edge.get("src", -1)))
        dst = int(edge.get("dst_index", edge.get("dst", -1)))
        if 0 <= src < n and 0 <= dst < n and src != dst:
            valid.append({**edge, "src_index": src, "dst_index": dst})
    d = np.zeros((len(valid), n), dtype=float)
    weights = np.zeros(len(valid), dtype=float)
    for i, edge in enumerate(valid):
        d[i, int(edge["src_index"])] = 1.0
        d[i, int(edge["dst_index"])] = -1.0
        weights[i] = float(edge.get("weight", 1.0) or 1.0)
    return d, weights, valid


def _build_linear_matrix(n: int, d: np.ndarray, rho: float) -> np.ndarray:
    dt_d = d.T @ d if d.size else np.zeros((n, n), dtype=float)
    return (1.0 + 2.0 * rho) * np.eye(n) + rho * dt_d


def solve_graph_sparse_admm(r: np.ndarray, graph_edges: list[dict[str, Any]], service_groups: dict[str, list[int]], config: GraphSparseConfig, node_ids: list[str] | None = None) -> dict[str, Any]:
    n = int(r.size)
    if node_ids is None:
        node_ids = [str(i) for i in range(n)]
    if graph_edges and isinstance(graph_edges[0].get("src"), str):
        d, weights, valid_edges = build_graph_incidence(node_ids, graph_edges)
    else:
        d, weights, valid_edges = _incidence_from_index_edges(n, graph_edges)
    m = d.shape[0]
    rho = float(config.rho)
    a = _build_linear_matrix(n, d, rho)
    z1 = np.zeros(n, dtype=float)
    y1 = np.zeros(n, dtype=float)
    z2 = np.zeros(m, dtype=float)
    y2 = np.zeros(m, dtype=float)
    z3 = {svc: np.zeros(len(indices), dtype=float) for svc, indices in service_groups.items()}
    y3 = {svc: np.zeros(len(indices), dtype=float) for svc, indices in service_groups.items()}
    x = np.zeros(n, dtype=float)
    trace: list[dict[str, Any]] = []
    status = "max_iter_reached"
    convergence_reason = "max_iter_reached"
    try:
        for it in range(1, int(config.max_iter) + 1):
            old_z1 = z1.copy()
            old_z2 = z2.copy()
            old_z3 = {svc: val.copy() for svc, val in z3.items()}
            group_rhs = np.zeros(n, dtype=float)
            for svc, indices in service_groups.items():
                group_rhs[indices] += z3[svc] - y3[svc]
            rhs = r + rho * (z1 - y1) + (rho * d.T @ (z2 - y2) if m else 0.0) + rho * group_rhs
            x = np.linalg.solve(a, rhs)
            if not np.all(np.isfinite(x)):
                status = "failed_numeric"
                convergence_reason = "nan_or_inf"
                break
            x_hat = config.over_relaxation * x + (1.0 - config.over_relaxation) * old_z1
            z1 = soft_threshold(x_hat + y1, config.lambda_l1 / rho)
            dx = d @ x if m else np.zeros(0, dtype=float)
            z2 = soft_threshold(dx + y2, (config.lambda_graph_tv * weights / rho) if m else 0.0)
            for svc, indices in service_groups.items():
                z3[svc] = group_shrink(x[indices] + y3[svc], config.lambda_group / rho)
            y1 += x_hat - z1
            if m:
                y2 += dx - z2
            for svc, indices in service_groups.items():
                y3[svc] += x[indices] - z3[svc]
            primal_parts = [np.linalg.norm(x - z1)]
            if m:
                primal_parts.append(np.linalg.norm(dx - z2))
            primal_parts.extend(np.linalg.norm(x[indices] - z3[svc]) for svc, indices in service_groups.items())
            primal = float(math.sqrt(sum(float(v) ** 2 for v in primal_parts)))
            dual = float(rho * (np.linalg.norm(z1 - old_z1) + (np.linalg.norm(d.T @ (z2 - old_z2)) if m else 0.0) + sum(np.linalg.norm(z3[svc] - old_z3[svc]) for svc in z3)))
            obj = compute_objective(r, x, valid_edges, service_groups, config, node_ids)
            trace.append({"iter": it, "objective": obj["total_objective"], "primal_residual": primal, "dual_residual": dual, "nonzero_count": int(np.sum(np.abs(x) > config.min_signal)), "rho": rho})
            if primal < config.abs_tol and dual < config.abs_tol:
                status = "converged"
                convergence_reason = "primal_and_dual_below_abs_tol"
                break
            if config.adaptive_rho and it % 10 == 0:
                new_rho = rho
                if primal > 10.0 * max(dual, 1e-12):
                    new_rho = rho * config.rho_increase
                elif dual > 10.0 * max(primal, 1e-12):
                    new_rho = rho / config.rho_decrease
                if new_rho != rho and np.isfinite(new_rho) and new_rho > 0:
                    scale = rho / new_rho
                    y1 *= scale
                    y2 *= scale
                    for svc in y3:
                        y3[svc] *= scale
                    rho = float(new_rho)
                    a = _build_linear_matrix(n, d, rho)
    except np.linalg.LinAlgError:
        status = "failed_numeric"
        convergence_reason = "linear_solve_failed"
    return {"x": x, "trace": trace, "solver_status": status, "iterations": len(trace), "graph_edges": valid_edges, "final_rho": rho, "convergence_reason": convergence_reason}


def compute_objective(r: np.ndarray, x: np.ndarray, graph_edges: list[dict[str, Any]], service_groups: dict[str, list[int]], config: GraphSparseConfig, node_ids: list[str] | None = None) -> dict[str, float]:
    if node_ids is None:
        node_ids = [str(i) for i in range(x.size)]
    idx = {node: i for i, node in enumerate(node_ids)}
    data_loss = 0.5 * float(np.sum((r - x) ** 2))
    l1 = float(config.lambda_l1 * np.sum(np.abs(x)))
    tv = 0.0
    for edge in graph_edges:
        if "src" in edge and isinstance(edge.get("src"), str):
            i = idx.get(str(edge["src"]))
            j = idx.get(str(edge["dst"]))
        else:
            i = int(edge.get("src_index", edge.get("src", -1)))
            j = int(edge.get("dst_index", edge.get("dst", -1)))
        if i is None or j is None or i < 0 or j < 0:
            continue
        tv += float(edge.get("weight", 1.0) or 1.0) * abs(float(x[i] - x[j]))
    graph_tv = float(config.lambda_graph_tv * tv)
    group = float(config.lambda_group * sum(float(np.linalg.norm(x[indices])) for indices in service_groups.values()))
    return {"data_loss": data_loss, "l1_penalty": l1, "graph_tv_penalty": graph_tv, "group_penalty": group, "total_objective": data_loss + l1 + graph_tv + group}


def post_sparsify_solution(x: np.ndarray, node_ids: list[str], node_to_service: dict[str, str], config: GraphSparseConfig) -> tuple[np.ndarray, dict[str, Any]]:
    pre = int(np.sum(np.abs(x) > config.min_signal))
    if not config.post_sparsify or x.size == 0:
        return x, {"pre_sparsify_nonzero_count": pre, "post_sparsify_nonzero_count": pre, "post_sparsify_applied": False}
    keep_limit = max(int(config.min_keep_nodes), int(math.ceil(float(x.size) * float(config.max_nonzero_ratio))))
    order = sorted(range(x.size), key=lambda i: (-abs(float(x[i])), node_ids[i]))
    service_counts: dict[str, int] = {}
    keep: list[int] = []
    for idx in order:
        if abs(float(x[idx])) <= config.min_signal:
            continue
        service = node_to_service[node_ids[idx]]
        if service_counts.get(service, 0) >= config.service_top_metric_keep:
            continue
        keep.append(idx)
        service_counts[service] = service_counts.get(service, 0) + 1
        if len(keep) >= keep_limit:
            break
    sparse = np.zeros_like(x)
    sparse[keep] = x[keep]
    return sparse, {"pre_sparsify_nonzero_count": pre, "post_sparsify_nonzero_count": int(np.sum(np.abs(sparse) > config.min_signal)), "post_sparsify_applied": True, "max_nonzero_ratio": config.max_nonzero_ratio, "service_top_metric_keep": config.service_top_metric_keep}


def build_sparse_rankings(x: np.ndarray, residual_signal: dict[str, Any], evidence_support: dict[str, Any], node_metadata: dict[str, Any], service_groups: dict[str, list[int]], config: GraphSparseConfig) -> dict[str, Any]:
    node_ids = node_metadata["node_ids"]
    support = evidence_support.get("evidence_support_by_node", {})
    signal_by_node = residual_signal["signal_by_node"]
    metric_rows: list[dict[str, Any]] = []
    for i, node in enumerate(node_ids):
        service = node_metadata["node_to_service"][node]
        metric = node_metadata["node_to_metric"][node]
        family = node_metadata["node_to_family"][node]
        ev = float(support.get(node, 0.0) or 0.0)
        abs_u = abs(float(x[i]))
        score = abs_u * (1.0 + config.evidence_support_weight * ev) if config.use_evidence_support_for_tiebreak else abs_u
        metric_rows.append({"node_id": node, "service": service, "metric": metric, "metric_family": family, "u_value": float(x[i]), "abs_u_value": abs_u, "residual_signal": float(signal_by_node.get(node, 0.0)), "evidence_support": ev, "metric_score": float(score), "signal_components": residual_signal.get("details_by_node", {}).get(node, {})})
    metric_rows.sort(key=lambda row: row["metric_score"], reverse=True)
    for rank, row in enumerate(metric_rows, start=1):
        row["rank"] = rank
    metric_by_node = {row["node_id"]: row for row in metric_rows}
    service_rows: list[dict[str, Any]] = []
    for service, indices in service_groups.items():
        group_norm = float(np.linalg.norm(x[indices]))
        service_metrics = [metric_by_node[node_ids[i]] for i in indices if node_ids[i] in metric_by_node]
        top_metric = max(service_metrics, key=lambda row: row["metric_score"])["node_id"] if service_metrics else None
        service_rows.append({"service": service, "service_score": group_norm, "group_norm": group_norm, "top_metric": top_metric})
    service_rows.sort(key=lambda row: row["service_score"], reverse=True)
    for rank, row in enumerate(service_rows, start=1):
        row["rank"] = rank
    return {"metric_scores": metric_rows, "service_scores": service_rows, "top_metric_nodes": metric_rows[:10], "top_services": service_rows[:10]}


def run_graph_sparse_inversion(candidate_dir: str, evidence_channel_dir: str, output_dir: str, config: GraphSparseConfig | None = None) -> dict[str, Any]:
    base_cfg = config or GraphSparseConfig()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    graph = load_candidate_graph(candidate_dir, base_cfg)
    residual_rows = load_calibrated_residuals(evidence_channel_dir)
    evidence_support = load_evidence_support(evidence_channel_dir)
    residual_signal = aggregate_residual_signal(residual_rows, graph["node_ids"], base_cfg, evidence_support, graph["node_to_family"])
    service_groups = _service_groups(graph["node_ids"], graph["node_to_service"])
    r = np.asarray([residual_signal["signal_by_node"][node] for node in graph["node_ids"]], dtype=float)
    cfg, effective_meta = _effective_config(r, service_groups, base_cfg)
    solve = solve_graph_sparse_admm(r, graph["graph_edges"], service_groups, cfg, graph["node_ids"])
    x_raw = solve["x"]
    x, sparsify_meta = post_sparsify_solution(x_raw, graph["node_ids"], graph["node_to_service"], cfg)
    objective = compute_objective(r, x, solve["graph_edges"], service_groups, cfg, graph["node_ids"])
    rankings = build_sparse_rankings(x, residual_signal, evidence_support, graph, service_groups, cfg)
    interventions = [row for row in rankings["metric_scores"] if row["abs_u_value"] > cfg.min_signal]
    write_jsonl(output / "sparse_interventions.jsonl", interventions)
    write_jsonl(output / "metric_scores.jsonl", rankings["metric_scores"])
    write_jsonl(output / "service_scores.jsonl", rankings["service_scores"])
    write_jsonl(output / "graph_sparse_objective_trace.jsonl", solve["trace"])
    metadata = {
        "candidate_dir": candidate_dir,
        "resolved_candidate_dir": graph["candidate_dir"],
        "evidence_channel_dir": evidence_channel_dir,
        "output_dir": output_dir,
        "node_count": len(graph["node_ids"]),
        "edge_count": len(solve["graph_edges"]),
        "raw_metric_edge_count": graph["metadata"].get("raw_metric_edge_count"),
        "capped_metric_edge_count": graph["metadata"].get("capped_metric_edge_count"),
        "max_graph_degree": graph["metadata"].get("max_graph_degree"),
        "degree_cap_applied": graph["metadata"].get("degree_cap_applied"),
        "service_group_count": len(service_groups),
        "nonzero_intervention_count": len(interventions),
        "solver_status": solve["solver_status"],
        "convergence_reason": solve.get("convergence_reason"),
        "iterations": solve["iterations"],
        "final_rho": solve.get("final_rho"),
        "final_objective": objective["total_objective"],
        "data_loss": objective["data_loss"],
        "l1_penalty": objective["l1_penalty"],
        "graph_tv_penalty": objective["graph_tv_penalty"],
        "group_penalty": objective["group_penalty"],
        **effective_meta,
        **sparsify_meta,
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "consumes_calibrated_residuals": True,
        "consumes_raw_residuals": False,
        "residual_lift_fallback_used": False,
        "optimization": "admm_graph_sparse_inversion",
        "source": "a8r_graph_sparse_inversion_repair",
    }
    _write_json(output / "graph_sparse_metadata.json", metadata)
    return {"metadata": metadata, "rankings": rankings, "residual_signal": residual_signal, "objective_trace": solve["trace"]}


def _root_type_to_family(root_type: str) -> str:
    value = root_type.lower()
    if "cpu" in value:
        return "CPU"
    if "network" in value or "net" in value:
        return "network"
    if "i/o" in value or "io" in value or "storage" in value:
        return "storage I/O"
    if "lock" in value:
        return "lock contention"
    if "memory" in value:
        return "memory"
    return "unknown"


def evaluate_graph_sparse_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    metrics = load_jsonl(Path(output_dir) / "metric_scores.jsonl")
    services = load_jsonl(Path(output_dir) / "service_scores.jsonl")
    incidents = load_jsonl(incidents_path)
    metric_rank = {row.get("node_id"): int(row.get("rank", 10**9)) for row in metrics}
    service_rank = {row.get("service"): int(row.get("rank", 10**9)) for row in services}
    family_by_node = {row.get("node_id"): row.get("metric_family") for row in metrics}
    metric_hit_at_3: list[float] = []
    service_hit_at_1: list[float] = []
    type_match: list[float] = []
    service_ranks: list[int] = []
    metric_ranks: list[int] = []
    for incident in incidents:
        root_service = incident.get("root_service")
        root_metric = incident.get("root_metric")
        root_type = incident.get("root_type")
        root_node = f"{root_service}.{root_metric}" if root_service and root_metric else None
        sr = service_rank.get(root_service, 10**9)
        mr = metric_rank.get(root_node, 10**9)
        service_ranks.append(sr)
        metric_ranks.append(mr)
        service_hit_at_1.append(1.0 if sr == 1 else 0.0)
        metric_hit_at_3.append(1.0 if mr <= 3 else 0.0)
        expected_family = _root_type_to_family(str(root_type or ""))
        type_match.append(1.0 if root_node and family_by_node.get(root_node) == expected_family else 0.0)
    return {
        "debug_only": True,
        "debug_root_service_rank": service_ranks,
        "debug_root_metric_rank": metric_ranks,
        "debug_metric_hit_at_3": _mean(metric_hit_at_3),
        "debug_service_hit_at_1": _mean(service_hit_at_1),
        "debug_root_type_match_by_metric_family": _mean(type_match),
        "uses_root_labels_for_inversion": False,
        "notes": "Root labels are used only after sparse inversion outputs are written for debug ranking.",
    }

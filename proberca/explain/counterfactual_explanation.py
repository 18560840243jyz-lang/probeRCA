"""A9 counterfactual explanation for A8R graph sparse inversion outputs.

This module computes Delta L_v = L(u^{-v}) - L(u_hat) by re-solving the
A8R graph sparse inversion objective after forbidding a candidate metric node
or all metric nodes belonging to a candidate service. It is label-free: root
labels, target labels, injected paths, and incident start/end times are not
used for candidate selection, re-optimization, or ranking.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import Any

import numpy as np

from proberca.inference.graph_sparse_inversion import (
    GraphSparseConfig,
    _service_groups,
    aggregate_residual_signal,
    compute_objective,
    load_calibrated_residuals,
    load_candidate_graph,
    load_evidence_support,
    post_sparsify_solution,
    solve_graph_sparse_admm,
)


@dataclass
class CounterfactualConfig:
    top_k_metrics: int = 10
    top_k_services: int = 5
    mode: str = "reoptimize_masked"
    max_reopt_iter: int = 500
    min_delta_loss: float = 0.0
    normalize_delta_loss: bool = True
    combine_with_a8_score: bool = True
    delta_loss_weight: float = 0.6
    sparse_score_weight: float = 0.4
    service_counterfactual: bool = True
    metric_counterfactual: bool = True


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return rows
    with p.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _max(values: list[float]) -> float:
    return float(np.max(np.asarray(values, dtype=float))) if values else 0.0


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _coerce_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def load_a8r_outputs(graph_sparse_dir: str) -> dict[str, Any]:
    base = Path(graph_sparse_dir)
    required = [
        "sparse_interventions.jsonl",
        "metric_scores.jsonl",
        "service_scores.jsonl",
        "graph_sparse_metadata.json",
        "graph_sparse_objective_trace.jsonl",
    ]
    missing = [name for name in required if not (base / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing A8R output files in {graph_sparse_dir}: {missing}")
    metadata = _load_json(base / "graph_sparse_metadata.json")
    if metadata.get("optimization") != "admm_graph_sparse_inversion":
        raise ValueError("A9 requires A8R admm_graph_sparse_inversion outputs")
    if metadata.get("consumes_calibrated_residuals") is not True:
        raise ValueError("A9 requires A8R outputs that consume calibrated residuals")
    if metadata.get("consumes_raw_residuals") is not False:
        raise ValueError("A9 refuses A8R outputs marked as consuming raw residuals")
    if metadata.get("residual_lift_fallback_used") is True:
        raise ValueError("A9 refuses residual-lift fallback outputs")
    if metadata.get("uses_root_labels") is not False:
        raise ValueError("A9 refuses A8R outputs marked uses_root_labels != false")
    if metadata.get("uses_target_config") is not False:
        raise ValueError("A9 refuses A8R outputs marked uses_target_config != false")
    objective_trace = load_jsonl(base / "graph_sparse_objective_trace.jsonl")
    base_objective = float(metadata.get("final_objective") or (objective_trace[-1].get("objective") if objective_trace else 0.0) or 0.0)
    return {
        "metric_scores": load_jsonl(base / "metric_scores.jsonl"),
        "service_scores": load_jsonl(base / "service_scores.jsonl"),
        "sparse_interventions": load_jsonl(base / "sparse_interventions.jsonl"),
        "base_metadata": metadata,
        "base_objective": base_objective,
        "objective_trace": objective_trace,
    }


def _config_from_a8r_metadata(metadata: dict[str, Any], cf_config: CounterfactualConfig) -> GraphSparseConfig:
    return GraphSparseConfig(
        lambda_l1=float(metadata.get("effective_lambda_l1", metadata.get("lambda_l1", 0.15)) or 0.15),
        lambda_graph_tv=float(metadata.get("effective_lambda_graph_tv", metadata.get("lambda_graph_tv", 0.08)) or 0.08),
        lambda_group=float(metadata.get("effective_lambda_group", metadata.get("lambda_group", 0.05)) or 0.05),
        max_iter=int(cf_config.max_reopt_iter),
        auto_lambda=False,
        adaptive_group_lambda=False,
        post_sparsify=bool(metadata.get("post_sparsify_applied", True)),
        max_nonzero_ratio=float(metadata.get("max_nonzero_ratio", 0.35) or 0.35),
        service_top_metric_keep=int(metadata.get("service_top_metric_keep", 2) or 2),
    )


def load_reconstruction_inputs(candidate_dir: str, evidence_channel_dir: str, a8r_metadata: dict[str, Any], cf_config: CounterfactualConfig) -> dict[str, Any]:
    gs_config = _config_from_a8r_metadata(a8r_metadata, cf_config)
    graph = load_candidate_graph(candidate_dir, gs_config)
    residual_rows = load_calibrated_residuals(evidence_channel_dir)
    evidence_support = load_evidence_support(evidence_channel_dir)
    residual_signal = aggregate_residual_signal(
        residual_rows,
        graph["node_ids"],
        gs_config,
        evidence_support,
        graph["node_to_family"],
    )
    r = np.asarray([float(residual_signal["signal_by_node"].get(node, 0.0)) for node in graph["node_ids"]], dtype=float)
    service_groups = _service_groups(graph["node_ids"], graph["node_to_service"])
    return {
        "graph": graph,
        "residual_rows": residual_rows,
        "evidence_support": evidence_support,
        "residual_signal": residual_signal,
        "r": r,
        "service_groups": service_groups,
        "graph_sparse_config": gs_config,
    }


def build_forbidden_mask(node_ids: list[str], forbidden_nodes: set[str]) -> np.ndarray:
    return np.asarray([node not in forbidden_nodes for node in node_ids], dtype=bool)


def _active_edges(graph_edges: list[dict[str, Any]], active: set[str]) -> list[dict[str, Any]]:
    return [edge for edge in graph_edges if str(edge.get("src")) in active and str(edge.get("dst")) in active]


def solve_counterfactual_with_forbidden_nodes(base_inputs: dict[str, Any], forbidden_nodes: set[str], config: CounterfactualConfig) -> dict[str, Any]:
    graph = base_inputs["graph"]
    node_ids: list[str] = list(graph["node_ids"])
    mask = build_forbidden_mask(node_ids, forbidden_nodes)
    active_nodes = [node for node, keep in zip(node_ids, mask) if keep]
    active_set = set(active_nodes)
    active_indices = [idx for idx, keep in enumerate(mask) if keep]
    if not active_nodes:
        raise ValueError("counterfactual would remove every candidate node")
    r_full: np.ndarray = base_inputs["r"]
    r_active = r_full[active_indices]
    node_to_service = graph["node_to_service"]
    active_node_to_service = {node: node_to_service[node] for node in active_nodes}
    active_groups = _service_groups(active_nodes, active_node_to_service)
    active_edges = _active_edges(graph["graph_edges"], active_set)
    gs_config: GraphSparseConfig = replace(base_inputs["graph_sparse_config"], max_iter=int(config.max_reopt_iter))
    solved = solve_graph_sparse_admm(r_active, active_edges, active_groups, gs_config, active_nodes)
    x_full = np.zeros(len(node_ids), dtype=float)
    x_full[active_indices] = solved["x"]
    x_full, sparsify_meta = post_sparsify_solution(x_full, node_ids, node_to_service, gs_config)
    objective = compute_objective(r_full, x_full, graph["graph_edges"], base_inputs["service_groups"], gs_config, node_ids)
    return {
        "forbidden_nodes": sorted(forbidden_nodes),
        "solver_status": solved["solver_status"],
        "iterations": solved["iterations"],
        "objective_without_candidate": objective["total_objective"],
        "data_loss_without_candidate": objective["data_loss"],
        "l1_penalty_without_candidate": objective["l1_penalty"],
        "graph_tv_penalty_without_candidate": objective["graph_tv_penalty"],
        "group_penalty_without_candidate": objective["group_penalty"],
        "nonzero_count_without_candidate": int(np.sum(np.abs(x_full) > gs_config.min_signal)),
        "x_full": x_full,
        "objective_parts": objective,
        "post_sparsify": sparsify_meta,
    }


def _metric_score_key(row: dict[str, Any]) -> tuple[float, int, str]:
    return (-float(row.get("metric_score", row.get("abs_u_value", 0.0)) or 0.0), int(row.get("rank", 10**9)), str(row.get("node_id", "")))


def _service_score_key(row: dict[str, Any]) -> tuple[float, int, str]:
    return (-float(row.get("service_score", row.get("group_norm", 0.0)) or 0.0), int(row.get("rank", 10**9)), str(row.get("service", "")))


def _normalized_scores(rows: list[dict[str, Any]], key: str) -> dict[str, float]:
    vals = [float(row.get(key, 0.0) or 0.0) for row in rows]
    max_val = max(vals) if vals else 0.0
    out: dict[str, float] = {}
    for row in rows:
        ident = str(row.get("node_id") or row.get("service") or "")
        out[ident] = (float(row.get(key, 0.0) or 0.0) / max_val) if max_val > 0 else 0.0
    return out


def _row_with_delta(base_objective: float, cf_result: dict[str, Any], original_score_norm: float, cf_config: CounterfactualConfig) -> dict[str, float]:
    delta = float(cf_result["objective_without_candidate"] - base_objective)
    norm_delta = delta / (abs(base_objective) + 1e-9) if cf_config.normalize_delta_loss else delta
    faith = max(norm_delta, 0.0)
    combined = cf_config.delta_loss_weight * faith + cf_config.sparse_score_weight * original_score_norm if cf_config.combine_with_a8_score else faith
    return {"delta_loss": delta, "normalized_delta_loss": norm_delta, "faithfulness_score": faith, "combined_score": combined}


def compute_metric_counterfactuals(graph_sparse_dir: str, candidate_dir: str, evidence_channel_dir: str, output_dir: str, config: CounterfactualConfig) -> list[dict[str, Any]]:
    a8r = load_a8r_outputs(graph_sparse_dir)
    base_inputs = load_reconstruction_inputs(candidate_dir, evidence_channel_dir, a8r["base_metadata"], config)
    candidates = sorted(a8r["metric_scores"], key=_metric_score_key)[: int(config.top_k_metrics)]
    score_norm = _normalized_scores(candidates, "metric_score")
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        node_id = str(candidate.get("node_id"))
        if not node_id or node_id == "None":
            continue
        cf = solve_counterfactual_with_forbidden_nodes(base_inputs, {node_id}, config)
        deltas = _row_with_delta(a8r["base_objective"], cf, score_norm.get(node_id, 0.0), config)
        rows.append({
            "candidate_type": "metric",
            "node_id": node_id,
            "service": candidate.get("service"),
            "metric": candidate.get("metric"),
            "metric_family": candidate.get("metric_family"),
            "original_rank": candidate.get("rank"),
            "original_metric_score": candidate.get("metric_score"),
            "original_u_value": candidate.get("u_value"),
            "base_objective": a8r["base_objective"],
            "objective_without_candidate": cf["objective_without_candidate"],
            "solver_status": cf["solver_status"],
            "iterations": cf["iterations"],
            "nonzero_count_without_candidate": cf["nonzero_count_without_candidate"],
            "explanation": f"Removing metric node {node_id} increases objective by {deltas['delta_loss']:.6g}.",
            "uses_root_labels": False,
            "uses_target_config": False,
            **deltas,
        })
    write_jsonl(Path(output_dir) / "counterfactual_metric_explanations.jsonl", rows)
    return rows


def compute_service_counterfactuals(graph_sparse_dir: str, candidate_dir: str, evidence_channel_dir: str, output_dir: str, config: CounterfactualConfig) -> list[dict[str, Any]]:
    a8r = load_a8r_outputs(graph_sparse_dir)
    base_inputs = load_reconstruction_inputs(candidate_dir, evidence_channel_dir, a8r["base_metadata"], config)
    graph = base_inputs["graph"]
    candidates = sorted(a8r["service_scores"], key=_service_score_key)[: int(config.top_k_services)]
    score_norm = _normalized_scores(candidates, "service_score")
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        service = str(candidate.get("service"))
        if not service or service == "None":
            continue
        forbidden = set(graph["service_to_nodes"].get(service, []))
        if not forbidden:
            continue
        cf = solve_counterfactual_with_forbidden_nodes(base_inputs, forbidden, config)
        deltas = _row_with_delta(a8r["base_objective"], cf, score_norm.get(service, 0.0), config)
        rows.append({
            "candidate_type": "service",
            "service": service,
            "forbidden_node_count": len(forbidden),
            "top_metric": candidate.get("top_metric"),
            "original_rank": candidate.get("rank"),
            "original_service_score": candidate.get("service_score"),
            "base_objective": a8r["base_objective"],
            "objective_without_candidate": cf["objective_without_candidate"],
            "solver_status": cf["solver_status"],
            "iterations": cf["iterations"],
            "nonzero_count_without_candidate": cf["nonzero_count_without_candidate"],
            "explanation": f"Removing service {service} ({len(forbidden)} metric nodes) increases objective by {deltas['delta_loss']:.6g}.",
            "uses_root_labels": False,
            **deltas,
        })
    write_jsonl(Path(output_dir) / "counterfactual_service_explanations.jsonl", rows)
    return rows


def build_counterfactual_rankings(metric_cf_rows: list[dict[str, Any]], service_cf_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    metric_rows = sorted(metric_cf_rows, key=lambda row: (-float(row.get("combined_score", 0.0) or 0.0), -float(row.get("delta_loss", 0.0) or 0.0), int(row.get("original_rank", 10**9)), str(row.get("node_id", ""))))
    service_rows = sorted(service_cf_rows, key=lambda row: (-float(row.get("combined_score", 0.0) or 0.0), -float(row.get("delta_loss", 0.0) or 0.0), int(row.get("original_rank", 10**9)), str(row.get("service", ""))))
    for idx, row in enumerate(metric_rows, start=1):
        row["counterfactual_rank"] = idx
    for idx, row in enumerate(service_rows, start=1):
        row["counterfactual_rank"] = idx
    return {"counterfactual_metric_ranking": metric_rows, "counterfactual_service_ranking": service_rows}


def run_counterfactual_explanation(graph_sparse_dir: str, candidate_dir: str, evidence_channel_dir: str, output_dir: str, config: CounterfactualConfig | None = None) -> dict[str, Any]:
    cfg = config or CounterfactualConfig()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    a8r = load_a8r_outputs(graph_sparse_dir)
    metric_rows = compute_metric_counterfactuals(graph_sparse_dir, candidate_dir, evidence_channel_dir, output_dir, cfg) if cfg.metric_counterfactual else []
    service_rows = compute_service_counterfactuals(graph_sparse_dir, candidate_dir, evidence_channel_dir, output_dir, cfg) if cfg.service_counterfactual else []
    rankings = build_counterfactual_rankings(metric_rows, service_rows)
    write_jsonl(output / "counterfactual_metric_ranking.jsonl", rankings["counterfactual_metric_ranking"])
    write_jsonl(output / "counterfactual_service_ranking.jsonl", rankings["counterfactual_service_ranking"])
    metadata = {
        "graph_sparse_dir": graph_sparse_dir,
        "candidate_dir": candidate_dir,
        "evidence_channel_dir": evidence_channel_dir,
        "output_dir": output_dir,
        "top_k_metrics": cfg.top_k_metrics,
        "top_k_services": cfg.top_k_services,
        "metric_counterfactual_count": len(metric_rows),
        "service_counterfactual_count": len(service_rows),
        "base_objective": a8r["base_objective"],
        "average_metric_delta_loss": _mean([float(row.get("delta_loss", 0.0) or 0.0) for row in metric_rows]),
        "average_service_delta_loss": _mean([float(row.get("delta_loss", 0.0) or 0.0) for row in service_rows]),
        "max_metric_delta_loss": _max([float(row.get("delta_loss", 0.0) or 0.0) for row in metric_rows]),
        "max_service_delta_loss": _max([float(row.get("delta_loss", 0.0) or 0.0) for row in service_rows]),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "consumes_a8r_sparse_interventions": True,
        "reoptimizes_with_candidate_removed": True,
        "fast_approximation_only": False,
        "source": "a9_counterfactual_explanation",
    }
    _write_json(output / "counterfactual_metadata.json", metadata)
    return {"metadata": metadata, "metric_rows": metric_rows, "service_rows": service_rows, "rankings": rankings}


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


def evaluate_counterfactual_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    metric_rows = load_jsonl(Path(output_dir) / "counterfactual_metric_ranking.jsonl")
    service_rows = load_jsonl(Path(output_dir) / "counterfactual_service_ranking.jsonl")
    incidents = load_jsonl(incidents_path)
    metric_rank = {row.get("node_id"): int(row.get("counterfactual_rank", 10**9)) for row in metric_rows}
    service_rank = {row.get("service"): int(row.get("counterfactual_rank", 10**9)) for row in service_rows}
    family_by_node = {row.get("node_id"): row.get("metric_family") for row in metric_rows}
    service_ranks: list[int] = []
    metric_ranks: list[int] = []
    service_hit: list[float] = []
    metric_hit: list[float] = []
    type_hit: list[float] = []
    for incident in incidents:
        root_service = incident.get("root_service")
        root_metric = incident.get("root_metric")
        root_type = incident.get("root_type")
        root_node = f"{root_service}.{root_metric}" if root_service and root_metric else None
        sr = service_rank.get(root_service, 10**9)
        mr = metric_rank.get(root_node, 10**9)
        service_ranks.append(sr)
        metric_ranks.append(mr)
        service_hit.append(1.0 if sr == 1 else 0.0)
        metric_hit.append(1.0 if mr <= 3 else 0.0)
        type_hit.append(1.0 if root_node and family_by_node.get(root_node) == _root_type_to_family(str(root_type or "")) else 0.0)
    return {
        "debug_only": True,
        "debug_root_service_counterfactual_rank": service_ranks,
        "debug_root_metric_counterfactual_rank": metric_ranks,
        "debug_counterfactual_service_hit_at_1": _mean(service_hit),
        "debug_counterfactual_metric_hit_at_3": _mean(metric_hit),
        "debug_root_type_by_top_metric_family": _mean(type_hit),
        "uses_root_labels_for_counterfactual": False,
        "notes": "Root labels are read only after counterfactual rankings are written for debug evaluation.",
    }

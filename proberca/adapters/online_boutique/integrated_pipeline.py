"""B1 integrated end-to-end blind RCA pipeline for Online Boutique raw data."""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.alert_gate import write_alert_outputs, evaluate_alert_windows_for_debug
from proberca.adapters.online_boutique.blind_evidence import generate_blind_evidence_from_alert_windows
from proberca.adapters.online_boutique.candidate_subgraph import (
    build_candidate_subgraphs_for_repeat,
    evaluate_candidate_subgraph_for_debug,
    load_jsonl,
    parse_service_graph,
)
from proberca.adapters.online_boutique.adaptive_probe_policy import write_probe_policy_outputs, evaluate_probe_policy_for_debug
from proberca.propagation.ipw_rls_online import run_ipw_rls_preview, evaluate_ipw_rls_debug
from proberca.propagation.structured_multilag import (
    StructuredPropagationConfig,
    compute_service_to_symptom_propagation_support,
    fit_structured_multilag_propagation,
    load_jsonl as load_structured_jsonl,
)
from proberca.evidence.evidence_channel import build_evidence_channel, evaluate_evidence_channel_debug
from proberca.inference.graph_sparse_inversion import run_graph_sparse_inversion, evaluate_graph_sparse_debug
from proberca.explain.counterfactual_explanation import run_counterfactual_explanation, evaluate_counterfactual_debug
from proberca.adapters.online_boutique.service_metric_identity import (
    assert_or_repair_node_ownership,
    make_node_id,
    metric_family as ownership_metric_family,
    split_node_id,
    validate_node_ownership,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _top_rows(path: Path, limit: int = 5) -> list[dict[str, Any]]:
    return load_jsonl(str(path))[:limit] if path.exists() else []


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize(values: dict[str, float]) -> dict[str, float]:
    max_value = max((abs(value) for value in values.values()), default=0.0)
    if max_value <= 0.0:
        return {key: 0.0 for key in values}
    return {key: max(0.0, abs(value) / max_value) for key, value in values.items()}


def _metric_family_to_root_type(family: str) -> str:
    mapping = {
        "CPU": "CPU",
        "network": "network",
        "storage I/O": "storage I/O",
        "lock contention": "lock contention",
        "memory": "memory",
        "load": "load",
    }
    return mapping.get(family, "unknown")


def metric_diagnostic_specificity(metric: str, metric_family: str) -> dict[str, Any]:
    metric = str(metric or "")
    family = str(metric_family or "unknown")
    high = "high"
    medium = "medium"
    low = "low"
    symptom = "symptom"
    rules = {
        "cpu.throttled_usec": (1.0, high, "direct CPU throttling mechanism metric"),
        "cpu.throttled_periods": (0.9, high, "CPU throttling period mechanism metric"),
        "cpu.throttle_ratio": (0.9, high, "CPU throttle ratio mechanism metric"),
        "cpu.usage": (0.55, medium, "CPU usage can indicate load or symptom, not always root cause"),
        "net.retrans": (1.0, high, "network retransmission mechanism metric"),
        "net.rtt_ms": (0.75, medium, "network round-trip latency metric"),
        "net.in_segs": (0.4, low, "network volume metric with weak diagnostic specificity"),
        "net.out_segs": (0.4, low, "network volume metric with weak diagnostic specificity"),
        "io.io_time_ms": (1.0, high, "I/O wait-time mechanism metric"),
        "io.write_bytes": (0.85, high, "I/O write throughput metric"),
        "io.write_ops": (0.8, high, "I/O write operation metric"),
        "io.read_bytes": (0.6, medium, "I/O read throughput metric"),
        "io.read_ops": (0.6, medium, "I/O read operation metric"),
        "lock.futex_wait_ms": (1.0, high, "lock futex wait mechanism metric"),
        "lock.contention_count": (0.9, high, "lock contention count mechanism metric"),
        "lock.wait_ms": (0.8, high, "lock wait duration metric"),
        "lock.wait_p95_ms": (0.8, high, "lock wait tail-latency metric"),
        "memory.oom": (1.0, high, "memory OOM mechanism metric"),
        "memory.events": (0.9, high, "memory event mechanism metric"),
        "memory.reclaim": (0.8, high, "memory reclaim pressure metric"),
        "memory.pressure": (0.8, high, "memory pressure metric"),
        "memory.usage": (0.25, low, "memory usage alone is broad and weakly diagnostic"),
        "request.p95_latency_ms": (0.15, symptom, "request latency is primarily a symptom metric"),
        "request.p99_latency_ms": (0.15, symptom, "request tail latency is primarily a symptom metric"),
        "request.error_rate": (0.2, symptom, "request errors are symptom evidence"),
        "request.rps": (0.1, symptom, "request rate is load context, not a root metric"),
    }
    score, level, reason = rules.get(metric, (0.2, low, f"default weak specificity for {family}"))
    return {"specificity_score": float(score), "specificity_level": level, "reason": reason}


def _strong_memory_evidence_by_service(blind_evidence_rows: list[dict[str, Any]]) -> set[str]:
    strong_metrics = {"memory.events", "memory.oom", "memory.reclaim", "memory.pressure"}
    services: set[str] = set()
    for row in blind_evidence_rows:
        metric = str(row.get("metric") or "")
        service = str(row.get("service") or "")
        if metric in strong_metrics and _as_float(row.get("evidence_score", row.get("value"))) >= 0.3:
            services.add(service)
    return services


def _root_type_confidence(primary: dict[str, Any], candidates: list[dict[str, Any]]) -> float:
    components = primary.get("score_components") if isinstance(primary.get("score_components"), dict) else {}
    diagnostic = _as_float(components.get("diagnostic_specificity"))
    family_evidence = _as_float(components.get("family_evidence_support"))
    family_scores: dict[str, float] = {}
    for row in candidates:
        family = str(row.get("metric_family", "unknown"))
        family_scores[family] = max(family_scores.get(family, 0.0), _as_float(row.get("final_candidate_score")))
    ordered = sorted(family_scores.items(), key=lambda item: (-item[1], item[0]))
    if ordered:
        top = ordered[0][1]
        second = ordered[1][1] if len(ordered) > 1 else 0.0
        margin = max(0.0, top - second) / (abs(top) + 1e-9)
    else:
        margin = 0.0
    return max(0.0, min(1.0, 0.45 * diagnostic + 0.35 * family_evidence + 0.20 * margin))


def _node_service(node_id: str) -> str:
    return split_node_id(node_id)[0]


def _score_margin(rows: list[dict[str, Any]], score_key: str) -> float:
    if not rows:
        return 0.0
    first = abs(_as_float(rows[0].get(score_key)))
    second = abs(_as_float(rows[1].get(score_key))) if len(rows) > 1 else 0.0
    return max(0.0, first - second) / (abs(first) + 1e-9)


def _repair_rows_with_ownership(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [assert_or_repair_node_ownership(row) for row in rows]


def build_metric_candidate_table(stage_dirs: dict[str, str]) -> list[dict[str, Any]]:
    graph_sparse_dir = Path(stage_dirs["graph_sparse"])
    counterfactual_dir = Path(stage_dirs["counterfactual"])
    evidence_channel_dir = Path(stage_dirs["evidence_channel"])
    blind_evidence_dir = Path(stage_dirs.get("blind_evidence", ""))
    metric_scores = _repair_rows_with_ownership(load_jsonl(str(graph_sparse_dir / "metric_scores.jsonl")))
    service_scores = load_jsonl(str(graph_sparse_dir / "service_scores.jsonl"))
    cf_rows = _repair_rows_with_ownership(load_jsonl(str(counterfactual_dir / "counterfactual_metric_ranking.jsonl"))) if (counterfactual_dir / "counterfactual_metric_ranking.jsonl").exists() else []
    evidence_rows = _repair_rows_with_ownership(load_jsonl(str(evidence_channel_dir / "evidence_vectors.jsonl"))) if (evidence_channel_dir / "evidence_vectors.jsonl").exists() else []
    blind_evidence_rows = _repair_rows_with_ownership(load_jsonl(str(blind_evidence_dir / "blind_evidence.jsonl"))) if (blind_evidence_dir / "blind_evidence.jsonl").exists() else []

    service_score_by_service = {str(row.get("service")): _as_float(row.get("service_score", row.get("group_norm"))) for row in service_scores if row.get("service")}
    cf_by_node = {str(row.get("node_id")): row for row in cf_rows if row.get("node_id")}
    evidence_by_node_row = {str(row.get("node_id")): row for row in evidence_rows if row.get("node_id")}
    strong_memory_services = _strong_memory_evidence_by_service(blind_evidence_rows)

    raw_node_evidence: dict[str, float] = {}
    raw_service_family_evidence: dict[tuple[str, str], float] = {}
    raw_family_global_evidence: dict[str, float] = {}

    def add_evidence(row: dict[str, Any], score: float) -> None:
        fixed = assert_or_repair_node_ownership(row)
        node_id = str(fixed.get("node_id") or "")
        service = str(fixed.get("service") or "unknown")
        metric = str(fixed.get("metric") or "unknown")
        family = str(fixed.get("metric_family") or ownership_metric_family(metric))
        if node_id and node_id != "unknown.unknown":
            raw_node_evidence[node_id] = max(raw_node_evidence.get(node_id, 0.0), score)
        raw_service_family_evidence[(service, family)] = max(raw_service_family_evidence.get((service, family), 0.0), score)
        raw_family_global_evidence[family] = max(raw_family_global_evidence.get(family, 0.0), score)

    for row in evidence_rows:
        add_evidence(row, _as_float(row.get("h_value", row.get("evidence_score", 0.0))))
    for row in blind_evidence_rows:
        add_evidence(row, _as_float(row.get("evidence_score", row.get("value", 0.0))))

    node_metric_score = {str(row.get("node_id")): _as_float(row.get("metric_score")) for row in metric_scores if row.get("node_id")}
    node_service_score = {str(row.get("node_id")): service_score_by_service.get(str(row.get("service")), 0.0) for row in metric_scores if row.get("node_id")}
    node_cf = {node: _as_float(cf_by_node.get(node, {}).get("delta_loss", cf_by_node.get(node, {}).get("combined_score", 0.0))) for node in node_metric_score}

    metric_norm = _normalize(node_metric_score)
    service_norm = _normalize(node_service_score)
    cf_norm = _normalize(node_cf)
    node_evidence_norm = _normalize(raw_node_evidence)
    sf_norm_raw = {f"{service}\t{family}": value for (service, family), value in raw_service_family_evidence.items()}
    service_family_evidence_norm = _normalize(sf_norm_raw)
    family_global_evidence_norm = _normalize(raw_family_global_evidence)

    combined_evidence_norm: dict[str, float] = {}
    for row in metric_scores:
        node_id = str(row.get("node_id") or "")
        service = str(row.get("service") or "unknown")
        family = str(row.get("metric_family") or ownership_metric_family(str(row.get("metric"))))
        service_family_key = f"{service}\t{family}"
        combined_evidence_norm[node_id] = (
            0.55 * node_evidence_norm.get(node_id, 0.0)
            + 0.35 * service_family_evidence_norm.get(service_family_key, 0.0)
            + 0.10 * family_global_evidence_norm.get(family, 0.0)
        )

    weights = {"metric": 0.35, "service": 0.15, "counterfactual": 0.15, "evidence": 0.15, "diagnostic": 0.20}
    if not any(value > 0 for value in cf_norm.values()):
        weights.pop("counterfactual")
    if not any(value > 0 for value in combined_evidence_norm.values()):
        weights.pop("evidence")
    total_weight = sum(weights.values()) or 1.0
    weights = {key: value / total_weight for key, value in weights.items()}

    table: list[dict[str, Any]] = []
    for row in metric_scores:
        fixed = assert_or_repair_node_ownership(row)
        node_id = str(fixed.get("node_id") or "")
        if not node_id or node_id == "unknown.unknown":
            continue
        service = str(fixed.get("service") or _node_service(node_id))
        metric = str(fixed.get("metric") or split_node_id(node_id)[1])
        family = str(fixed.get("metric_family") or ownership_metric_family(metric))
        ownership = validate_node_ownership(fixed)
        specificity = metric_diagnostic_specificity(metric, family)
        diagnostic_score = _as_float(specificity.get("specificity_score"))
        service_family_key = f"{service}\t{family}"
        components = {
            "metric_score_norm": metric_norm.get(node_id, 0.0),
            "service_score_norm": service_norm.get(node_id, 0.0),
            "counterfactual_norm": cf_norm.get(node_id, 0.0),
            "evidence_norm": combined_evidence_norm.get(node_id, 0.0),
            "node_evidence_support": node_evidence_norm.get(node_id, 0.0),
            "service_family_evidence_support": service_family_evidence_norm.get(service_family_key, 0.0),
            "family_global_evidence_support": family_global_evidence_norm.get(family, 0.0),
            "family_global_evidence_weight": 0.10,
            "service_local_family_support": service_family_evidence_norm.get(service_family_key, 0.0),
            "diagnostic_specificity": diagnostic_score,
            "specificity_level": specificity.get("specificity_level"),
            "specificity_reason": specificity.get("reason"),
            "symptom_penalty_applied": False,
            "weak_memory_usage_penalty_applied": False,
            "strong_memory_evidence_available": service in strong_memory_services,
            "cpu_diagnostic_boost_applied": False,
            "ownership_valid": bool(ownership.get("ownership_valid")),
            "ownership_issue": ownership.get("ownership_issue", ""),
            "node_id_service": ownership.get("node_id_service"),
            "service_field": service,
            "service_matches_node_id": bool(ownership.get("service_matches_node_id")),
            "metric_matches_node_id": bool(ownership.get("metric_matches_node_id")),
            "ownership_repaired": bool(fixed.get("ownership_repaired", False)),
            "weights": weights,
        }
        score_before_penalty = (
            weights.get("metric", 0.0) * components["metric_score_norm"]
            + weights.get("service", 0.0) * components["service_score_norm"]
            + weights.get("counterfactual", 0.0) * components["counterfactual_norm"]
            + weights.get("evidence", 0.0) * components["evidence_norm"]
            + weights.get("diagnostic", 0.0) * diagnostic_score
        )
        final_score = score_before_penalty
        if family == "load":
            final_score *= 0.5
            components["symptom_penalty_applied"] = True
        if metric == "memory.usage" and service not in strong_memory_services:
            final_score *= 0.35
            components["weak_memory_usage_penalty_applied"] = True
        if family == "CPU" and metric in {"cpu.throttled_usec", "cpu.throttle_ratio", "cpu.throttled_periods"} and components["evidence_norm"] > 0.0:
            final_score *= 1.25
            components["cpu_diagnostic_boost_applied"] = True
        if not components["ownership_valid"]:
            final_score *= 0.2
        components["final_candidate_score_before_penalty"] = score_before_penalty
        components["final_candidate_score"] = final_score
        cf = cf_by_node.get(node_id, {})
        table.append({
            "node_id": node_id,
            "service": service,
            "metric": metric,
            "metric_family": family,
            "metric_score": node_metric_score.get(node_id, 0.0),
            "service_score": node_service_score.get(node_id, 0.0),
            "service_score_from_a8r": node_service_score.get(node_id, 0.0),
            "evidence_support": combined_evidence_norm.get(node_id, 0.0),
            "node_evidence_support": components["node_evidence_support"],
            "service_family_evidence_support": components["service_family_evidence_support"],
            "family_global_evidence_support": components["family_global_evidence_support"],
            "counterfactual_delta_loss": _as_float(cf.get("delta_loss")),
            "counterfactual_score": _as_float(cf.get("combined_score")),
            "diagnostic_specificity": diagnostic_score,
            "ownership_valid": components["ownership_valid"],
            "ownership_issue": components["ownership_issue"],
            "node_id_service": components["node_id_service"],
            "service_matches_node_id": components["service_matches_node_id"],
            "final_candidate_score": final_score,
            "final_metric_score": final_score,
            "score_components": components,
            "source": "metric_candidate_table",
        })
    table.sort(key=lambda item: (-_as_float(item.get("final_candidate_score")), -_as_float(item.get("metric_score")), str(item.get("service")), str(item.get("metric"))))
    for rank, item in enumerate(table, start=1):
        item["rank"] = rank
    return table

def select_primary_candidate(metric_candidate_table: list[dict[str, Any]]) -> dict[str, Any]:
    if not metric_candidate_table:
        return {"node_id": "unknown.unknown", "service": "unknown", "metric": "unknown", "metric_family": "unknown", "final_candidate_score": 0.0, "rank": 0}
    return dict(metric_candidate_table[0])


def build_top_services_from_candidates(metric_candidate_table: list[dict[str, Any]], top_k: int = 5) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in metric_candidate_table:
        grouped[str(row.get("service", "unknown"))].append(row)
    services: list[dict[str, Any]] = []
    for service, rows in grouped.items():
        ranked = sorted(rows, key=lambda item: (-_as_float(item.get("final_candidate_score")), str(item.get("metric"))))
        top = ranked[0]
        top2 = ranked[:2]
        service_final_score = max(_as_float(item.get("final_candidate_score")) for item in ranked)
        services.append({
            "service": service,
            "service_final_score": service_final_score,
            "top2_mean_candidate_score": sum(_as_float(item.get("final_candidate_score")) for item in top2) / len(top2),
            "top_metric": top.get("node_id"),
            "service_score_from_a8r": top.get("service_score", 0.0),
            "candidate_metric_count": len(rows),
        })
    services.sort(key=lambda item: (-_as_float(item.get("service_final_score")), -_as_float(item.get("top2_mean_candidate_score")), str(item.get("service"))))
    for rank, item in enumerate(services[:top_k], start=1):
        item["rank"] = rank
    return services[:top_k]


def _metric_family_from_metric(metric: str) -> str:
    return ownership_metric_family(metric)


def _topk_support(values: list[float], scale: float = 10.0) -> float:
    if not values:
        return 0.0
    ranked = sorted((abs(float(v)) for v in values), reverse=True)
    k = max(1, min(len(ranked), max(1, int(len(ranked) * 0.2))))
    return max(0.0, min(1.0, (sum(ranked[:k]) / k) / scale))


def compute_calibrated_residual_support(stage_dirs: dict[str, str]) -> dict[str, Any]:
    path = Path(stage_dirs["evidence_channel"]) / "calibrated_residuals.jsonl"
    rows = load_jsonl(str(path)) if path.exists() else []
    service_family_values: dict[tuple[str, str], list[float]] = defaultdict(list)
    service_request_values: dict[str, list[float]] = defaultdict(list)
    service_any_values: dict[str, list[float]] = defaultdict(list)
    node_values: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        service = str(row.get("service") or _node_service(str(row.get("node_id") or "")))
        metric = str(row.get("metric") or "")
        family = str(row.get("metric_family") or _metric_family_from_metric(metric))
        node_id = str(row.get("node_id") or (f"{service}.{metric}" if service and metric else ""))
        value = _as_float(row.get("calibrated_residual"))
        service_family_values[(service, family)].append(value)
        service_any_values[service].append(value)
        if family == "load":
            service_request_values[service].append(value)
        if node_id:
            node_values[node_id].append(value)
    return {
        "service_family_support": {key: _topk_support(values) for key, values in service_family_values.items()},
        "service_request_support": {key: _topk_support(values) for key, values in service_request_values.items()},
        "service_any_support": {key: _topk_support(values) for key, values in service_any_values.items()},
        "node_residual_support": {key: _topk_support(values) for key, values in node_values.items()},
    }


def _service_graph_adjacency(service_graph: dict[str, Any]) -> dict[str, set[str]]:
    # service_graph uses src -> dst as caller -> callee. For root-cause impact
    # paths we traverse callee -> caller, e.g. paymentservice -> checkoutservice -> frontend.
    adjacency: dict[str, set[str]] = {str(service): set() for service in service_graph.get("services", [])}
    for edge in service_graph.get("edges", []):
        src = str(edge.get("src", ""))
        dst = str(edge.get("dst", ""))
        if src and dst:
            adjacency.setdefault(dst, set()).add(src)
            adjacency.setdefault(src, set())
    return adjacency


def _bfs_path(adjacency: dict[str, set[str]], start: str, goal: str) -> list[str]:
    if not start or not goal:
        return []
    queue: deque[list[str]] = deque([[start]])
    seen = {start}
    while queue:
        current = queue.popleft()
        node = current[-1]
        if node == goal:
            return current
        for nxt in sorted(adjacency.get(node, set())):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(current + [nxt])
    return []


def compute_path_to_symptom_support(service: str, symptom_service: str, service_graph: dict[str, Any], calibrated_residual_support: dict[str, Any]) -> dict[str, Any]:
    adjacency = _service_graph_adjacency(service_graph)
    path = _bfs_path(adjacency, service, symptom_service)
    has_path = bool(path)
    if has_path:
        path_length = max(0, len(path) - 1)
        path_support = 1.0 / (1.0 + path_length)
        request_support = calibrated_residual_support.get("service_request_support", {})
        downstream_values = [float(request_support.get(node, 0.0)) for node in path[1:]] or [float(request_support.get(symptom_service, 0.0))]
        downstream_load_support = max(downstream_values) if downstream_values else 0.0
    else:
        path_length = None
        path_support = 0.0
        downstream_load_support = 0.0
    return {
        "has_path_to_symptom": has_path,
        "best_path_to_symptom": path,
        "path_length": path_length,
        "path_to_symptom_support": path_support,
        "downstream_load_support": downstream_load_support,
        "path_source": "service_graph_bfs",
        "uses_injected_path": False,
    }


def _load_service_counterfactual(counterfactual_dir: Path) -> dict[str, dict[str, Any]]:
    path = counterfactual_dir / "counterfactual_service_ranking.jsonl"
    rows = load_jsonl(str(path)) if path.exists() else []
    return {str(row.get("service")): row for row in rows if row.get("service")}



def _structured_propagation_payload(stage_dirs: dict[str, str], service_graph: dict[str, Any], residual_support: dict[str, Any]) -> dict[str, Any]:
    prop_dir = Path(stage_dirs.get("structured_propagation", ""))
    edges_path = prop_dir / "structured_propagation_edges.jsonl"
    metadata_path = prop_dir / "structured_propagation_metadata.json"
    edges = load_structured_jsonl(edges_path) if edges_path.exists() else []
    metadata = _read_json(metadata_path) if metadata_path.exists() else {}
    impact_children = {svc: sorted(children) for svc, children in _service_graph_adjacency(service_graph).items()}
    return {
        "edges": edges,
        "metadata": metadata,
        "service_graph": {"children": impact_children, "direction_assumption": metadata.get("direction_assumption")},
        "residual_support": residual_support,
        "available": bool(edges),
    }

def build_service_candidate_table(metric_candidate_table: list[dict[str, Any]], stage_dirs: dict[str, str], symptom_service: str, service_graph: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in metric_candidate_table:
        grouped[str(row.get("service", "unknown"))].append(row)
    residual_support = compute_calibrated_residual_support(stage_dirs)
    propagation_payload = _structured_propagation_payload(stage_dirs, service_graph, residual_support)
    service_cf = _load_service_counterfactual(Path(stage_dirs["counterfactual"]))
    raw_best = {}
    raw_top2 = {}
    raw_a8r = {}
    raw_cf = {}
    raw_local = {}
    raw_diag = {}
    for service, rows in grouped.items():
        ranked = sorted(rows, key=lambda item: (-_as_float(item.get("final_metric_score", item.get("final_candidate_score"))), -_as_float(item.get("diagnostic_specificity")), str(item.get("metric"))))
        top = ranked[0]
        top2 = ranked[:2]
        family = str(top.get("metric_family", "unknown"))
        raw_best[service] = _as_float(top.get("final_metric_score", top.get("final_candidate_score")))
        raw_top2[service] = sum(_as_float(item.get("final_metric_score", item.get("final_candidate_score"))) for item in top2) / len(top2)
        raw_a8r[service] = max(_as_float(item.get("service_score_from_a8r", item.get("service_score"))) for item in rows)
        raw_cf[service] = _as_float(service_cf.get(service, {}).get("delta_loss", service_cf.get(service, {}).get("combined_score", 0.0)))
        raw_local[service] = max(_as_float(item.get("service_family_evidence_support")) for item in rows if str(item.get("metric_family")) == family) if rows else 0.0
        raw_diag[service] = max(_as_float(item.get("diagnostic_specificity")) for item in rows if str(item.get("metric_family")) == family) if rows else 0.0
    norm_best = _normalize(raw_best)
    norm_top2 = _normalize(raw_top2)
    norm_a8r = _normalize(raw_a8r)
    norm_cf = _normalize(raw_cf)
    norm_local = _normalize(raw_local)
    norm_diag = _normalize(raw_diag)
    services: list[dict[str, Any]] = []
    for service, rows in grouped.items():
        ranked = sorted(rows, key=lambda item: (-_as_float(item.get("final_metric_score", item.get("final_candidate_score"))), -_as_float(item.get("diagnostic_specificity")), str(item.get("metric"))))
        top = ranked[0]
        top2 = ranked[:2]
        family = str(top.get("metric_family", "unknown"))
        path_support = compute_path_to_symptom_support(service, symptom_service, service_graph, residual_support)
        structured_support = compute_service_to_symptom_propagation_support(
            service,
            symptom_service,
            propagation_payload["edges"],
            propagation_payload["service_graph"],
            propagation_payload["residual_support"],
            StructuredPropagationConfig(),
        )
        local_family_support = norm_local.get(service, 0.0)
        diagnostic_family_support = norm_diag.get(service, 0.0)
        components = {
            "best_metric_score_norm": norm_best.get(service, 0.0),
            "top2_metric_mean_norm": norm_top2.get(service, 0.0),
            "a8r_service_score_norm": norm_a8r.get(service, 0.0),
            "service_counterfactual_norm": norm_cf.get(service, 0.0),
            "local_family_support": local_family_support,
            "path_to_symptom_support": path_support["path_to_symptom_support"],
            "downstream_load_support": path_support["downstream_load_support"],
            "structured_propagation_support": structured_support["structured_propagation_support"],
            "path_edge_support": structured_support["path_edge_support"],
            "downstream_response_support": structured_support["downstream_response_support"],
            "lag_support": structured_support["lag_support"],
            "propagation_relation_count": structured_support["propagation_relation_count"],
            "propagation_support_components": structured_support["support_components"],
            "structured_propagation_fallback_used": not propagation_payload["available"],
            "diagnostic_family_support": diagnostic_family_support,
            "path_penalty_applied": False,
            "symptom_service_load_penalty_applied": False,
            "cpu_service_diagnostic_boost_applied": False,
            "service_local_support_used": True,
            "global_family_support_weight_limited": True,
            "weights": {"best_metric": 0.22, "top2": 0.13, "a8r_service": 0.12, "service_counterfactual": 0.10, "service_local_family": 0.18, "structured_propagation": 0.20, "downstream_load": 0.05},
        }
        propagation_component = components["structured_propagation_support"] if propagation_payload["available"] else components["path_to_symptom_support"]
        final = (
            0.22 * components["best_metric_score_norm"]
            + 0.13 * components["top2_metric_mean_norm"]
            + 0.12 * components["a8r_service_score_norm"]
            + 0.10 * components["service_counterfactual_norm"]
            + 0.18 * components["local_family_support"]
            + 0.20 * propagation_component
            + 0.05 * components["downstream_load_support"]
        )
        if not path_support["has_path_to_symptom"]:
            final *= 0.3
            components["path_penalty_applied"] = True
        if service == symptom_service and family == "load":
            final *= 0.4
            components["symptom_service_load_penalty_applied"] = True
        if family == "CPU" and str(top.get("metric")) in {"cpu.throttled_usec", "cpu.throttle_ratio", "cpu.throttled_periods"} and _as_float(top.get("evidence_support")) > 0.0:
            final *= 1.15
            components["cpu_service_diagnostic_boost_applied"] = True
        components["final_service_score"] = final
        cf = service_cf.get(service, {})
        services.append({
            "service": service,
            "best_metric": top.get("node_id"),
            "best_metric_family": family,
            "best_metric_score": raw_best.get(service, 0.0),
            "top2_metric_mean": raw_top2.get(service, 0.0),
            "a8r_service_score": raw_a8r.get(service, 0.0),
            "service_counterfactual_delta_loss": _as_float(cf.get("delta_loss")),
            "service_counterfactual_score": _as_float(cf.get("combined_score")),
            "local_family_support": local_family_support,
            "path_to_symptom_support": path_support["path_to_symptom_support"],
            "downstream_load_support": path_support["downstream_load_support"],
            "structured_propagation_support": structured_support["structured_propagation_support"],
            "path_edge_support": structured_support["path_edge_support"],
            "downstream_response_support": structured_support["downstream_response_support"],
            "lag_support": structured_support["lag_support"],
            "propagation_relation_count": structured_support["propagation_relation_count"],
            "diagnostic_family_support": diagnostic_family_support,
            "path_support": path_support,
            "final_service_score": final,
            "score_components": components,
            "candidate_metric_count": len(rows),
            "source": "service_candidate_table",
        })
    services.sort(key=lambda item: (-_as_float(item.get("final_service_score")), -_as_float(item.get("best_metric_score")), str(item.get("service"))))
    for rank, item in enumerate(services, start=1):
        item["rank"] = rank
    return services


def select_root_service(service_candidate_table: list[dict[str, Any]]) -> dict[str, Any]:
    if not service_candidate_table:
        return {"service": "unknown", "final_service_score": 0.0, "rank": 0}
    return dict(service_candidate_table[0])


def select_root_metric_within_service(metric_candidate_table: list[dict[str, Any]], root_service: str) -> dict[str, Any]:
    rows = [row for row in metric_candidate_table if str(row.get("service")) == root_service]
    if not rows:
        return {"node_id": f"{root_service}.unknown", "service": root_service, "metric": "unknown", "metric_family": "unknown", "final_metric_score": 0.0, "rank": 0, "selection_error": "no_metric_candidates_for_root_service"}
    ranked = sorted(rows, key=lambda item: (not bool(item.get("ownership_valid", True)), -_as_float(item.get("final_metric_score", item.get("final_candidate_score"))), -_as_float(item.get("diagnostic_specificity")), -_as_float(item.get("evidence_support")), str(item.get("metric"))))
    return dict(ranked[0])


def build_top_metrics_within_service(metric_candidate_table: list[dict[str, Any]], root_service: str, top_k: int = 5) -> list[dict[str, Any]]:
    rows = [dict(row) for row in metric_candidate_table if str(row.get("service")) == root_service]
    rows.sort(key=lambda item: (-_as_float(item.get("final_metric_score", item.get("final_candidate_score"))), -_as_float(item.get("diagnostic_specificity")), -_as_float(item.get("evidence_support")), str(item.get("metric"))))
    for rank, item in enumerate(rows[:top_k], start=1):
        item["rank_within_service"] = rank
        item["primary_scope"] = "root_service_only"
    return rows[:top_k]


def build_top_services(service_candidate_table: list[dict[str, Any]], top_k: int = 5) -> list[dict[str, Any]]:
    rows = [dict(row) for row in service_candidate_table[:top_k]]
    for rank, item in enumerate(rows, start=1):
        item["rank"] = rank
    return rows


def _root_type_confidence_service_first(root_metric: dict[str, Any], service_candidate_table: list[dict[str, Any]]) -> float:
    diagnostic = _as_float(root_metric.get("diagnostic_specificity"))
    evidence = _as_float(root_metric.get("score_components", {}).get("evidence_norm")) if isinstance(root_metric.get("score_components"), dict) else 0.0
    margin = _score_margin(service_candidate_table, "final_service_score")
    return max(0.0, min(1.0, 0.40 * diagnostic + 0.35 * evidence + 0.25 * margin))


def build_path_explanation(service_graph_path: str, root_service: str, symptom_service: str) -> dict[str, Any]:
    graph = parse_service_graph(service_graph_path)
    adjacency = _service_graph_adjacency(graph)
    if not root_service or not symptom_service:
        path: list[str] = []
        status = "missing_service"
    else:
        queue: deque[list[str]] = deque([[root_service]])
        seen = {root_service}
        path = []
        status = "not_found"
        while queue:
            current = queue.popleft()
            node = current[-1]
            if node == symptom_service:
                path = current
                status = "found"
                break
            for nxt in sorted(adjacency.get(node, set())):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(current + [nxt])
    return {
        "path": path,
        "path_status": status,
        "path_source": "service_graph_bfs",
        "path_uses_injected_path": False,
        "path_root_service": root_service,
        "path_symptom_service": symptom_service,
    }


def _counterfactual_summary(counterfactual_dir: Path) -> dict[str, Any]:
    service = _top_rows(counterfactual_dir / "counterfactual_service_ranking.jsonl", 3)
    metric = _top_rows(counterfactual_dir / "counterfactual_metric_ranking.jsonl", 3)
    md = _read_json(counterfactual_dir / "counterfactual_metadata.json")
    return {
        "top_service_counterfactuals": service,
        "top_metric_counterfactuals": metric,
        "average_service_delta_loss": md.get("average_service_delta_loss"),
        "average_metric_delta_loss": md.get("average_metric_delta_loss"),
        "source": "a9_counterfactual_explanation",
    }


def _evidence_summary(evidence_dir: Path, channel_dir: Path) -> dict[str, Any]:
    ev_md = _read_json(evidence_dir / "blind_evidence_metadata.json")
    ch_md = _read_json(channel_dir / "evidence_channel_metadata.json")
    return {
        "blind_evidence_count": ev_md.get("evidence_count", 0),
        "evidence_types": ev_md.get("evidence_types", []),
        "baseline_strategy": ev_md.get("baseline_strategy"),
        "calibrated_residual_count": ch_md.get("calibrated_residual_count", 0),
        "average_abs_calibrated_residual": ch_md.get("average_abs_calibrated_residual"),
    }


def _propagation_summary(rls_dir: Path) -> dict[str, Any]:
    md = _read_json(rls_dir / "ipw_rls_metadata.json")
    return {"node_count": md.get("node_count"), "total_updates": md.get("total_updates"), "skipped_updates": md.get("skipped_updates"), "average_abs_residual": md.get("average_abs_residual"), "update_mode": md.get("update_mode"), "batch_ridge_used": md.get("batch_ridge_used")}


def _sparse_summary(graph_sparse_dir: Path) -> dict[str, Any]:
    md = _read_json(graph_sparse_dir / "graph_sparse_metadata.json")
    return {"node_count": md.get("node_count"), "edge_count": md.get("edge_count"), "nonzero_intervention_count": md.get("nonzero_intervention_count"), "solver_status": md.get("solver_status"), "optimization": md.get("optimization"), "consumes_calibrated_residuals": md.get("consumes_calibrated_residuals"), "consumes_raw_residuals": md.get("consumes_raw_residuals")}


def _result_confidence(primary: dict[str, Any], candidates: list[dict[str, Any]], cf: dict[str, Any]) -> float:
    return max(0.0, min(1.0, 0.55 * _score_margin(candidates, "final_candidate_score") + 0.25 * _score_margin(candidates, "metric_score") + 0.20 * min(1.0, abs(_as_float(cf.get("average_metric_delta_loss"))))))


def build_final_result_from_stages(stage_dirs: dict[str, str], raw_input_dir: str, output_dir: str, top_k: int = 5) -> dict[str, Any]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    alert_dir = Path(stage_dirs["alert_gate"])
    evidence_dir = Path(stage_dirs["blind_evidence"])
    rls_dir = Path(stage_dirs["ipw_rls"])
    channel_dir = Path(stage_dirs["evidence_channel"])
    graph_sparse_dir = Path(stage_dirs["graph_sparse"])
    counterfactual_dir = Path(stage_dirs["counterfactual"])
    raw_dir = Path(raw_input_dir)

    windows = load_jsonl(str(alert_dir / "alert_windows.jsonl"))
    metric_candidate_table = build_metric_candidate_table(stage_dirs)
    ownership_invalid_count = sum(1 for row in metric_candidate_table if not bool(row.get("ownership_valid", True)))
    ownership_repaired_count = sum(1 for row in metric_candidate_table if bool(row.get("score_components", {}).get("ownership_repaired", False)))
    global_top_metrics_auxiliary = [dict(row, auxiliary=True, auxiliary_reason="global_metric_ranking_not_primary") for row in metric_candidate_table[:top_k]]
    service_graph = parse_service_graph(str(raw_dir / "service_graph.jsonl"))
    cf = _counterfactual_summary(counterfactual_dir)
    _write_jsonl(out / "metric_candidate_table.jsonl", metric_candidate_table)

    results: list[dict[str, Any]] = []
    first_metadata: dict[str, Any] = {}
    for index, window in enumerate(windows, start=1):
        symptom_service = str(window.get("symptom_service", "unknown"))
        service_candidate_table = build_service_candidate_table(metric_candidate_table, stage_dirs, symptom_service, service_graph)
        root_service_candidate = select_root_service(service_candidate_table)
        root_service = str(root_service_candidate.get("service", "unknown"))
        root_metric_candidate = select_root_metric_within_service(metric_candidate_table, root_service)
        service_components = root_service_candidate.get("score_components") if isinstance(root_service_candidate.get("score_components"), dict) else {}
        metric_components = root_metric_candidate.get("score_components") if isinstance(root_metric_candidate.get("score_components"), dict) else {}
        metric_components = dict(metric_components)
        for key in ("structured_propagation_support", "path_edge_support", "lag_support", "downstream_response_support", "propagation_relation_count", "propagation_support_components"):
            if key in service_components:
                metric_components[key] = service_components[key]
        root_metric_candidate["score_components"] = metric_components
        root_metric_candidate["structured_propagation_support"] = metric_components.get("structured_propagation_support", 0.0)
        root_metric_candidate["path_edge_support"] = metric_components.get("path_edge_support", 0.0)
        root_metric_candidate["lag_support"] = metric_components.get("lag_support", 0.0)
        root_metric = str(root_metric_candidate.get("node_id", f"{root_service}.unknown"))
        top_metrics = build_top_metrics_within_service(metric_candidate_table, root_service, top_k=top_k)
        top_services = build_top_services(service_candidate_table, top_k=top_k)
        predicted_root_type = _metric_family_to_root_type(str(root_metric_candidate.get("metric_family", "unknown")))
        root_type_confidence = _root_type_confidence_service_first(root_metric_candidate, service_candidate_table)
        path = build_path_explanation(str(raw_dir / "service_graph.jsonl"), root_service, symptom_service)
        confidence = max(0.0, min(1.0, 0.50 * _score_margin(service_candidate_table, "final_service_score") + 0.30 * _score_margin(top_metrics, "final_metric_score") + 0.20 * root_type_confidence))
        top_service_metric_consistent = root_service == _node_service(root_metric)
        service_candidate_summary = {
            "service_candidate_count": len(service_candidate_table),
            "top_services": top_services[:top_k],
            "selection_mode": "service_first",
        }
        result = {
            "alert_window_id": str(window.get("alert_window_id", f"alert-window-{index:04d}")),
            "symptom_service": symptom_service,
            "trigger_metrics": window.get("trigger_metrics", []),
            "service_first": True,
            "primary_metric_conditioned_on_service": True,
            "primary_candidate": root_metric_candidate,
            "root_service_candidate": root_service_candidate,
            "root_metric_candidate": root_metric_candidate,
            "top_services": top_services,
            "top_metrics": top_metrics,
            "global_top_metrics_auxiliary": global_top_metrics_auxiliary,
            "global_top_metrics_primary": False,
            "predicted_top1_service": root_service,
            "predicted_top1_metric": root_metric,
            "predicted_root_type": predicted_root_type,
            "root_type_confidence": root_type_confidence,
            "root_type_source": "primary_metric_family",
            "root_type_uses_labels": False,
            "path": path.get("path", []),
            "path_status": path.get("path_status"),
            "path_explanation": path,
            "counterfactual_summary": cf,
            "evidence_summary": _evidence_summary(evidence_dir, channel_dir),
            "propagation_summary": _propagation_summary(rls_dir),
            "sparse_summary": _sparse_summary(graph_sparse_dir),
            "service_candidate_table_summary": service_candidate_summary,
            "root_service_selection_reason": "highest final_service_score from service_candidate_table",
            "root_metric_selection_reason": "highest final_metric_score within selected root_service",
            "confidence": confidence,
            "label_safety": {"uses_root_labels": False, "uses_target_config": False, "uses_injected_path": False, "uses_incident_start_end": False, "uses_legacy_evidence": False, "uses_alert_windows": True, "runs_old_p1_rca": False},
            "source": "b2m_ownership_integrated_blind_rca_pipeline",
        }
        results.append(result)
        if index == 1:
            first_metadata = {
                "top1_service": root_service,
                "top1_metric": root_metric,
                "predicted_root_type": predicted_root_type,
                "root_type_confidence": root_type_confidence,
                "path_status": result.get("path_status"),
                "top_service_metric_consistent": top_service_metric_consistent,
                "service_candidate_count": len(service_candidate_table),
                "primary_candidate_ownership_valid": bool(root_metric_candidate.get("ownership_valid", False)),
            }
            _write_jsonl(out / "service_candidate_table.jsonl", service_candidate_table)
            _write_jsonl(out / "top_services.jsonl", top_services)
    _write_jsonl(out / "integrated_rca_results.jsonl", results)

    aggregate: dict[str, Any] | None = None
    if results:
        selected = max(results, key=lambda item: (_as_float(item.get("confidence")), str(item.get("alert_window_id"))))
        aggregate = {"aggregation_mode": "highest_confidence_window", "selected_alert_window_id": selected.get("alert_window_id"), "result": selected}
        _write_json(out / "integrated_rca_aggregate.json", aggregate)

    metadata = {
        "raw_input_dir": str(raw_dir),
        "output_dir": str(out),
        "alert_windows_count": len(windows),
        "per_window_results_count": len(results),
        "final_results_count": len(results),
        "aggregate_result_count": 1 if aggregate else 0,
        "final_results_are_per_window": True,
        "aggregate_result_available": bool(aggregate),
        "top1_service": first_metadata.get("top1_service", "unknown"),
        "top1_metric": first_metadata.get("top1_metric", "unknown.unknown"),
        "predicted_root_type": first_metadata.get("predicted_root_type", "unknown"),
        "root_type_confidence": first_metadata.get("root_type_confidence", 0.0),
        "root_type_source": "primary_metric_family",
        "root_type_uses_labels": False,
        "path_status": first_metadata.get("path_status"),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "uses_legacy_evidence": False,
        "uses_alert_windows": True,
        "runs_old_p1_rca": False,
        "actual_probe_activation": False,
        "structured_propagation_enabled": True,
        "structured_propagation_model": "structured_multilag_ridge",
        "structured_propagation_uses_labels": False,
        "structured_propagation_uses_injected_path": False,
        "structured_propagation_uses_incident_start_end": False,
        "propagation_drift_used": False,
        "stable_only": True,
        "service_local_support_used": True,
        "global_family_support_weight_limited": True,
        "family_global_evidence_weight": 0.10,
        "ownership_invalid_count": ownership_invalid_count,
        "ownership_repaired_count": ownership_repaired_count,
        "primary_candidate_ownership_valid": bool(first_metadata.get("primary_candidate_ownership_valid", False)),
        "service_first_enabled": True,
        "primary_candidate_source": "service_candidate_table",
        "primary_service_source": "service_candidate_table",
        "primary_metric_source": "metric_candidates_within_root_service",
        "primary_metric_conditioned_on_service": True,
        "global_top_metrics_primary": False,
        "top_service_metric_consistent": all(str(result.get("predicted_top1_service")) == _node_service(str(result.get("predicted_top1_metric"))) for result in results),
        "per_window_results_match_alert_windows": len(results) == len(windows),
        "source": "b2m_ownership_integrated_blind_rca_pipeline",
    }
    _write_json(out / "integrated_rca_metadata.json", metadata)
    return {"metadata": metadata, "results": results, "aggregate": aggregate, "metric_candidate_table": metric_candidate_table, "top_services": _top_rows(out / "top_services.jsonl", top_k)}

def evaluate_integrated_result_debug(output_dir: str, incidents_path: str) -> dict[str, Any]:
    result_path = Path(output_dir) / "09_final_result" / "integrated_rca_results.jsonl"
    results = load_jsonl(str(result_path)) if result_path.exists() else []
    incidents = load_jsonl(incidents_path) if Path(incidents_path).exists() else []
    if not results or not incidents:
        debug = {"debug_available": False, "debug_notes": "missing results or incidents"}
    else:
        incident = incidents[0]
        root_service = str(incident.get("root_service", ""))
        root_metric = str(incident.get("root_metric", ""))
        root_type = str(incident.get("root_type", ""))
        per_result: list[dict[str, Any]] = []
        for result in results:
            top_metrics = [str(row.get("node_id", "")) for row in result.get("top_metrics", [])]
            per_result.append({
                "alert_window_id": result.get("alert_window_id"),
                "debug_service_hit_at_1": 1.0 if result.get("predicted_top1_service") == root_service else 0.0,
                "debug_metric_hit_at_3": 1.0 if root_metric in top_metrics[:3] or f"{root_service}.{root_metric}" in top_metrics[:3] else 0.0,
                "debug_root_type_accuracy": 1.0 if str(result.get("predicted_root_type", "")).lower() == root_type.lower() else 0.0,
                "debug_path_fidelity": 1.0 if root_service in result.get("path", []) else 0.0,
            })
        debug = {"debug_available": True, "debug_only": True, "per_result": per_result, "debug_service_hit_at_1": per_result[0]["debug_service_hit_at_1"], "debug_metric_hit_at_3": per_result[0]["debug_metric_hit_at_3"], "debug_root_type_accuracy": per_result[0]["debug_root_type_accuracy"], "debug_path_fidelity": per_result[0]["debug_path_fidelity"], "debug_notes": "post-hoc only; not used by integrated result"}
    _write_json(Path(output_dir) / "09_final_result" / "integrated_debug_evaluation.json", debug)
    return debug


def run_integrated_blind_rca(raw_input_dir: str, output_dir: str, config: dict[str, Any] | None = None, debug_evaluate_incidents: bool = False) -> dict[str, Any]:
    cfg = config or {}
    raw = Path(raw_input_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not (raw / "metrics.jsonl").exists():
        raise FileNotFoundError(f"missing metrics.jsonl: {raw / 'metrics.jsonl'}")
    if not (raw / "service_graph.jsonl").exists():
        raise FileNotFoundError(f"missing service_graph.jsonl: {raw / 'service_graph.jsonl'}")
    stages = {"alert_gate": out / "01_alert_gate", "blind_evidence": out / "02_blind_evidence", "candidate_subgraph": out / "03_candidate_subgraph", "probe_policy": out / "04_probe_policy", "ipw_rls": out / "05_ipw_rls", "structured_propagation": out / "05b_structured_propagation", "evidence_channel": out / "06_evidence_channel", "graph_sparse": out / "07_graph_sparse", "counterfactual": out / "08_counterfactual", "final_result": out / "09_final_result"}
    write_alert_outputs(str(raw), str(stages["alert_gate"]), cfg.get("alert_gate"))
    generate_blind_evidence_from_alert_windows(str(raw / "metrics.jsonl"), str(stages["alert_gate"] / "alert_windows.jsonl"), str(stages["blind_evidence"]), min_score=float(cfg.get("min_evidence_score", 0.05)), top_k_per_type=int(cfg.get("top_k_per_type", 20)))
    build_candidate_subgraphs_for_repeat(str(raw), str(stages["alert_gate"]), str(stages["candidate_subgraph"]))
    write_probe_policy_outputs(str(stages["alert_gate"]), str(stages["candidate_subgraph"]), str(stages["probe_policy"]), str(stages["blind_evidence"]), budget=float(cfg.get("budget", 12.0)))
    fit_structured_multilag_propagation(str(raw), str(stages["candidate_subgraph"]), str(stages["probe_policy"]), str(stages["alert_gate"]), str(stages["structured_propagation"]))
    run_ipw_rls_preview(str(raw), str(stages["candidate_subgraph"]), str(stages["probe_policy"]), str(stages["ipw_rls"]))
    build_evidence_channel(str(stages["blind_evidence"]), str(stages["probe_policy"]), str(stages["ipw_rls"]), str(stages["evidence_channel"]))
    run_graph_sparse_inversion(str(stages["candidate_subgraph"]), str(stages["evidence_channel"]), str(stages["graph_sparse"]))
    run_counterfactual_explanation(str(stages["graph_sparse"]), str(stages["candidate_subgraph"]), str(stages["evidence_channel"]), str(stages["counterfactual"]))
    final = build_final_result_from_stages({key: str(path) for key, path in stages.items()}, str(raw), str(stages["final_result"]), top_k=int(cfg.get("top_k", 5)))
    debug: dict[str, Any] | None = None
    incidents_path = raw / "incidents.jsonl"
    if debug_evaluate_incidents and incidents_path.exists():
        debug = evaluate_integrated_result_debug(str(out), str(incidents_path))
        evaluate_alert_windows_for_debug(str(stages["alert_gate"] / "alert_windows.jsonl"), str(incidents_path))
        evaluate_candidate_subgraph_for_debug(str(stages["candidate_subgraph"] / "repeat_candidate_summary.json"), str(incidents_path))
        evaluate_probe_policy_for_debug(str(stages["probe_policy"]), str(incidents_path))
        evaluate_ipw_rls_debug(str(stages["ipw_rls"]), str(incidents_path))
        evaluate_evidence_channel_debug(str(stages["evidence_channel"]), str(incidents_path))
        evaluate_graph_sparse_debug(str(stages["graph_sparse"]), str(incidents_path))
        evaluate_counterfactual_debug(str(stages["counterfactual"]), str(incidents_path))
    metadata = {**final["metadata"], "stage_dirs": {key: str(path) for key, path in stages.items()}, "debug_evaluation": debug, "runs_old_p1_rca": False, "reinjects_faults": False, "actual_probe_activation": False}
    _write_json(out / "integrated_pipeline_metadata.json", metadata)
    return {"metadata": metadata, "results": final["results"], "aggregate": final.get("aggregate"), "debug": debug}

"""P1E IPW semantic path explanation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import IPWSemanticEvidenceConfig, score_ipw_semantic_evidence
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


@dataclass
class IPWPathExplanationConfig:
    """Configuration for P1E IPW semantic path explanations."""

    top_k_candidates: int = 5
    max_path_length: int = 5
    use_reverse_edges: bool = True
    use_undirected_fallback: bool = True
    propagation_support_weight: float = 0.30
    semantic_score_weight: float = 1.00
    confidence_weight: float = 0.20
    missing_path_penalty: float = 0.50
    min_edge_support: float = 0.0


@dataclass
class IPWPathExplanationRecord:
    """Path explanation for one P1D semantic candidate."""

    incident_id: str
    candidate_service: str
    candidate_metric: str
    candidate_node: str
    semantic_rank: int
    semantic_score: float
    root_type_candidate: str
    symptom_service: str
    path_services: list[str]
    path_edges: list[dict]
    path_length: int
    path_missing: bool
    path_score: float
    propagation_support: float
    confidence: float
    reason: str
    source: str = "ipw_semantic_path_explanation"


_ALLOWED_EDGE_TYPES = {"call", "trace", "cohost", "resource", "synthetic"}


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], dict, list[dict], list[dict]]:
    """Load P1E inputs and fail clearly when a required file is missing."""

    input_path = Path(input_dir)
    required = {
        "ipw_semantic_interventions": input_path / "ipw_semantic_interventions.jsonl",
        "ipw_semantic_type_scores": input_path / "ipw_semantic_type_scores.jsonl",
        "ipw_stable_propagation_model": input_path / "ipw_stable_propagation_model.json",
        "service_graph": input_path / "service_graph.jsonl",
        "incidents": input_path / "incidents.jsonl",
    }
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required P1E input for {name}: {path}")
    model = json.loads(required["ipw_stable_propagation_model"].read_text(encoding="utf-8"))
    if not isinstance(model, dict):
        raise ValueError(f"IPW propagation model is not a JSON object: {required['ipw_stable_propagation_model']}")
    return (
        read_jsonl(required["ipw_semantic_interventions"]),
        read_jsonl(required["ipw_semantic_type_scores"]),
        model,
        read_jsonl(required["service_graph"]),
        read_jsonl(required["incidents"]),
    )


def _infer_known_services(graph_edges: list[dict], semantic_records: list[dict] | None = None, incidents: list[dict] | None = None) -> list[str]:
    services: set[str] = set()
    for edge in graph_edges:
        for endpoint in (str(edge.get("src", "")), str(edge.get("dst", ""))):
            if endpoint:
                services.add(endpoint.split(".", 1)[0])
    for row in semantic_records or []:
        if row.get("service"):
            services.add(str(row["service"]))
    for incident in incidents or []:
        if incident.get("symptom_service"):
            services.add(str(incident["symptom_service"]))
    return sorted(services, key=lambda item: (-len(item), item))


def _node_to_service(node: str, known_services: list[str]) -> str | None:
    for service in sorted(known_services, key=lambda item: (-len(item), item)):
        if node == service or node.startswith(f"{service}."):
            return service
    return None


def _edge_record(src: str, dst: str, edge: dict, mode: str) -> dict:
    return {
        "src": src,
        "dst": dst,
        "edge_type": str(edge.get("edge_type", "unknown")),
        "weight": float(edge.get("weight", 1.0)),
        "mode": mode,
    }


def _add_edge(adjacency: dict[str, dict[str, list[dict]]], src: str, dst: str, record: dict) -> None:
    adjacency.setdefault(src, {}).setdefault(dst, []).append(record)


def build_service_graph(graph_edges: list[dict], config: IPWPathExplanationConfig) -> dict[str, Any]:
    """Build directed, reverse, and undirected service-level adjacency maps."""

    known_services = _infer_known_services(graph_edges)
    directed: dict[str, dict[str, list[dict]]] = {}
    reverse: dict[str, dict[str, list[dict]]] = {}
    undirected: dict[str, dict[str, list[dict]]] = {}

    for edge in graph_edges:
        edge_type = str(edge.get("edge_type", ""))
        if edge_type not in _ALLOWED_EDGE_TYPES:
            continue
        src = _node_to_service(str(edge.get("src", "")), known_services)
        dst = _node_to_service(str(edge.get("dst", "")), known_services)
        if not src or not dst or src == dst:
            continue
        _add_edge(directed, src, dst, _edge_record(src, dst, edge, "directed"))
        if config.use_reverse_edges:
            _add_edge(reverse, dst, src, _edge_record(dst, src, edge, "reverse"))
        if config.use_undirected_fallback:
            _add_edge(undirected, src, dst, _edge_record(src, dst, edge, "undirected"))
            _add_edge(undirected, dst, src, _edge_record(dst, src, edge, "undirected"))
    return {"directed": directed, "reverse": reverse, "undirected": undirected, "known_services": known_services}


def build_propagation_support_index(ipw_model: dict) -> dict[tuple[str, str], float]:
    """Build service-service propagation support from IPW propagation coefficients."""

    support: dict[tuple[str, str], float] = {}
    coefficients = ipw_model.get("coefficients", []) if isinstance(ipw_model, dict) else []
    for row in coefficients if isinstance(coefficients, list) else []:
        if not isinstance(row, dict):
            continue
        parent = str(row.get("parent", ""))
        target = str(row.get("target", ""))
        if "." not in parent or "." not in target:
            continue
        parent_service = parent.split(".", 1)[0]
        target_service = target.split(".", 1)[0]
        if not parent_service or not target_service or parent_service == target_service:
            continue
        try:
            value = abs(float(row.get("coefficient", 0.0)))
        except (TypeError, ValueError):
            continue
        support[(parent_service, target_service)] = max(support.get((parent_service, target_service), 0.0), value)
        support[(target_service, parent_service)] = max(support.get((target_service, parent_service), 0.0), value)
    return support


def _best_edge(adjacency: dict[str, dict[str, list[dict]]], src: str, dst: str) -> dict:
    edges = adjacency.get(src, {}).get(dst, [])
    if not edges:
        return {"src": src, "dst": dst, "edge_type": "unknown", "weight": 0.0, "mode": "missing"}
    return dict(sorted(edges, key=lambda item: (-float(item.get("weight", 0.0)), str(item.get("edge_type", "")), str(item.get("mode", ""))))[0])


def _bfs(adjacency: dict[str, dict[str, list[dict]]], start: str, end: str, max_path_length: int) -> list[str] | None:
    if start == end:
        return [start]
    queue: list[list[str]] = [[start]]
    while queue:
        path = queue.pop(0)
        if len(path) - 1 >= max_path_length:
            continue
        for next_service in sorted(adjacency.get(path[-1], {})):
            if next_service in path:
                continue
            next_path = path + [next_service]
            if next_service == end:
                return next_path
            queue.append(next_path)
    return None


def _path_edges(path: list[str], adjacency: dict[str, dict[str, list[dict]]]) -> list[dict]:
    return [_best_edge(adjacency, src, dst) for src, dst in zip(path[:-1], path[1:])]


def find_candidate_paths(
    candidate_service: str,
    symptom_service: str,
    service_graph: dict[str, Any],
    config: IPWPathExplanationConfig,
) -> tuple[list[str], list[dict], str]:
    """Search a service-level path for a candidate and return its mode."""

    if candidate_service == symptom_service:
        return [candidate_service], [], "self"

    directed = service_graph["directed"]
    path = _bfs(directed, candidate_service, symptom_service, config.max_path_length)
    if path is not None:
        return path, _path_edges(path, directed), "directed"

    if config.use_reverse_edges:
        reverse = service_graph["reverse"]
        path = _bfs(reverse, candidate_service, symptom_service, config.max_path_length)
        if path is not None:
            return path, _path_edges(path, reverse), "reverse"

    if config.use_undirected_fallback:
        undirected = service_graph["undirected"]
        path = _bfs(undirected, candidate_service, symptom_service, config.max_path_length)
        if path is not None:
            return path, _path_edges(path, undirected), "undirected"

    return [candidate_service, symptom_service], [], "missing"


def _mean_support(path_edges: list[dict], support_index: dict[tuple[str, str], float], config: IPWPathExplanationConfig) -> float:
    if not path_edges:
        return 1.0
    values = [support_index.get((str(edge["src"]), str(edge["dst"])), 0.0) for edge in path_edges]
    values = [value for value in values if value >= config.min_edge_support]
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def score_path(candidate: dict, path_edges: list[dict], propagation_support_index: dict[tuple[str, str], float], config: IPWPathExplanationConfig, path_missing: bool = False) -> tuple[float, float]:
    """Score a P1E path using semantic score, support, confidence, and missing penalty."""

    propagation_support = _mean_support(path_edges, propagation_support_index, config)
    semantic_score = float(candidate["semantic_score"])
    confidence = float(candidate.get("confidence", 0.0))
    path_score = (
        config.semantic_score_weight
        * semantic_score
        * (1.0 + config.propagation_support_weight * propagation_support)
        * (1.0 + config.confidence_weight * confidence)
    )
    if path_missing:
        path_score *= config.missing_path_penalty
    return float(path_score), float(propagation_support)


def _type_rank_one(type_records: list[dict], incident_id: str) -> str:
    rows = [row for row in type_records if str(row.get("incident_id")) == incident_id]
    rows = sorted(rows, key=lambda row: (int(row.get("rank", 10**9)), str(row.get("root_type_candidate", "unknown"))))
    return str(rows[0].get("root_type_candidate", "unknown")) if rows else "unknown"


def _path_intersects_injected(path_services: list[str], injected_path: list[str] | None) -> bool | None:
    if not injected_path:
        return None
    injected_services = {str(node).split(".", 1)[0] for node in injected_path if node}
    return any(service in injected_services for service in path_services)


def explain_ipw_paths(input_dir: str | Path, output_dir: str | Path | None = None, config: IPWPathExplanationConfig | None = None) -> dict:
    """Generate P1E path explanations for IPW semantic candidates."""

    cfg = config or IPWPathExplanationConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    output_path.mkdir(parents=True, exist_ok=True)

    semantic_records, type_records, ipw_model, graph_edges, incidents = load_required_dataset(input_path)
    service_graph = build_service_graph(graph_edges, cfg)
    support_index = build_propagation_support_index(ipw_model)

    all_records: list[dict] = []
    per_incident: list[dict] = []
    fidelity_values: list[float] = []

    for incident in incidents:
        incident_id = str(incident["incident_id"])
        symptom_service = str(incident["symptom_service"])
        root_type_candidate = _type_rank_one(type_records, incident_id)
        candidates = [row for row in semantic_records if str(row.get("incident_id")) == incident_id and int(row.get("semantic_rank", 10**9)) <= cfg.top_k_candidates]
        candidates = sorted(candidates, key=lambda row: (int(row["semantic_rank"]), str(row["node"])))

        incident_records: list[dict] = []
        for candidate in candidates:
            candidate_service = str(candidate["service"])
            path_services, path_edges, path_mode = find_candidate_paths(candidate_service, symptom_service, service_graph, cfg)
            path_missing = path_mode == "missing"
            path_score, propagation_support = score_path(candidate, path_edges, support_index, cfg, path_missing)
            reason = path_mode if not path_missing else "path_missing"
            record = asdict(
                IPWPathExplanationRecord(
                    incident_id=incident_id,
                    candidate_service=candidate_service,
                    candidate_metric=str(candidate["metric"]),
                    candidate_node=str(candidate["node"]),
                    semantic_rank=int(candidate["semantic_rank"]),
                    semantic_score=float(candidate["semantic_score"]),
                    root_type_candidate=root_type_candidate,
                    symptom_service=symptom_service,
                    path_services=path_services,
                    path_edges=path_edges,
                    path_length=max(len(path_services) - 1, 0) if not path_missing else 0,
                    path_missing=path_missing,
                    path_score=path_score,
                    propagation_support=propagation_support,
                    confidence=float(candidate.get("confidence", 0.0)),
                    reason=reason,
                )
            )
            incident_records.append(record)
            all_records.append(record)

        sorted_records = sorted(incident_records, key=lambda row: (-float(row["path_score"]), int(row["semantic_rank"]), str(row["candidate_node"])))
        top_record = sorted_records[0] if sorted_records else None
        path_intersects = _path_intersects_injected(top_record["path_services"], incident.get("injected_path")) if top_record else None
        if path_intersects is not None:
            fidelity_values.append(1.0 if path_intersects else 0.0)
        true_node = f"{incident['root_service']}.{incident['root_metric']}"
        true_candidate = next((row for row in candidates if str(row.get("node")) == true_node), None)
        per_incident.append(
            {
                "incident_id": incident_id,
                "top_path_candidate": top_record["candidate_node"] if top_record else None,
                "top_path_score": float(top_record["path_score"]) if top_record else None,
                "top_path_services": top_record["path_services"] if top_record else [],
                "top_path_missing": bool(top_record["path_missing"]) if top_record else True,
                "true_root_semantic_rank_debug": int(true_candidate["semantic_rank"]) if true_candidate else None,
                "path_intersects_injected_path_debug": path_intersects,
            }
        )

    paths_missing_count = sum(1 for row in all_records if row["path_missing"])
    summary = {
        "incidents_count": len(incidents),
        "path_records_count": len(all_records),
        "candidates_explained_count": sum(min(cfg.top_k_candidates, len([row for row in semantic_records if str(row.get("incident_id")) == str(incident["incident_id"])])) for incident in incidents),
        "paths_missing_count": paths_missing_count,
        "path_fidelity_debug": float(np.mean(fidelity_values)) if fidelity_values else None,
        "per_incident": per_incident,
    }
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "top_k_candidates": cfg.top_k_candidates,
        "max_path_length": cfg.max_path_length,
        "use_reverse_edges": cfg.use_reverse_edges,
        "use_undirected_fallback": cfg.use_undirected_fallback,
        "path_records_count": len(all_records),
        "paths_missing_count": paths_missing_count,
    }

    write_jsonl(output_path / "ipw_path_explanations.jsonl", all_records)
    (output_path / "ipw_path_explanation_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_path / "ipw_path_explanation_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "ipw_path_explanations_path": str(output_path / "ipw_path_explanations.jsonl"),
        "ipw_path_explanation_summary_path": str(output_path / "ipw_path_explanation_summary.json"),
        "ipw_path_explanation_metadata_path": str(output_path / "ipw_path_explanation_metadata.json"),
        "summary": summary,
        "metadata": metadata,
    }


def run_p1e_pipeline(
    output_dir: str | Path,
    seed: int = 7,
    baseline_windows: int = 30,
    faulty_windows: int = 30,
    instances_per_service: int = 2,
    config: IPWPathExplanationConfig | None = None,
) -> dict:
    """Run P1E pipeline through path explanation only, without RCAResult."""

    output_path = Path(output_dir)
    generate_dataset(
        SyntheticConfig(
            seed=seed,
            output_dir=str(output_path),
            baseline_windows=baseline_windows,
            faulty_windows=faulty_windows,
            instances_per_service=instances_per_service,
        )
    )
    normalize_dataset(output_path, output_path)
    simulate_adaptive_observation(output_path, output_path, ObservationPolicyConfig(seed=seed))
    train_ipw_masked_propagation(output_path, output_path, IPWPropagationConfig())
    solve_ipw_sparse_inversion(output_path, output_path, IPWSparseInversionConfig())
    score_ipw_semantic_evidence(output_path, output_path, IPWSemanticEvidenceConfig())
    return explain_ipw_paths(output_path, output_path, config or IPWPathExplanationConfig())

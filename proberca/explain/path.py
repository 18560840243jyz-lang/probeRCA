"""Path explanation for probeRCA P0 Step 7."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl


@dataclass
class PathExplanationConfig:
    """Configuration for service-level path explanation."""

    top_k_candidates_per_incident: int = 5
    max_path_length: int = 5
    propagation_weight: float = 1.0
    semantic_weight: float = 1.0
    prefer_shorter_path_weight: float = 0.1
    include_synthetic_edges: bool = True
    include_call_edges: bool = True
    include_trace_edges: bool = True
    include_cohost_edges: bool = True
    include_resource_edges: bool = True
    top_k_paths_per_candidate: int = 3


@dataclass
class PathExplanationRecord:
    """Path explanation from a candidate root node to a symptom service."""

    incident_id: str
    candidate_service: str
    candidate_metric: str
    candidate_node: str
    symptom_service: str
    semantic_rank: int
    semantic_score: float
    path: list[str]
    path_edges: list[dict]
    path_score: float
    path_length: int
    path_rank: int
    intersects_injected_path: bool | None = None
    source: str = "path_explanation"


_EDGE_CONFIG = {
    "synthetic": "include_synthetic_edges",
    "call": "include_call_edges",
    "trace": "include_trace_edges",
    "cohost": "include_cohost_edges",
    "resource": "include_resource_edges",
}


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict], dict | None]:
    """Load Step 7 inputs from a generated probeRCA dataset directory."""

    input_path = Path(input_dir)
    required = {
        "semantic_interventions": input_path / "semantic_interventions.jsonl",
        "service_graph": input_path / "service_graph.jsonl",
        "incidents": input_path / "incidents.jsonl",
    }
    for name, required_path in required.items():
        if not required_path.exists():
            raise FileNotFoundError(f"missing required {name} file: {required_path}")

    stable_path = input_path / "stable_propagation_model.json"
    stable_model: dict | None = None
    if stable_path.exists():
        with stable_path.open("r", encoding="utf-8") as handle:
            stable_model = json.load(handle)
    return read_jsonl(required["semantic_interventions"]), read_jsonl(required["service_graph"]), read_jsonl(required["incidents"]), stable_model


def _infer_known_services(graph_edges: list[dict]) -> list[str]:
    services: set[str] = set()
    for edge in graph_edges:
        for endpoint in (str(edge.get("src", "")), str(edge.get("dst", ""))):
            if not endpoint:
                continue
            if "." not in endpoint:
                services.add(endpoint)
            else:
                services.add(endpoint.split(".", 1)[0])
    return sorted(services, key=lambda item: (-len(item), item))


def node_to_service(node: str, known_services: list[str]) -> str | None:
    """Extract a service name from a service or service.metric node key."""

    ordered_services = sorted(known_services, key=lambda item: (-len(item), item))
    for service in ordered_services:
        if node == service or node.startswith(f"{service}."):
            return service
    return None


def build_service_adjacency(graph_edges: list[dict], config: PathExplanationConfig) -> dict[str, dict[str, list[dict]]]:
    """Build service-level adjacency from service graph edges."""

    known_services = _infer_known_services(graph_edges)
    adjacency: dict[str, dict[str, list[dict]]] = {}
    for edge in graph_edges:
        edge_type = str(edge.get("edge_type", ""))
        config_field = _EDGE_CONFIG.get(edge_type)
        if config_field is None or not bool(getattr(config, config_field)):
            continue
        src_service = node_to_service(str(edge.get("src", "")), known_services)
        dst_service = node_to_service(str(edge.get("dst", "")), known_services)
        if not src_service or not dst_service or src_service == dst_service:
            continue
        edge_record = {
            "src": src_service,
            "dst": dst_service,
            "edge_type": edge_type,
            "weight": float(edge.get("weight", 1.0)),
        }
        adjacency.setdefault(src_service, {}).setdefault(dst_service, []).append(edge_record)
    return adjacency


def load_propagation_coefficients(stable_model: dict | None) -> dict[tuple[str, str, str], float]:
    """Load absolute propagation coefficients keyed by incident, parent, and target."""

    if not isinstance(stable_model, dict):
        return {}
    coefficient_rows: list[dict] = []
    if isinstance(stable_model.get("coefficients"), list):
        coefficient_rows.extend(row for row in stable_model["coefficients"] if isinstance(row, dict))
    for incident_model in stable_model.get("incidents", []):
        if isinstance(incident_model, dict) and isinstance(incident_model.get("coefficients"), list):
            coefficient_rows.extend(row for row in incident_model["coefficients"] if isinstance(row, dict))

    coefficients: dict[tuple[str, str, str], float] = {}
    for row in coefficient_rows:
        try:
            key = (str(row["incident_id"]), str(row["parent"]), str(row["target"]))
            value = abs(float(row["coefficient"]))
        except (KeyError, TypeError, ValueError):
            continue
        coefficients[key] = max(coefficients.get(key, 0.0), value)
    return coefficients


def find_paths(adjacency: dict[str, dict[str, list[dict]]], start_service: str, end_service: str, max_path_length: int) -> list[list[str]]:
    """Find simple service paths from start_service to end_service."""

    if start_service == end_service:
        return [[start_service]]
    queue: list[list[str]] = [[start_service]]
    results: list[list[str]] = []
    while queue:
        path = queue.pop(0)
        edge_count = len(path) - 1
        if edge_count >= max_path_length:
            continue
        for next_service in sorted(adjacency.get(path[-1], {})):
            if next_service in path:
                continue
            next_path = path + [next_service]
            if next_service == end_service:
                results.append(next_path)
            else:
                queue.append(next_path)
    return sorted(results, key=lambda item: (len(item), "->".join(item)))


def _best_edge(adjacency: dict[str, dict[str, list[dict]]], src: str, dst: str) -> dict:
    candidates = adjacency.get(src, {}).get(dst, [])
    if not candidates:
        return {"src": src, "dst": dst, "edge_type": "unknown", "weight": 0.0}
    return dict(sorted(candidates, key=lambda item: (-float(item.get("weight", 1.0)), str(item.get("edge_type", ""))))[0])


def _edge_strength(src: str, dst: str, incident_id: str, propagation_coefficients: dict[tuple[str, str, str], float]) -> float:
    strengths = [
        value
        for (coef_incident_id, parent, target), value in propagation_coefficients.items()
        if coef_incident_id == incident_id and parent.startswith(f"{src}.") and target.startswith(f"{dst}.")
    ]
    if not strengths:
        return 1.0
    return float(max(strengths))


def score_path(
    path: list[str],
    candidate_record: dict,
    incident_id: str,
    propagation_coefficients: dict[tuple[str, str, str], float],
    config: PathExplanationConfig,
) -> float:
    """Score a path using semantic score, propagation strength, and shortness."""

    path_length = max(len(path) - 1, 0)
    semantic_score = float(candidate_record["semantic_score"])
    shortness_bonus = 1.0 / (1.0 + path_length)
    edge_strengths = [
        _edge_strength(src, dst, incident_id, propagation_coefficients)
        for src, dst in zip(path[:-1], path[1:])
    ]
    path_propagation = float(np.prod(np.asarray(edge_strengths, dtype=float))) if edge_strengths else 1.0
    return float(
        config.semantic_weight * semantic_score
        + config.propagation_weight * path_propagation
        + config.prefer_shorter_path_weight * shortness_bonus
    )


def path_intersects_injected_path(path: list[str], injected_path: list[str] | None) -> bool | None:
    """Check whether a service-level path intersects the synthetic injected path."""

    if not injected_path:
        return None
    injected_services = {str(node).split(".", 1)[0] for node in injected_path if node}
    return any(service in injected_services for service in path)


def explain_paths_for_incident(
    semantic_records: list[dict],
    graph_edges: list[dict],
    incident: dict,
    stable_model: dict | None,
    config: PathExplanationConfig,
) -> tuple[list[dict], dict]:
    """Generate path explanations for one incident."""

    incident_id = str(incident["incident_id"])
    records = [row for row in semantic_records if row.get("incident_id") == incident_id]
    records = sorted(records, key=lambda row: (int(row["semantic_rank"]), str(row["node"])))[: config.top_k_candidates_per_incident]
    if not records:
        raise ValueError(f"no semantic intervention records found for incident_id={incident_id}")

    symptom_service = str(incident["symptom_service"])
    adjacency = build_service_adjacency(graph_edges, config)
    propagation_coefficients = load_propagation_coefficients(stable_model)
    output: list[dict] = []
    paths_missing_count = 0

    for record in records:
        candidate_service = str(record["service"])
        candidate_metric = str(record["metric"])
        candidate_node = str(record["node"])
        candidate_paths = find_paths(adjacency, candidate_service, symptom_service, config.max_path_length)
        candidate_rows: list[dict] = []
        if not candidate_paths:
            # path_missing fallback only keeps the candidate explainable; it does not invent a propagation path.
            paths_missing_count += 1
            candidate_rows.append(
                {
                    **asdict(
                        PathExplanationRecord(
                            incident_id=incident_id,
                            candidate_service=candidate_service,
                            candidate_metric=candidate_metric,
                            candidate_node=candidate_node,
                            symptom_service=symptom_service,
                            semantic_rank=int(record["semantic_rank"]),
                            semantic_score=float(record["semantic_score"]),
                            path=[candidate_service],
                            path_edges=[],
                            path_score=float(record["semantic_score"]) * 0.1,
                            path_length=0,
                            path_rank=1,
                            intersects_injected_path=path_intersects_injected_path([candidate_service], incident.get("injected_path")),
                        )
                    ),
                    "path_missing": True,
                }
            )
        else:
            for path_item in candidate_paths:
                path_edges = [_best_edge(adjacency, src, dst) for src, dst in zip(path_item[:-1], path_item[1:])]
                candidate_rows.append(
                    asdict(
                        PathExplanationRecord(
                            incident_id=incident_id,
                            candidate_service=candidate_service,
                            candidate_metric=candidate_metric,
                            candidate_node=candidate_node,
                            symptom_service=symptom_service,
                            semantic_rank=int(record["semantic_rank"]),
                            semantic_score=float(record["semantic_score"]),
                            path=path_item,
                            path_edges=path_edges,
                            path_score=score_path(path_item, record, incident_id, propagation_coefficients, config),
                            path_length=max(len(path_item) - 1, 0),
                            path_rank=0,
                            intersects_injected_path=path_intersects_injected_path(path_item, incident.get("injected_path")),
                        )
                    )
                )
            candidate_rows = sorted(candidate_rows, key=lambda row: (-float(row["path_score"]), int(row["path_length"]), "->".join(row["path"])))
            candidate_rows = candidate_rows[: config.top_k_paths_per_candidate]
            for index, row in enumerate(candidate_rows, start=1):
                row["path_rank"] = index
        output.extend(candidate_rows)

    sorted_output = sorted(output, key=lambda row: (-float(row["path_score"]), int(row["semantic_rank"]), int(row["path_rank"]), "->".join(row["path"])))
    top_record = sorted_output[0] if sorted_output else None
    path_fidelity_debug = {
        "has_injected_path": bool(incident.get("injected_path")),
        "top_path_intersects_injected_path": top_record.get("intersects_injected_path") if top_record else None,
    }
    summary = {
        "incident_id": incident_id,
        "candidates_explained": len(records),
        "paths_count": len(output),
        "paths_missing_count": paths_missing_count,
        "top_path_candidate": top_record["candidate_node"] if top_record else None,
        "top_path_score": top_record["path_score"] if top_record else None,
        "path_fidelity_debug": path_fidelity_debug,
    }
    return output, summary


def explain_paths(input_dir: str | Path, output_dir: str | Path | None = None, config: PathExplanationConfig | None = None) -> dict:
    """Run Step 7 path explanation for all incidents in a dataset directory."""

    cfg = config or PathExplanationConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    output_path.mkdir(parents=True, exist_ok=True)

    semantic_records, graph_edges, incidents, stable_model = load_required_dataset(input_path)
    all_records: list[dict] = []
    summaries: list[dict] = []
    for incident in incidents:
        records, summary = explain_paths_for_incident(semantic_records, graph_edges, incident, stable_model, cfg)
        all_records.extend(records)
        summaries.append(summary)

    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "path_records_count": len(all_records),
        "candidates_explained_count": sum(int(item["candidates_explained"]) for item in summaries),
        "paths_missing_count": sum(int(item["paths_missing_count"]) for item in summaries),
        "top_k_candidates_per_incident": cfg.top_k_candidates_per_incident,
        "max_path_length": cfg.max_path_length,
        "top_k_paths_per_candidate": cfg.top_k_paths_per_candidate,
    }

    write_jsonl(output_path / "path_explanations.jsonl", all_records)
    with (output_path / "path_explanation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump({"summaries": summaries}, handle, ensure_ascii=False, indent=2)
    with (output_path / "path_explanation_metadata.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, ensure_ascii=False, indent=2)

    return {
        "path_explanations_path": str(output_path / "path_explanations.jsonl"),
        "path_explanation_summary_path": str(output_path / "path_explanation_summary.json"),
        "path_explanation_metadata_path": str(output_path / "path_explanation_metadata.json"),
        "metadata": metadata,
        "summaries": summaries,
    }

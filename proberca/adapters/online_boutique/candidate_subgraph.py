"""Candidate subgraph builder for A4 Online Boutique blind workflow.

A4 builds candidate services and service-metric nodes from alert windows,
service_graph, and observed metrics only. Incident labels are accepted only by
post-build debug helpers and never influence graph construction.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import numpy as np

SERVICE_KEYS = ("service", "service_name", "pod_service", "svc")
METRIC_KEYS = ("metric", "metric_name", "name")
SRC_KEYS = ("src", "source", "caller", "from", "upstream")
DST_KEYS = ("dst", "target", "callee", "to", "downstream")
LABEL_KEYS = ("node", "host", "pod", "container", "namespace", "instance")


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


def _first_present(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _sets_to_sorted(mapping: dict[str, set[str]]) -> dict[str, list[str]]:
    return {key: sorted(values) for key, values in sorted(mapping.items())}


def parse_service_graph(path: str) -> dict[str, Any]:
    rows = load_jsonl(path)
    services: set[str] = set()
    edges: list[dict[str, str]] = []
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)

    for row in rows:
        src = _first_present(row, SRC_KEYS)
        dst = _first_present(row, DST_KEYS)
        if src is None or dst is None:
            continue
        src_s = str(src)
        dst_s = str(dst)
        edge_type = str(row.get("edge_type", row.get("type", "call")))
        services.update([src_s, dst_s])
        edges.append({"src": src_s, "dst": dst_s, "edge_type": edge_type})
        parents[dst_s].add(src_s)
        children[src_s].add(dst_s)
        parents.setdefault(src_s, set())
        children.setdefault(dst_s, set())

    edge_pairs = {(edge["src"], edge["dst"]) for edge in edges}
    if ("frontend", "checkoutservice") in edge_pairs or ("frontend", "cartservice") in edge_pairs:
        graph_direction_assumption = "src_calls_dst; upstream dependency candidates from symptom follow children/downstream edges"
        upstream_direction = "children"
        context_direction = "parents"
    else:
        graph_direction_assumption = "src_influences_dst; upstream candidates from symptom follow parents/incoming edges"
        upstream_direction = "parents"
        context_direction = "children"

    return {
        "services": sorted(services),
        "edges": edges,
        "parents": parents,
        "children": children,
        "graph_direction_assumption": graph_direction_assumption,
        "upstream_direction": upstream_direction,
        "context_direction": context_direction,
    }


def parse_metric_services(metrics_path: str) -> dict[str, Any]:
    rows = load_jsonl(metrics_path)
    service_to_metrics: dict[str, set[str]] = defaultdict(set)
    labels_by_service: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for row in rows:
        service = _first_present(row, SERVICE_KEYS)
        metric = _first_present(row, METRIC_KEYS)
        if service is None or metric is None:
            continue
        service_s = str(service)
        metric_s = str(metric)
        service_to_metrics[service_s].add(metric_s)
        for key in LABEL_KEYS:
            value = row.get(key)
            if value not in (None, ""):
                labels_by_service[service_s][key].add(str(value))

    return {
        "service_to_metrics": _sets_to_sorted(service_to_metrics),
        "labels_by_service": {
            service: {key: sorted(values) for key, values in labels.items()}
            for service, labels in sorted(labels_by_service.items())
        },
        "metrics_count": len(rows),
    }


def load_alert_windows(path: str) -> list[dict[str, Any]]:
    rows = load_jsonl(path)
    allowed = ("alert_window_id", "start_ts", "end_ts", "symptom_service", "trigger_metrics", "severity", "max_z_score")
    return [{key: row.get(key) for key in allowed if key in row} for row in rows]


def _traverse(start: str, adjacency: dict[str, set[str]], max_hops: int) -> dict[str, int]:
    distances: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque([(start, 0)])
    seen = {start}
    while queue:
        current, hop = queue.popleft()
        if hop >= max_hops:
            continue
        for neighbor in sorted(adjacency.get(current, set())):
            if neighbor in seen:
                continue
            seen.add(neighbor)
            next_hop = hop + 1
            distances[neighbor] = next_hop
            queue.append((neighbor, next_hop))
    return distances


def build_candidate_services(
    symptom_service: str,
    graph: dict[str, Any],
    metric_services: dict[str, Any],
    reverse_hops: int = 2,
    forward_hops: int = 1,
    include_symptom: bool = True,
    include_all_if_graph_missing: bool = False,
) -> dict[str, Any]:
    service_to_metrics = metric_services.get("service_to_metrics", metric_services)
    graph_services = set(graph.get("services", []))
    graph_missing = not graph_services or not graph.get("edges")
    included_by: dict[str, set[str]] = defaultdict(set)
    hops: dict[str, int | None] = {}

    if include_symptom and symptom_service:
        included_by[symptom_service].add("symptom")
        hops[symptom_service] = 0

    if graph_missing:
        if include_all_if_graph_missing:
            for service in service_to_metrics:
                included_by[str(service)].add("graph_missing_all_metrics")
                hops.setdefault(str(service), None)
        elif symptom_service in service_to_metrics:
            included_by[symptom_service].add("graph_missing_symptom_only")
        return {
            "candidate_services": sorted(included_by),
            "included_by": {service: sorted(labels) for service, labels in sorted(included_by.items())},
            "hops": {service: hops.get(service) for service in sorted(included_by)},
            "low_confidence": True,
            "graph_direction_assumption": graph.get("graph_direction_assumption", "graph_missing"),
        }

    upstream_key = graph.get("upstream_direction", "parents")
    context_key = graph.get("context_direction", "children")
    upstream_adj = graph.get(upstream_key, {})
    context_adj = graph.get(context_key, {})

    for service, hop in _traverse(symptom_service, upstream_adj, int(reverse_hops)).items():
        included_by[service].add(f"reverse_hop_{hop}")
        hops[service] = min(hops.get(service, hop) or hop, hop)
    for service, hop in _traverse(symptom_service, context_adj, int(forward_hops)).items():
        included_by[service].add(f"forward_hop_{hop}")
        hops[service] = min(hops.get(service, hop) or hop, hop)

    known_metric_services = set(str(service) for service in service_to_metrics)
    candidates = {service for service in included_by if service in known_metric_services or service in graph_services}
    return {
        "candidate_services": sorted(candidates),
        "included_by": {service: sorted(included_by[service]) for service in sorted(candidates)},
        "hops": {service: hops.get(service) for service in sorted(candidates)},
        "low_confidence": False,
        "graph_direction_assumption": graph.get("graph_direction_assumption"),
    }


def build_resource_neighbors(metrics: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    by_label: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in metrics:
        service = _first_present(row, SERVICE_KEYS)
        if service is None:
            continue
        service_s = str(service)
        for key in ("node", "host", "pod", "namespace"):
            value = row.get(key)
            if value not in (None, ""):
                by_label[key][str(value)].add(service_s)

    co_node: dict[str, set[str]] = defaultdict(set)
    co_pod: dict[str, set[str]] = defaultdict(set)
    co_namespace: dict[str, set[str]] = defaultdict(set)
    for label_key, target in [("node", co_node), ("host", co_node), ("pod", co_pod), ("namespace", co_namespace)]:
        for services in by_label.get(label_key, {}).values():
            for service in services:
                target[service].update(other for other in services if other != service)

    available = any(values for values in list(co_node.values()) + list(co_pod.values()) + list(co_namespace.values()))
    return {
        "co_node": _sets_to_sorted(co_node),
        "co_pod": _sets_to_sorted(co_pod),
        "co_namespace": _sets_to_sorted(co_namespace),
        "resource_neighbors_available": bool(available),
    }


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


def build_candidate_metric_nodes(
    candidate_services: list[str],
    service_to_metrics: dict[str, list[str]],
    metric_family_filter: str | None = None,
) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for service in sorted(candidate_services):
        for metric in sorted(service_to_metrics.get(service, [])):
            family = metric_family(metric)
            if metric_family_filter and family != metric_family_filter:
                continue
            nodes.append({
                "service": service,
                "metric": metric,
                "node_id": f"{service}.{metric}",
                "metric_family": family,
                "included": True,
            })
    return nodes


def _load_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _read_metrics(raw_input_dir: Path) -> list[dict[str, Any]]:
    return load_jsonl(str(raw_input_dir / "metrics.jsonl"))


def _candidate_edges(graph: dict[str, Any], candidate_services: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for edge in graph.get("edges", []):
        if edge["src"] in candidate_services and edge["dst"] in candidate_services:
            rows.append({**edge, "included": True})
    return rows


def build_candidate_subgraph_for_window(
    raw_input_dir: str,
    alert_window: dict[str, Any],
    output_dir: str,
    reverse_hops: int = 2,
    forward_hops: int = 1,
) -> dict[str, Any]:
    raw_dir = Path(raw_input_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    metrics_path = raw_dir / "metrics.jsonl"
    graph_path = raw_dir / "service_graph.jsonl"
    if not metrics_path.exists():
        raise FileNotFoundError(f"missing metrics.jsonl: {metrics_path}")
    graph = parse_service_graph(str(graph_path)) if graph_path.exists() else parse_service_graph(str(graph_path))
    metric_services = parse_metric_services(str(metrics_path))
    metrics_rows = _read_metrics(raw_dir)
    metadata = _load_metadata(raw_dir / "metadata.json")
    symptom_service = str(alert_window.get("symptom_service") or "")

    service_result = build_candidate_services(symptom_service, graph, metric_services, reverse_hops, forward_hops)
    candidate_services = list(service_result["candidate_services"])
    included_by = {service: list(labels) for service, labels in service_result["included_by"].items()}
    hops = service_result["hops"]

    resource_neighbors = build_resource_neighbors(metrics_rows, metadata)
    if resource_neighbors.get("resource_neighbors_available"):
        extras: set[str] = set()
        for bucket in ("co_node", "co_pod", "co_namespace"):
            for service in candidate_services:
                for neighbor in resource_neighbors.get(bucket, {}).get(service, []):
                    extras.add(neighbor)
                    included_by.setdefault(neighbor, []).append(f"resource_{bucket}")
                    hops.setdefault(neighbor, None)
        candidate_services = sorted(set(candidate_services) | extras)

    service_to_metrics = metric_services["service_to_metrics"]
    metric_nodes = build_candidate_metric_nodes(candidate_services, service_to_metrics)
    edges = _candidate_edges(graph, set(candidate_services))

    alert_window_id = str(alert_window.get("alert_window_id", "alert-window-unknown"))
    service_rows = [
        {
            "alert_window_id": alert_window_id,
            "service": service,
            "included_by": sorted(set(included_by.get(service, []))),
            "hop": hops.get(service),
            "source": "a4_candidate_subgraph_builder",
        }
        for service in candidate_services
    ]
    metric_rows = [{"alert_window_id": alert_window_id, **node} for node in metric_nodes]
    edge_rows = [{"alert_window_id": alert_window_id, **edge} for edge in edges]
    metadata_payload = {
        "raw_input_dir": str(raw_dir),
        "alert_window_id": alert_window_id,
        "symptom_service": symptom_service,
        "reverse_hops": int(reverse_hops),
        "forward_hops": int(forward_hops),
        "candidate_service_count": len(candidate_services),
        "candidate_metric_node_count": len(metric_nodes),
        "edge_count": len(edges),
        "uses_root_labels": False,
        "uses_target_config": False,
        "uses_injected_path": False,
        "uses_incident_start_end": False,
        "source": "a4_candidate_subgraph_builder",
        "graph_direction_assumption": service_result.get("graph_direction_assumption"),
        "graph_upstream_direction": graph.get("upstream_direction"),
        "resource_neighbors_available": bool(resource_neighbors.get("resource_neighbors_available")),
        "low_confidence": bool(service_result.get("low_confidence", False)),
    }

    _write_jsonl(out / "candidate_services.jsonl", service_rows)
    _write_jsonl(out / "candidate_metric_nodes.jsonl", metric_rows)
    _write_jsonl(out / "candidate_edges.jsonl", edge_rows)
    _write_json(out / "candidate_subgraph_metadata.json", metadata_payload)
    return {"metadata": metadata_payload, "candidate_services": service_rows, "candidate_metric_nodes": metric_rows, "candidate_edges": edge_rows}


def build_candidate_subgraphs_for_repeat(
    raw_input_dir: str,
    alert_output_dir: str,
    candidate_output_dir: str,
    reverse_hops: int = 2,
    forward_hops: int = 1,
) -> dict[str, Any]:
    raw_dir = Path(raw_input_dir)
    alert_dir = Path(alert_output_dir)
    out = Path(candidate_output_dir)
    out.mkdir(parents=True, exist_ok=True)
    windows = load_alert_windows(str(alert_dir / "alert_windows.jsonl"))
    window_summaries: list[dict[str, Any]] = []
    all_services: set[str] = set()
    all_metric_nodes: set[str] = set()

    for index, window in enumerate(windows, start=1):
        window_out = out / f"window_{index:02d}"
        result = build_candidate_subgraph_for_window(str(raw_dir), window, str(window_out), reverse_hops, forward_hops)
        metadata = result["metadata"]
        services = [row["service"] for row in result["candidate_services"]]
        nodes = [row["node_id"] for row in result["candidate_metric_nodes"]]
        all_services.update(services)
        all_metric_nodes.update(nodes)
        window_summaries.append({
            "alert_window_id": metadata["alert_window_id"],
            "window_output_dir": str(window_out),
            "candidate_service_count": metadata["candidate_service_count"],
            "candidate_metric_node_count": metadata["candidate_metric_node_count"],
            "edge_count": metadata["edge_count"],
            "symptom_service": metadata["symptom_service"],
            "candidate_services": sorted(services),
        })

    summary = {
        "raw_input_dir": str(raw_dir),
        "alert_output_dir": str(alert_dir),
        "candidate_output_dir": str(out),
        "alert_windows_count": len(windows),
        "window_summaries": window_summaries,
        "candidate_service_count": len(all_services),
        "candidate_metric_node_count": len(all_metric_nodes),
        "candidate_services_union": sorted(all_services),
        "uses_root_labels_for_building": False,
        "uses_target_config_for_building": False,
        "uses_injected_path_for_building": False,
        "uses_incident_start_end_for_building": False,
        "runs_rca_pipeline": False,
        "reinjects_faults": False,
    }
    _write_json(out / "repeat_candidate_summary.json", summary)
    return summary


def evaluate_candidate_subgraph_for_debug(candidate_summary_path: str, incidents_path: str) -> dict[str, Any]:
    summary = json.loads(Path(candidate_summary_path).read_text(encoding="utf-8"))
    candidate_services = set(summary.get("candidate_services_union", []))
    candidate_nodes: set[str] = set()
    for window in summary.get("window_summaries", []):
        nodes_path = Path(window["window_output_dir"]) / "candidate_metric_nodes.jsonl"
        for row in load_jsonl(str(nodes_path)):
            node_id = row.get("node_id")
            if node_id:
                candidate_nodes.add(str(node_id))

    incidents = load_jsonl(incidents_path)
    service_hits: list[bool] = []
    metric_hits: list[bool] = []
    details: list[dict[str, Any]] = []
    for incident in incidents:
        root_service = incident.get("root_service")
        root_metric = incident.get("root_metric")
        root_node = f"{root_service}.{root_metric}" if root_service and root_metric else None
        service_hit = bool(root_service in candidate_services) if root_service else False
        metric_hit = bool(root_node in candidate_nodes) if root_node else False
        service_hits.append(service_hit)
        metric_hits.append(metric_hit)
        details.append({
            "incident_id": incident.get("incident_id"),
            "root_service_in_candidate_debug": service_hit,
            "root_metric_in_candidate_metric_nodes_debug": metric_hit,
        })
    return {
        "debug_only": True,
        "ground_truth_incidents": len(incidents),
        "root_service_in_candidate_debug": any(service_hits) if service_hits else False,
        "root_metric_in_candidate_metric_nodes_debug": any(metric_hits) if metric_hits else False,
        "root_service_hit_rate_debug": float(np.mean(np.asarray(service_hits, dtype=float))) if service_hits else 0.0,
        "root_metric_hit_rate_debug": float(np.mean(np.asarray(metric_hits, dtype=float))) if metric_hits else 0.0,
        "details": details,
    }

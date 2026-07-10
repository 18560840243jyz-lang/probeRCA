"""Service/metric ownership helpers for Online Boutique integrated RCA.

These helpers are label-free. They only use observable service, metric, and
node_id fields carried by pipeline artifacts. They never read root labels,
target config, injected paths, or incident time windows.
"""

from __future__ import annotations

from typing import Any


def canonical_service_name(value: str) -> str:
    service = str(value or "").strip()
    return service or "unknown"


def canonical_metric_name(value: str) -> str:
    metric = str(value or "").strip()
    if not metric:
        return "unknown"
    known_prefixes = ("cpu.", "net.", "io.", "lock.", "memory.", "request.")
    if metric.startswith(known_prefixes):
        return metric
    if "." in metric:
        _, rest = metric.split(".", 1)
        if rest.startswith(known_prefixes):
            return rest
    return metric


def make_node_id(service: str, metric: str) -> str:
    service_name = canonical_service_name(service)
    metric_name = canonical_metric_name(metric)
    return f"{service_name}.{metric_name}"


def split_node_id(node_id: str) -> tuple[str, str]:
    node = str(node_id or "").strip()
    if not node:
        return "unknown", "unknown"
    if "." not in node:
        return "unknown", node
    service, metric = node.split(".", 1)
    return canonical_service_name(service), canonical_metric_name(metric)


def metric_family(metric: str) -> str:
    name = canonical_metric_name(metric)
    if name.startswith("cpu."):
        return "CPU"
    if name.startswith("net."):
        return "network"
    if name.startswith("io."):
        return "storage I/O"
    if name.startswith("lock."):
        return "lock contention"
    if name.startswith("memory."):
        return "memory"
    if name.startswith("request."):
        return "load"
    return "unknown"


def validate_node_ownership(row: dict[str, Any]) -> dict[str, Any]:
    raw_node = str(row.get("node_id") or row.get("node") or "").strip()
    raw_service = str(row.get("service") or "").strip()
    raw_metric = str(row.get("metric") or "").strip()

    if raw_node:
        node_service, node_metric = split_node_id(raw_node)
        node_id = make_node_id(node_service, node_metric) if node_service != "unknown" else raw_node
    elif raw_service and raw_metric:
        node_service = canonical_service_name(raw_service)
        node_metric = canonical_metric_name(raw_metric)
        node_id = make_node_id(node_service, node_metric)
    else:
        node_service = "unknown"
        node_metric = canonical_metric_name(raw_metric) if raw_metric else "unknown"
        node_id = raw_node or (make_node_id(raw_service, raw_metric) if raw_service and raw_metric else "unknown.unknown")

    service = canonical_service_name(raw_service) if raw_service else node_service
    metric = canonical_metric_name(raw_metric) if raw_metric else node_metric
    service_matches = service == node_service
    metric_matches = metric == node_metric
    valid = bool(node_id) and node_service != "unknown" and node_metric != "unknown" and service_matches and metric_matches
    issue = ""
    if node_service == "unknown" or node_metric == "unknown":
        issue = "missing_or_unparseable_node_id"
    elif not service_matches and not metric_matches:
        issue = "service_and_metric_mismatch_node_id"
    elif not service_matches:
        issue = "service_mismatch_node_id"
    elif not metric_matches:
        issue = "metric_mismatch_node_id"
    return {
        "node_id": node_id,
        "service": service,
        "metric": metric,
        "node_id_service": node_service,
        "node_id_metric": node_metric,
        "service_matches_node_id": service_matches,
        "metric_matches_node_id": metric_matches,
        "ownership_valid": valid,
        "ownership_issue": issue,
    }


def assert_or_repair_node_ownership(row: dict[str, Any], prefer_node_id: bool = True) -> dict[str, Any]:
    repaired = dict(row)
    before = validate_node_ownership(repaired)
    raw_node = str(repaired.get("node_id") or repaired.get("node") or "").strip()
    if prefer_node_id and raw_node:
        service, metric = split_node_id(raw_node)
        if service != "unknown" and metric != "unknown":
            repaired["service"] = service
            repaired["metric"] = metric
            repaired["node_id"] = make_node_id(service, metric)
            repaired.setdefault("node", repaired["node_id"])
            repaired["ownership_repaired"] = not before["ownership_valid"]
            repaired["ownership_repair_source"] = "node_id"
    elif repaired.get("service") and repaired.get("metric"):
        service = canonical_service_name(str(repaired.get("service")))
        metric = canonical_metric_name(str(repaired.get("metric")))
        repaired["service"] = service
        repaired["metric"] = metric
        repaired["node_id"] = make_node_id(service, metric)
        repaired.setdefault("node", repaired["node_id"])
        repaired["ownership_repaired"] = not before["ownership_valid"]
        repaired["ownership_repair_source"] = "service_metric"
    after = validate_node_ownership(repaired)
    repaired.update(after)
    repaired["metric_family"] = repaired.get("metric_family") or metric_family(str(repaired.get("metric")))
    return repaired

"""Evaluation metrics for P1 single-seed RCA results."""

from __future__ import annotations

import numpy as np

from proberca.eval.metrics import hit_at_k, reciprocal_rank
from proberca.eval.p0_result import canonical_type


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _rank_of(items: list[str], target: str) -> int | None:
    try:
        return items.index(target) + 1
    except ValueError:
        return None


def _path_intersects(path_services: list[str], injected_path: list[str]) -> bool:
    path_set = {str(item).split(".", 1)[0] for item in path_services if item}
    injected_set = {str(item).split(".", 1)[0] for item in injected_path if item}
    return bool(path_set & injected_set)


def evaluate_p1_results(results: list[dict], incidents: list[dict], path_summary: dict | None = None) -> dict:
    """Evaluate final P1 RCAResult records against synthetic labels."""

    results_by_incident = {str(row["incident_id"]): row for row in results}
    per_incident: list[dict] = []
    for incident in incidents:
        incident_id = str(incident["incident_id"])
        result = results_by_incident.get(incident_id)
        if result is None:
            raise ValueError(f"missing P1 RCAResult for incident_id={incident_id}")

        service_items = [str(item["service"]) for item in result.get("top_services", [])]
        metric_items = [str(item.get("node", f"{item['service']}.{item['metric']}")) for item in result.get("top_metrics", [])]
        true_service = str(incident["root_service"])
        true_metric = f"{incident['root_service']}.{incident['root_metric']}"
        predicted_top1_service = service_items[0] if service_items else ""
        predicted_top1_metric = metric_items[0] if metric_items else ""
        predicted_root_type = str(result.get("root_type", "unknown"))
        true_root_type = str(incident.get("root_type", "unknown"))
        path_services = [str(item) for item in result.get("path", {}).get("path_services", [])]
        path_hit = _path_intersects(path_services, [str(item) for item in incident.get("injected_path", [])])

        per_incident.append(
            {
                "incident_id": incident_id,
                "true_root_service_debug": true_service,
                "predicted_top1_service": predicted_top1_service,
                "service_rank_debug": _rank_of(service_items, true_service),
                "true_root_metric_debug": true_metric,
                "predicted_top1_metric": predicted_top1_metric,
                "metric_rank_debug": _rank_of(metric_items, true_metric),
                "true_root_type_debug": true_root_type,
                "predicted_root_type": predicted_root_type,
                "path_intersects_injected_path_debug": path_hit,
            }
        )

    service_ranked = [[str(item["service"]) for item in results_by_incident[str(row["incident_id"])].get("top_services", [])] for row in incidents]
    metric_ranked = [[str(item.get("node", f"{item['service']}.{item['metric']}")) for item in results_by_incident[str(row["incident_id"])].get("top_metrics", [])] for row in incidents]
    service_targets = [str(row["root_service"]) for row in incidents]
    metric_targets = [f"{row['root_service']}.{row['root_metric']}" for row in incidents]
    type_hits = [1.0 if canonical_type(item["predicted_root_type"]) == canonical_type(item["true_root_type_debug"]) else 0.0 for item in per_incident]
    observed_ratio = _mean([float(row.get("observation", {}).get("observed_ratio", 0.0)) for row in results])

    return {
        "incidents_count": len(incidents),
        "service_hit_at_1": _mean([hit_at_k(items, target, 1) for items, target in zip(service_ranked, service_targets)]),
        "service_hit_at_3": _mean([hit_at_k(items, target, 3) for items, target in zip(service_ranked, service_targets)]),
        "service_hit_at_5": _mean([hit_at_k(items, target, 5) for items, target in zip(service_ranked, service_targets)]),
        "service_mrr": _mean([reciprocal_rank(items, target) for items, target in zip(service_ranked, service_targets)]),
        "metric_hit_at_1": _mean([hit_at_k(items, target, 1) for items, target in zip(metric_ranked, metric_targets)]),
        "metric_hit_at_3": _mean([hit_at_k(items, target, 3) for items, target in zip(metric_ranked, metric_targets)]),
        "metric_hit_at_5": _mean([hit_at_k(items, target, 5) for items, target in zip(metric_ranked, metric_targets)]),
        "metric_mrr": _mean([reciprocal_rank(items, target) for items, target in zip(metric_ranked, metric_targets)]),
        "root_type_accuracy": _mean(type_hits),
        "path_fidelity": _mean([1.0 if item["path_intersects_injected_path_debug"] else 0.0 for item in per_incident]),
        "observed_ratio": observed_ratio,
        "per_incident": per_incident,
    }

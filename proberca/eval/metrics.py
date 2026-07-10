"""Evaluation metrics for probeRCA P0 experiments."""

from __future__ import annotations

import numpy as np

from proberca.eval.p0_result import canonical_type


def hit_at_k(ranked_items, target, k) -> float:
    """Return 1.0 if target appears in the first k ranked items."""

    return 1.0 if target in list(ranked_items)[:k] else 0.0


def reciprocal_rank(ranked_items, target) -> float:
    """Return reciprocal rank of target, or 0.0 if target is absent."""

    for index, item in enumerate(ranked_items, start=1):
        if item == target:
            return 1.0 / float(index)
    return 0.0


def _rank_of(ranked_items: list[str], target: str) -> int | None:
    try:
        return ranked_items.index(target) + 1
    except ValueError:
        return None


def _path_intersects_injected_path(path: list[str], injected_path: list[str]) -> bool:
    path_services = {str(item).split(".", 1)[0] for item in path if item}
    injected_services = {str(item).split(".", 1)[0] for item in injected_path if item}
    return bool(path_services & injected_services)


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def evaluate_results(results: list[dict], incidents: list[dict]) -> dict:
    """Evaluate final P0 RCAResult records against synthetic incident labels."""

    results_by_incident = {str(row["incident_id"]): row for row in results}
    per_incident: list[dict] = []
    for incident in incidents:
        incident_id = str(incident["incident_id"])
        result = results_by_incident.get(incident_id)
        if result is None:
            raise ValueError(f"missing RCAResult for incident_id={incident_id}")

        service_items = [str(item["service"]) for item in result.get("top_services", [])]
        metric_items = [f"{item['service']}.{item['metric']}" for item in result.get("top_metrics", [])]
        target_service = str(incident["root_service"])
        target_metric = f"{incident['root_service']}.{incident['root_metric']}"
        predicted_root_type = str(result.get("root_type", "unknown"))
        true_root_type = str(incident.get("root_type", "unknown"))
        root_type_correct = canonical_type(predicted_root_type) == canonical_type(true_root_type)
        path_hit = _path_intersects_injected_path([str(item) for item in result.get("path", [])], [str(item) for item in incident.get("injected_path", [])])

        per_incident.append(
            {
                "incident_id": incident_id,
                "root_service": target_service,
                "predicted_service_rank": _rank_of(service_items, target_service),
                "service_hit_at_1": hit_at_k(service_items, target_service, 1),
                "service_hit_at_3": hit_at_k(service_items, target_service, 3),
                "root_metric": target_metric,
                "predicted_metric_rank": _rank_of(metric_items, target_metric),
                "metric_hit_at_1": hit_at_k(metric_items, target_metric, 1),
                "metric_hit_at_3": hit_at_k(metric_items, target_metric, 3),
                "root_type": true_root_type,
                "predicted_root_type": predicted_root_type,
                "root_type_correct": root_type_correct,
                "path_intersects_injected_path": path_hit,
            }
        )

    service_ranked = [[str(item["service"]) for item in results_by_incident[row["incident_id"]].get("top_services", [])] for row in incidents]
    metric_ranked = [[f"{item['service']}.{item['metric']}" for item in results_by_incident[row["incident_id"]].get("top_metrics", [])] for row in incidents]
    service_targets = [str(row["root_service"]) for row in incidents]
    metric_targets = [f"{row['root_service']}.{row['root_metric']}" for row in incidents]

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
        "root_type_accuracy": _mean([1.0 if item["root_type_correct"] else 0.0 for item in per_incident]),
        "path_fidelity": _mean([1.0 if item["path_intersects_injected_path"] else 0.0 for item in per_incident]),
        "per_incident": per_incident,
    }

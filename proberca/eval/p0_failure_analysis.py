"""Failure analysis for P0 multi-seed audit results."""

from __future__ import annotations

import json
from pathlib import Path
from collections import Counter

from proberca.data.io import read_jsonl, write_jsonl
from proberca.evidence.semantic import metric_to_evidence_type


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _seed_from_dir(path: Path) -> int | str:
    name = path.name
    if name.startswith("seed_"):
        raw = name.split("_", 1)[1]
        try:
            return int(raw)
        except ValueError:
            return raw
    return name


def _top_metric_node(result: dict) -> str | None:
    metrics = result.get("top_metrics", [])
    if not metrics:
        return None
    top = metrics[0]
    return f"{top['service']}.{top['metric']}"


def _top_service(result: dict) -> str | None:
    services = result.get("top_services", [])
    return services[0].get("service") if services else None


def _rank_metric(result: dict, target: str) -> int | None:
    for idx, item in enumerate(result.get("top_metrics", []), start=1):
        if f"{item['service']}.{item['metric']}" == target:
            return idx
    return None


def _failure_patterns(incident: dict, result: dict) -> list[str]:
    root_service = str(incident["root_service"])
    root_metric = str(incident["root_metric"])
    predicted_service = _top_service(result)
    predicted_metric_node = _top_metric_node(result)
    patterns: list[str] = []
    if predicted_service != root_service:
        patterns.append("service_wrong")
    elif predicted_metric_node != f"{root_service}.{root_metric}":
        patterns.append("same_service_wrong_metric")
    if predicted_metric_node:
        pred_service, pred_metric = predicted_metric_node.split(".", 1)
        if pred_metric.startswith("request.") and pred_service != root_service:
            patterns.append("downstream_symptom_metric")
        if pred_service == root_service and metric_to_evidence_type(pred_metric) == metric_to_evidence_type(root_metric) and pred_metric != root_metric:
            patterns.append("same_type_evidence_sibling")
    return patterns or ["unknown"]


def analyze_p0_failures(audit_dir: str) -> dict:
    """Analyze seed and incident failures from a P0 audit directory."""

    audit_path = Path(audit_dir)
    multi_seed = audit_path / "multi_seed"
    if not multi_seed.exists():
        raise FileNotFoundError(f"missing multi_seed audit directory: {multi_seed}")

    failed_seeds: list[int | str] = []
    failed_incidents: list[str] = []
    per_seed_metric_hit_at_1: dict[str, float] = {}
    per_incident_failures: list[dict] = []
    pattern_counter: Counter[str] = Counter()

    for seed_dir in sorted(path for path in multi_seed.iterdir() if path.is_dir()):
        seed = _seed_from_dir(seed_dir)
        eval_path = seed_dir / "p0_evaluation_summary.json"
        results_path = seed_dir / "p0_results.jsonl"
        incidents_path = seed_dir / "incidents.jsonl"
        if not eval_path.exists() or not results_path.exists() or not incidents_path.exists():
            continue
        evaluation = _read_json(eval_path)
        per_seed_metric_hit_at_1[str(seed)] = float(evaluation.get("metric_hit_at_1", 0.0))
        if float(evaluation.get("metric_hit_at_1", 0.0)) < 1.0:
            failed_seeds.append(seed)
        results = {row["incident_id"]: row for row in read_jsonl(results_path)}
        incidents = {row["incident_id"]: row for row in read_jsonl(incidents_path)}
        semantic_records = read_jsonl(seed_dir / "semantic_interventions.jsonl") if (seed_dir / "semantic_interventions.jsonl").exists() else []
        sparse_records = read_jsonl(seed_dir / "sparse_interventions.jsonl") if (seed_dir / "sparse_interventions.jsonl").exists() else []

        for item in evaluation.get("per_incident", []):
            if float(item.get("metric_hit_at_1", 0.0)) >= 1.0:
                continue
            incident_id = str(item["incident_id"])
            incident = incidents[incident_id]
            result = results[incident_id]
            top5 = [f"{row['service']}.{row['metric']}" for row in result.get("top_metrics", [])[:5]]
            patterns = _failure_patterns(incident, result)
            pattern_counter.update(patterns)
            failed_incidents.append(f"seed_{seed}:{incident_id}")
            semantic_top5 = [
                {"node": row.get("node"), "semantic_rank": row.get("semantic_rank"), "semantic_score": row.get("semantic_score"), "evidence_type": row.get("evidence_type")}
                for row in sorted([r for r in semantic_records if r.get("incident_id") == incident_id], key=lambda r: int(r.get("semantic_rank", 999999)))[:5]
            ]
            sparse_top5 = [
                {"node": row.get("node"), "rank": row.get("rank"), "intervention_score": row.get("intervention_score")}
                for row in sorted([r for r in sparse_records if r.get("incident_id") == incident_id], key=lambda r: int(r.get("rank", 999999)))[:5]
            ]
            per_incident_failures.append(
                {
                    "seed": seed,
                    "incident_id": incident_id,
                    "root_metric": f"{incident['root_service']}.{incident['root_metric']}",
                    "predicted_top1_metric": _top_metric_node(result),
                    "root_service": incident["root_service"],
                    "predicted_top1_service": _top_service(result),
                    "root_type": incident["root_type"],
                    "predicted_root_type": result.get("root_type"),
                    "root_metric_rank": _rank_metric(result, f"{incident['root_service']}.{incident['root_metric']}"),
                    "top5_metrics": top5,
                    "failure_patterns": patterns,
                    "semantic_top5": semantic_top5,
                    "sparse_top5": sparse_top5,
                }
            )

    analysis = {
        "audit_dir": str(audit_path),
        "failed_seeds": failed_seeds,
        "failed_incidents": failed_incidents,
        "per_seed_metric_hit_at_1": per_seed_metric_hit_at_1,
        "per_incident_failures": per_incident_failures,
        "failure_patterns": dict(sorted(pattern_counter.items())),
    }
    out_path = audit_path / "p0_failure_analysis.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    return analysis

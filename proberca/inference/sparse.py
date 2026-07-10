"""Sparse inversion solver for probeRCA P0 Step 5."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl


@dataclass
class SparseInversionConfig:
    """Configuration for residual-lift based sparse intervention scoring."""

    l1_lambda: float = 0.5
    group_lambda: float = 0.1
    graph_lambda: float = 0.0
    eps: float = 1e-6
    clip_score: float | None = 100.0
    aggregation: str = "mean_abs_residual_lift"
    top_k_debug: int = 5


@dataclass
class SparseInterventionRecord:
    """Sparse intervention candidate score for one service.metric node."""

    incident_id: str
    service: str
    metric: str
    node: str
    baseline_abs_residual: float
    faulty_abs_residual: float
    residual_lift: float
    intervention_score: float
    signed_intervention: float
    rank: int
    source: str = "sparse_inversion"


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict]]:
    """Load stable residuals and incident labels required by sparse inversion."""

    input_path = Path(input_dir)
    residuals_path = input_path / "stable_residuals.jsonl"
    incidents_path = input_path / "incidents.jsonl"
    if not residuals_path.exists():
        raise FileNotFoundError(f"missing required stable residuals file: {residuals_path}")
    if not incidents_path.exists():
        raise FileNotFoundError(f"missing required incidents file: {incidents_path}")
    return read_jsonl(residuals_path), read_jsonl(incidents_path)


def soft_threshold(x, lam):
    """Apply L1 soft thresholding to a float or numpy array."""

    array = np.asarray(x)
    result = np.sign(array) * np.maximum(np.abs(array) - lam, 0.0)
    if np.isscalar(x):
        return float(result)
    return result


def group_shrink(scores_by_metric: dict[str, float], group_lambda: float, eps: float) -> dict[str, float]:
    """Apply group-lasso style shrinkage inside one service group."""

    norm = float(np.sqrt(sum(float(score) ** 2 for score in scores_by_metric.values())))
    factor = max(0.0, 1.0 - group_lambda / (norm + eps))
    return {metric: float(score) * factor for metric, score in scores_by_metric.items()}

INTERVENTION_METRIC_WEIGHTS = {
    "cpu.throttled_usec": 4.0,
    "cpu.pressure": 1.5,
    "net.retrans": 2.0,
    "net.rtt_ms": 1.4,
    "io.bio_latency_ms": 2.0,
    "io.queue_depth": 1.4,
    "lock.futex_wait_ms": 1.6,
}


def _intervention_metric_weight(metric: str) -> float:
    if metric in INTERVENTION_METRIC_WEIGHTS:
        return INTERVENTION_METRIC_WEIGHTS[metric]
    if metric.startswith("request."):
        return 0.25
    return 1.0


def _node(service: str, metric: str) -> str:
    return f"{service}.{metric}"


def _incident_residuals(residuals: list[dict], incident: dict) -> list[dict]:
    incident_id = str(incident["incident_id"])
    return [row for row in residuals if row.get("incident_id") == incident_id]


def _aggregate_node_stats(rows: list[dict], incident: dict) -> tuple[dict[str, list[float]], dict[str, list[float]], dict[str, list[float]]]:
    start_ts = float(incident["start_ts"])
    end_ts = float(incident["end_ts"])
    baseline_abs: dict[str, list[float]] = {}
    faulty_abs: dict[str, list[float]] = {}
    faulty_signed: dict[str, list[float]] = {}

    for row in rows:
        timestamp = float(row["timestamp"])
        node = _node(str(row["service"]), str(row["metric"]))
        residual = float(row["residual"])
        if timestamp < start_ts:
            baseline_abs.setdefault(node, []).append(abs(residual))
        elif start_ts <= timestamp <= end_ts:
            faulty_abs.setdefault(node, []).append(abs(residual))
            faulty_signed.setdefault(node, []).append(residual)
    return baseline_abs, faulty_abs, faulty_signed


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(np.mean(np.asarray(values, dtype=float)))


def _rank_records(records: list[SparseInterventionRecord]) -> list[SparseInterventionRecord]:
    ordered = sorted(records, key=lambda item: (-item.intervention_score, item.node))
    return [SparseInterventionRecord(**{**asdict(record), "rank": index}) for index, record in enumerate(ordered, start=1)]


def solve_sparse_inversion_for_incident(
    residuals: list[dict],
    incident: dict,
    config: SparseInversionConfig,
) -> tuple[list[dict], dict]:
    """Solve sparse intervention candidates for one incident from stable residuals."""

    incident_id = str(incident["incident_id"])
    rows = _incident_residuals(residuals, incident)
    if not rows:
        raise ValueError(f"no residual rows found for incident_id={incident_id}")

    baseline_abs, faulty_abs, faulty_signed = _aggregate_node_stats(rows, incident)
    nodes = sorted({_node(str(row["service"]), str(row["metric"])) for row in rows})
    raw_by_service: dict[str, dict[str, float]] = {}
    intermediate: dict[str, dict[str, float | str]] = {}
    baseline_rows = 0
    faulty_rows = 0

    for node in nodes:
        service, metric = node.split(".", 1)
        baseline_values = baseline_abs.get(node, [])
        faulty_values = faulty_abs.get(node, [])
        baseline_rows += len(baseline_values)
        faulty_rows += len(faulty_values)
        baseline_mean = _mean(baseline_values)
        faulty_mean = _mean(faulty_values)
        residual_lift = max(0.0, faulty_mean - baseline_mean)
        signed_intervention = _mean(faulty_signed.get(node, []))
        score = max(float(soft_threshold(residual_lift, config.l1_lambda)), 0.0)
        score *= _intervention_metric_weight(metric)
        raw_by_service.setdefault(service, {})[metric] = score
        intermediate[node] = {
            "service": service,
            "metric": metric,
            "baseline_abs_residual": baseline_mean,
            "faulty_abs_residual": faulty_mean,
            "residual_lift": residual_lift,
            "signed_intervention": signed_intervention,
        }

    shrunk_scores: dict[str, float] = {}
    for service, metric_scores in raw_by_service.items():
        for metric, score in group_shrink(metric_scores, config.group_lambda, config.eps).items():
            if config.clip_score is not None:
                score = float(np.clip(score, 0.0, config.clip_score))
            shrunk_scores[_node(service, metric)] = float(score)

    records = []
    for node in nodes:
        fields = intermediate[node]
        records.append(
            SparseInterventionRecord(
                incident_id=incident_id,
                service=str(fields["service"]),
                metric=str(fields["metric"]),
                node=node,
                baseline_abs_residual=float(fields["baseline_abs_residual"]),
                faulty_abs_residual=float(fields["faulty_abs_residual"]),
                residual_lift=float(fields["residual_lift"]),
                intervention_score=shrunk_scores[node],
                signed_intervention=float(fields["signed_intervention"]),
                rank=0,
            )
        )

    ranked = _rank_records(records)
    ranked_dicts = [asdict(record) for record in ranked]
    root_node = _node(str(incident["root_service"]), str(incident["root_metric"]))
    root_record = next((record for record in ranked_dicts if record["node"] == root_node), None)
    top_debug = [
        {
            "rank": record["rank"],
            "node": record["node"],
            "intervention_score": record["intervention_score"],
            "signed_intervention": record["signed_intervention"],
        }
        for record in ranked_dicts[: config.top_k_debug]
    ]
    summary = {
        "incident_id": incident_id,
        "node_count": len(nodes),
        "baseline_rows": baseline_rows,
        "faulty_rows": faulty_rows,
        "candidates_count": len(ranked_dicts),
        "nonzero_candidates_count": sum(1 for record in ranked_dicts if record["intervention_score"] > 0.0),
        "top_debug_candidates": top_debug,
        "true_root_rank": root_record["rank"] if root_record else None,
        "true_root_score": root_record["intervention_score"] if root_record else None,
    }
    return ranked_dicts, summary


def solve_sparse_inversion(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: SparseInversionConfig | None = None,
) -> dict:
    """Solve sparse intervention candidates for all incidents and write outputs."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    config = config or SparseInversionConfig()
    residuals, incidents = load_required_dataset(input_path)

    all_records: list[dict] = []
    summaries: list[dict] = []
    for incident in incidents:
        records, summary = solve_sparse_inversion_for_incident(residuals, incident, config)
        all_records.extend(records)
        summaries.append(summary)

    expected_candidates_count = sum(summary["node_count"] for summary in summaries)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "candidates_count": len(all_records),
        "expected_candidates_count": int(expected_candidates_count),
        "candidates_count_matches_expected": len(all_records) == expected_candidates_count,
        "nonzero_candidates_count": sum(1 for record in all_records if record["intervention_score"] > 0.0),
        "l1_lambda": float(config.l1_lambda),
        "group_lambda": float(config.group_lambda),
        "graph_lambda": float(config.graph_lambda),
    }

    output_path.mkdir(parents=True, exist_ok=True)
    interventions_path = output_path / "sparse_interventions.jsonl"
    summary_path = output_path / "sparse_inversion_summary.json"
    metadata_path = output_path / "sparse_inversion_metadata.json"
    write_jsonl(interventions_path, all_records)
    summary_path.write_text(json.dumps({"config": asdict(config), "summaries": summaries}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "sparse_interventions_path": str(interventions_path),
        "sparse_inversion_summary_path": str(summary_path),
        "sparse_inversion_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summaries": summaries,
    }

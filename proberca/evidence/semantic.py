"""Semantic evidence scoring for probeRCA P0 Step 6."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl


@dataclass
class SemanticEvidenceConfig:
    """Configuration for semantic evidence fusion over sparse candidates."""

    evidence_weight: float = 3.0
    exact_metric_bonus: float = 2.0
    same_type_bonus: float = 1.0
    service_level_bonus: float = 0.5
    min_evidence_score: float = 0.0
    clip_semantic_score: float | None = 500.0
    top_k_debug: int = 5
    use_metric_specificity: bool = True
    evidence_anchor_weight: float = 1.0


@dataclass
class SemanticInterventionRecord:
    """Sparse intervention candidate after semantic evidence score fusion."""

    incident_id: str
    service: str
    metric: str
    node: str
    sparse_rank: int
    sparse_score: float
    evidence_type: str
    evidence_score: float
    evidence_metrics: list[str]
    semantic_score: float
    semantic_rank: int
    source: str = "semantic_evidence"


@dataclass
class SemanticTypeScoreRecord:
    """Candidate root-cause type score from semantic interventions."""

    incident_id: str
    root_type_candidate: str
    type_score: float
    rank: int
    supporting_services: list[str]
    supporting_metrics: list[str]
    source: str = "semantic_evidence"


def metric_to_evidence_type(metric: str) -> str:
    """Map a metric name to a semantic evidence type."""

    if metric.startswith("cpu."):
        return "CPU"
    if metric.startswith("net."):
        return "Net"
    if metric.startswith("io."):
        return "IO"
    if metric.startswith("lock."):
        return "Lock"
    if metric.startswith("memory."):
        return "Mem"
    if metric in {"request.in_flight", "request.rps", "request.error_rate", "request.p99_latency_ms"}:
        return "Load"
    return "Unknown"


def canonical_root_type(evidence_type: str) -> str:
    """Map semantic evidence type to a candidate root type label."""

    return {
        "CPU": "CPU",
        "Net": "network",
        "IO": "storage I/O",
        "Lock": "lock contention",
        "Mem": "memory",
        "Load": "load",
        "Unknown": "unknown",
    }.get(evidence_type, "unknown")


def metric_specificity_weight(metric: str) -> float:
    """Return a label-free diagnostic specificity prior for a metric name."""

    weights = {
        "cpu.throttled_usec": 2.20,
        "net.retrans": 2.20,
        "io.bio_latency_ms": 2.20,
        "lock.futex_wait_ms": 2.20,
        "cpu.pressure": 0.95,
        "memory.pressure": 1.05,
        "net.rtt_ms": 0.95,
        "io.queue_depth": 0.95,
        "request.p99_latency_ms": 0.60,
        "request.p95_latency_ms": 0.65,
        "request.p50_latency_ms": 0.70,
        "request.in_flight": 0.75,
        "request.error_rate": 0.75,
        "request.rps": 0.75,
        "cpu.usage": 0.90,
        "memory.usage": 0.90,
    }
    return weights.get(metric, 1.0)


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], list[dict], list[dict]]:
    """Load Step 6 inputs and verify the Step 5 summary file is present."""

    input_path = Path(input_dir)
    required = {
        "sparse_interventions": input_path / "sparse_interventions.jsonl",
        "evidence": input_path / "evidence.jsonl",
        "incidents": input_path / "incidents.jsonl",
        "sparse_inversion_summary": input_path / "sparse_inversion_summary.json",
    }
    for name, required_path in required.items():
        if not required_path.exists():
            raise FileNotFoundError(f"missing required {name} file: {required_path}")
    with required["sparse_inversion_summary"].open("r", encoding="utf-8") as fh:
        json.load(fh)
    return read_jsonl(required["sparse_interventions"]), read_jsonl(required["evidence"]), read_jsonl(required["incidents"])


def _abs_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(np.mean(np.abs(np.asarray(values, dtype=float))))


def compute_evidence_strength_for_incident(evidence_records: list[dict], incident: dict) -> dict[str, Any]:
    """Compute exact and type-level evidence strength for one incident."""

    incident_id = str(incident["incident_id"])
    records = [row for row in evidence_records if row.get("incident_id") == incident_id]
    exact_values: dict[tuple[str, str], list[float]] = {}
    type_values: dict[tuple[str, str], list[float]] = {}
    support_index: dict[tuple[str, str], set[str]] = {}

    for row in records:
        service = str(row["service"])
        metric = str(row["metric"])
        evidence_type = str(row.get("evidence_type") or metric_to_evidence_type(metric))
        value = float(row["value"])
        exact_values.setdefault((service, metric), []).append(value)
        type_values.setdefault((service, evidence_type), []).append(value)
        support_index.setdefault((service, evidence_type), set()).add(metric)
        support_index.setdefault((service, metric), set()).add(metric)

    exact_index = {key: _abs_mean(values) for key, values in exact_values.items()}
    type_index = {key: _abs_mean(values) for key, values in type_values.items()}
    support_lists = {key: sorted(values) for key, values in support_index.items()}
    return {"exact_index": exact_index, "type_index": type_index, "support_index": support_lists}


def _service_strength(service: str, type_index: dict[tuple[str, str], float]) -> float:
    values = [strength for (candidate_service, _evidence_type), strength in type_index.items() if candidate_service == service]
    return max(values) if values else 0.0


def _support_metrics(service: str, metric: str, evidence_type: str, support_index: dict[tuple[str, str], list[str]]) -> list[str]:
    values = set(support_index.get((service, metric), []))
    values.update(support_index.get((service, evidence_type), []))
    return sorted(values)


def _rank_semantic(records: list[SemanticInterventionRecord]) -> list[SemanticInterventionRecord]:
    ordered = sorted(records, key=lambda item: (-item.semantic_score, item.node))
    return [SemanticInterventionRecord(**{**asdict(record), "semantic_rank": index}) for index, record in enumerate(ordered, start=1)]


def score_semantic_interventions_for_incident(
    sparse_records: list[dict],
    evidence_records: list[dict],
    incident: dict,
    config: SemanticEvidenceConfig,
) -> tuple[list[dict], dict]:
    """Fuse sparse intervention candidates with semantic evidence for one incident."""

    incident_id = str(incident["incident_id"])
    sparse_for_incident = [row for row in sparse_records if row.get("incident_id") == incident_id]
    if not sparse_for_incident:
        raise ValueError(f"no sparse intervention records found for incident_id={incident_id}")

    evidence_strength = compute_evidence_strength_for_incident(evidence_records, incident)
    exact_index = evidence_strength["exact_index"]
    type_index = evidence_strength["type_index"]
    support_index = evidence_strength["support_index"]

    raw_by_node: dict[str, float] = {}
    candidate_types: dict[str, str] = {}
    for record in sparse_for_incident:
        service = str(record["service"])
        metric = str(record["metric"])
        node = str(record["node"])
        candidate_type = metric_to_evidence_type(metric)
        candidate_types[node] = candidate_type
        exact_strength = exact_index.get((service, metric), 0.0)
        same_type_strength = type_index.get((service, candidate_type), 0.0)
        service_strength = _service_strength(service, type_index)
        raw_by_node[node] = (
            config.exact_metric_bonus * exact_strength
            + config.same_type_bonus * same_type_strength
            + config.service_level_bonus * service_strength
        )

    max_raw_evidence = max(raw_by_node.values()) if raw_by_node else 0.0
    max_sparse_score = max(float(row["intervention_score"]) for row in sparse_for_incident) if sparse_for_incident else 0.0
    semantic_records: list[SemanticInterventionRecord] = []
    for record in sparse_for_incident:
        service = str(record["service"])
        metric = str(record["metric"])
        node = str(record["node"])
        sparse_score = float(record["intervention_score"])
        raw_evidence = raw_by_node[node]
        evidence_score = raw_evidence / (max_raw_evidence + 1e-12) if max_raw_evidence > 0.0 else 0.0
        evidence_score = max(float(evidence_score), config.min_evidence_score)
        specificity_weight = metric_specificity_weight(metric) if config.use_metric_specificity else 1.0
        base_score = sparse_score * (1.0 + config.evidence_weight * evidence_score)
        evidence_anchor = config.evidence_anchor_weight * config.evidence_weight * evidence_score * max_sparse_score
        semantic_score = (base_score + evidence_anchor) * specificity_weight
        if sparse_score == 0.0 and evidence_score > 0.0:
            semantic_score = (evidence_score + evidence_anchor) * specificity_weight
        if config.clip_semantic_score is not None:
            semantic_score = float(np.clip(semantic_score, 0.0, config.clip_semantic_score))
        evidence_type = candidate_types[node]
        semantic_records.append(
            SemanticInterventionRecord(
                incident_id=incident_id,
                service=service,
                metric=metric,
                node=node,
                sparse_rank=int(record["rank"]),
                sparse_score=sparse_score,
                evidence_type=evidence_type,
                evidence_score=float(evidence_score),
                evidence_metrics=_support_metrics(service, metric, evidence_type, support_index),
                semantic_score=float(semantic_score),
                semantic_rank=0,
            )
        )

    ranked = _rank_semantic(semantic_records)
    ranked_dicts = [asdict(record) for record in ranked]
    root_node = f"{incident['root_service']}.{incident['root_metric']}"
    root_record = next((record for record in ranked_dicts if record["node"] == root_node), None)
    sparse_root = next((record for record in sparse_for_incident if record["node"] == root_node), None)
    top_debug = [
        {
            "semantic_rank": record["semantic_rank"],
            "node": record["node"],
            "semantic_score": record["semantic_score"],
            "sparse_rank": record["sparse_rank"],
            "sparse_score": record["sparse_score"],
            "evidence_type": record["evidence_type"],
            "evidence_score": record["evidence_score"],
        }
        for record in ranked_dicts[: config.top_k_debug]
    ]
    summary = {
        "incident_id": incident_id,
        "candidates_count": len(ranked_dicts),
        "nonzero_semantic_candidates_count": sum(1 for record in ranked_dicts if record["semantic_score"] > 0.0),
        "true_root_sparse_rank": int(sparse_root["rank"]) if sparse_root else None,
        "true_root_semantic_rank": int(root_record["semantic_rank"]) if root_record else None,
        "true_root_sparse_score": float(sparse_root["intervention_score"]) if sparse_root else None,
        "true_root_semantic_score": float(root_record["semantic_score"]) if root_record else None,
        "top_debug_candidates": top_debug,
    }
    return ranked_dicts, summary


def compute_type_scores_for_incident(semantic_records: list[dict], incident: dict) -> list[dict]:
    """Compute candidate root-cause type scores for one incident."""

    incident_id = str(incident["incident_id"])
    records = [row for row in semantic_records if row.get("incident_id") == incident_id]
    by_type: dict[str, list[dict]] = {}
    for record in records:
        root_type_candidate = canonical_root_type(str(record.get("evidence_type", "Unknown")))
        by_type.setdefault(root_type_candidate, []).append(record)

    type_records: list[SemanticTypeScoreRecord] = []
    for root_type_candidate, type_rows in by_type.items():
        ordered = sorted(type_rows, key=lambda row: (-float(row["semantic_score"]), str(row["node"])))
        top_rows = ordered[:10]
        type_score = float(sum(float(row["semantic_score"]) for row in top_rows))
        supporting_services = sorted({str(row["service"]) for row in top_rows})
        supporting_metrics = sorted({str(row["metric"]) for row in top_rows})
        type_records.append(
            SemanticTypeScoreRecord(
                incident_id=incident_id,
                root_type_candidate=root_type_candidate,
                type_score=type_score,
                rank=0,
                supporting_services=supporting_services,
                supporting_metrics=supporting_metrics,
            )
        )

    ordered_types = sorted(type_records, key=lambda item: (-item.type_score, item.root_type_candidate))
    return [asdict(SemanticTypeScoreRecord(**{**asdict(item), "rank": index})) for index, item in enumerate(ordered_types, start=1)]


def score_semantic_evidence(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: SemanticEvidenceConfig | None = None,
) -> dict:
    """Score semantic evidence for all sparse intervention candidates."""

    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    config = config or SemanticEvidenceConfig()
    sparse_records, evidence_records, incidents = load_required_dataset(input_path)

    all_semantic: list[dict] = []
    all_type_scores: list[dict] = []
    summaries: list[dict] = []
    for incident in incidents:
        semantic_records, summary = score_semantic_interventions_for_incident(sparse_records, evidence_records, incident, config)
        type_scores = compute_type_scores_for_incident(semantic_records, incident)
        all_semantic.extend(semantic_records)
        all_type_scores.extend(type_scores)
        summary["top_type_candidates"] = type_scores[: config.top_k_debug]
        summaries.append(summary)

    expected_candidates_count = len(incidents) * 176
    candidates_count = len(all_semantic)
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "candidates_count": candidates_count,
        "expected_candidates_count": expected_candidates_count,
        "candidates_count_matches_expected": candidates_count == expected_candidates_count,
        "semantic_records_count": len(all_semantic),
        "type_scores_count": len(all_type_scores),
        "evidence_weight": float(config.evidence_weight),
        "exact_metric_bonus": float(config.exact_metric_bonus),
        "same_type_bonus": float(config.same_type_bonus),
        "service_level_bonus": float(config.service_level_bonus),
    }

    output_path.mkdir(parents=True, exist_ok=True)
    semantic_path = output_path / "semantic_interventions.jsonl"
    type_scores_path = output_path / "semantic_type_scores.jsonl"
    summary_path = output_path / "semantic_evidence_summary.json"
    metadata_path = output_path / "semantic_evidence_metadata.json"
    write_jsonl(semantic_path, all_semantic)
    write_jsonl(type_scores_path, all_type_scores)
    summary_path.write_text(json.dumps({"config": asdict(config), "summaries": summaries}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "semantic_interventions_path": str(semantic_path),
        "semantic_type_scores_path": str(type_scores_path),
        "semantic_evidence_summary_path": str(summary_path),
        "semantic_evidence_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summaries": summaries,
    }

"""Semantic evidence scoring for P1D IPW sparse candidates."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.semantic import canonical_root_type, metric_specificity_weight, metric_to_evidence_type
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import IPWSparseInversionConfig, solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


@dataclass
class IPWSemanticEvidenceConfig:
    """Configuration for P1D semantic evidence scoring."""

    evidence_weight: float = 3.0
    type_weight: float = 1.0
    specificity_weight_enabled: bool = True
    semantic_anchor_enabled: bool = True
    semantic_anchor_weight: float = 0.50
    semantic_anchor_quantile: float = 0.75
    min_sparse_score_for_anchor: float = 0.0
    low_confidence_penalty_enabled: bool = True
    low_confidence_extra_penalty: float = 0.85


@dataclass
class IPWSemanticInterventionCandidate:
    """P1D semantic-scored candidate built from a P1C sparse candidate."""

    incident_id: str
    service: str
    metric: str
    node: str
    sparse_score: float
    residual_lift: float
    evidence_type: str
    evidence_score: float
    type_score: float
    specificity_weight: float
    semantic_anchor_bonus: float
    diagnostic_priority_bonus: float
    semantic_score: float
    semantic_rank: int
    confidence: float
    low_confidence: bool
    source: str = "ipw_semantic_evidence"


@dataclass
class IPWSemanticTypeScore:
    """Candidate root type score for one incident."""

    incident_id: str
    root_type_candidate: str
    type_score: float
    rank: int
    supporting_nodes: list[str]
    source: str = "ipw_semantic_evidence"


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], dict, list[dict], list[dict]]:
    """Load P1D sparse candidates, sparse summary, evidence records, and incidents."""

    input_path = Path(input_dir)
    required = {
        "ipw_sparse_interventions": input_path / "ipw_sparse_interventions.jsonl",
        "ipw_sparse_inversion_summary": input_path / "ipw_sparse_inversion_summary.json",
        "evidence": input_path / "evidence.jsonl",
        "incidents": input_path / "incidents.jsonl",
    }
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required input file for {name}: {path}")
    summary = json.loads(required["ipw_sparse_inversion_summary"].read_text(encoding="utf-8"))
    if not isinstance(summary, dict):
        raise ValueError(f"sparse summary is not a JSON object: {required['ipw_sparse_inversion_summary']}")
    return read_jsonl(required["ipw_sparse_interventions"]), summary, read_jsonl(required["evidence"]), read_jsonl(required["incidents"])


def _incident_id(record: dict) -> str:
    value = record.get("incident_id")
    return "" if value is None else str(value)


def build_evidence_index(evidence_records: list[dict]) -> dict[tuple[str, str, str], float]:
    """Build normalized incident-service-evidence_type evidence scores."""

    buckets: dict[tuple[str, str, str], list[float]] = {}
    for record in evidence_records:
        incident_id = _incident_id(record)
        service = str(record["service"])
        evidence_type = str(record.get("evidence_type") or metric_to_evidence_type(str(record.get("metric", ""))))
        buckets.setdefault((incident_id, service, evidence_type), []).append(abs(float(record.get("value", 0.0))))

    raw_scores = {key: float(np.mean(values)) for key, values in buckets.items()}
    max_by_incident: dict[str, float] = {}
    for (incident_id, _service, _evidence_type), score in raw_scores.items():
        max_by_incident[incident_id] = max(max_by_incident.get(incident_id, 0.0), score)

    normalized: dict[tuple[str, str, str], float] = {}
    for key, score in raw_scores.items():
        incident_id = key[0]
        scale = max_by_incident.get(incident_id, 0.0)
        normalized[key] = float(score / scale) if scale > 0 else 0.0
    return normalized


def _semantic_anchor_scale(records_for_incident: list[dict], config: IPWSemanticEvidenceConfig) -> float:
    """Return the incident-local sparse score scale used by label-free anchors."""

    sparse_scores = np.asarray([float(record.get("intervention_score", 0.0)) for record in records_for_incident], dtype=float)
    if sparse_scores.size == 0:
        return 0.0
    return float(np.quantile(sparse_scores, config.semantic_anchor_quantile))


def compute_semantic_anchor_bonus(
    records_for_incident: list[dict],
    candidate: dict,
    config: IPWSemanticEvidenceConfig,
) -> float:
    """Compute label-free semantic evidence anchor from sparse score distribution."""

    if not config.semantic_anchor_enabled:
        return 0.0
    anchor_scale = _semantic_anchor_scale(records_for_incident, config)
    if anchor_scale <= 0.0:
        return 0.0
    evidence_score = float(candidate.get("evidence_score", 0.0))
    sparse_score = float(candidate.get("sparse_score", candidate.get("intervention_score", 0.0)))
    if evidence_score > 0.0 and sparse_score > config.min_sparse_score_for_anchor:
        return float(config.semantic_anchor_weight * anchor_scale * evidence_score)
    return 0.0


def diagnostic_priority_bonus(metric: str, evidence_score: float, anchor_scale: float) -> float:
    """Return label-free diagnostic priority bonus for mechanism-specific metrics."""

    strong_diagnostic = {
        "cpu.throttled_usec",
        "net.retrans",
        "io.bio_latency_ms",
        "lock.futex_wait_ms",
    }
    medium_diagnostic = {
        "cpu.pressure",
        "memory.pressure",
        "net.rtt_ms",
        "io.queue_depth",
    }
    if anchor_scale <= 0.0:
        return 0.0
    if metric in strong_diagnostic:
        return float(0.35 * anchor_scale * max(float(evidence_score), 0.1))
    if metric in medium_diagnostic:
        return float(0.05 * anchor_scale * float(evidence_score))
    if metric.startswith("request."):
        return 0.0
    return 0.0


def _semantic_sort_key(record: dict) -> tuple[float, float, float, str]:
    return (
        -float(record["semantic_score"]),
        -float(record["sparse_score"]),
        -float(record["confidence"]),
        str(record["node"]),
    )


def _type_sort_key(record: dict) -> tuple[float, str]:
    return (-float(record["type_score"]), str(record["root_type_candidate"]))


def _type_scores_for_incident(incident_id: str, semantic_records: list[dict]) -> list[dict]:
    by_type: dict[str, list[dict]] = {}
    for record in semantic_records:
        root_type = canonical_root_type(str(record["evidence_type"]))
        by_type.setdefault(root_type, []).append(record)

    type_records: list[dict] = []
    for root_type, rows in by_type.items():
        rows_sorted = sorted(rows, key=_semantic_sort_key)
        top_score = float(rows_sorted[0]["semantic_score"]) if rows_sorted else 0.0
        type_records.append(
            asdict(
                IPWSemanticTypeScore(
                    incident_id=incident_id,
                    root_type_candidate=root_type,
                    type_score=top_score,
                    rank=0,
                    supporting_nodes=[row["node"] for row in rows_sorted[:10]],
                )
            )
        )
    type_records.sort(key=_type_sort_key)
    for rank, record in enumerate(type_records, start=1):
        record["rank"] = rank
    return type_records


def score_ipw_semantic_evidence(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: IPWSemanticEvidenceConfig | None = None,
) -> dict:
    """Score P1C IPW sparse candidates with semantic evidence."""

    cfg = config or IPWSemanticEvidenceConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    sparse_records, _sparse_summary, evidence_records, incidents = load_required_dataset(input_path)
    evidence_index = build_evidence_index(evidence_records)

    all_semantic_records: list[dict] = []
    all_type_records: list[dict] = []
    per_incident: list[dict] = []
    true_root_ranks: list[float] = []

    for incident in incidents:
        incident_id = str(incident["incident_id"])
        records_for_incident = [row for row in sparse_records if str(row.get("incident_id")) == incident_id]
        semantic_records: list[dict] = []
        anchor_scale = _semantic_anchor_scale(records_for_incident, cfg)

        for sparse in records_for_incident:
            service = str(sparse["service"])
            metric = str(sparse["metric"])
            node = str(sparse["node"])
            sparse_score = float(sparse["intervention_score"])
            evidence_type = metric_to_evidence_type(metric)
            evidence_score = evidence_index.get((incident_id, service, evidence_type), 0.0)
            type_score = float(evidence_score * cfg.type_weight)
            specificity = metric_specificity_weight(metric) if cfg.specificity_weight_enabled else 1.0
            draft = {
                "sparse_score": sparse_score,
                "intervention_score": sparse_score,
                "evidence_score": evidence_score,
            }
            anchor_bonus = compute_semantic_anchor_bonus(records_for_incident, draft, cfg)
            priority_bonus = diagnostic_priority_bonus(metric, evidence_score, anchor_scale)
            semantic_score = (
                sparse_score * (1.0 + cfg.evidence_weight * evidence_score) * specificity
                + anchor_bonus
                + priority_bonus
            )
            low_confidence = bool(sparse.get("low_confidence", False))
            if low_confidence and cfg.low_confidence_penalty_enabled:
                semantic_score *= float(cfg.low_confidence_extra_penalty)
            semantic_records.append(
                asdict(
                    IPWSemanticInterventionCandidate(
                        incident_id=incident_id,
                        service=service,
                        metric=metric,
                        node=node,
                        sparse_score=sparse_score,
                        residual_lift=float(sparse["residual_lift"]),
                        evidence_type=evidence_type,
                        evidence_score=float(evidence_score),
                        type_score=float(type_score),
                        specificity_weight=float(specificity),
                        semantic_anchor_bonus=float(anchor_bonus),
                        diagnostic_priority_bonus=float(priority_bonus),
                        semantic_score=float(semantic_score),
                        semantic_rank=0,
                        confidence=float(sparse.get("confidence", 0.0)),
                        low_confidence=low_confidence,
                    )
                )
            )

        semantic_records.sort(key=_semantic_sort_key)
        for rank, record in enumerate(semantic_records, start=1):
            record["semantic_rank"] = rank

        type_records = _type_scores_for_incident(incident_id, semantic_records)
        true_root_node = f"{incident['root_service']}.{incident['root_metric']}"
        true_root_record = next((record for record in semantic_records if record["node"] == true_root_node), None)
        sparse_true = next((record for record in records_for_incident if record["node"] == true_root_node), None)
        true_root_semantic_rank = int(true_root_record["semantic_rank"]) if true_root_record else None
        true_root_sparse_rank = int(sparse_true["rank"]) if sparse_true else None
        true_root_semantic_score = float(true_root_record["semantic_score"]) if true_root_record else 0.0
        if true_root_semantic_rank is not None:
            true_root_ranks.append(float(true_root_semantic_rank))
        top_record = semantic_records[0] if semantic_records else None
        top_type = type_records[0] if type_records else None
        per_incident.append(
            {
                "incident_id": incident_id,
                "top_candidate": top_record["node"] if top_record else "",
                "top_semantic_score": float(top_record["semantic_score"]) if top_record else 0.0,
                "top_type_candidate": top_type["root_type_candidate"] if top_type else "unknown",
                "true_root_sparse_rank_debug": true_root_sparse_rank,
                "true_root_semantic_rank_debug": true_root_semantic_rank,
                "true_root_semantic_score_debug": true_root_semantic_score,
            }
        )
        all_semantic_records.extend(semantic_records)
        all_type_records.extend(type_records)

    output_path.mkdir(parents=True, exist_ok=True)
    semantic_path = output_path / "ipw_semantic_interventions.jsonl"
    type_scores_path = output_path / "ipw_semantic_type_scores.jsonl"
    summary_path = output_path / "ipw_semantic_evidence_summary.json"
    metadata_path = output_path / "ipw_semantic_evidence_metadata.json"

    write_jsonl(semantic_path, all_semantic_records)
    write_jsonl(type_scores_path, all_type_records)
    summary = {
        "incidents_count": len(incidents),
        "candidates_count": len(all_semantic_records),
        "type_scores_count": len(all_type_records),
        "mean_true_root_semantic_rank_debug": float(np.mean(true_root_ranks)) if true_root_ranks else None,
        "metric_hit_at_1_debug": float(np.mean([1.0 if rank <= 1 else 0.0 for rank in true_root_ranks])) if true_root_ranks else None,
        "metric_hit_at_3_debug": float(np.mean([1.0 if rank <= 3 else 0.0 for rank in true_root_ranks])) if true_root_ranks else None,
        "per_incident": per_incident,
    }
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "candidates_count": len(all_semantic_records),
        "type_scores_count": len(all_type_records),
        "evidence_weight": float(cfg.evidence_weight),
        "type_weight": float(cfg.type_weight),
        "specificity_weight_enabled": bool(cfg.specificity_weight_enabled),
        "semantic_anchor_enabled": bool(cfg.semantic_anchor_enabled),
        "semantic_anchor_weight": float(cfg.semantic_anchor_weight),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "ipw_semantic_interventions_path": str(semantic_path),
        "ipw_semantic_type_scores_path": str(type_scores_path),
        "ipw_semantic_evidence_summary_path": str(summary_path),
        "ipw_semantic_evidence_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summary": summary,
    }


def run_p1d_pipeline(
    output_dir: str | Path,
    seed: int = 7,
    baseline_windows: int = 30,
    faulty_windows: int = 30,
    instances_per_service: int = 2,
    config: IPWSemanticEvidenceConfig | None = None,
) -> dict:
    """Run P1D pipeline through semantic evidence only."""

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
    return score_ipw_semantic_evidence(output_path, output_path, config or IPWSemanticEvidenceConfig())

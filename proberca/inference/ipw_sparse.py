"""Sparse inversion on IPW stable propagation residuals for probeRCA P1C."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import read_jsonl, write_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import IPWPropagationConfig, train_ipw_masked_propagation


@dataclass
class IPWSparseInversionConfig:
    """Configuration for sparse inversion on IPW residuals."""

    l1_lambda: float = 0.5
    min_baseline_observations: int = 2
    min_faulty_observations: int = 2
    min_sampling_probability: float = 0.05
    max_ipw_weight: float = 20.0
    use_ipw_weighted_mean: bool = True
    confidence_min_observations: int = 3
    low_confidence_penalty: float = 0.75


@dataclass
class IPWSparseInterventionCandidate:
    """P1C sparse intervention candidate from IPW residual lift."""

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
    baseline_observed_count: int
    faulty_observed_count: int
    mean_sampling_probability: float
    mean_ipw_weight: float
    confidence: float
    low_confidence: bool
    source: str = "ipw_sparse_inversion"


def load_required_dataset(input_dir: str | Path) -> tuple[list[dict], dict, list[dict]]:
    """Load IPW residuals, propagation metadata, and incidents."""

    input_path = Path(input_dir)
    residuals_path = input_path / "ipw_stable_residuals.jsonl"
    metadata_path = input_path / "ipw_propagation_metadata.json"
    incidents_path = input_path / "incidents.jsonl"
    for name, path in {
        "ipw_stable_residuals": residuals_path,
        "ipw_propagation_metadata": metadata_path,
        "incidents": incidents_path,
    }.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required input file for {name}: {path}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError(f"metadata file is not a JSON object: {metadata_path}")
    return read_jsonl(residuals_path), metadata, read_jsonl(incidents_path)


def safe_weight(record: dict, min_probability: float, max_weight: float) -> float:
    """Return a clipped IPW weight for a residual record."""

    if "ipw_weight" in record and record.get("ipw_weight") is not None:
        weight = float(record["ipw_weight"])
    else:
        probability = float(record.get("sampling_probability", min_probability))
        weight = 1.0 / max(probability, min_probability)
    return float(min(max_weight, max(0.0, weight)))


def weighted_mean_abs_residual(records: list[dict], config: IPWSparseInversionConfig) -> dict:
    """Compute weighted absolute and signed residual means for one phase."""

    if not records:
        return {
            "abs_mean": 0.0,
            "signed_mean": 0.0,
            "count": 0,
            "mean_sampling_probability": 0.0,
            "mean_ipw_weight": 0.0,
        }

    residuals = np.asarray([float(record["residual"]) for record in records], dtype=float)
    probabilities = np.asarray(
        [float(record.get("sampling_probability", config.min_sampling_probability)) for record in records],
        dtype=float,
    )
    weights = np.asarray(
        [safe_weight(record, config.min_sampling_probability, config.max_ipw_weight) for record in records],
        dtype=float,
    )
    if config.use_ipw_weighted_mean:
        denominator = float(np.sum(weights))
        if denominator <= 0:
            abs_mean = 0.0
            signed_mean = 0.0
        else:
            abs_mean = float(np.sum(weights * np.abs(residuals)) / denominator)
            signed_mean = float(np.sum(weights * residuals) / denominator)
    else:
        abs_mean = float(np.mean(np.abs(residuals)))
        signed_mean = float(np.mean(residuals))

    return {
        "abs_mean": abs_mean,
        "signed_mean": signed_mean,
        "count": int(len(records)),
        "mean_sampling_probability": float(np.mean(probabilities)),
        "mean_ipw_weight": float(np.mean(weights)),
    }


def compute_candidate_score(baseline_stats: dict, faulty_stats: dict, config: IPWSparseInversionConfig) -> dict:
    """Compute sparse residual-lift score without using root labels."""

    baseline_count = int(baseline_stats.get("count", 0))
    faulty_count = int(faulty_stats.get("count", 0))
    baseline_abs = float(baseline_stats.get("abs_mean", 0.0))
    faulty_abs = float(faulty_stats.get("abs_mean", 0.0))
    residual_lift = max(0.0, faulty_abs - baseline_abs)
    score = max(residual_lift - float(config.l1_lambda), 0.0)
    low_confidence = baseline_count < config.min_baseline_observations or faulty_count < config.min_faulty_observations
    if low_confidence:
        score *= float(config.low_confidence_penalty)
    confidence = min(1.0, min(baseline_count, faulty_count) / float(config.confidence_min_observations))
    return {
        "residual_lift": float(residual_lift),
        "intervention_score": float(score),
        "confidence": float(confidence),
        "low_confidence": bool(low_confidence),
    }


def _node(record: dict) -> str:
    return f"{record['service']}.{record['metric']}"


def _split_node(node: str) -> tuple[str, str]:
    service, metric = node.split(".", 1)
    return service, metric


def _mean_nonzero(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(np.mean(values))


def _records_for_incident(residuals: list[dict], incident_id: str) -> list[dict]:
    return [record for record in residuals if str(record.get("incident_id")) == incident_id]


def _candidate_sort_key(candidate: dict) -> tuple[float, float, float, str]:
    return (
        -float(candidate["intervention_score"]),
        -float(candidate["residual_lift"]),
        -float(candidate["confidence"]),
        str(candidate["node"]),
    )


def solve_ipw_sparse_inversion(
    input_dir: str | Path,
    output_dir: str | Path | None = None,
    config: IPWSparseInversionConfig | None = None,
) -> dict:
    """Solve P1C sparse inversion candidates from IPW residuals."""

    cfg = config or IPWSparseInversionConfig()
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir is not None else input_path
    residuals, _propagation_metadata, incidents = load_required_dataset(input_path)

    all_candidates: list[dict] = []
    per_incident: list[dict] = []
    true_root_ranks: list[float] = []

    for incident in incidents:
        incident_id = str(incident["incident_id"])
        start_ts = float(incident["start_ts"])
        incident_records = _records_for_incident(residuals, incident_id)
        grouped: dict[str, dict[str, list[dict]]] = {}
        for record in incident_records:
            phase = "baseline" if float(record["timestamp"]) < start_ts else "faulty"
            grouped.setdefault(_node(record), {"baseline": [], "faulty": []})[phase].append(record)

        incident_candidates: list[dict] = []
        for node in sorted(grouped):
            service, metric = _split_node(node)
            baseline_stats = weighted_mean_abs_residual(grouped[node]["baseline"], cfg)
            faulty_stats = weighted_mean_abs_residual(grouped[node]["faulty"], cfg)
            score_stats = compute_candidate_score(baseline_stats, faulty_stats, cfg)
            mean_sampling_probability = _mean_nonzero(
                [
                    value
                    for value in [
                        baseline_stats["mean_sampling_probability"],
                        faulty_stats["mean_sampling_probability"],
                    ]
                    if value > 0
                ]
            )
            mean_ipw_weight = _mean_nonzero(
                [
                    value
                    for value in [baseline_stats["mean_ipw_weight"], faulty_stats["mean_ipw_weight"]]
                    if value > 0
                ]
            )
            candidate = asdict(
                IPWSparseInterventionCandidate(
                    incident_id=incident_id,
                    service=service,
                    metric=metric,
                    node=node,
                    baseline_abs_residual=float(baseline_stats["abs_mean"]),
                    faulty_abs_residual=float(faulty_stats["abs_mean"]),
                    residual_lift=float(score_stats["residual_lift"]),
                    intervention_score=float(score_stats["intervention_score"]),
                    signed_intervention=float(faulty_stats["signed_mean"]),
                    rank=0,
                    baseline_observed_count=int(baseline_stats["count"]),
                    faulty_observed_count=int(faulty_stats["count"]),
                    mean_sampling_probability=float(mean_sampling_probability),
                    mean_ipw_weight=float(mean_ipw_weight),
                    confidence=float(score_stats["confidence"]),
                    low_confidence=bool(score_stats["low_confidence"]),
                )
            )
            incident_candidates.append(candidate)

        incident_candidates.sort(key=_candidate_sort_key)
        for rank, candidate in enumerate(incident_candidates, start=1):
            candidate["rank"] = rank

        true_root_node = f"{incident['root_service']}.{incident['root_metric']}"
        true_root = next((candidate for candidate in incident_candidates if candidate["node"] == true_root_node), None)
        true_root_rank = int(true_root["rank"]) if true_root else None
        true_root_score = float(true_root["intervention_score"]) if true_root else 0.0
        if true_root_rank is not None:
            true_root_ranks.append(float(true_root_rank))
        top_candidate = incident_candidates[0]["node"] if incident_candidates else ""
        top_score = float(incident_candidates[0]["intervention_score"]) if incident_candidates else 0.0
        per_incident.append(
            {
                "incident_id": incident_id,
                "candidates_count": len(incident_candidates),
                "nonzero_candidates_count": sum(1 for candidate in incident_candidates if candidate["intervention_score"] > 0),
                "top_candidate": top_candidate,
                "top_score": top_score,
                "true_root_rank_debug": true_root_rank,
                "true_root_score_debug": true_root_score,
                "low_confidence_candidates_count": sum(1 for candidate in incident_candidates if candidate["low_confidence"]),
            }
        )
        all_candidates.extend(incident_candidates)

    output_path.mkdir(parents=True, exist_ok=True)
    candidates_path = output_path / "ipw_sparse_interventions.jsonl"
    summary_path = output_path / "ipw_sparse_inversion_summary.json"
    metadata_path = output_path / "ipw_sparse_inversion_metadata.json"

    write_jsonl(candidates_path, all_candidates)
    nonzero_candidates_count = sum(1 for candidate in all_candidates if candidate["intervention_score"] > 0)
    summary = {
        "incidents_count": len(incidents),
        "candidates_count": len(all_candidates),
        "nonzero_candidates_count": nonzero_candidates_count,
        "mean_true_root_rank_debug": float(np.mean(true_root_ranks)) if true_root_ranks else None,
        "per_incident": per_incident,
    }
    metadata = {
        "input_dir": str(input_path),
        "output_dir": str(output_path),
        "incidents_count": len(incidents),
        "candidates_count": len(all_candidates),
        "l1_lambda": float(cfg.l1_lambda),
        "use_ipw_weighted_mean": bool(cfg.use_ipw_weighted_mean),
        "min_baseline_observations": int(cfg.min_baseline_observations),
        "min_faulty_observations": int(cfg.min_faulty_observations),
        "min_sampling_probability": float(cfg.min_sampling_probability),
        "max_ipw_weight": float(cfg.max_ipw_weight),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "ipw_sparse_interventions_path": str(candidates_path),
        "ipw_sparse_inversion_summary_path": str(summary_path),
        "ipw_sparse_inversion_metadata_path": str(metadata_path),
        "metadata": metadata,
        "summary": summary,
    }


def run_p1c_pipeline(
    output_dir: str | Path,
    seed: int = 7,
    baseline_windows: int = 30,
    faulty_windows: int = 30,
    instances_per_service: int = 2,
    config: IPWSparseInversionConfig | None = None,
) -> dict:
    """Run generate, normalize, observe, IPW propagation, then IPW sparse inversion."""

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
    return solve_ipw_sparse_inversion(output_path, output_path, config or IPWSparseInversionConfig())

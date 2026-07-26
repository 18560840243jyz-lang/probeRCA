"""Immutable outputs and internal identities for the final control-plane algorithm."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from proberca.dataplane.contracts import fingerprint


@dataclass(frozen=True, order=True)
class MetricNode:
    node_id: str
    entity_id: str
    entity_type: str
    metric_name: str
    role: str
    root_category: str | None
    root_eligible: bool


@dataclass(frozen=True)
class NormalizedObservation:
    metric: MetricNode
    signed_z: float
    anomaly: float
    quality: float
    source_record_id: str


@dataclass(frozen=True)
class CandidateEntityGraph:
    seed_services: tuple[str, ...]
    seed_edges: tuple[str, ...]
    services: tuple[str, ...]
    hosts: tuple[str, ...]
    edges: tuple[str, ...]
    strong_service_relations: tuple[tuple[str, str, float], ...]
    topology_snapshot_id: str


@dataclass(frozen=True)
class MetricPropagationModel:
    node_ids: tuple[str, ...]
    lags: tuple[int, ...]
    coefficients: dict[tuple[str, str, int], float]
    semantic_mask: tuple[tuple[str, str], ...]
    training_rows: int
    healthy_cutoff_ns: int

    def cross_prediction(
        self, target_node_id: str, history: dict[int, dict[str, float]],
        sequence: int,
    ) -> float:
        """Predict with A_v,x only: self-history is deliberately never subtracted."""
        value = 0.0
        for lag in self.lags:
            row = history.get(sequence - lag, {})
            for parent_node_id in self.node_ids:
                if parent_node_id == target_node_id:
                    continue
                coefficient = self.coefficients.get(
                    (target_node_id, parent_node_id, lag), 0.0,
                )
                value += coefficient * row.get(parent_node_id, 0.0)
        return float(value)


@dataclass(frozen=True)
class FISTAResult:
    theta: tuple[float, ...]
    converged: bool
    iterations: int
    objective: float
    lipschitz: float


@dataclass(frozen=True)
class RootCandidateScore:
    candidate_id: str
    entity_id: str
    entity_type: str
    root_category: str
    score: float
    metric_node_ids: tuple[str, ...]
    metric_contributions: dict[str, float]
    signed_residuals: dict[str, float]
    observation_quality: dict[str, float]
    burst_evidence_strength: float
    burst_evidence_ids: tuple[str, ...]
    burst_evidence: tuple[dict[str, Any], ...]
    base_group_penalty: float
    effective_group_penalty: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FinalRCAResult:
    schema_version: str
    incident_id: str
    hard_alert_timestamp_ns: int
    analysis_cutoff_ns: int
    symptom_services: tuple[str, ...]
    symptom_edges: tuple[str, ...]
    candidates: tuple[RootCandidateScore, ...]
    top_k: tuple[RootCandidateScore, ...]
    candidate_graph: CandidateEntityGraph
    residual_signal: str
    solver: FISTAResult
    model_metadata: dict[str, Any]
    result_fingerprint: str

    @classmethod
    def create(cls, **values) -> "FinalRCAResult":
        payload = dict(values)
        payload["schema_version"] = "probeRCA-final-result-v1"
        payload["result_fingerprint"] = ""
        serializable = _plain(payload)
        return cls(**(payload | {"result_fingerprint": fingerprint(serializable)}))

    def to_dict(self) -> dict[str, Any]:
        return _plain(asdict(self))


@dataclass(frozen=True)
class ControlPlaneRun:
    schema_version: str
    dataset_id: str
    dataset_fingerprint: str
    collection_contract_fingerprint: str
    control_config_fingerprint: str
    processed_window_count: int
    state_timeline: tuple[dict[str, Any], ...]
    results: tuple[FinalRCAResult, ...]
    run_fingerprint: str

    @classmethod
    def create(cls, **values) -> "ControlPlaneRun":
        payload = dict(values)
        payload["schema_version"] = "probeRCA-final-control-run-v1"
        payload["run_fingerprint"] = ""
        return cls(**(payload | {"run_fingerprint": fingerprint(_plain(payload))}))

    def to_dict(self) -> dict[str, Any]:
        return _plain(asdict(self))


def _plain(value):
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, dict):
        return {
            ("|".join(map(str, key)) if isinstance(key, tuple) else str(key)): _plain(child)
            for key, child in value.items()
        }
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value

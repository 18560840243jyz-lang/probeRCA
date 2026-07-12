"""Strict, serializable contracts for canonical P9 diagnosis."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


class DiagnosisInputMismatchError(ValueError):
    """P6, P7, P8, alert, candidate, or propagation inputs do not align."""


class SolverResultNotUsableError(ValueError):
    """The supplied P8 result is not converged and solver-usable."""


class CandidateConstructionError(ValueError):
    """A root candidate violates the canonical P9 contract."""


class CandidateOverflowError(ValueError):
    """The active candidate count exceeds the configured hard limit."""


class CounterfactualProblemError(ValueError):
    """A reduced delete-and-reoptimize problem cannot be constructed."""


class CounterfactualSolverError(ValueError):
    """A required counterfactual P8 solve did not produce a usable result."""


class CounterfactualNegativeDeltaError(ValueError):
    """Counterfactual loss decreased beyond the numerical tolerance."""


class CandidateContributionError(ValueError):
    """A weighted fitted-component contribution cannot be evaluated."""


class SymptomAlignmentError(ValueError):
    """Current anomaly and frozen P5 prediction records do not align."""


class PropagationPathError(ValueError):
    """Propagation path inputs or a generated path are structurally invalid."""


class IdentifiabilityError(ValueError):
    """Identifiability inputs are invalid or non-finite."""


class ConfidenceComputationError(ValueError):
    """Configured confidence terms cannot form an auditable score."""


class RCAReportValidationError(ValueError):
    """A diagnosis cannot be represented by the unified RCA report contract."""


class DiagnosisSerializationError(ValueError):
    """Diagnosis persistence failed strict serialization validation."""


class DiagnosisFingerprintError(ValueError):
    """Persisted diagnosis content or context does not match its fingerprint."""


@dataclass(frozen=True)
class DiagnosisCandidate:
    candidate_id: str
    candidate_type: str
    fault_mode: str
    edge_subtype: str | None
    edge_kind: str | None
    variable_block: str
    group_id: str
    variable_indices: list[int]
    variable_ids: list[str]
    raw_values: list[float]
    contribution_vector: list[float]
    weighted_contribution_energy: float
    active: bool
    dominant_member_id: str
    member_diagnostics: list[dict[str, Any]]
    metadata: dict[str, Any]
    solver_result_id: str
    problem_id: str
    counterfactual_status: str = "not_evaluated"
    counterfactual_solver_result_id: str | None = None
    counterfactual_iterations: int = 0
    delta_loss: float | None = None
    relative_delta_loss: float | None = None
    counterfactual_support: float | None = None
    margin: float | None = None
    candidate_quality: float = 0.0
    coherence: float = 0.0
    lag_entropy: float = 0.0
    best_path_score: float = 0.0
    identifiability: float | None = None
    confidence: float | None = None
    rank: int | None = None
    status: str = "active"
    quality_issues: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.candidate_type not in {"node", "edge"} or self.variable_block not in {"node", "propagation", "shock"}:
            raise CandidateConstructionError("invalid diagnosis candidate type")
        expected = {"node": ("self", None), "propagation": ("edge", "propagated-edge"),
                    "shock": ("edge", "exogenous-edge-shock")}[self.variable_block]
        if (self.fault_mode, self.edge_subtype) != expected:
            raise CandidateConstructionError("candidate fault mode conflicts with variable block")
        if not self.variable_indices or self.variable_indices != sorted(set(self.variable_indices)):
            raise CandidateConstructionError("candidate variable indices must be sorted and unique")
        if len(self.variable_ids) != len(self.variable_indices) or len(self.raw_values) != len(self.variable_ids):
            raise CandidateConstructionError("candidate variables are not aligned")
        numeric = [*self.raw_values, *self.contribution_vector, self.weighted_contribution_energy,
                   self.candidate_quality, self.coherence, self.lag_entropy, self.best_path_score]
        numeric.extend(value for value in (self.delta_loss, self.relative_delta_loss,
                                           self.counterfactual_support, self.margin,
                                           self.identifiability, self.confidence) if value is not None)
        if any(not math.isfinite(value) for value in numeric) or self.weighted_contribution_energy < 0:
            raise CandidateConstructionError("candidate contains non-finite values")
        for value in (self.candidate_quality, self.coherence, self.lag_entropy, self.best_path_score):
            if not 0 <= value <= 1:
                raise CandidateConstructionError("candidate normalized diagnostic is outside [0,1]")


class NodeRootCandidate(DiagnosisCandidate):
    """Candidate backed by one node self-fault variable."""


class PropagationEdgeCandidate(DiagnosisCandidate):
    """Candidate backed by one non-overlapping propagation variable group."""


class ShockEdgeCandidate(DiagnosisCandidate):
    """Candidate backed by one non-overlapping physical-edge shock group."""


@dataclass(frozen=True)
class PropagatedSymptom:
    node_id: str
    service_id: str
    metric_name: str
    actual_signed_z: float
    predicted_signed_z: float
    residual: float
    anomaly_score: float
    explained_ratio: float
    mode: str = "propagated"
    upstream_root_candidate_id: str | None = None
    best_path_id: str | None = None


@dataclass(frozen=True)
class PropagationPath:
    path_id: str
    root_candidate_id: str
    target_id: str
    path_level: str
    node_ids: list[str]
    steps: list[dict[str, Any]]
    path_score: float
    status: str = "supported"


@dataclass(frozen=True)
class EvidenceChainItem:
    evidence_item_id: str
    candidate_id: str
    item_type: str
    source_ids: list[str]
    timestamp_ns: int
    numeric_value: float
    normalized_value: float | None
    reason_code: str
    provenance: dict[str, Any]


@dataclass(frozen=True)
class DiagnosisResult:
    diagnosis_result_id: str
    problem_id: str
    solver_result_id: str
    status: str
    primary_candidate_id: str | None
    ranked_candidates: list[DiagnosisCandidate]
    symptoms: list[PropagatedSymptom]
    paths: list[PropagationPath]
    evidence_chain: list[EvidenceChainItem]
    ambiguity_reasons: list[str]
    config_fingerprint: str
    diagnosis_fingerprint: str
    runtime: dict[str, Any]
    quality_issues: list[dict[str, Any]]

"""Strict contracts for the P6 joint inversion system."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy import sparse


P5_METRIC_SIGNAL_KIND = "signed_z"


class ResidualAlignmentError(ValueError):
    """Raised when residual inputs cannot be aligned exactly."""


class ResidualNotReadyError(ValueError):
    """Raised when a formal residual system is not ready."""


class SignalKindMismatchError(ValueError):
    """Raised when signed signal contracts disagree."""


class CandidateModelMismatchError(ValueError):
    """Raised when alert, candidate, topology, and model identities differ."""


class MissingNodeResidualError(ResidualAlignmentError):
    """Raised when a formal node residual row is unavailable."""


class MissingEdgeResidualError(ResidualAlignmentError):
    """Raised when a formal edge residual row is unavailable."""


class PropagationDictionaryError(ValueError):
    """Raised when P5 structural propagation metadata is inconsistent."""


class ShockTemplateConflictError(ValueError):
    """Raised when multiple exact shock templates match one edge metric."""


class ShockProjectionError(ValueError):
    """Raised when a configured shock cannot project to candidate nodes."""


class DictionaryDimensionError(ValueError):
    """Raised when dictionary dimensions and references disagree."""


class DictionaryOverflowError(ValueError):
    """Raised when a configured formal-system limit is exceeded."""


class JointSystemSerializationError(ValueError):
    """Raised when strict joint-system persistence validation fails."""


class NonFiniteJointSystemError(ValueError):
    """Raised when a formal vector or sparse matrix is non-finite."""


@dataclass(frozen=True)
class ResidualRowRef:
    row_index: int
    row_type: str
    object_id: str
    observation_quality: float
    source_record_id: str
    timestamp_ns: int

    def __post_init__(self) -> None:
        if self.row_index < 0 or self.row_type not in {"node", "edge"}:
            raise ValueError("invalid residual row reference")
        if not self.object_id or not self.source_record_id or self.timestamp_ns < 0:
            raise ValueError("residual row reference requires stable source metadata")
        if not 0.0 <= self.observation_quality <= 1.0:
            raise ValueError("residual observation_quality must be in [0, 1]")


@dataclass(frozen=True)
class NodeVariableRef:
    column_index: int
    node_id: str
    row_index: int


@dataclass(frozen=True)
class PropagationVariableRef:
    column_index: int
    propagation_id: str
    parent_node_id: str
    target_node_id: str
    lag: int
    learned_coefficient: float
    positive_support: float
    relation_types: list[str]
    relation_ids: list[str]
    rule_ids: list[str]
    parent_value: float
    target_row_index: int
    model_snapshot_id: str


@dataclass(frozen=True)
class ShockVariableRef:
    column_index: int
    shock_id: str
    edge_metric_id: str
    physical_edge_id: str
    src_service_id: str
    dst_service_id: str
    protocol: str
    metric_name: str
    template_id: str
    edge_row_index: int
    projected_node_rows: list[int]
    projection_weights: list[float]
    source_edge_anomaly_record_id: str


@dataclass
class JointInversionSystem:
    schema_version: str
    record_type: str
    system_id: str
    alert_id: str
    candidate_id: str
    topology_snapshot_id: str
    metric_model_snapshot_id: str
    timestamp_ns: int
    signal_kind: str
    node_row_refs: list[ResidualRowRef]
    edge_row_refs: list[ResidualRowRef]
    node_variable_refs: list[NodeVariableRef]
    propagation_variable_refs: list[PropagationVariableRef]
    shock_variable_refs: list[ShockVariableRef]
    actual_node_values: np.ndarray
    predicted_node_values: np.ndarray
    node_residual: np.ndarray
    edge_residual: np.ndarray
    joint_residual: np.ndarray
    node_observation_quality: np.ndarray
    edge_observation_quality: np.ndarray
    source_prediction_ids: list[str]
    source_anomaly_record_ids: list[str]
    U: sparse.csr_matrix
    X_prop: sparse.csc_matrix
    X_shock: sparse.csc_matrix
    U_shape: list[int]
    X_prop_shape: list[int]
    X_shock_shape: list[int]
    U_nnz: int
    X_prop_nnz: int
    X_shock_nnz: int
    config_fingerprint: str
    structure_fingerprint: str
    solver_eligible: bool
    quality_issues: list[dict]
    build_duration_ms: float

    def __post_init__(self) -> None:
        if self.schema_version != "1.0" or self.record_type != "joint_inversion_system":
            raise DictionaryDimensionError("invalid joint inversion system version or type")
        if self.signal_kind != "signed_z" or not self.solver_eligible:
            raise DictionaryDimensionError("formal joint system must be signed_z and solver eligible")
        rows = [*self.node_row_refs, *self.edge_row_refs]
        if [item.row_index for item in rows] != list(range(len(rows))):
            raise DictionaryDimensionError("residual row indices must be continuous")
        if len({item.object_id for item in rows}) != len(rows):
            raise DictionaryDimensionError("residual row object IDs must be unique")
        n_node, n_edge = len(self.node_row_refs), len(self.edge_row_refs)
        expected_rows = n_node + n_edge
        vectors = (
            (self.actual_node_values, n_node), (self.predicted_node_values, n_node),
            (self.node_residual, n_node), (self.edge_residual, n_edge),
            (self.joint_residual, expected_rows),
            (self.node_observation_quality, n_node),
            (self.edge_observation_quality, n_edge),
        )
        for vector, expected in vectors:
            if not isinstance(vector, np.ndarray) or vector.shape != (expected,) or not np.isfinite(vector).all():
                raise NonFiniteJointSystemError("joint system vector has invalid shape or values")
        if not np.array_equal(self.joint_residual, np.concatenate((self.node_residual, self.edge_residual))):
            raise DictionaryDimensionError("joint residual must concatenate node then edge residuals")
        matrices = (
            (self.U, self.U_shape, self.U_nnz, [expected_rows, n_node]),
            (self.X_prop, self.X_prop_shape, self.X_prop_nnz,
             [expected_rows, len(self.propagation_variable_refs)]),
            (self.X_shock, self.X_shock_shape, self.X_shock_nnz,
             [expected_rows, len(self.shock_variable_refs)]),
        )
        for matrix, shape, nnz, expected in matrices:
            if not sparse.issparse(matrix) or list(matrix.shape) != expected or shape != expected or matrix.nnz != nnz:
                raise DictionaryDimensionError("sparse matrix metadata does not match its structure")
            if not np.isfinite(matrix.data).all():
                raise NonFiniteJointSystemError("sparse matrix contains non-finite data")
        if len(self.node_variable_refs) != n_node:
            raise DictionaryDimensionError("node variable references do not match U")
        if len(self.source_prediction_ids) != n_node or len(self.source_anomaly_record_ids) != n_node:
            raise DictionaryDimensionError("node residual source references are incomplete")
        if not math.isfinite(self.build_duration_ms) or self.build_duration_ms < 0:
            raise ValueError("build_duration_ms must be finite and non-negative")

"""Canonical P6 joint inversion-system construction API."""

from .contracts import (
    CandidateModelMismatchError,
    DictionaryDimensionError,
    DictionaryOverflowError,
    JointInversionSystem,
    JointSystemSerializationError,
    MissingEdgeResidualError,
    MissingNodeResidualError,
    NonFiniteJointSystemError,
    PropagationDictionaryError,
    ResidualAlignmentError,
    ResidualNotReadyError,
    ShockProjectionError,
    ShockTemplateConflictError,
    SignalKindMismatchError,
)
from .joint_system import (
    build_joint_inversion_system,
    load_joint_inversion_system,
    save_joint_inversion_system,
)
from .residuals import edge_anomaly_from_p2

__all__ = [
    "build_joint_inversion_system", "save_joint_inversion_system",
    "load_joint_inversion_system", "edge_anomaly_from_p2", "JointInversionSystem",
    "ResidualAlignmentError", "ResidualNotReadyError", "SignalKindMismatchError",
    "CandidateModelMismatchError", "MissingNodeResidualError", "MissingEdgeResidualError",
    "PropagationDictionaryError", "ShockTemplateConflictError", "ShockProjectionError",
    "DictionaryDimensionError", "DictionaryOverflowError", "JointSystemSerializationError",
    "NonFiniteJointSystemError",
]

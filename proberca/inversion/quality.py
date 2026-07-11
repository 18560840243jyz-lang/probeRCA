"""Sparse observation-quality weighting for the P6 residual rows."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy import sparse

from proberca.config import QualityConfig
from proberca.inversion.contracts import JointInversionSystem


class QualityWeightError(ValueError):
    """P6 row quality cannot produce a valid observation weight."""


@dataclass(frozen=True)
class ObservationWeightResult:
    node_weights: np.ndarray
    edge_weights: np.ndarray
    joint_weights: np.ndarray
    W: sparse.csr_matrix


def build_observation_weights(joint_system, config):
    if not isinstance(joint_system, JointInversionSystem) or not isinstance(config, QualityConfig):
        raise TypeError("quality weighting requires JointInversionSystem and QualityConfig")
    node = np.asarray(joint_system.node_observation_quality, dtype=float)
    edge = np.asarray(joint_system.edge_observation_quality, dtype=float)
    for name, values in (("node", node), ("edge", edge)):
        if not np.isfinite(values).all() or np.any(values < 0) or np.any(values > 1):
            raise QualityWeightError(f"{name} quality must be finite in [0, 1]")
    node_weights = np.clip(node, config.quality_weight_floor, 1.0)
    edge_weights = np.clip(edge, config.quality_weight_floor, 1.0)
    joint = np.concatenate((node_weights, edge_weights))
    W = sparse.diags(joint, offsets=0, format="csr", dtype=float)
    return ObservationWeightResult(node_weights, edge_weights, joint, W)

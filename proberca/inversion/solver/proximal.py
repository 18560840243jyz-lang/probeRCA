"""Exact proximal operator for non-overlapping weighted sparse groups."""

from __future__ import annotations

import numpy as np

from .objective import FISTAProblemValidationError, PreparedFISTAProblem


def _validate_partition(groups, size: int, expected_type: str | None) -> None:
    indices = [index for group in groups for index in group.indices]
    if expected_type is not None and any(group.variable_type != expected_type for group in groups):
        raise FISTAProblemValidationError("group variable type does not match its block")
    if sorted(indices) != list(range(size)) or len(indices) != len(set(indices)):
        raise FISTAProblemValidationError("groups must form a complete non-overlapping partition")


def sparse_group_prox_block(values, step_size: float, l1_penalties, groups,
                            group_penalty: float, *, expected_type: str | None = None):
    vector = np.asarray(values, dtype=float)
    penalties = np.asarray(l1_penalties, dtype=float)
    if vector.ndim != 1 or penalties.shape != vector.shape:
        raise FISTAProblemValidationError("proximal block and penalties must be aligned vectors")
    if not np.isfinite(vector).all() or not np.isfinite(penalties).all() \
            or np.any(penalties < 0):
        raise FISTAProblemValidationError("proximal inputs must be finite with non-negative penalties")
    if not np.isfinite(step_size) or step_size <= 0 \
            or not np.isfinite(group_penalty) or group_penalty < 0:
        raise FISTAProblemValidationError("proximal step and group penalty are invalid")
    _validate_partition(groups, vector.size, expected_type)
    soft = np.sign(vector) * np.maximum(np.abs(vector) - step_size * penalties, 0.0)
    result = soft.copy()
    for group in groups:
        norm = float(np.linalg.norm(soft[group.indices]))
        if norm == 0.0:
            result[group.indices] = 0.0
        else:
            factor = max(1.0 - step_size * group_penalty / norm, 0.0)
            result[group.indices] = factor * soft[group.indices]
    if not np.isfinite(result).all():
        raise FISTAProblemValidationError("proximal result is non-finite")
    return result


def compute_prox(prepared: PreparedFISTAProblem, values, step_size: float) -> np.ndarray:
    vector = np.asarray(values, dtype=float)
    if vector.shape != (prepared.layout.total_size,):
        raise FISTAProblemValidationError("proximal vector does not match variable layout")
    problem, layout = prepared.problem, prepared.layout
    blocks = (
        (layout.u_slice, problem.lambda_u_effective, problem.node_groups,
         problem.lambda_node_group, "node"),
        (layout.delta_slice, problem.lambda_delta_effective, problem.propagation_groups,
         problem.lambda_propagation_group, "propagation"),
        (layout.xi_slice, problem.lambda_xi_effective, problem.shock_groups,
         problem.lambda_shock_group, "shock"),
    )
    result = np.empty_like(vector)
    for block_slice, penalties, groups, group_penalty, variable_type in blocks:
        result[block_slice] = sparse_group_prox_block(
            vector[block_slice], step_size, penalties, groups, group_penalty,
            expected_type=variable_type,
        )
    return result

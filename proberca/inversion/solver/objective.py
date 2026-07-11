"""Sparse weighted objective used by the canonical P8 FISTA solver."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy import sparse

from ..weighted_problem import (
    WeightedJointInversionProblem,
    WeightedProblemFingerprintError,
    validate_problem_fingerprint,
)


class FISTAProblemValidationError(ValueError):
    """The P7 weighted problem cannot be solved without changing its semantics."""


@dataclass(frozen=True)
class BlockLayout:
    u_slice: slice
    delta_slice: slice
    xi_slice: slice
    total_size: int


@dataclass(frozen=True)
class PreparedFISTAProblem:
    problem: WeightedJointInversionProblem
    B: sparse.csr_matrix
    layout: BlockLayout


@dataclass(frozen=True)
class ObjectiveComponents:
    data_fit: float
    node_l1: float
    node_group: float
    propagation_l1: float
    propagation_group: float
    shock_l1: float
    shock_group: float
    total: float


def _finite_vector(name: str, value, size: int) -> np.ndarray:
    array = np.asarray(value, dtype=float)
    if array.shape != (size,) or not np.isfinite(array).all():
        raise FISTAProblemValidationError(f"{name} must be a finite vector of length {size}")
    return array


def _validate_groups(groups, size: int, variable_type: str) -> None:
    indices = [index for group in groups for index in group.indices]
    if any(group.variable_type != variable_type for group in groups) \
            or sorted(indices) != list(range(size)) or len(indices) != len(set(indices)):
        raise FISTAProblemValidationError(
            f"{variable_type} groups must be a complete non-overlapping partition"
        )


def prepare_problem(problem: WeightedJointInversionProblem) -> PreparedFISTAProblem:
    if not isinstance(problem, WeightedJointInversionProblem):
        raise FISTAProblemValidationError("problem must be WeightedJointInversionProblem")
    if not problem.solver_eligible:
        raise FISTAProblemValidationError("weighted problem is not solver eligible")
    matrices = (problem.U, problem.X_prop, problem.X_shock)
    row_count = problem.joint_residual.size
    if any(not sparse.issparse(matrix) or matrix.shape[0] != row_count for matrix in matrices):
        raise FISTAProblemValidationError("P7 dictionary matrices must be sparse and row-aligned")
    if any(not np.isfinite(matrix.data).all() for matrix in matrices):
        raise FISTAProblemValidationError("P7 dictionary matrices contain non-finite values")
    if not sparse.isspmatrix_csr(problem.W) or problem.W.shape != (row_count, row_count):
        raise FISTAProblemValidationError("W must be a sparse row-aligned diagonal matrix")
    diagonal = problem.W.diagonal()
    if problem.W.nnz != row_count or diagonal.shape != (row_count,) \
            or not np.isfinite(diagonal).all() or np.any(diagonal <= 0):
        raise FISTAProblemValidationError("W diagonal must be finite and strictly positive")
    if (problem.W - sparse.diags(diagonal, format="csr")).nnz:
        raise FISTAProblemValidationError("W must be diagonal")
    n_u, n_delta, n_xi = (matrix.shape[1] for matrix in matrices)
    _finite_vector("joint_residual", problem.joint_residual, row_count)
    for name, values, size in (
        ("lambda_u_effective", problem.lambda_u_effective, n_u),
        ("lambda_delta_effective", problem.lambda_delta_effective, n_delta),
        ("lambda_xi_effective", problem.lambda_xi_effective, n_xi),
    ):
        array = _finite_vector(name, values, size)
        if array.size and np.any(array <= 0):
            raise FISTAProblemValidationError(f"{name} must be strictly positive")
    for name in ("lambda_node_group", "lambda_propagation_group", "lambda_shock_group"):
        value = getattr(problem, name)
        if not np.isfinite(value) or value < 0:
            raise FISTAProblemValidationError(f"{name} must be finite and non-negative")
    _validate_groups(problem.node_groups, n_u, "node")
    _validate_groups(problem.propagation_groups, n_delta, "propagation")
    _validate_groups(problem.shock_groups, n_xi, "shock")
    try:
        validate_problem_fingerprint(problem)
    except (WeightedProblemFingerprintError, ValueError) as error:
        raise FISTAProblemValidationError(str(error)) from error
    B = sparse.hstack(matrices, format="csr")
    layout = BlockLayout(
        slice(0, n_u), slice(n_u, n_u + n_delta),
        slice(n_u + n_delta, n_u + n_delta + n_xi), n_u + n_delta + n_xi,
    )
    return PreparedFISTAProblem(problem, B, layout)


def data_fit_gradient(prepared: PreparedFISTAProblem, theta) -> tuple[float, np.ndarray]:
    vector = _finite_vector("theta", theta, prepared.layout.total_size)
    error = prepared.B @ vector - prepared.problem.joint_residual
    weighted_error = prepared.problem.W @ error
    value = 0.5 * float(error @ weighted_error)
    gradient = np.asarray(prepared.B.T @ weighted_error, dtype=float).reshape(-1)
    if not np.isfinite(value) or not np.isfinite(gradient).all():
        raise FISTAProblemValidationError("data fit or gradient is non-finite")
    return value, gradient


def _group_penalty(values: np.ndarray, groups, penalty: float) -> float:
    return float(penalty * sum(np.linalg.norm(values[group.indices]) for group in groups))


def evaluate_objective(prepared: PreparedFISTAProblem, theta) -> ObjectiveComponents:
    vector = _finite_vector("theta", theta, prepared.layout.total_size)
    problem, layout = prepared.problem, prepared.layout
    u, delta, xi = vector[layout.u_slice], vector[layout.delta_slice], vector[layout.xi_slice]
    data_fit = data_fit_gradient(prepared, vector)[0]
    values = (
        data_fit,
        float(problem.lambda_u_effective @ np.abs(u)),
        _group_penalty(u, problem.node_groups, problem.lambda_node_group),
        float(problem.lambda_delta_effective @ np.abs(delta)),
        _group_penalty(delta, problem.propagation_groups, problem.lambda_propagation_group),
        float(problem.lambda_xi_effective @ np.abs(xi)),
        _group_penalty(xi, problem.shock_groups, problem.lambda_shock_group),
    )
    total = float(sum(values))
    if not np.isfinite(total):
        raise FISTAProblemValidationError("objective is non-finite")
    return ObjectiveComponents(*values, total)

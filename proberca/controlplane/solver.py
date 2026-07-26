"""Non-negative Sparse-Group FISTA for final root-coordinate selection."""

from __future__ import annotations

import math

import numpy as np

from .model import FISTAResult


class FinalFISTAError(ValueError):
    """Final-scheme FISTA inputs or numerical state are invalid."""


def _objective(theta, residual, weights, l1, groups) -> float:
    error = residual - theta
    data_fit = 0.5 * float(error @ (weights * error))
    sparse = float(l1 @ theta)
    grouped = sum(penalty * float(np.linalg.norm(theta[indices]))
                  for indices, penalty in groups)
    return data_fit + sparse + grouped


def _prox(values, step, l1, groups):
    thresholded = np.maximum(values - step * l1, 0.0)
    output = thresholded.copy()
    for indices, penalty in groups:
        norm = float(np.linalg.norm(thresholded[indices]))
        factor = 0.0 if norm == 0.0 else max(1.0 - step * penalty / norm, 0.0)
        output[indices] = factor * thresholded[indices]
    return output


def solve_nonnegative_sparse_group(
    residual,
    quality_weights,
    *,
    l1_penalties,
    groups,
    max_iterations: int,
    tolerance: float,
) -> FISTAResult:
    residual = np.asarray(residual, dtype=float)
    weights = np.asarray(quality_weights, dtype=float)
    l1 = np.asarray(l1_penalties, dtype=float)
    if residual.ndim != 1 or weights.shape != residual.shape or l1.shape != residual.shape:
        raise FinalFISTAError("FISTA vectors are not aligned")
    if residual.size == 0:
        raise FinalFISTAError("FISTA requires at least one root coordinate")
    if not np.isfinite(residual).all() or not np.isfinite(weights).all() \
            or not np.isfinite(l1).all():
        raise FinalFISTAError("FISTA inputs must be finite")
    if np.any(weights < 0) or np.any(weights > 1) or np.any(l1 < 0):
        raise FinalFISTAError("FISTA weights and penalties are out of range")
    normalized_groups = []
    covered = []
    for indices, penalty in groups:
        index = np.asarray(tuple(indices), dtype=int)
        if index.size == 0 or np.any(index < 0) or np.any(index >= residual.size):
            raise FinalFISTAError("FISTA group indices are invalid")
        if not math.isfinite(float(penalty)) or penalty < 0:
            raise FinalFISTAError("FISTA group penalty is invalid")
        normalized_groups.append((index, float(penalty)))
        covered.extend(index.tolist())
    if sorted(covered) != list(range(residual.size)) or len(covered) != len(set(covered)):
        raise FinalFISTAError("FISTA groups must be a non-overlapping complete partition")
    lipschitz = float(np.max(weights))
    if lipschitz <= 0:
        raise FinalFISTAError("FISTA has no positive-quality root coordinate")
    step = 1.0 / lipschitz
    theta = np.zeros_like(residual)
    accelerated = theta.copy()
    momentum = 1.0
    objective = _objective(theta, residual, weights, l1, normalized_groups)
    converged = False
    iteration = 0
    for iteration in range(1, max_iterations + 1):
        gradient = weights * (accelerated - residual)
        candidate = _prox(
            accelerated - step * gradient, step, l1, normalized_groups,
        )
        if not np.isfinite(candidate).all():
            raise FinalFISTAError("FISTA produced a non-finite iterate")
        candidate_objective = _objective(
            candidate, residual, weights, l1, normalized_groups,
        )
        if candidate_objective > objective + 1.0e-12:
            accelerated = theta.copy()
            momentum = 1.0
            gradient = weights * (accelerated - residual)
            candidate = _prox(
                accelerated - step * gradient, step, l1, normalized_groups,
            )
            candidate_objective = _objective(
                candidate, residual, weights, l1, normalized_groups,
            )
        difference = float(np.linalg.norm(candidate - theta))
        scale = max(1.0, float(np.linalg.norm(theta)))
        previous = theta
        theta = candidate
        objective = candidate_objective
        if difference <= tolerance * scale:
            converged = True
            break
        next_momentum = (1.0 + math.sqrt(1.0 + 4.0 * momentum * momentum)) / 2.0
        accelerated = theta + (momentum - 1.0) / next_momentum * (theta - previous)
        momentum = next_momentum
    return FISTAResult(
        theta=tuple(float(value) for value in theta),
        converged=converged,
        iterations=iteration,
        objective=float(objective),
        lipschitz=lipschitz,
    )

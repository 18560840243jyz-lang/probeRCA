"""Canonical P8 weighted sparse-group solver API."""

from .fista import (
    FISTABacktrackingError,
    FISTAConvergenceError,
    FISTANumericalError,
    FISTAWarmStartError,
    solve_weighted_joint_problem,
)
from .objective import FISTAProblemValidationError, evaluate_objective
from .proximal import compute_prox
from .solver_result import (
    FISTAFingerprintError,
    FISTAResultSerializationError,
    FISTASolverResult,
    load_fista_result,
    save_fista_result,
)

__all__ = [
    "solve_weighted_joint_problem", "evaluate_objective", "compute_prox",
    "save_fista_result", "load_fista_result", "FISTASolverResult",
    "FISTAProblemValidationError", "FISTANumericalError", "FISTABacktrackingError",
    "FISTAWarmStartError", "FISTAConvergenceError", "FISTAResultSerializationError",
    "FISTAFingerprintError",
]

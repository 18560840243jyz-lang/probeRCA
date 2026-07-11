"""Canonical sparse monotone FISTA solver for the P7 weighted joint problem."""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import asdict, replace

import numpy as np

from proberca.config import ProbeRCAConfig, SolverConfig

from .objective import (
    FISTAProblemValidationError,
    PreparedFISTAProblem,
    data_fit_gradient,
    evaluate_objective,
    prepare_problem,
)
from .proximal import compute_prox
from .solver_result import (
    FISTASolverResult,
    compute_result_fingerprint,
    solver_config_fingerprint,
)


class FISTANumericalError(RuntimeError):
    """FISTA encountered a non-finite or otherwise invalid numeric operation."""


class FISTABacktrackingError(FISTANumericalError):
    """No candidate satisfied the sparse majorization condition."""


class FISTAWarmStartError(ValueError):
    """Warm-start variables do not match the current stable P7 layout."""


class FISTAConvergenceError(RuntimeError):
    """Strict convergence was requested but the required conditions were not met."""


def _solver_config(config) -> SolverConfig:
    solver = config.solver if isinstance(config, ProbeRCAConfig) else config
    if not isinstance(solver, SolverConfig):
        raise TypeError("config must be SolverConfig or ProbeRCAConfig")
    if solver.method != "fista":
        raise FISTAProblemValidationError("only the canonical fista method is supported")
    return solver


def _backtracking_step(prepared: PreparedFISTAProblem, point: np.ndarray, lipschitz: float,
                       config: SolverConfig):
    smooth, gradient = data_fit_gradient(prepared, point)
    value = max(float(lipschitz), config.lipschitz_floor)
    if not np.isfinite(value) or value <= 0:
        raise FISTANumericalError("Lipschitz estimate must be finite and positive")
    for step in range(config.max_backtracking_steps + 1):
        with np.errstate(over="raise", divide="raise", invalid="raise"):
            candidate = compute_prox(prepared, point - gradient / value, 1.0 / value)
        candidate_smooth = data_fit_gradient(prepared, candidate)[0]
        displacement = candidate - point
        upper = smooth + float(gradient @ displacement) + 0.5 * value * float(displacement @ displacement)
        tolerance = 1e-12 * max(1.0, abs(smooth), abs(candidate_smooth))
        if candidate_smooth <= upper + tolerance:
            return candidate, value, step
        value *= config.backtracking_factor
        if not np.isfinite(value):
            raise FISTANumericalError("backtracking produced a non-finite Lipschitz estimate")
    raise FISTABacktrackingError("majorization failed within max_backtracking_steps")


def _gradient_mapping(prepared, point, lipschitz):
    _, gradient = data_fit_gradient(prepared, point)
    prox = compute_prox(prepared, point - gradient / lipschitz, 1.0 / lipschitz)
    mapping = lipschitz * (point - prox)
    norm = float(np.linalg.norm(mapping))
    if not np.isfinite(norm):
        raise FISTANumericalError("proximal gradient mapping is non-finite")
    return norm


def _initial_vector(prepared, config, warm_start_result):
    size = prepared.layout.total_size
    issues = []
    if warm_start_result is None:
        return np.zeros(size), False, None, issues
    if not config.warm_start_enabled:
        issues.append({"reason_code": "warm_start_disabled", "detail": "external result ignored"})
        return np.zeros(size), False, None, issues
    if not isinstance(warm_start_result, FISTASolverResult):
        raise FISTAWarmStartError("warm start must be FISTASolverResult")
    problem = prepared.problem
    if (warm_start_result.node_variable_ids != problem.node_variable_ids
            or warm_start_result.propagation_variable_ids != problem.propagation_variable_ids
            or warm_start_result.shock_variable_ids != problem.shock_variable_ids):
        raise FISTAWarmStartError("warm start stable variable IDs do not match")
    vector = np.concatenate((warm_start_result.u_values, warm_start_result.delta_values,
                             warm_start_result.xi_values)).astype(float, copy=True)
    if vector.shape != (size,) or not np.isfinite(vector).all():
        raise FISTAWarmStartError("warm start values are non-finite or misaligned")
    return vector, True, warm_start_result.result_id, issues


def _build_result(prepared, solver, x, *, status, iterations, accepted_iterations,
                  restart_count, backtracking_steps, lipschitz, initial_objective,
                  relative_change, mapping_norm, objective_trace, lipschitz_trace,
                  mapping_trace, warm_used, warm_id, quality_issues, runtime_ms):
    problem, layout = prepared.problem, prepared.layout
    u = x[layout.u_slice].copy(); delta = x[layout.delta_slice].copy(); xi = x[layout.xi_slice].copy()
    node_component = np.asarray(problem.U @ u).reshape(-1)
    propagation_component = np.asarray(problem.X_prop @ delta).reshape(-1)
    shock_component = np.asarray(problem.X_shock @ xi).reshape(-1)
    fitted = node_component + propagation_component + shock_component
    residual = problem.joint_residual - fitted
    weighted_norm = float(math.sqrt(max(0.0, float(residual @ (problem.W @ residual)))))
    objective = evaluate_objective(prepared, x)
    components = asdict(objective)
    converged = status == "converged"
    config_fingerprint = solver_config_fingerprint(solver)
    provisional = FISTASolverResult(
        "1.0", "fista_solver_result", "pending", problem.problem_id,
        problem.problem_fingerprint, problem.alert_id, problem.candidate_id,
        problem.timestamp_ns, status, converged, converged, iterations,
        accepted_iterations, restart_count, backtracking_steps, float(lipschitz),
        float(initial_objective), float(objective.total), float(relative_change),
        float(mapping_norm), u, delta, xi, list(problem.node_variable_ids),
        list(problem.propagation_variable_ids), list(problem.shock_variable_ids),
        node_component, propagation_component, shock_component, fitted, residual,
        weighted_norm, components, [float(value) for value in objective_trace],
        [float(value) for value in lipschitz_trace], [float(value) for value in mapping_trace],
        warm_used, warm_id, config_fingerprint, "pending", float(runtime_ms), quality_issues,
    )
    fingerprint = compute_result_fingerprint(provisional)
    result_id = hashlib.sha256(
        f"{problem.problem_id}:{config_fingerprint}:{fingerprint}".encode()
    ).hexdigest()
    return replace(provisional, result_id=result_id, result_fingerprint=fingerprint)


def solve_weighted_joint_problem(problem, config, warm_start_result=None) -> FISTASolverResult:
    """Solve the exact P7 objective using sparse monotone FISTA with backtracking."""
    started = time.perf_counter()
    prepared = prepare_problem(problem)
    solver = _solver_config(config)
    x, warm_used, warm_id, quality_issues = _initial_vector(
        prepared, solver, warm_start_result
    )
    y = x.copy()
    momentum = 1.0
    lipschitz = max(solver.initial_lipschitz, solver.lipschitz_floor)
    current_objective = evaluate_objective(prepared, x).total
    initial_objective = current_objective
    objective_trace = [current_objective]
    lipschitz_trace = []
    mapping_trace = []
    restart_count = 0
    total_backtracking = 0
    patience = 0
    relative_change = math.inf
    mapping_norm = math.inf
    accepted = 0
    status = "max_iterations"
    for iteration in range(1, solver.max_iterations + 1):
        try:
            candidate, accepted_lipschitz, steps = _backtracking_step(
                prepared, y, lipschitz, solver
            )
        except FISTABacktrackingError:
            raise
        except (FISTAProblemValidationError, FISTANumericalError, FloatingPointError) as error:
            status = "numerical_failure"
            relative_change = 0.0
            mapping_norm = float(np.finfo(float).max)
            quality_issues.append({
                "reason_code": "numerical_failure",
                "detail": f"{type(error).__name__}: {error}",
                "iteration": iteration,
            })
            break
        total_backtracking += steps
        candidate_objective = evaluate_objective(prepared, candidate).total
        numerical_tolerance = 1e-12 * max(1.0, abs(current_objective))
        if candidate_objective > current_objective + numerical_tolerance:
            restart_count += 1
            y = x.copy(); momentum = 1.0
            candidate, accepted_lipschitz, steps = _backtracking_step(
                prepared, y, accepted_lipschitz * solver.backtracking_factor, solver
            )
            total_backtracking += steps + 1
            candidate_objective = evaluate_objective(prepared, candidate).total
            if candidate_objective > current_objective + numerical_tolerance:
                raise FISTABacktrackingError("monotone restart could not produce a decreasing step")
        previous = current_objective
        relative_change = abs(candidate_objective - previous) / max(1.0, abs(previous))
        mapping_norm = _gradient_mapping(prepared, candidate, accepted_lipschitz)
        accepted += 1
        objective_trace.append(candidate_objective)
        lipschitz_trace.append(accepted_lipschitz)
        mapping_trace.append(mapping_norm)
        adaptive_restart = float((candidate - x) @ (y - candidate)) > 0.0
        previous_x = x
        x = candidate
        current_objective = candidate_objective
        if adaptive_restart:
            restart_count += 1
            momentum = 1.0
            y = x.copy()
        else:
            next_momentum = 0.5 * (1.0 + math.sqrt(1.0 + 4.0 * momentum * momentum))
            y = x + ((momentum - 1.0) / next_momentum) * (x - previous_x)
            momentum = next_momentum
        lipschitz = max(accepted_lipschitz, solver.lipschitz_floor)
        mapping_threshold = solver.gradient_mapping_tolerance * max(1.0, float(np.linalg.norm(x)))
        if iteration >= solver.minimum_iterations \
                and relative_change <= solver.objective_tolerance \
                and mapping_norm <= mapping_threshold:
            patience += 1
        else:
            patience = 0
        if patience >= solver.convergence_patience:
            status = "converged"
            break
    result = _build_result(
        prepared, solver, x, status=status, iterations=iteration,
        accepted_iterations=accepted, restart_count=restart_count,
        backtracking_steps=total_backtracking, lipschitz=lipschitz,
        initial_objective=initial_objective, relative_change=relative_change,
        mapping_norm=mapping_norm, objective_trace=objective_trace,
        lipschitz_trace=lipschitz_trace, mapping_trace=mapping_trace,
        warm_used=warm_used, warm_id=warm_id, quality_issues=quality_issues,
        runtime_ms=(time.perf_counter() - started) * 1000.0,
    )
    if result.status == "max_iterations" and solver.strict_convergence:
        raise FISTAConvergenceError(
            f"FISTA did not converge after {result.iterations} iterations; "
            f"mapping={result.gradient_mapping_norm}"
        )
    return result

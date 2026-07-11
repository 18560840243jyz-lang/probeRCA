from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest
from scipy import sparse

from proberca.inversion.solver.fista import FISTAWarmStartError, solve_weighted_joint_problem
from proberca.inversion.solver.objective import FISTAProblemValidationError, evaluate_objective, prepare_problem
from proberca.inversion.solver.solver_result import solver_config_fingerprint

from test_p7_weighted_problem import problem as p7_problem
from test_p8_fista_solver import identity_problem, solver_config, tiny_problem


@pytest.mark.parametrize("residual,penalty", [
    (3.0, 0.1), (3.0, 1.0), (-3.0, 0.1), (-3.0, 1.0), (0.5, 0.1), (0.0, 1.0),
])
def test_converged_solution_satisfies_proximal_fixed_point(residual, penalty):
    value = identity_problem("node", (residual,), (penalty,))
    result = solve_weighted_joint_problem(value, solver_config())
    scale = max(1.0, np.linalg.norm(result.u_values))
    assert result.gradient_mapping_norm <= 1e-8 * scale


@pytest.mark.parametrize("block", ["node", "propagation", "shock"])
@pytest.mark.parametrize("residual", [-2.0, 2.0])
def test_solution_objective_never_exceeds_zero_solution(block, residual):
    value = identity_problem(block, (residual,), (0.2,))
    prepared = prepare_problem(value)
    result = solve_weighted_joint_problem(value, solver_config())
    zero_objective = evaluate_objective(prepared, np.zeros(prepared.layout.total_size)).total
    assert result.final_objective <= zero_objective + 1e-10


@pytest.mark.parametrize("favored", ["node", "propagation", "shock"])
def test_effective_l1_competition_favors_lower_penalty_block(favored):
    column = sparse.csr_matrix([[1.0]])
    penalties = {"node": 2.0, "propagation": 2.0, "shock": 2.0}
    penalties[favored] = 0.1
    value = tiny_problem(
        U=column, X_prop=column, X_shock=column, residual=[3.0],
        lambda_u=[penalties["node"]], lambda_delta=[penalties["propagation"]],
        lambda_xi=[penalties["shock"]],
    )
    result = solve_weighted_joint_problem(value, solver_config())
    magnitudes = {"node": abs(result.u_values[0]), "propagation": abs(result.delta_values[0]),
                  "shock": abs(result.xi_values[0])}
    assert magnitudes[favored] == max(magnitudes.values())
    assert magnitudes[favored] > 1.0


@pytest.mark.parametrize("group_penalty", [0.0, 0.5, 2.0, 10.0])
def test_node_group_penalty_monotonically_suppresses_group(group_penalty):
    value = tiny_problem(
        U=sparse.eye(2, format="csr"), X_prop=sparse.csr_matrix((2, 0)),
        X_shock=sparse.csr_matrix((2, 0)), residual=[3.0, 4.0],
        lambda_u=[0.1, 0.1], node_group=group_penalty,
    )
    result = solve_weighted_joint_problem(value, solver_config())
    expected_soft = np.asarray([2.9, 3.9])
    expected = max(1 - group_penalty / np.linalg.norm(expected_soft), 0) * expected_soft
    assert result.u_values == pytest.approx(expected, abs=1e-7)


def test_shock_group_penalty_suppresses_same_edge_group():
    base = tiny_problem(
        U=sparse.csr_matrix((2, 0)), X_prop=sparse.csr_matrix((2, 0)),
        X_shock=sparse.eye(2, format="csr"), residual=[3.0, 4.0],
        lambda_xi=[0.1, 0.1], shock_group=0.0,
    )
    penalized = replace(base, lambda_shock_group=2.0)
    from test_p8_fista_solver import _fingerprint_for
    penalized = replace(penalized, problem_fingerprint=_fingerprint_for(penalized))
    low = solve_weighted_joint_problem(base, solver_config())
    high = solve_weighted_joint_problem(penalized, solver_config())
    assert np.linalg.norm(high.xi_values) < np.linalg.norm(low.xi_values)


def test_adaptive_restart_is_triggered_on_deterministic_correlated_problem():
    rng = np.random.default_rng(0)
    design = rng.normal(size=(6, 3)); truth = rng.normal(size=3)
    value = tiny_problem(
        U=sparse.csr_matrix(design), X_prop=sparse.csr_matrix((6, 0)),
        X_shock=sparse.csr_matrix((6, 0)), residual=design @ truth,
        lambda_u=[0.01] * 3,
    )
    result = solve_weighted_joint_problem(value, solver_config(initial_lipschitz=0.01))
    assert result.restart_count > 0
    assert all(a + 1e-10 >= b for a, b in zip(result.objective_trace, result.objective_trace[1:]))


def test_two_dimensional_solution_matches_independent_brute_force_grid():
    design = np.asarray([[1.0, 0.4], [0.2, 1.0]])
    residual = np.asarray([1.4, -0.8])
    value = tiny_problem(
        U=sparse.csr_matrix(design), X_prop=sparse.csr_matrix((2, 0)),
        X_shock=sparse.csr_matrix((2, 0)), residual=residual,
        lambda_u=[0.15, 0.2],
    )
    result = solve_weighted_joint_problem(value, solver_config())
    grid = np.linspace(-2.0, 2.0, 801)
    best = float("inf")
    for first in grid:
        predictions = design[:, 0, None] * first + design[:, 1, None] * grid[None, :]
        errors = residual[:, None] - predictions
        objectives = 0.5 * np.sum(errors * errors, axis=0) + 0.15 * abs(first) + 0.2 * np.abs(grid)
        best = min(best, float(np.min(objectives)))
    assert result.final_objective <= best + 2e-4


def test_three_dimensional_solution_matches_independent_slow_ista_reference():
    design = np.asarray([[1.0, 0.2, 0.0], [0.1, 1.0, 0.3], [0.0, 0.2, 1.0]])
    residual = np.asarray([1.3, -0.7, 2.1]); penalties = np.asarray([0.1, 0.2, 0.15])
    value = tiny_problem(
        U=sparse.csr_matrix(design), X_prop=sparse.csr_matrix((3, 0)),
        X_shock=sparse.csr_matrix((3, 0)), residual=residual, lambda_u=penalties,
    )
    result = solve_weighted_joint_problem(value, solver_config())
    reference = np.zeros(3); step = 0.99 / (np.linalg.norm(design, 2) ** 2)
    for _ in range(100000):
        gradient = design.T @ (design @ reference - residual)
        candidate = np.sign(reference - step * gradient) * np.maximum(
            np.abs(reference - step * gradient) - step * penalties, 0)
        if np.linalg.norm(candidate - reference) < 1e-13:
            reference = candidate; break
        reference = candidate
    assert result.u_values == pytest.approx(reference, abs=2e-7)


def test_solver_does_not_mutate_p7_numeric_inputs():
    value = p7_problem()
    residual = value.joint_residual.copy(); u_data = value.U.data.copy()
    prop_data = value.X_prop.data.copy(); shock_data = value.X_shock.data.copy()
    penalties = (value.lambda_u_effective.copy(), value.lambda_delta_effective.copy(),
                 value.lambda_xi_effective.copy())
    solve_weighted_joint_problem(value, solver_config())
    assert np.array_equal(value.joint_residual, residual)
    assert np.array_equal(value.U.data, u_data)
    assert np.array_equal(value.X_prop.data, prop_data)
    assert np.array_equal(value.X_shock.data, shock_data)
    assert all(np.array_equal(before, after) for before, after in zip(
        penalties, (value.lambda_u_effective, value.lambda_delta_effective,
                    value.lambda_xi_effective)))


@pytest.mark.parametrize("weights", [[0.0], [-1.0], [np.nan], [np.inf]])
def test_non_positive_or_non_finite_weight_is_rejected(weights):
    value = identity_problem("node")
    object.__setattr__(value, "W", sparse.diags(weights, format="csr"))
    with pytest.raises(FISTAProblemValidationError):
        solve_weighted_joint_problem(value, solver_config())


def test_non_finite_sparse_dictionary_is_rejected():
    value = identity_problem("node")
    corrupted = replace(value, U=sparse.csr_matrix([[np.nan]]))
    with pytest.raises(FISTAProblemValidationError):
        solve_weighted_joint_problem(corrupted, solver_config())


def test_runtime_numerical_failure_returns_explicit_unusable_status():
    value = tiny_problem(
        U=sparse.csr_matrix([[1e308]]), X_prop=sparse.csr_matrix((1, 0)),
        X_shock=sparse.csr_matrix((1, 0)), residual=[1.0], lambda_u=[0.1],
    )
    result = solve_weighted_joint_problem(value, solver_config())
    assert result.status == "numerical_failure"
    assert not result.converged and not result.solver_usable
    assert any(issue["reason_code"] == "numerical_failure" for issue in result.quality_issues)


def test_problem_fingerprint_mismatch_is_rejected():
    value = replace(p7_problem(), problem_fingerprint="f" * 64)
    with pytest.raises(FISTAProblemValidationError):
        solve_weighted_joint_problem(value, solver_config())


def test_nonfinite_warm_start_is_rejected():
    value = p7_problem(); warm = solve_weighted_joint_problem(value, solver_config())
    object.__setattr__(warm, "u_values", np.full_like(warm.u_values, np.nan))
    with pytest.raises(FISTAWarmStartError):
        solve_weighted_joint_problem(value, solver_config(), warm_start_result=warm)


@pytest.mark.parametrize("change", [
    {"objective_tolerance": 1e-9}, {"gradient_mapping_tolerance": 1e-5},
    {"backtracking_factor": 3.0}, {"max_iterations": 3000},
])
def test_solver_config_fingerprint_changes_deterministically(change):
    base = solver_config(); modified = solver_config(**change)
    assert solver_config_fingerprint(base) != solver_config_fingerprint(modified)
    assert solver_config_fingerprint(modified) == solver_config_fingerprint(modified)


@pytest.mark.parametrize("forbidden_field", [
    "ranking", "fault_mode", "confidence", "counterfactual", "propagation_paths",
])
def test_solver_result_contains_no_p9_semantics(forbidden_field):
    result = solve_weighted_joint_problem(p7_problem(), solver_config())
    assert not hasattr(result, forbidden_field)

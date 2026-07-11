from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest
from scipy import sparse

from proberca.config import ProbeRCAConfig, SolverConfig
from proberca.inversion.groups import VariableGroup
from proberca.inversion.solver.objective import (
    FISTAProblemValidationError,
    data_fit_gradient,
    evaluate_objective,
    prepare_problem,
)
from proberca.inversion.solver.proximal import compute_prox, sparse_group_prox_block

from test_p7_evidence_weighting import config as p7_config
from test_p7_weighted_problem import problem


def solver_config(**changes):
    payload = p7_config().to_dict()["solver"]
    payload.update(changes)
    return SolverConfig.from_dict(payload)


def test_p7_problem_exposes_stable_variable_ids_for_all_blocks():
    value = problem()
    assert value.node_variable_ids == [
        "cluster-a::ns::api::cpu.use", "cluster-a::ns::api::request.lat",
        "cluster-a::ns::db::cpu.use", "cluster-a::ns::db::request.lat",
    ]
    assert len(value.propagation_variable_ids) == value.X_prop.shape[1]
    assert value.shock_variable_ids == ["cluster-a::ns::api->db::tcp::shock::tcp.retrans_rate"]


def test_sparse_block_layout_and_design_matrix_are_exact():
    prepared = prepare_problem(problem())
    assert prepared.layout.u_slice == slice(0, 4)
    assert prepared.layout.delta_slice == slice(4, 5)
    assert prepared.layout.xi_slice == slice(5, 6)
    assert prepared.layout.total_size == 6
    assert sparse.isspmatrix_csr(prepared.B)
    assert prepared.B.shape == (5, 6)
    assert prepared.B.nnz == problem().U.nnz + problem().X_prop.nnz + problem().X_shock.nnz


def test_data_fit_and_gradient_match_manual_sparse_formula():
    prepared = prepare_problem(problem())
    theta = np.asarray([0.2, -0.3, 0.4, 0.1, -0.5, 0.7])
    data_fit, gradient = data_fit_gradient(prepared, theta)
    error = prepared.B @ theta - prepared.problem.joint_residual
    assert data_fit == pytest.approx(0.5 * float(error @ (prepared.problem.W @ error)))
    assert np.allclose(gradient, prepared.B.T @ (prepared.problem.W @ error))
    epsilon = 1e-6
    for index in range(theta.size):
        direction = np.zeros_like(theta); direction[index] = epsilon
        plus = data_fit_gradient(prepared, theta + direction)[0]
        minus = data_fit_gradient(prepared, theta - direction)[0]
        assert gradient[index] == pytest.approx((plus - minus) / (2 * epsilon), abs=1e-5)


def test_objective_breakdown_sums_exactly_and_uses_each_block_penalty():
    prepared = prepare_problem(problem())
    theta = np.asarray([0.2, -0.3, 0.4, 0.1, -0.5, 0.7])
    result = evaluate_objective(prepared, theta)
    parts = [
        result.data_fit, result.node_l1, result.node_group,
        result.propagation_l1, result.propagation_group,
        result.shock_l1, result.shock_group,
    ]
    assert result.total == pytest.approx(sum(parts))
    assert result.node_l1 == pytest.approx(
        float(np.dot(prepared.problem.lambda_u_effective, np.abs(theta[:4]))))
    assert result.propagation_l1 == pytest.approx(
        prepared.problem.lambda_delta_effective[0] * abs(theta[4]))
    assert result.shock_l1 == pytest.approx(
        prepared.problem.lambda_xi_effective[0] * abs(theta[5]))


@pytest.mark.parametrize("value,penalty,expected", [
    (3.0, 1.0, 2.0), (-3.0, 1.0, -2.0), (0.5, 1.0, 0.0),
    (-0.5, 1.0, 0.0), (2.0, 0.0, 2.0), (-2.0, 0.0, -2.0),
])
def test_singleton_weighted_soft_threshold_closed_form(value, penalty, expected):
    result = sparse_group_prox_block(
        np.asarray([value]), 1.0, np.asarray([penalty]),
        [VariableGroup("node", "g", [0])], 0.0,
    )
    assert result.tolist() == pytest.approx([expected])


def test_group_only_and_l1_plus_group_closed_forms():
    group = [VariableGroup("node", "g", [0, 1])]
    group_only = sparse_group_prox_block(
        np.asarray([3.0, 4.0]), 1.0, np.zeros(2), group, 2.0)
    assert group_only.tolist() == pytest.approx([1.8, 2.4])
    both = sparse_group_prox_block(
        np.asarray([4.0, 3.0]), 1.0, np.asarray([1.0, 1.0]), group, 1.0)
    soft = np.asarray([3.0, 2.0]); expected = (1 - 1 / np.linalg.norm(soft)) * soft
    assert both.tolist() == pytest.approx(expected.tolist())


@pytest.mark.parametrize("values,l1,group_penalty", [
    ([0.5, -0.5], [1.0, 1.0], 1.0),
    ([1.0, -1.0], [1.0, 1.0], 2.0),
    ([0.0, 0.0], [0.1, 0.2], 0.5),
])
def test_prox_can_shrink_complete_group_to_zero(values, l1, group_penalty):
    result = sparse_group_prox_block(
        np.asarray(values), 1.0, np.asarray(l1),
        [VariableGroup("node", "g", [0, 1])], group_penalty)
    assert np.array_equal(result, np.zeros(2))


def test_integrated_prox_uses_three_distinct_penalty_blocks():
    prepared = prepare_problem(problem())
    vector = np.full(6, 20.0)
    result = compute_prox(prepared, vector, 0.1)
    assert result.shape == (6,) and np.isfinite(result).all()
    assert result[0] != result[4] and result[4] != result[5]


def test_empty_block_prox_is_legal():
    assert sparse_group_prox_block(
        np.asarray([]), 1.0, np.asarray([]), [], 1.0).shape == (0,)


@pytest.mark.parametrize("mutation", ["overlap", "missing", "empty", "wrong_type"])
def test_invalid_group_partition_fails(mutation):
    if mutation == "overlap": groups = [VariableGroup("node", "a", [0]), VariableGroup("node", "b", [0, 1])]
    elif mutation == "missing": groups = [VariableGroup("node", "a", [0])]
    elif mutation == "empty":
        object.__new__(VariableGroup)
        groups = []
    else: groups = [VariableGroup("shock", "a", [0, 1])]
    with pytest.raises((ValueError, FISTAProblemValidationError)):
        sparse_group_prox_block(np.asarray([1.0, 2.0]), 1.0, np.ones(2), groups, 1.0,
                                expected_type="node")


@pytest.mark.parametrize("field,value", [
    ("method", "admm"), ("max_iterations", 0),
    ("objective_tolerance", 0.0), ("gradient_mapping_tolerance", 0.0),
    ("backtracking_factor", 1.0), ("max_backtracking_steps", 0),
    ("initial_lipschitz", 0.0), ("lipschitz_floor", 0.0),
    ("monotone", False), ("adaptive_restart", False),
    ("minimum_iterations", 0), ("convergence_patience", 0),
    ("strict_convergence", "yes"), ("warm_start_enabled", "yes"),
    ("diagnostic_zero_tolerance", -1.0),
])
def test_solver_config_strict_validation(field, value):
    payload = p7_config().to_dict()["solver"]
    payload.update({
        "objective_tolerance": 1e-8, "gradient_mapping_tolerance": 1e-6,
        "backtracking_factor": 2.0, "max_backtracking_steps": 30,
        "initial_lipschitz": 1.0, "lipschitz_floor": 1e-12,
        "monotone": True, "adaptive_restart": True, "minimum_iterations": 2,
        "convergence_patience": 2, "warm_start_enabled": True,
        "strict_convergence": True, "diagnostic_zero_tolerance": 1e-12,
    })
    payload[field] = value
    with pytest.raises((TypeError, ValueError)):
        SolverConfig.from_dict(payload)

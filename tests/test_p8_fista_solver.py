from __future__ import annotations

import json
from dataclasses import asdict, replace

import numpy as np
import pytest
from scipy import sparse

from proberca.config import SolverConfig
from proberca.inversion.solver.fista import (
    FISTABacktrackingError,
    FISTAConvergenceError,
    FISTAWarmStartError,
    solve_weighted_joint_problem,
)
from proberca.inversion.groups import VariableGroup
from proberca.inversion.solver.objective import prepare_problem
from proberca.inversion.solver.solver_result import (
    FISTAFingerprintError,
    load_fista_result,
    save_fista_result,
)
from proberca.inversion.weighted_problem import _fingerprint, _group_payload, _sparse_payload

from test_p7_evidence_weighting import config as p7_config
from test_p7_weighted_problem import problem as p7_problem


def solver_config(**changes):
    payload = p7_config().to_dict()["solver"]
    payload.update({
        "max_iterations": 2000,
        "objective_tolerance": 1e-12,
        "gradient_mapping_tolerance": 1e-8,
        "backtracking_factor": 2.0,
        "max_backtracking_steps": 80,
        "initial_lipschitz": 0.25,
        "lipschitz_floor": 1e-12,
        "monotone": True,
        "adaptive_restart": True,
        "minimum_iterations": 2,
        "convergence_patience": 2,
        "warm_start_enabled": True,
        "strict_convergence": True,
        "diagnostic_zero_tolerance": 1e-12,
    })
    payload.update(changes)
    return SolverConfig.from_dict(payload)


def _fingerprint_for(value):
    return _fingerprint({
        "p6_structure_fingerprint": value.p6_structure_fingerprint,
        "W": _sparse_payload(value.W),
        "node_h": value.node_evidence_h.tolist(),
        "propagation_h": value.propagation_evidence_h.tolist(),
        "shock_h": value.shock_evidence_h.tolist(),
        "lambda_u_effective": value.lambda_u_effective.tolist(),
        "lambda_delta_effective": value.lambda_delta_effective.tolist(),
        "lambda_xi_effective": value.lambda_xi_effective.tolist(),
        "node_groups": _group_payload(value.node_groups),
        "propagation_groups": _group_payload(value.propagation_groups),
        "shock_groups": _group_payload(value.shock_groups),
        "group_penalties": [value.lambda_node_group, value.lambda_propagation_group,
                            value.lambda_shock_group],
        "config_fingerprint": value.config_fingerprint,
        "evidence_fingerprint": value.evidence_fingerprint,
    })


def tiny_problem(*, U, X_prop, X_shock, residual, weights=None,
                 lambda_u=None, lambda_delta=None, lambda_xi=None,
                 node_group=0.0, propagation_group=0.0, shock_group=0.0):
    base = p7_problem()
    U, X_prop, X_shock = U.tocsr(), X_prop.tocsr(), X_shock.tocsr()
    residual = np.asarray(residual, dtype=float)
    n_u, n_delta, n_xi = U.shape[1], X_prop.shape[1], X_shock.shape[1]
    n_rows = residual.size
    weights = np.ones(n_rows) if weights is None else np.asarray(weights, dtype=float)
    groups = lambda kind, size: [] if size == 0 else [VariableGroup(kind, f"{kind}-g", list(range(size)))]
    value = replace(
        base,
        joint_residual=residual, U=U, X_prop=X_prop, X_shock=X_shock,
        W=sparse.diags(weights, format="csr"),
        node_variable_ids=[f"node-{i}" for i in range(n_u)],
        propagation_variable_ids=[f"prop-{i}" for i in range(n_delta)],
        shock_variable_ids=[f"shock-{i}" for i in range(n_xi)],
        node_evidence_h=np.zeros(n_u), propagation_evidence_h=np.zeros(n_delta),
        shock_evidence_h=np.zeros(n_xi), node_incoming_prop_h=np.zeros(n_u),
        node_projected_shock_h=np.zeros(n_u), node_quality_weights=np.ones(n_u),
        edge_quality_weights=np.ones(n_rows - n_u),
        lambda_u_effective=np.full(n_u, 0.1) if lambda_u is None else np.asarray(lambda_u, float),
        lambda_delta_effective=np.full(n_delta, 0.1) if lambda_delta is None else np.asarray(lambda_delta, float),
        lambda_xi_effective=np.full(n_xi, 0.1) if lambda_xi is None else np.asarray(lambda_xi, float),
        node_groups=groups("node", n_u), propagation_groups=groups("propagation", n_delta),
        shock_groups=groups("shock", n_xi), lambda_node_group=node_group,
        lambda_propagation_group=propagation_group, lambda_shock_group=shock_group,
        problem_fingerprint="0" * 64,
    )
    return replace(value, problem_fingerprint=_fingerprint_for(value))


def identity_problem(block="node", residual=(3.0,), penalty=(1.0,), weights=None):
    rows = len(residual)
    zero = sparse.csr_matrix((rows, 0))
    eye = sparse.eye(rows, format="csr")
    matrices = {"node": (eye, zero, zero), "propagation": (zero, eye, zero),
                "shock": (zero, zero, eye)}
    kwargs = {"lambda_u": None, "lambda_delta": None, "lambda_xi": None}
    kwargs[{"node": "lambda_u", "propagation": "lambda_delta", "shock": "lambda_xi"}[block]] = penalty
    return tiny_problem(U=matrices[block][0], X_prop=matrices[block][1],
                        X_shock=matrices[block][2], residual=residual, weights=weights, **kwargs)


@pytest.mark.parametrize("block", ["node", "propagation", "shock"])
def test_identity_lasso_closed_form_for_each_block(block):
    result = solve_weighted_joint_problem(identity_problem(block), solver_config())
    values = {"node": result.u_values, "propagation": result.delta_values,
              "shock": result.xi_values}[block]
    assert values == pytest.approx([2.0], abs=1e-7)
    assert result.converged and result.solver_usable


@pytest.mark.parametrize("residual,penalty,expected", [
    (3.0, 1.0, 2.0), (-3.0, 1.0, -2.0), (0.5, 1.0, 0.0),
    (-0.5, 1.0, 0.0), (0.0, 1.0, 0.0), (3.0, 5.0, 0.0),
])
def test_node_identity_lasso_numeric_cases(residual, penalty, expected):
    result = solve_weighted_joint_problem(
        identity_problem("node", (residual,), (penalty,)), solver_config())
    assert result.u_values[0] == pytest.approx(expected, abs=1e-7)


@pytest.mark.parametrize("weight,residual,penalty,expected", [
    (2.0, 3.0, 1.0, 2.5), (4.0, 3.0, 2.0, 2.5),
    (0.5, 3.0, 1.0, 1.0), (3.0, -2.0, 1.5, -1.5),
])
def test_weighted_identity_lasso_closed_form(weight, residual, penalty, expected):
    result = solve_weighted_joint_problem(identity_problem(
        "node", (residual,), (penalty,), weights=(weight,)), solver_config())
    assert result.u_values[0] == pytest.approx(expected, abs=1e-7)


@pytest.mark.parametrize("block", ["node", "propagation", "shock"])
def test_empty_other_blocks_preserve_fixed_layout(block):
    value = identity_problem(block)
    result = solve_weighted_joint_problem(value, solver_config())
    assert len(result.u_values) == value.U.shape[1]
    assert len(result.delta_values) == value.X_prop.shape[1]
    assert len(result.xi_values) == value.X_shock.shape[1]


def test_three_orthogonal_blocks_have_independent_closed_form_solution():
    value = tiny_problem(
        U=sparse.csr_matrix([[1.0], [0.0], [0.0]]),
        X_prop=sparse.csr_matrix([[0.0], [1.0], [0.0]]),
        X_shock=sparse.csr_matrix([[0.0], [0.0], [1.0]]),
        residual=[3.0, -4.0, 5.0], lambda_u=[1.0], lambda_delta=[2.0], lambda_xi=[3.0],
    )
    result = solve_weighted_joint_problem(value, solver_config())
    assert result.u_values == pytest.approx([2.0], abs=1e-7)
    assert result.delta_values == pytest.approx([-2.0], abs=1e-7)
    assert result.xi_values == pytest.approx([2.0], abs=1e-7)


def test_two_variable_sparse_group_matches_closed_form():
    value = tiny_problem(
        U=sparse.eye(2, format="csr"), X_prop=sparse.csr_matrix((2, 0)),
        X_shock=sparse.csr_matrix((2, 0)), residual=[4.0, 3.0],
        lambda_u=[1.0, 1.0], node_group=1.0,
    )
    expected_soft = np.asarray([3.0, 2.0])
    expected = (1 - 1 / np.linalg.norm(expected_soft)) * expected_soft
    result = solve_weighted_joint_problem(value, solver_config())
    assert result.u_values == pytest.approx(expected, abs=1e-7)


def test_backtracking_produces_monotone_accepted_objective_trace():
    result = solve_weighted_joint_problem(identity_problem("node"), solver_config(initial_lipschitz=1e-6))
    assert result.total_backtracking_steps > 0
    assert all(a + 1e-10 >= b for a, b in zip(result.objective_trace, result.objective_trace[1:]))
    assert all(np.isfinite(result.lipschitz_trace)) and min(result.lipschitz_trace) > 0


def test_backtracking_limit_is_explicit_failure():
    with pytest.raises(FISTABacktrackingError):
        solve_weighted_joint_problem(identity_problem("node"), solver_config(
            initial_lipschitz=1e-15, max_backtracking_steps=1))


def test_max_iterations_is_unusable_in_non_strict_mode():
    result = solve_weighted_joint_problem(identity_problem("node"), solver_config(
        max_iterations=1, minimum_iterations=1, convergence_patience=2,
        strict_convergence=False))
    assert result.status == "max_iterations"
    assert not result.converged and not result.solver_usable


def test_max_iterations_raises_in_strict_mode():
    with pytest.raises(FISTAConvergenceError):
        solve_weighted_joint_problem(identity_problem("node"), solver_config(
            max_iterations=1, minimum_iterations=1, convergence_patience=2,
            strict_convergence=True))


@pytest.mark.parametrize("minimum,patience", [(3, 1), (1, 3), (3, 3), (5, 2)])
def test_minimum_iterations_and_patience_are_enforced(minimum, patience):
    result = solve_weighted_joint_problem(identity_problem("node", (0.0,), (1.0,)),
                                          solver_config(minimum_iterations=minimum,
                                                        convergence_patience=patience))
    assert result.iterations >= max(minimum, patience)
    assert result.gradient_mapping_norm <= 1e-8


def test_edge_shock_zero_parent_case_selects_xi_and_covers_both_rows():
    value = tiny_problem(
        U=sparse.csr_matrix([[1.0], [0.0]]),
        X_prop=sparse.csr_matrix([[0.0], [0.0]]),
        X_shock=sparse.csr_matrix([[1.0], [1.0]]),
        residual=[2.0, 2.0], lambda_u=[1.5], lambda_delta=[0.5], lambda_xi=[0.1],
    )
    result = solve_weighted_joint_problem(value, solver_config())
    assert result.xi_values[0] > 1.0
    assert result.delta_values[0] == pytest.approx(0.0)
    assert result.shock_component[0] != 0 and result.shock_component[1] != 0
    without_shock = tiny_problem(
        U=value.U, X_prop=value.X_prop, X_shock=sparse.csr_matrix((2, 0)),
        residual=[2.0, 2.0], lambda_u=[1.5], lambda_delta=[0.5], lambda_xi=[],
    )
    no_shock = solve_weighted_joint_problem(without_shock, solver_config())
    assert no_shock.final_objective > result.final_objective + 1.0


def test_components_residual_and_objective_breakdown_are_exact():
    value = p7_problem()
    result = solve_weighted_joint_problem(value, solver_config())
    assert np.allclose(result.node_component, value.U @ result.u_values)
    assert np.allclose(result.propagation_component, value.X_prop @ result.delta_values)
    assert np.allclose(result.shock_component, value.X_shock @ result.xi_values)
    assert np.allclose(result.fitted_values, result.node_component + result.propagation_component + result.shock_component)
    assert np.allclose(result.solver_residual, value.joint_residual - result.fitted_values)
    expected_norm = np.sqrt(result.solver_residual @ (value.W @ result.solver_residual))
    assert result.weighted_residual_norm == pytest.approx(expected_norm)
    assert result.final_objective == pytest.approx(sum(
        value for key, value in result.objective_components.items() if key != "total"))


def test_compatible_warm_start_reaches_same_solution_and_records_provenance():
    value = p7_problem()
    cold = solve_weighted_joint_problem(value, solver_config())
    warm = solve_weighted_joint_problem(value, solver_config(), warm_start_result=cold)
    assert warm.warm_start_used and warm.warm_start_result_id == cold.result_id
    assert warm.final_objective == pytest.approx(cold.final_objective, abs=1e-9)
    assert warm.iterations <= cold.iterations


def test_incompatible_warm_start_ids_fail():
    value = p7_problem()
    result = solve_weighted_joint_problem(value, solver_config())
    corrupted = replace(result, node_variable_ids=list(reversed(result.node_variable_ids)))
    with pytest.raises(FISTAWarmStartError):
        solve_weighted_joint_problem(value, solver_config(), warm_start_result=corrupted)


def test_disabled_warm_start_is_ignored_but_reported():
    value = p7_problem()
    first = solve_weighted_joint_problem(value, solver_config())
    second = solve_weighted_joint_problem(value, solver_config(warm_start_enabled=False),
                                          warm_start_result=first)
    assert not second.warm_start_used
    assert any(issue["reason_code"] == "warm_start_disabled" for issue in second.quality_issues)


def test_result_json_npz_round_trip_and_fingerprint(tmp_path):
    value = p7_problem()
    config = solver_config()
    result = solve_weighted_joint_problem(value, config)
    output = tmp_path / "fista"
    save_fista_result(output, result)
    restored = load_fista_result(output, value, config)
    assert restored.result_fingerprint == result.result_fingerprint
    assert np.array_equal(restored.u_values, result.u_values)
    assert np.array_equal(restored.fitted_values, result.fitted_values)
    assert {item.name for item in output.iterdir()} == {"metadata.json", "arrays.npz"}


@pytest.mark.parametrize("corruption", ["problem", "config", "ids", "objective", "fingerprint"])
def test_result_corruption_fails_fast(tmp_path, corruption):
    value = p7_problem(); config = solver_config()
    result = solve_weighted_joint_problem(value, config)
    output = tmp_path / corruption
    save_fista_result(output, result)
    metadata_path = output / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    if corruption == "problem": metadata["problem_fingerprint"] = "f" * 64
    elif corruption == "config": metadata["config_fingerprint"] = "e" * 64
    elif corruption == "ids": metadata["node_variable_ids"][0] = "bad"
    elif corruption == "objective": metadata["objective_components"]["total"] += 1
    else: metadata["result_fingerprint"] = "d" * 64
    metadata_path.write_text(json.dumps(metadata, sort_keys=True, separators=(",", ":")))
    with pytest.raises(FISTAFingerprintError):
        load_fista_result(output, value, config)


def test_runtime_does_not_affect_result_fingerprint():
    first = solve_weighted_joint_problem(p7_problem(), solver_config())
    second = replace(first, runtime_ms=first.runtime_ms + 1000)
    assert first.result_fingerprint == second.result_fingerprint


@pytest.mark.parametrize("forbidden", [
    "graph_sparse_admm", "evidence_channel", "sklearn", "lstsq", "pinv",
    ".toarray(", ".todense(", ".A", "IncidentLabel", "paymentservice",
    "checkoutservice", "Online Boutique",
])
def test_p8_production_modules_do_not_use_forbidden_fallbacks(forbidden):
    from pathlib import Path
    root = Path(__file__).parents[1] / "proberca" / "inversion" / "solver"
    text = "\n".join((root / name).read_text() for name in (
        "objective.py", "proximal.py", "fista.py", "solver_result.py"))
    assert forbidden not in text

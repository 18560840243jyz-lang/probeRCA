from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

import test_p6_joint_system as p6
from proberca.config import DiagnosisConfig
from proberca.diagnosis.candidates import (
    CandidateOverflowError,
    DiagnosisInputMismatchError,
    SolverResultNotUsableError,
    build_root_candidates,
    validate_diagnosis_inputs,
)
from proberca.diagnosis.counterfactual import (
    CounterfactualNegativeDeltaError,
    build_reduced_problem,
    evaluate_counterfactuals,
)
from proberca.inversion.solver import solve_weighted_joint_problem

from test_p7_evidence_weighting import config as base_config, joint as p7_joint
from test_p7_weighted_problem import problem


def context():
    config = base_config()
    config = replace(config, penalties=replace(
        config.penalties, c_u=0.01, c_delta=0.01, c_xi=0.01,
        group_ratio_u=0.0, group_ratio_delta=0.0, group_ratio_xi=0.0,
    ))
    weighted = problem(cfg=config)
    result = solve_weighted_joint_problem(weighted, config)
    joint = p7_joint()
    return weighted, result, joint, config


def test_diagnosis_config_defaults_are_strict_and_complete():
    value = DiagnosisConfig()
    value.validate()
    assert value.counterfactual_top_k <= value.max_active_candidates
    assert sum((value.confidence_cf_weight, value.confidence_margin_weight,
                value.confidence_quality_weight,
                value.confidence_identifiability_weight)) == pytest.approx(1.0)


@pytest.mark.parametrize("field,value", [
    ("diagnostic_zero_tolerance", -1.0), ("max_active_candidates", 0),
    ("counterfactual_top_k", 0), ("counterfactual_top_k", 101),
    ("counterfactual_numerical_tolerance", -1.0),
    ("propagated_explained_ratio_threshold", 1.1),
    ("max_path_length", 0), ("max_paths_per_root", 0),
    ("path_length_penalty", -0.1), ("minimum_path_edge_support", -0.1),
    ("strong_identifiability_threshold", 0.1),
])
def test_diagnosis_config_rejects_invalid_values(field, value):
    payload = DiagnosisConfig().to_dict(); payload[field] = value
    with pytest.raises((TypeError, ValueError)):
        DiagnosisConfig.from_dict(payload)


def test_valid_inputs_align_real_p6_p7_p8_objects():
    weighted, result, joint, _ = context()
    validate_diagnosis_inputs(weighted, result, joint)


@pytest.mark.parametrize("mutation", ["unusable", "problem", "candidate", "timestamp", "ids"])
def test_input_mismatch_fails_fast(mutation):
    weighted, result, joint, _ = context()
    if mutation == "unusable":
        result = replace(result, status="max_iterations", converged=False, solver_usable=False)
        error = SolverResultNotUsableError
    elif mutation == "problem":
        result = replace(result, problem_id="other")
        error = DiagnosisInputMismatchError
    elif mutation == "candidate":
        object.__setattr__(joint, "candidate_id", "other")
        error = DiagnosisInputMismatchError
    elif mutation == "timestamp":
        object.__setattr__(joint, "timestamp_ns", joint.timestamp_ns + 1)
        error = DiagnosisInputMismatchError
    else:
        result = replace(result, node_variable_ids=list(reversed(result.node_variable_ids)))
        error = DiagnosisInputMismatchError
    with pytest.raises(error):
        validate_diagnosis_inputs(weighted, result, joint)


def test_builds_one_candidate_per_node_and_one_per_nonoverlapping_edge_group():
    weighted, result, joint, config = context()
    candidates = build_root_candidates(weighted, result, joint, config.diagnosis)
    assert len([item for item in candidates if item.candidate_type == "node"]) == weighted.U.shape[1]
    assert len([item for item in candidates if item.edge_subtype == "propagated-edge"]) == len(weighted.propagation_groups)
    assert len([item for item in candidates if item.edge_subtype == "exogenous-edge-shock"]) == len(weighted.shock_groups)


@pytest.mark.parametrize("candidate_type,subtype,mode", [
    ("node", None, "self"),
    ("edge", "propagated-edge", "edge"),
    ("edge", "exogenous-edge-shock", "edge"),
])
def test_candidate_fault_mode_comes_only_from_type(candidate_type, subtype, mode):
    weighted, result, joint, config = context()
    match = next(item for item in build_root_candidates(weighted, result, joint, config.diagnosis)
                 if item.candidate_type == candidate_type and item.edge_subtype == subtype)
    assert match.fault_mode == mode


def test_node_raw_values_are_exact_and_diagnostic_tolerance_does_not_modify_solution():
    weighted, result, joint, config = context()
    diagnosis = replace(config.diagnosis, diagnostic_zero_tolerance=1e6)
    candidates = build_root_candidates(weighted, result, joint, diagnosis)
    nodes = [item for item in candidates if item.candidate_type == "node"]
    by_node = {item.metadata["node_id"]: item.raw_values[0] for item in nodes}
    assert [by_node[node_id] for node_id in weighted.node_variable_ids] == pytest.approx(result.u_values)
    assert not any(item.active for item in nodes)
    assert np.array_equal(result.u_values, solve_weighted_joint_problem(weighted, config).u_values)


@pytest.mark.parametrize("kind", ["node", "propagation", "shock"])
def test_weighted_contribution_energy_uses_same_formula_for_all_types(kind):
    weighted, result, joint, config = context()
    candidates = build_root_candidates(weighted, result, joint, config.diagnosis)
    candidate = next(item for item in candidates if item.variable_block == kind)
    vector = np.asarray(candidate.contribution_vector)
    expected = np.sqrt(vector @ (weighted.W @ vector))
    assert candidate.weighted_contribution_energy == pytest.approx(expected)
    assert np.any(vector != np.abs(vector)) or np.all(vector >= 0)


def test_contribution_energy_does_not_multiply_evidence_or_penalty():
    weighted, result, joint, config = context()
    candidates = build_root_candidates(weighted, result, joint, config.diagnosis)
    node = next(item for item in candidates if item.variable_block == "node")
    index = node.variable_indices[0]
    expected_vector = np.asarray(weighted.U[:, index] @ np.asarray([result.u_values[index]])).reshape(-1)
    assert node.contribution_vector == pytest.approx(expected_vector.tolist())


def test_dominant_member_uses_energy_then_absolute_value_then_stable_id():
    weighted, result, joint, config = context()
    groups = [item for item in build_root_candidates(weighted, result, joint, config.diagnosis)
              if len(item.variable_ids) > 1]
    for item in groups:
        ranking = sorted(item.member_diagnostics,
                         key=lambda value: (-value["energy"], -abs(value["raw_value"]), value["variable_id"]))
        assert item.dominant_member_id == ranking[0]["variable_id"]


def test_candidate_order_is_stable_id_order():
    weighted, result, joint, config = context()
    identifiers = [item.candidate_id for item in build_root_candidates(
        weighted, result, joint, config.diagnosis)]
    assert identifiers == sorted(identifiers)


def test_active_candidate_overflow_fails_without_truncation():
    weighted, result, joint, config = context()
    diagnosis = replace(config.diagnosis, max_active_candidates=1,
                        counterfactual_top_k=1, fail_on_candidate_overflow=True)
    with pytest.raises(CandidateOverflowError):
        build_root_candidates(weighted, result, joint, diagnosis)


@pytest.mark.parametrize("block", ["node", "propagation", "shock"])
def test_reduced_problem_removes_entire_candidate_and_remaps_groups(block):
    weighted, result, joint, config = context()
    candidate = next(item for item in build_root_candidates(weighted, result, joint, config.diagnosis)
                     if item.variable_block == block and item.active)
    reduced, kept = build_reduced_problem(weighted, candidate)
    original_size = {"node": weighted.U.shape[1], "propagation": weighted.X_prop.shape[1],
                     "shock": weighted.X_shock.shape[1]}[block]
    reduced_size = {"node": reduced.U.shape[1], "propagation": reduced.X_prop.shape[1],
                    "shock": reduced.X_shock.shape[1]}[block]
    assert reduced_size == original_size - len(candidate.variable_indices)
    assert not set(candidate.variable_ids) & set(kept[block])
    assert np.array_equal(reduced.joint_residual, weighted.joint_residual)
    assert (reduced.W != weighted.W).nnz == 0


def test_exact_counterfactual_reoptimizes_and_preserves_original_result():
    weighted, result, joint, config = context()
    before = (result.u_values.copy(), result.delta_values.copy(), result.xi_values.copy())
    candidates = build_root_candidates(weighted, result, joint, config.diagnosis)
    evaluated = evaluate_counterfactuals(weighted, result, candidates, config)
    successful = [item for item in evaluated if item.counterfactual_status == "evaluated"]
    assert successful
    assert all(item.counterfactual_solver_result_id for item in successful)
    assert all(item.delta_loss is not None and item.relative_delta_loss is not None for item in successful)
    assert all(item.counterfactual_iterations > 0 for item in successful)
    assert np.array_equal(result.u_values, before[0])
    assert np.array_equal(result.delta_values, before[1])
    assert np.array_equal(result.xi_values, before[2])


def test_counterfactual_budget_marks_unevaluated_without_dropping_candidates():
    weighted, result, joint, config = context()
    diagnosis = replace(config.diagnosis, counterfactual_top_k=1)
    config = replace(config, diagnosis=diagnosis)
    candidates = build_root_candidates(weighted, result, joint, diagnosis)
    evaluated = evaluate_counterfactuals(weighted, result, candidates, config)
    assert len(evaluated) == len(candidates)
    assert sum(item.counterfactual_status == "evaluated" for item in evaluated) == 1
    assert all(item.delta_loss is None for item in evaluated if item.counterfactual_status == "not_evaluated")


def test_negative_delta_beyond_tolerance_fails_fast():
    weighted, result, joint, config = context()
    candidate = next(item for item in build_root_candidates(weighted, result, joint, config.diagnosis)
                     if item.active)
    impossible = result
    object.__setattr__(impossible, "final_objective", result.final_objective + 1e6)
    config = replace(config, solver=replace(config.solver, warm_start_enabled=False))
    with pytest.raises(CounterfactualNegativeDeltaError):
        evaluate_counterfactuals(weighted, impossible, [candidate], config)

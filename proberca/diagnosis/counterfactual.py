"""Exact delete-and-reoptimize counterfactuals using canonical P8 FISTA."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import numpy as np

from proberca.config import ProbeRCAConfig
from proberca.inversion.groups import VariableGroup
from proberca.inversion.solver import solve_weighted_joint_problem
from proberca.inversion.solver.fista import FISTAConvergenceError, FISTANumericalError
from proberca.inversion.weighted_problem import compute_problem_fingerprint

from .contracts import (
    CounterfactualNegativeDeltaError, CounterfactualProblemError,
    CounterfactualSolverError, DiagnosisCandidate,
)


def _remap_groups(groups, kept):
    mapping = {old: new for new, old in enumerate(kept)}
    output = []
    for group in groups:
        indices = [mapping[index] for index in group.indices if index in mapping]
        if indices:
            output.append(VariableGroup(group.variable_type, group.group_key, indices))
    return output


def _kept(size, removed):
    removed_set = set(removed)
    return [index for index in range(size) if index not in removed_set]


def build_reduced_problem(problem, candidate: DiagnosisCandidate):
    if not candidate.active:
        raise CounterfactualProblemError("inactive candidate cannot be counterfactually removed")
    keep_u = _kept(problem.U.shape[1], candidate.variable_indices if candidate.variable_block == "node" else [])
    keep_delta = _kept(problem.X_prop.shape[1], candidate.variable_indices if candidate.variable_block == "propagation" else [])
    keep_xi = _kept(problem.X_shock.shape[1], candidate.variable_indices if candidate.variable_block == "shock" else [])
    layout = {"node": [problem.node_variable_ids[index] for index in keep_u],
              "propagation": [problem.propagation_variable_ids[index] for index in keep_delta],
              "shock": [problem.shock_variable_ids[index] for index in keep_xi]}
    structure = hashlib.sha256((problem.p6_structure_fingerprint + "|" + "|".join(
        layout["node"] + layout["propagation"] + layout["shock"])).encode()).hexdigest()
    reduced = replace(
        problem,
        problem_id="pending", problem_fingerprint="0" * 64,
        p6_structure_fingerprint=structure,
        U=problem.U[:, keep_u].tocsr(), X_prop=problem.X_prop[:, keep_delta].tocsc(),
        X_shock=problem.X_shock[:, keep_xi].tocsc(),
        node_variable_ids=layout["node"], propagation_variable_ids=layout["propagation"],
        shock_variable_ids=layout["shock"],
        node_evidence_h=problem.node_evidence_h[keep_u],
        propagation_evidence_h=problem.propagation_evidence_h[keep_delta],
        shock_evidence_h=problem.shock_evidence_h[keep_xi],
        node_incoming_prop_h=problem.node_incoming_prop_h[keep_u],
        node_projected_shock_h=problem.node_projected_shock_h[keep_u],
        lambda_u_effective=problem.lambda_u_effective[keep_u],
        lambda_delta_effective=problem.lambda_delta_effective[keep_delta],
        lambda_xi_effective=problem.lambda_xi_effective[keep_xi],
        node_groups=_remap_groups(problem.node_groups, keep_u),
        propagation_groups=_remap_groups(problem.propagation_groups, keep_delta),
        shock_groups=_remap_groups(problem.shock_groups, keep_xi),
    )
    fingerprint = compute_problem_fingerprint(reduced)
    problem_id = hashlib.sha256((problem.problem_id + candidate.candidate_id + fingerprint).encode()).hexdigest()
    return replace(reduced, problem_id=problem_id, problem_fingerprint=fingerprint), layout


def _project_warm_start(result, candidate, layout):
    remove_ids = set(candidate.variable_ids)
    u = np.asarray([value for identifier, value in zip(result.node_variable_ids, result.u_values)
                    if identifier not in remove_ids])
    delta = np.asarray([value for identifier, value in zip(result.propagation_variable_ids, result.delta_values)
                        if identifier not in remove_ids])
    xi = np.asarray([value for identifier, value in zip(result.shock_variable_ids, result.xi_values)
                     if identifier not in remove_ids])
    return replace(result, u_values=u, delta_values=delta, xi_values=xi,
                   node_variable_ids=layout["node"],
                   propagation_variable_ids=layout["propagation"],
                   shock_variable_ids=layout["shock"])


def evaluate_counterfactuals(problem, result, candidates, config: ProbeRCAConfig):
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    diagnosis = config.diagnosis; diagnosis.validate()
    active = [item for item in candidates if item.active]
    selected = {item.candidate_id for item in sorted(
        active, key=lambda item: (-item.weighted_contribution_energy, item.candidate_id)
    )[:diagnosis.counterfactual_top_k]}
    output = []
    for candidate in candidates:
        if candidate.candidate_id not in selected:
            output.append(replace(candidate, counterfactual_status="not_evaluated"))
            continue
        try:
            reduced, layout = build_reduced_problem(problem, candidate)
            warm = (_project_warm_start(result, candidate, layout)
                    if config.solver.warm_start_enabled else None)
            counterfactual = solve_weighted_joint_problem(reduced, config, warm_start_result=warm)
            if not counterfactual.converged or not counterfactual.solver_usable:
                raise CounterfactualSolverError("counterfactual FISTA was not usable")
            delta = counterfactual.final_objective - result.final_objective
            issues = list(candidate.quality_issues)
            if delta < -diagnosis.counterfactual_numerical_tolerance:
                raise CounterfactualNegativeDeltaError(
                    f"counterfactual delta {delta} is below numerical tolerance")
            if delta < 0:
                issues.append({"reason_code": "negative_delta_within_tolerance", "raw_delta": delta})
                delta = 0.0
            relative = max(delta, 0.0) / max(1.0, abs(result.final_objective))
            output.append(replace(
                candidate, counterfactual_status="evaluated",
                counterfactual_solver_result_id=counterfactual.result_id,
                counterfactual_iterations=counterfactual.iterations,
                delta_loss=float(delta), relative_delta_loss=float(relative), quality_issues=issues,
            ))
        except CounterfactualNegativeDeltaError:
            raise
        except (FISTAConvergenceError, FISTANumericalError, CounterfactualProblemError,
                CounterfactualSolverError, ValueError) as error:
            output.append(replace(
                candidate, counterfactual_status="counterfactual_unavailable", status="rejected",
                quality_issues=[*candidate.quality_issues, {
                    "reason_code": "counterfactual_unavailable", "detail": str(error)}],
            ))
    return sorted(output, key=lambda item: item.candidate_id)

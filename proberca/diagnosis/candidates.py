"""Deterministic P9 root-candidate construction from the raw P8 solution."""

from __future__ import annotations

import hashlib
import json
import math

import numpy as np

from proberca.config import DiagnosisConfig
from proberca.inversion.contracts import JointInversionSystem
from proberca.inversion.solver.solver_result import FISTASolverResult
from proberca.inversion.weighted_problem import WeightedJointInversionProblem, validate_problem_fingerprint

from .contracts import (
    CandidateConstructionError, CandidateOverflowError, DiagnosisCandidate,
    DiagnosisInputMismatchError, NodeRootCandidate, PropagationEdgeCandidate,
    ShockEdgeCandidate, SolverResultNotUsableError,
)
from .contribution import contribution_and_energy, member_diagnostics


def _sha(value) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(payload).hexdigest()


def _service(node_id: str) -> str:
    parts = node_id.split("::")
    if len(parts) != 4:
        raise CandidateConstructionError(f"invalid node ID {node_id}")
    return "::".join(parts[:3])


def validate_diagnosis_inputs(problem, result, joint_system) -> None:
    if not isinstance(problem, WeightedJointInversionProblem) or not problem.solver_eligible:
        raise DiagnosisInputMismatchError("P9 requires a solver-eligible weighted problem")
    if not isinstance(result, FISTASolverResult) or not result.converged or not result.solver_usable:
        raise SolverResultNotUsableError("P9 requires a converged usable P8 result")
    if not isinstance(joint_system, JointInversionSystem) or not joint_system.solver_eligible:
        raise DiagnosisInputMismatchError("P9 requires a formal P6 joint system")
    validate_problem_fingerprint(problem)
    if result.problem_id != problem.problem_id or result.problem_fingerprint != problem.problem_fingerprint:
        raise DiagnosisInputMismatchError("P8 result does not match P7 problem")
    if (result.node_variable_ids != problem.node_variable_ids
            or result.propagation_variable_ids != problem.propagation_variable_ids
            or result.shock_variable_ids != problem.shock_variable_ids):
        raise DiagnosisInputMismatchError("P8 variable IDs do not match P7 layout")
    identities = (
        (problem.joint_system_id, joint_system.system_id),
        (problem.alert_id, joint_system.alert_id),
        (problem.candidate_id, joint_system.candidate_id),
        (problem.topology_snapshot_id, joint_system.topology_snapshot_id),
        (problem.metric_model_snapshot_id, joint_system.metric_model_snapshot_id),
        (problem.timestamp_ns, joint_system.timestamp_ns),
    )
    if any(left != right for left, right in identities):
        raise DiagnosisInputMismatchError("P6/P7 alert, candidate, topology, model, or time mismatch")
    for left, right in ((problem.U, joint_system.U), (problem.X_prop, joint_system.X_prop),
                        (problem.X_shock, joint_system.X_shock)):
        if left.shape != right.shape or (left != right).nnz:
            raise DiagnosisInputMismatchError("P6/P7 dictionary mismatch")


def _dominant(diagnostics):
    return sorted(diagnostics, key=lambda item: (-item["energy"], -abs(item["raw_value"]),
                                                 item["variable_id"]))[0]["variable_id"]


def _candidate(cls, problem, result, matrix, block, group_id, indices, ids, raw_values,
               metadata, tolerance):
    vector, energy = contribution_and_energy(matrix, indices, raw_values, problem.W)
    diagnostics = member_diagnostics(matrix, indices, raw_values, ids, problem.W)
    subtype = None if block == "node" else (
        "propagated-edge" if block == "propagation" else "exogenous-edge-shock")
    candidate_type = "node" if block == "node" else "edge"
    candidate_id = _sha({"block": block, "group_id": group_id, "variable_ids": ids})
    norm = abs(raw_values[0]) if block == "node" else float(np.linalg.norm(raw_values))
    return cls(
        candidate_id, candidate_type, "self" if block == "node" else "edge", subtype,
        None if block == "node" else ("logical_propagation_relation" if block == "propagation" else "physical_edge"),
        block, group_id, list(indices), list(ids), [float(value) for value in raw_values],
        vector.tolist(), energy, bool(norm > tolerance), _dominant(diagnostics), diagnostics,
        metadata, result.result_id, problem.problem_id,
    )


def build_root_candidates(problem, result, joint_system, config: DiagnosisConfig):
    validate_diagnosis_inputs(problem, result, joint_system)
    if not isinstance(config, DiagnosisConfig):
        raise TypeError("config must be DiagnosisConfig")
    config.validate()
    output = []
    node_group = {index: group.group_key for group in problem.node_groups for index in group.indices}
    for index, (node_id, raw) in enumerate(zip(problem.node_variable_ids, result.u_values)):
        ref = joint_system.node_variable_refs[index]
        metadata = {"node_id": node_id, "service_id": _service(node_id),
                    "metric_name": node_id.split("::")[-1], "source_row_index": ref.row_index}
        output.append(_candidate(NodeRootCandidate, problem, result, problem.U, "node",
                                 node_group[index], [index], [node_id], [raw], metadata,
                                 config.diagnostic_zero_tolerance))
    for group in problem.propagation_groups:
        refs = [joint_system.propagation_variable_refs[index] for index in group.indices]
        ids = [problem.propagation_variable_ids[index] for index in group.indices]
        raw = [result.delta_values[index] for index in group.indices]
        metadata = {
            "parent_service_id": _service(refs[0].parent_node_id),
            "target_service_id": _service(refs[0].target_node_id),
            "relation_types": sorted({item for ref in refs for item in ref.relation_types}),
            "member_parent_node_ids": [ref.parent_node_id for ref in refs],
            "member_target_node_ids": [ref.target_node_id for ref in refs],
            "member_lags": [ref.lag for ref in refs],
            "member_coefficients": [ref.learned_coefficient for ref in refs],
            "target_row_indices": [ref.target_row_index for ref in refs],
        }
        output.append(_candidate(PropagationEdgeCandidate, problem, result, problem.X_prop,
                                 "propagation", group.group_key, group.indices, ids, raw, metadata,
                                 config.diagnostic_zero_tolerance))
    for group in problem.shock_groups:
        refs = [joint_system.shock_variable_refs[index] for index in group.indices]
        ids = [problem.shock_variable_ids[index] for index in group.indices]
        raw = [result.xi_values[index] for index in group.indices]
        metadata = {
            "physical_edge_id": refs[0].physical_edge_id,
            "src_service_id": refs[0].src_service_id,
            "dst_service_id": refs[0].dst_service_id,
            "protocol": refs[0].protocol,
            "member_metric_names": [ref.metric_name for ref in refs],
            "projection_node_ids": sorted({joint_system.node_row_refs[row].object_id
                                           for ref in refs for row in ref.projected_node_rows}),
            "edge_row_indices": [ref.edge_row_index for ref in refs],
            "projection_rows_and_weights": sorted(
                [[row, weight] for ref in refs
                 for row, weight in zip(ref.projected_node_rows, ref.projection_weights)]
            ),
        }
        output.append(_candidate(ShockEdgeCandidate, problem, result, problem.X_shock,
                                 "shock", group.group_key, group.indices, ids, raw, metadata,
                                 config.diagnostic_zero_tolerance))
    output.sort(key=lambda item: item.candidate_id)
    active_count = sum(item.active for item in output)
    if active_count > config.max_active_candidates and config.fail_on_candidate_overflow:
        raise CandidateOverflowError(
            f"active candidates {active_count} exceed limit {config.max_active_candidates}")
    return output

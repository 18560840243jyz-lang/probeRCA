"""Canonical P7 weighted joint problem construction and strict persistence."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
from scipy import sparse

from proberca.config import ProbeRCAConfig
from proberca.evidence.aggregation import aggregate_evidence
from proberca.evidence.propagation_support import compute_propagation_support
from proberca.inversion.contracts import JointInversionSystem
from proberca.inversion.joint_system import _structure_fingerprint

from .groups import GroupPartitionResult, VariableGroup, build_group_partitions
from .penalties import PenaltyResult, compute_penalties
from .quality import build_observation_weights


WEIGHTED_PROBLEM_VERSION = "1"


class WeightedProblemDimensionError(ValueError):
    """Weighted problem arrays, matrices, or partitions do not align."""


class WeightedProblemSerializationError(ValueError):
    """Weighted problem persistence is missing or incompatible."""


class WeightedProblemFingerprintError(ValueError):
    """Weighted problem structure or content fingerprint changed."""


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _fingerprint(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sparse_payload(matrix) -> dict:
    value = matrix.tocsr()
    return {
        "shape": list(value.shape), "data": value.data.tolist(),
        "indices": value.indices.tolist(), "indptr": value.indptr.tolist(),
    }


def _group_payload(groups):
    return [asdict(item) for item in groups]


@dataclass
class WeightedJointInversionProblem:
    schema_version: str
    record_type: str
    problem_id: str
    joint_system_id: str
    alert_id: str
    candidate_id: str
    topology_snapshot_id: str
    metric_model_snapshot_id: str
    timestamp_ns: int
    analysis_cutoff_ns: int
    signal_kind: str
    node_variable_ids: list[str]
    propagation_variable_ids: list[str]
    shock_variable_ids: list[str]
    joint_residual: np.ndarray
    U: sparse.spmatrix
    X_prop: sparse.spmatrix
    X_shock: sparse.spmatrix
    W: sparse.csr_matrix
    node_evidence_h: np.ndarray
    propagation_evidence_h: np.ndarray
    shock_evidence_h: np.ndarray
    node_incoming_prop_h: np.ndarray
    node_projected_shock_h: np.ndarray
    node_quality_weights: np.ndarray
    edge_quality_weights: np.ndarray
    residual_scale_raw: float
    residual_scale_used: float
    lambda_u_base: float
    lambda_delta_base: float
    lambda_xi_base: float
    lambda_u_effective: np.ndarray
    lambda_delta_effective: np.ndarray
    lambda_xi_effective: np.ndarray
    node_groups: list[VariableGroup]
    propagation_groups: list[VariableGroup]
    shock_groups: list[VariableGroup]
    lambda_node_group: float
    lambda_propagation_group: float
    lambda_shock_group: float
    evidence_provenance: list[dict]
    excluded_evidence: list[dict]
    quality_issues: list[dict]
    config_fingerprint: str
    evidence_fingerprint: str
    problem_fingerprint: str
    p6_structure_fingerprint: str
    solver_eligible: bool
    build_duration_ms: float

    def __post_init__(self) -> None:
        if self.schema_version != "1.0" or self.record_type != "weighted_joint_inversion_problem":
            raise WeightedProblemDimensionError("invalid weighted problem type or schema")
        if not self.solver_eligible or self.signal_kind != "signed_z":
            raise WeightedProblemDimensionError("formal weighted problem must be signed_z and solver eligible")
        n_rows = self.joint_residual.size
        n_u, n_delta, n_xi = self.U.shape[1], self.X_prop.shape[1], self.X_shock.shape[1]
        for ids, size, name in (
            (self.node_variable_ids, n_u, "node"),
            (self.propagation_variable_ids, n_delta, "propagation"),
            (self.shock_variable_ids, n_xi, "shock"),
        ):
            if len(ids) != size or len(ids) != len(set(ids)) \
                    or any(not isinstance(item, str) or not item for item in ids):
                raise WeightedProblemDimensionError(f"{name} variable IDs are inconsistent")
        if not isinstance(self.node_quality_weights, np.ndarray) \
                or self.node_quality_weights.ndim != 1 \
                or not np.isfinite(self.node_quality_weights).all():
            raise WeightedProblemDimensionError("node quality weights must be a finite vector")
        node_row_count = self.node_quality_weights.size
        arrays = (
            (self.node_evidence_h, n_u), (self.propagation_evidence_h, n_delta),
            (self.shock_evidence_h, n_xi), (self.node_incoming_prop_h, n_u),
            (self.node_projected_shock_h, n_u),
            (self.edge_quality_weights, n_rows - node_row_count), (self.lambda_u_effective, n_u),
            (self.lambda_delta_effective, n_delta), (self.lambda_xi_effective, n_xi),
        )
        if not isinstance(self.joint_residual, np.ndarray) or not np.isfinite(self.joint_residual).all():
            raise WeightedProblemDimensionError("joint residual must remain a finite vector")
        if node_row_count > n_rows:
            raise WeightedProblemDimensionError("node quality row count exceeds residual rows")
        for values, size in arrays:
            if not isinstance(values, np.ndarray) or values.shape != (size,) or not np.isfinite(values).all():
                raise WeightedProblemDimensionError("weighted problem array dimensions are inconsistent")
        for values in (
            self.node_evidence_h, self.propagation_evidence_h, self.shock_evidence_h,
            self.node_incoming_prop_h, self.node_projected_shock_h,
            self.node_quality_weights, self.edge_quality_weights,
        ):
            if np.any(values < 0) or np.any(values > 1):
                raise WeightedProblemDimensionError("evidence and quality arrays must be in [0, 1]")
        for values in (self.lambda_u_effective, self.lambda_delta_effective, self.lambda_xi_effective):
            if values.size and np.any(values <= 0):
                raise WeightedProblemDimensionError("effective penalties must be strictly positive")
        if not sparse.isspmatrix_csr(self.W) or self.W.shape != (n_rows, n_rows) \
                or self.W.nnz != n_rows:
            raise WeightedProblemDimensionError("W must be a complete sparse diagonal matrix")
        partitions = (
            (self.node_groups, n_u, "node"),
            (self.propagation_groups, n_delta, "propagation"),
            (self.shock_groups, n_xi, "shock"),
        )
        for groups, size, variable_type in partitions:
            indices = [index for group in groups for index in group.indices]
            if any(group.variable_type != variable_type for group in groups) \
                    or sorted(indices) != list(range(size)) or len(indices) != len(set(indices)):
                raise WeightedProblemDimensionError(f"{variable_type} group partition is incomplete")


def _verify_p6_structure(joint_system):
    computed = _structure_fingerprint(
        joint_system.candidate_id, joint_system.metric_model_snapshot_id,
        joint_system.config_fingerprint, joint_system.signal_kind,
        joint_system.node_row_refs, joint_system.edge_row_refs,
        joint_system.node_variable_refs, joint_system.propagation_variable_refs,
        joint_system.shock_variable_refs, joint_system.U,
        joint_system.X_prop, joint_system.X_shock,
    )
    if computed != joint_system.structure_fingerprint:
        raise WeightedProblemFingerprintError("P6 structure fingerprint does not match its matrices")


def _problem_fingerprint(joint_system, W, node_h, propagation_h, shock_h, penalties,
                         groups, config_fingerprint, evidence_fingerprint):
    return _fingerprint({
        "p6_structure_fingerprint": joint_system.structure_fingerprint,
        "W": _sparse_payload(W),
        "node_h": node_h.tolist(), "propagation_h": propagation_h.tolist(),
        "shock_h": shock_h.tolist(),
        "lambda_u_effective": penalties.lambda_u_effective.tolist(),
        "lambda_delta_effective": penalties.lambda_delta_effective.tolist(),
        "lambda_xi_effective": penalties.lambda_xi_effective.tolist(),
        "node_groups": _group_payload(groups.node_groups),
        "propagation_groups": _group_payload(groups.propagation_groups),
        "shock_groups": _group_payload(groups.shock_groups),
        "group_penalties": [penalties.lambda_node_group, penalties.lambda_propagation_group,
                            penalties.lambda_shock_group],
        "config_fingerprint": config_fingerprint,
        "evidence_fingerprint": evidence_fingerprint,
    })


def compute_problem_fingerprint(problem: "WeightedJointInversionProblem") -> str:
    """Compute the deterministic P7/P9 problem fingerprint from read-only inputs."""
    if not isinstance(problem, WeightedJointInversionProblem):
        raise TypeError("problem must be WeightedJointInversionProblem")
    return _fingerprint({
        "p6_structure_fingerprint": problem.p6_structure_fingerprint,
        "W": _sparse_payload(problem.W),
        "node_h": problem.node_evidence_h.tolist(),
        "propagation_h": problem.propagation_evidence_h.tolist(),
        "shock_h": problem.shock_evidence_h.tolist(),
        "lambda_u_effective": problem.lambda_u_effective.tolist(),
        "lambda_delta_effective": problem.lambda_delta_effective.tolist(),
        "lambda_xi_effective": problem.lambda_xi_effective.tolist(),
        "node_groups": _group_payload(problem.node_groups),
        "propagation_groups": _group_payload(problem.propagation_groups),
        "shock_groups": _group_payload(problem.shock_groups),
        "group_penalties": [problem.lambda_node_group, problem.lambda_propagation_group,
                            problem.lambda_shock_group],
        "config_fingerprint": problem.config_fingerprint,
        "evidence_fingerprint": problem.evidence_fingerprint,
    })


def validate_problem_fingerprint(problem: "WeightedJointInversionProblem") -> None:
    """Recompute the persisted fingerprint without changing any numeric input."""
    computed = compute_problem_fingerprint(problem)
    if computed != problem.problem_fingerprint:
        raise WeightedProblemFingerprintError("weighted problem fingerprint mismatch")


def build_weighted_joint_problem(joint_system, evidence_observations, topology_store,
                                 metric_model_training_timestamps, config,
                                 analysis_cutoff_ns):
    started = time.perf_counter()
    if not isinstance(joint_system, JointInversionSystem) or not joint_system.solver_eligible:
        raise WeightedProblemDimensionError("P7 requires a complete solver-eligible P6 system")
    _verify_p6_structure(joint_system)
    if not isinstance(config, ProbeRCAConfig):
        raise TypeError("config must be ProbeRCAConfig")
    before_residual = joint_system.joint_residual.copy()
    before_structure = joint_system.structure_fingerprint
    evidence = aggregate_evidence(joint_system, evidence_observations, config, analysis_cutoff_ns)
    propagation = compute_propagation_support(
        joint_system, topology_store, metric_model_training_timestamps, config
    )
    quality = build_observation_weights(joint_system, config.quality)
    penalties = compute_penalties(
        joint_system, evidence.node_h, propagation.propagation_h,
        evidence.shock_h, quality, config.penalties,
    )
    groups = build_group_partitions(joint_system)
    if not np.array_equal(joint_system.joint_residual, before_residual) \
            or joint_system.structure_fingerprint != before_structure:
        raise WeightedProblemDimensionError("P7 must not modify P6 residuals or dictionaries")
    config_fingerprint = _fingerprint({
        "evidence": asdict(config.evidence), "quality": asdict(config.quality),
        "penalties": asdict(config.penalties),
        "impact_derivation_rules": [asdict(item) for item in config.impact_derivation_rules],
        "allow_cross_namespace": config.candidate_graph.allow_cross_namespace,
    })
    evidence_fingerprint = _fingerprint({
        "analysis_cutoff_ns": analysis_cutoff_ns,
        "included": evidence.evidence_provenance,
        "excluded": evidence.excluded_evidence,
    })
    problem_fingerprint = _problem_fingerprint(
        joint_system, quality.W, evidence.node_h, propagation.propagation_h,
        evidence.shock_h, penalties, groups, config_fingerprint, evidence_fingerprint,
    )
    problem_id = _fingerprint({
        "joint_system_id": joint_system.system_id,
        "analysis_cutoff_ns": analysis_cutoff_ns,
        "problem_fingerprint": problem_fingerprint,
    })
    return WeightedJointInversionProblem(
        "1.0", "weighted_joint_inversion_problem", problem_id, joint_system.system_id,
        joint_system.alert_id, joint_system.candidate_id, joint_system.topology_snapshot_id,
        joint_system.metric_model_snapshot_id, joint_system.timestamp_ns, analysis_cutoff_ns,
        joint_system.signal_kind,
        [item.node_id for item in joint_system.node_variable_refs],
        [item.propagation_id for item in joint_system.propagation_variable_refs],
        [item.shock_id for item in joint_system.shock_variable_refs],
        joint_system.joint_residual, joint_system.U,
        joint_system.X_prop, joint_system.X_shock, quality.W,
        evidence.node_h, propagation.propagation_h, evidence.shock_h,
        penalties.node_incoming_prop_h, penalties.node_projected_shock_h,
        quality.node_weights, quality.edge_weights,
        penalties.residual_scale_raw, penalties.residual_scale_used,
        penalties.lambda_u_base, penalties.lambda_delta_base, penalties.lambda_xi_base,
        penalties.lambda_u_effective, penalties.lambda_delta_effective,
        penalties.lambda_xi_effective, groups.node_groups, groups.propagation_groups,
        groups.shock_groups, penalties.lambda_node_group,
        penalties.lambda_propagation_group, penalties.lambda_shock_group,
        evidence.evidence_provenance, evidence.excluded_evidence, [], config_fingerprint,
        evidence_fingerprint, problem_fingerprint, joint_system.structure_fingerprint,
        True, (time.perf_counter() - started) * 1000.0,
    )


def save_weighted_joint_problem(path, problem):
    if not isinstance(problem, WeightedJointInversionProblem):
        raise TypeError("problem must be WeightedJointInversionProblem")
    output = Path(path)
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "format_version": WEIGHTED_PROBLEM_VERSION,
        "schema_version": problem.schema_version, "record_type": problem.record_type,
        "problem_id": problem.problem_id, "joint_system_id": problem.joint_system_id,
        "alert_id": problem.alert_id, "candidate_id": problem.candidate_id,
        "topology_snapshot_id": problem.topology_snapshot_id,
        "metric_model_snapshot_id": problem.metric_model_snapshot_id,
        "timestamp_ns": problem.timestamp_ns, "analysis_cutoff_ns": problem.analysis_cutoff_ns,
        "signal_kind": problem.signal_kind,
        "node_variable_ids": problem.node_variable_ids,
        "propagation_variable_ids": problem.propagation_variable_ids,
        "shock_variable_ids": problem.shock_variable_ids,
        "node_groups": _group_payload(problem.node_groups),
        "propagation_groups": _group_payload(problem.propagation_groups),
        "shock_groups": _group_payload(problem.shock_groups),
        "evidence_provenance": problem.evidence_provenance,
        "excluded_evidence": problem.excluded_evidence, "quality_issues": problem.quality_issues,
        "config_fingerprint": problem.config_fingerprint,
        "evidence_fingerprint": problem.evidence_fingerprint,
        "problem_fingerprint": problem.problem_fingerprint,
        "p6_structure_fingerprint": problem.p6_structure_fingerprint,
        "solver_eligible": problem.solver_eligible, "build_duration_ms": problem.build_duration_ms,
    }
    (output / "metadata.json").write_bytes(_canonical(metadata))
    np.savez(
        output / "arrays.npz", joint_residual=problem.joint_residual,
        node_evidence_h=problem.node_evidence_h,
        propagation_evidence_h=problem.propagation_evidence_h,
        shock_evidence_h=problem.shock_evidence_h,
        node_incoming_prop_h=problem.node_incoming_prop_h,
        node_projected_shock_h=problem.node_projected_shock_h,
        node_quality_weights=problem.node_quality_weights,
        edge_quality_weights=problem.edge_quality_weights,
        lambda_u_effective=problem.lambda_u_effective,
        lambda_delta_effective=problem.lambda_delta_effective,
        lambda_xi_effective=problem.lambda_xi_effective,
        scalars=np.asarray([
            problem.residual_scale_raw, problem.residual_scale_used,
            problem.lambda_u_base, problem.lambda_delta_base, problem.lambda_xi_base,
            problem.lambda_node_group, problem.lambda_propagation_group,
            problem.lambda_shock_group,
        ], dtype=float),
    )
    sparse.save_npz(output / "W.npz", problem.W)


def load_weighted_joint_problem(path, joint_system, *, expected_config_fingerprint=None,
                                expected_evidence_fingerprint=None,
                                expected_analysis_cutoff_ns=None):
    source = Path(path)
    if not isinstance(joint_system, JointInversionSystem):
        raise TypeError("joint_system must be JointInversionSystem")
    required = ["metadata.json", "arrays.npz", "W.npz"]
    if any(not (source / name).is_file() for name in required):
        raise WeightedProblemSerializationError("weighted problem files are incomplete")
    try:
        metadata = json.loads((source / "metadata.json").read_text(encoding="utf-8"))
        if metadata["format_version"] != WEIGHTED_PROBLEM_VERSION:
            raise WeightedProblemSerializationError("weighted problem version mismatch")
        if metadata["joint_system_id"] != joint_system.system_id \
                or metadata["p6_structure_fingerprint"] != joint_system.structure_fingerprint:
            raise WeightedProblemFingerprintError("P6 structure fingerprint mismatch")
        checks = (
            ("config_fingerprint", expected_config_fingerprint),
            ("evidence_fingerprint", expected_evidence_fingerprint),
            ("analysis_cutoff_ns", expected_analysis_cutoff_ns),
        )
        for name, expected in checks:
            if expected is not None and metadata[name] != expected:
                raise WeightedProblemSerializationError(f"weighted problem {name} mismatch")
        with np.load(source / "arrays.npz", allow_pickle=False) as values:
            arrays = {name: values[name].copy() for name in values.files}
        scalars = arrays.pop("scalars")
        W = sparse.load_npz(source / "W.npz").tocsr()
        problem = WeightedJointInversionProblem(
            metadata["schema_version"], metadata["record_type"], metadata["problem_id"],
            metadata["joint_system_id"], metadata["alert_id"], metadata["candidate_id"],
            metadata["topology_snapshot_id"], metadata["metric_model_snapshot_id"],
            metadata["timestamp_ns"], metadata["analysis_cutoff_ns"], metadata["signal_kind"],
            metadata["node_variable_ids"], metadata["propagation_variable_ids"],
            metadata["shock_variable_ids"],
            arrays["joint_residual"], joint_system.U, joint_system.X_prop,
            joint_system.X_shock, W, arrays["node_evidence_h"],
            arrays["propagation_evidence_h"], arrays["shock_evidence_h"],
            arrays["node_incoming_prop_h"], arrays["node_projected_shock_h"],
            arrays["node_quality_weights"], arrays["edge_quality_weights"],
            *scalars[:5], arrays["lambda_u_effective"], arrays["lambda_delta_effective"],
            arrays["lambda_xi_effective"],
            [VariableGroup(**item) for item in metadata["node_groups"]],
            [VariableGroup(**item) for item in metadata["propagation_groups"]],
            [VariableGroup(**item) for item in metadata["shock_groups"]],
            *scalars[5:8], metadata["evidence_provenance"], metadata["excluded_evidence"],
            metadata["quality_issues"], metadata["config_fingerprint"],
            metadata["evidence_fingerprint"], metadata["problem_fingerprint"],
            metadata["p6_structure_fingerprint"], metadata["solver_eligible"],
            metadata["build_duration_ms"],
        )
        computed = _problem_fingerprint(
            joint_system, problem.W, problem.node_evidence_h,
            problem.propagation_evidence_h, problem.shock_evidence_h,
            PenaltyResult(
                problem.residual_scale_raw, problem.residual_scale_used,
                problem.lambda_u_base, problem.lambda_delta_base, problem.lambda_xi_base,
                problem.lambda_u_effective, problem.lambda_delta_effective,
                problem.lambda_xi_effective, problem.node_incoming_prop_h,
                problem.node_projected_shock_h,
                np.zeros(len(problem.propagation_evidence_h)),
                np.zeros(len(problem.shock_evidence_h)),
                np.zeros(len(problem.shock_evidence_h)),
                problem.lambda_node_group, problem.lambda_propagation_group,
                problem.lambda_shock_group,
            ),
            GroupPartitionResult(problem.node_groups, problem.propagation_groups, problem.shock_groups),
            problem.config_fingerprint, problem.evidence_fingerprint,
        )
        if computed != problem.problem_fingerprint:
            raise WeightedProblemFingerprintError("weighted problem fingerprint mismatch")
        return problem
    except (WeightedProblemSerializationError, WeightedProblemFingerprintError):
        raise
    except Exception as error:
        raise WeightedProblemSerializationError(f"failed to load weighted problem: {error}") from error

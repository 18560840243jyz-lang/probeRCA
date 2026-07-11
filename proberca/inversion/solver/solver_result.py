"""Strict P8 solver result contract and JSON/NPZ persistence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np

from proberca.config import ProbeRCAConfig, SolverConfig

from .objective import evaluate_objective, prepare_problem
from ..weighted_problem import WeightedJointInversionProblem


FISTA_RESULT_VERSION = "1"


class FISTAResultSerializationError(ValueError):
    """Persisted P8 result data is incomplete or malformed."""


class FISTAFingerprintError(ValueError):
    """Persisted P8 result does not belong to the supplied problem/configuration."""


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha256(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def solver_config_fingerprint(config) -> str:
    solver = config.solver if isinstance(config, ProbeRCAConfig) else config
    if not isinstance(solver, SolverConfig):
        raise TypeError("config must be SolverConfig or ProbeRCAConfig")
    return _sha256(asdict(solver))


@dataclass(frozen=True)
class FISTASolverResult:
    schema_version: str
    record_type: str
    result_id: str
    problem_id: str
    problem_fingerprint: str
    alert_id: str
    candidate_id: str
    timestamp_ns: int
    status: str
    converged: bool
    solver_usable: bool
    iterations: int
    accepted_iterations: int
    restart_count: int
    total_backtracking_steps: int
    final_lipschitz: float
    initial_objective: float
    final_objective: float
    relative_objective_change: float
    gradient_mapping_norm: float
    u_values: np.ndarray
    delta_values: np.ndarray
    xi_values: np.ndarray
    node_variable_ids: list[str]
    propagation_variable_ids: list[str]
    shock_variable_ids: list[str]
    node_component: np.ndarray
    propagation_component: np.ndarray
    shock_component: np.ndarray
    fitted_values: np.ndarray
    solver_residual: np.ndarray
    weighted_residual_norm: float
    objective_components: dict[str, float]
    objective_trace: list[float]
    lipschitz_trace: list[float]
    gradient_mapping_trace: list[float]
    warm_start_used: bool
    warm_start_result_id: str | None
    config_fingerprint: str
    result_fingerprint: str
    runtime_ms: float
    quality_issues: list[dict]

    def __post_init__(self) -> None:
        if self.schema_version != "1.0" or self.record_type != "fista_solver_result":
            raise ValueError("invalid FISTA result schema or type")
        if self.status not in {"converged", "max_iterations", "numerical_failure", "invalid_problem"}:
            raise ValueError("invalid FISTA result status")
        if self.solver_usable != self.converged or (self.converged != (self.status == "converged")):
            raise ValueError("FISTA result status, convergence, and usability disagree")
        if min(self.iterations, self.accepted_iterations, self.restart_count,
               self.total_backtracking_steps) < 0:
            raise ValueError("FISTA diagnostics cannot be negative")
        arrays = (
            (self.u_values, len(self.node_variable_ids)),
            (self.delta_values, len(self.propagation_variable_ids)),
            (self.xi_values, len(self.shock_variable_ids)),
        )
        for values, size in arrays:
            if not isinstance(values, np.ndarray) or values.shape != (size,) \
                    or not np.isfinite(values).all():
                raise ValueError("FISTA variable arrays and IDs do not align")
        row_arrays = (self.node_component, self.propagation_component, self.shock_component,
                      self.fitted_values, self.solver_residual)
        if any(not isinstance(values, np.ndarray) or values.shape != self.fitted_values.shape
               or not np.isfinite(values).all() for values in row_arrays):
            raise ValueError("FISTA fitted component arrays do not align")
        if not np.allclose(self.node_component + self.propagation_component + self.shock_component,
                           self.fitted_values, rtol=1e-10, atol=1e-12):
            raise ValueError("FISTA components do not sum to fitted_values")
        scalar_values = (
            self.final_lipschitz, self.initial_objective, self.final_objective,
            self.relative_objective_change, self.gradient_mapping_norm,
            self.weighted_residual_norm, self.runtime_ms,
            *self.objective_components.values(), *self.objective_trace,
            *self.lipschitz_trace, *self.gradient_mapping_trace,
        )
        if not all(np.isfinite(value) for value in scalar_values):
            raise ValueError("FISTA result contains non-finite diagnostics")
        expected_keys = {"data_fit", "node_l1", "node_group", "propagation_l1",
                         "propagation_group", "shock_l1", "shock_group", "total"}
        if set(self.objective_components) != expected_keys:
            raise ValueError("FISTA objective component keys are incomplete")
        component_sum = sum(value for key, value in self.objective_components.items() if key != "total")
        if not np.isclose(component_sum, self.objective_components["total"], rtol=1e-10, atol=1e-12) \
                or not np.isclose(self.final_objective, self.objective_components["total"],
                                  rtol=1e-10, atol=1e-12):
            raise ValueError("FISTA objective components do not sum to final_objective")


def result_fingerprint_payload(result: FISTASolverResult) -> dict:
    return {
        "problem_fingerprint": result.problem_fingerprint,
        "config_fingerprint": result.config_fingerprint,
        "status": result.status,
        "converged": result.converged,
        "solver_usable": result.solver_usable,
        "iterations": result.iterations,
        "accepted_iterations": result.accepted_iterations,
        "restart_count": result.restart_count,
        "total_backtracking_steps": result.total_backtracking_steps,
        "final_lipschitz": result.final_lipschitz,
        "initial_objective": result.initial_objective,
        "final_objective": result.final_objective,
        "relative_objective_change": result.relative_objective_change,
        "gradient_mapping_norm": result.gradient_mapping_norm,
        "node_variable_ids": result.node_variable_ids,
        "propagation_variable_ids": result.propagation_variable_ids,
        "shock_variable_ids": result.shock_variable_ids,
        "u_values": result.u_values.tolist(), "delta_values": result.delta_values.tolist(),
        "xi_values": result.xi_values.tolist(),
        "node_component": result.node_component.tolist(),
        "propagation_component": result.propagation_component.tolist(),
        "shock_component": result.shock_component.tolist(),
        "fitted_values": result.fitted_values.tolist(),
        "solver_residual": result.solver_residual.tolist(),
        "weighted_residual_norm": result.weighted_residual_norm,
        "objective_components": result.objective_components,
        "objective_trace": result.objective_trace,
        "lipschitz_trace": result.lipschitz_trace,
        "gradient_mapping_trace": result.gradient_mapping_trace,
        "warm_start_used": result.warm_start_used,
        "warm_start_result_id": result.warm_start_result_id,
        "quality_issues": result.quality_issues,
    }


def compute_result_fingerprint(result: FISTASolverResult) -> str:
    return _sha256(result_fingerprint_payload(result))


def save_fista_result(path, result: FISTASolverResult) -> None:
    if not isinstance(result, FISTASolverResult):
        raise TypeError("result must be FISTASolverResult")
    output = Path(path)
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "format_version": FISTA_RESULT_VERSION,
        **{name: getattr(result, name) for name in (
            "schema_version", "record_type", "result_id", "problem_id", "problem_fingerprint",
            "alert_id", "candidate_id", "timestamp_ns", "status", "converged",
            "solver_usable", "iterations", "accepted_iterations", "restart_count",
            "total_backtracking_steps", "final_lipschitz", "initial_objective",
            "final_objective", "relative_objective_change", "gradient_mapping_norm",
            "node_variable_ids", "propagation_variable_ids", "shock_variable_ids",
            "weighted_residual_norm", "objective_components", "warm_start_used",
            "warm_start_result_id", "config_fingerprint", "result_fingerprint",
            "runtime_ms", "quality_issues",
        )},
    }
    (output / "metadata.json").write_bytes(_canonical(metadata))
    np.savez(
        output / "arrays.npz", u_values=result.u_values, delta_values=result.delta_values,
        xi_values=result.xi_values, node_component=result.node_component,
        propagation_component=result.propagation_component, shock_component=result.shock_component,
        fitted_values=result.fitted_values, solver_residual=result.solver_residual,
        objective_trace=np.asarray(result.objective_trace),
        lipschitz_trace=np.asarray(result.lipschitz_trace),
        gradient_mapping_trace=np.asarray(result.gradient_mapping_trace),
    )


def load_fista_result(path, problem: WeightedJointInversionProblem, config) -> FISTASolverResult:
    source = Path(path)
    try:
        metadata = json.loads((source / "metadata.json").read_text(encoding="utf-8"))
        if metadata["format_version"] != FISTA_RESULT_VERSION:
            raise FISTAFingerprintError("FISTA result version mismatch")
        if metadata["problem_id"] != problem.problem_id \
                or metadata["problem_fingerprint"] != problem.problem_fingerprint:
            raise FISTAFingerprintError("FISTA result problem mismatch")
        expected_config = solver_config_fingerprint(config)
        if metadata["config_fingerprint"] != expected_config:
            raise FISTAFingerprintError("FISTA result solver config mismatch")
        expected_ids = (problem.node_variable_ids, problem.propagation_variable_ids,
                        problem.shock_variable_ids)
        actual_ids = (metadata["node_variable_ids"], metadata["propagation_variable_ids"],
                      metadata["shock_variable_ids"])
        if actual_ids != expected_ids:
            raise FISTAFingerprintError("FISTA result variable IDs mismatch")
        with np.load(source / "arrays.npz", allow_pickle=False) as archive:
            arrays = {name: archive[name].copy() for name in archive.files}
        try:
            result = FISTASolverResult(
                **{name: metadata[name] for name in (
                "schema_version", "record_type", "result_id", "problem_id", "problem_fingerprint",
                "alert_id", "candidate_id", "timestamp_ns", "status", "converged",
                "solver_usable", "iterations", "accepted_iterations", "restart_count",
                "total_backtracking_steps", "final_lipschitz", "initial_objective",
                "final_objective", "relative_objective_change", "gradient_mapping_norm",
                "node_variable_ids", "propagation_variable_ids", "shock_variable_ids",
                "weighted_residual_norm", "objective_components", "warm_start_used",
                "warm_start_result_id", "config_fingerprint", "result_fingerprint",
                "runtime_ms", "quality_issues",
                )},
                u_values=arrays["u_values"], delta_values=arrays["delta_values"],
                xi_values=arrays["xi_values"], node_component=arrays["node_component"],
                propagation_component=arrays["propagation_component"],
                shock_component=arrays["shock_component"], fitted_values=arrays["fitted_values"],
                solver_residual=arrays["solver_residual"],
                objective_trace=arrays["objective_trace"].tolist(),
                lipschitz_trace=arrays["lipschitz_trace"].tolist(),
                gradient_mapping_trace=arrays["gradient_mapping_trace"].tolist(),
            )
        except ValueError as error:
            raise FISTAFingerprintError(f"FISTA result integrity mismatch: {error}") from error
        theta = np.concatenate((result.u_values, result.delta_values, result.xi_values))
        objective = evaluate_objective(prepare_problem(problem), theta)
        if not np.isclose(objective.total, result.final_objective, rtol=1e-9, atol=1e-11):
            raise FISTAFingerprintError("FISTA result objective mismatch")
        if compute_result_fingerprint(result) != result.result_fingerprint:
            raise FISTAFingerprintError("FISTA result fingerprint mismatch")
        return result
    except FISTAFingerprintError:
        raise
    except Exception as error:
        raise FISTAResultSerializationError(f"failed to load FISTA result: {error}") from error

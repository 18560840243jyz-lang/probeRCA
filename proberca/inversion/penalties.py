"""Robust base penalties and monotone evidence-aware three-way competition."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from proberca.config import PenaltyConfig
from proberca.inversion.contracts import JointInversionSystem

from .quality import ObservationWeightResult


class PenaltyComputationError(ValueError):
    """Penalty inputs or dimensions are inconsistent."""


class NonFinitePenaltyError(ValueError):
    """A base or effective penalty is non-finite or non-positive."""


@dataclass(frozen=True)
class PenaltyResult:
    residual_scale_raw: float
    residual_scale_used: float
    lambda_u_base: float
    lambda_delta_base: float
    lambda_xi_base: float
    lambda_u_effective: np.ndarray
    lambda_delta_effective: np.ndarray
    lambda_xi_effective: np.ndarray
    node_incoming_prop_h: np.ndarray
    node_projected_shock_h: np.ndarray
    propagation_shock_competition: np.ndarray
    shock_endpoint_node_h: np.ndarray
    shock_projected_prop_h: np.ndarray
    lambda_node_group: float
    lambda_propagation_group: float
    lambda_shock_group: float


def _noisy_or(values) -> float:
    product = 1.0
    for value in values:
        product *= 1.0 - value
    return 1.0 - product


def _base(coefficient, scale, count):
    return coefficient * scale * math.sqrt(2.0 * math.log(count + 1)) if count else 0.0


def compute_penalties(joint_system, node_h, propagation_h, shock_h,
                      quality, config):
    if not isinstance(joint_system, JointInversionSystem) \
            or not isinstance(quality, ObservationWeightResult) \
            or not isinstance(config, PenaltyConfig):
        raise TypeError("penalty computation requires P6 system, quality result, and PenaltyConfig")
    node_h = np.asarray(node_h, dtype=float)
    propagation_h = np.asarray(propagation_h, dtype=float)
    shock_h = np.asarray(shock_h, dtype=float)
    expected = (
        (node_h, len(joint_system.node_variable_refs)),
        (propagation_h, len(joint_system.propagation_variable_refs)),
        (shock_h, len(joint_system.shock_variable_refs)),
    )
    for values, size in expected:
        if values.shape != (size,) or not np.isfinite(values).all() \
                or np.any(values < 0) or np.any(values > 1):
            raise PenaltyComputationError("evidence vectors must be finite in [0, 1] and dimension aligned")
    residual = np.asarray(joint_system.joint_residual, dtype=float)
    center = float(np.median(residual)) if residual.size else 0.0
    mad = float(np.median(np.abs(residual - center))) if residual.size else 0.0
    sigma_raw = 1.4826 * mad
    sigma = max(sigma_raw, config.residual_scale_floor)
    n_u, n_delta, n_xi = (len(item) for item in (
        joint_system.node_variable_refs,
        joint_system.propagation_variable_refs,
        joint_system.shock_variable_refs,
    ))
    lambda_u = _base(config.c_u, sigma, n_u)
    lambda_delta = _base(config.c_delta, sigma, n_delta)
    lambda_xi = _base(config.c_xi, sigma, n_xi)
    incoming = np.zeros(n_u, dtype=float)
    by_target = {index: [] for index in range(n_u)}
    for item in joint_system.propagation_variable_refs:
        by_target[item.target_row_index].append(propagation_h[item.column_index])
    for index, values in by_target.items():
        incoming[index] = _noisy_or(values)
    projected_shock = np.zeros(n_u, dtype=float)
    shock_contributions = {index: [] for index in range(n_u)}
    for item in joint_system.shock_variable_refs:
        for row, weight in zip(item.projected_node_rows, item.projection_weights):
            shock_contributions[row].append(min(1.0, abs(weight) * shock_h[item.column_index]))
    for index, values in shock_contributions.items():
        projected_shock[index] = _noisy_or(values)
    shock_competition = np.asarray([
        projected_shock[item.target_row_index]
        for item in joint_system.propagation_variable_refs
    ], dtype=float)
    endpoint_node = np.zeros(n_xi, dtype=float)
    projected_prop = np.zeros(n_xi, dtype=float)
    for item in joint_system.shock_variable_refs:
        endpoint_node[item.column_index] = max(node_h[row] for row in item.projected_node_rows)
        values = [
            propagation_h[prop.column_index]
            for prop in joint_system.propagation_variable_refs
            if prop.target_row_index in set(item.projected_node_rows)
        ]
        projected_prop[item.column_index] = max(values, default=0.0)
    lambda_u_eff = lambda_u * (1 + config.rho_p * incoming + config.rho_s * projected_shock) \
        / (1 + config.eta_v * node_h) * (1 + config.rho_m * (1 - quality.node_weights))
    target_h = np.asarray([
        node_h[item.target_row_index] for item in joint_system.propagation_variable_refs
    ], dtype=float)
    target_quality = np.asarray([
        quality.node_weights[item.target_row_index]
        for item in joint_system.propagation_variable_refs
    ], dtype=float)
    lambda_delta_eff = lambda_delta * (
        1 + config.rho_v * target_h + config.rho_s * shock_competition
    ) / (1 + config.eta_p * propagation_h) * (1 + config.rho_m * (1 - target_quality))
    edge_quality = np.asarray([
        quality.edge_weights[item.edge_row_index - n_u]
        for item in joint_system.shock_variable_refs
    ], dtype=float)
    lambda_xi_eff = lambda_xi * (
        1 + config.rho_v * endpoint_node + config.rho_p * projected_prop
    ) / (1 + config.eta_s * shock_h) * (1 + config.rho_m * (1 - edge_quality))
    for values in (lambda_u_eff, lambda_delta_eff, lambda_xi_eff):
        if values.size and (not np.isfinite(values).all() or np.any(values <= 0)):
            raise NonFinitePenaltyError("effective penalties must be positive and finite")
    return PenaltyResult(
        sigma_raw, sigma, lambda_u, lambda_delta, lambda_xi,
        lambda_u_eff, lambda_delta_eff, lambda_xi_eff,
        incoming, projected_shock, shock_competition, endpoint_node, projected_prop,
        config.group_ratio_u * lambda_u,
        config.group_ratio_delta * lambda_delta,
        config.group_ratio_xi * lambda_xi,
    )

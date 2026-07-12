"""Counterfactual separation, coherence, lag entropy, and identifiability."""

from __future__ import annotations

import math
import statistics
import numpy as np

from .contracts import DiagnosisCandidate, IdentifiabilityError


EPSILON = 1e-12


def compute_local_coherence(candidates, W):
    diagonal = W.diagonal()
    if not np.isfinite(diagonal).all() or np.any(diagonal <= 0):
        raise IdentifiabilityError("W must have a positive finite diagonal")
    signatures = {
        item.candidate_id: np.sqrt(diagonal) * np.asarray(item.contribution_vector)
        for item in candidates
    }
    result = {}
    for candidate in candidates:
        current = signatures[candidate.candidate_id]
        norm = float(np.linalg.norm(current))
        if norm == 0:
            result[candidate.candidate_id] = 0.0
            continue
        values = []
        for other in candidates:
            if other.candidate_id == candidate.candidate_id:
                continue
            signature = signatures[other.candidate_id]
            other_norm = float(np.linalg.norm(signature))
            values.append(abs(float(current @ signature)) / (norm * other_norm + EPSILON))
        result[candidate.candidate_id] = min(max(max(values, default=0.0), 0.0), 1.0)
    return result


def compute_lag_entropy(candidate: DiagnosisCandidate) -> float:
    if candidate.variable_block != "propagation":
        return 0.0
    masses = {}
    for lag, value in zip(candidate.metadata["member_lags"], candidate.raw_values):
        masses[lag] = masses.get(lag, 0.0) + abs(value)
    positive = [value for value in masses.values() if value > 0]
    if len(positive) <= 1:
        return 0.0
    total = sum(positive)
    probabilities = [value / total for value in positive]
    return min(max(-sum(value * math.log(value) for value in probabilities) / math.log(len(positive)), 0.0), 1.0)


def compute_counterfactual_support(candidates):
    positives = [item.relative_delta_loss for item in candidates
                 if item.counterfactual_status == "evaluated" and item.relative_delta_loss > 0]
    result = {}
    for candidate in candidates:
        if candidate.counterfactual_status != "evaluated" or candidate.relative_delta_loss is None:
            continue
        others = [value for value in positives if value != candidate.relative_delta_loss]
        if not others:
            result[candidate.candidate_id] = 1.0
        else:
            median = statistics.median(others)
            value = candidate.relative_delta_loss / (candidate.relative_delta_loss + median + EPSILON)
            result[candidate.candidate_id] = min(max(value, 0.0), 1.0)
    return result


def compute_margins(candidates):
    ordered = sorted(
        [item for item in candidates if item.counterfactual_status == "evaluated"
         and item.relative_delta_loss is not None],
        key=lambda item: (-item.relative_delta_loss, -item.weighted_contribution_energy,
                          item.candidate_id),
    )
    result = {}
    for index, candidate in enumerate(ordered):
        if len(ordered) == 1:
            result[candidate.candidate_id] = 1.0
        else:
            following = ordered[index + 1].relative_delta_loss if index + 1 < len(ordered) else 0.0
            result[candidate.candidate_id] = max(candidate.relative_delta_loss - following, 0.0) / max(
                candidate.relative_delta_loss, EPSILON)
    return result


def compute_identifiability(cf, path_support, margin, coherence, lag_entropy, config):
    values = (cf, path_support, margin, coherence, lag_entropy)
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in values):
        raise IdentifiabilityError("identifiability inputs must be finite in [0,1]")
    positive_weights = (config.ident_cf_weight, config.ident_path_weight, config.ident_margin_weight)
    uncertainty_weights = (config.ident_coherence_weight, config.ident_lag_entropy_weight)
    if sum(positive_weights) <= 0 or sum(uncertainty_weights) <= 0:
        raise IdentifiabilityError("identifiability weight sums must be positive")
    positive = sum(weight * value for weight, value in zip(positive_weights, (cf, path_support, margin))) / sum(positive_weights)
    uncertainty = sum(weight * value for weight, value in zip(uncertainty_weights, (coherence, lag_entropy))) / sum(uncertainty_weights)
    return min(max(positive * (1.0 - uncertainty), 0.0), 1.0)

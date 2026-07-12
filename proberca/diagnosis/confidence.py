"""Auditable P9 confidence score; this score is not a probability."""

from __future__ import annotations

import math

from .contracts import ConfidenceComputationError


def compute_confidence(cf, margin, quality, identifiability, config):
    values = (cf, margin, quality, identifiability)
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in values):
        raise ConfidenceComputationError("confidence inputs must be finite in [0,1]")
    weights = (config.confidence_cf_weight, config.confidence_margin_weight,
               config.confidence_quality_weight, config.confidence_identifiability_weight)
    if any(weight < 0 for weight in weights) or not math.isclose(sum(weights), 1.0, abs_tol=1e-9):
        raise ConfidenceComputationError("confidence weights must be non-negative and sum to one")
    return min(max(sum(weight * value for weight, value in zip(weights, values)), 0.0), 1.0)

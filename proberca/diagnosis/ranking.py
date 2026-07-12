"""Deterministic cross-type ranking without reusing evidence as a score."""

from __future__ import annotations

from dataclasses import replace
import numpy as np

from .confidence import compute_confidence
from .identifiability import (
    compute_counterfactual_support, compute_identifiability, compute_lag_entropy,
    compute_local_coherence, compute_margins,
)


def _quality(candidate, problem):
    diagonal = problem.W.diagonal()
    if candidate.variable_block == "node":
        return float(diagonal[candidate.metadata["source_row_index"]])
    if candidate.variable_block == "propagation":
        return float(min(diagonal[index] for index in candidate.metadata["target_row_indices"]))
    edge_quality = min(diagonal[index] for index in candidate.metadata["edge_row_indices"])
    pairs = candidate.metadata["projection_rows_and_weights"]
    total = sum(abs(weight) for _, weight in pairs)
    projected = sum(abs(weight) * diagonal[row] for row, weight in pairs) / total
    return float(min(edge_quality, projected))


def rank_candidates(candidates, problem, joint_system, config):
    active = [item for item in candidates if item.active]
    coherence = compute_local_coherence(active, problem.W)
    cf = compute_counterfactual_support(active)
    margins = compute_margins(active)
    enriched = []
    for candidate in active:
        quality = _quality(candidate, problem)
        lag_entropy = compute_lag_entropy(candidate)
        if candidate.counterfactual_status == "evaluated":
            cf_value = cf[candidate.candidate_id]
            margin = margins[candidate.candidate_id]
            ident = compute_identifiability(
                cf_value, candidate.best_path_score, margin,
                coherence[candidate.candidate_id], lag_entropy, config.diagnosis)
            confidence = compute_confidence(cf_value, margin, quality, ident, config.diagnosis)
        else:
            cf_value = None; margin = None; ident = None; confidence = None
        enriched.append(replace(
            candidate, counterfactual_support=cf_value, margin=margin,
            candidate_quality=quality, coherence=coherence[candidate.candidate_id],
            lag_entropy=lag_entropy, identifiability=ident, confidence=confidence,
        ))
    enriched.sort(key=lambda item: (
        0 if item.counterfactual_status == "evaluated" else 1,
        -(item.relative_delta_loss if item.relative_delta_loss is not None else -1.0),
        -item.weighted_contribution_energy, item.candidate_id,
    ))
    return [replace(item, rank=index + 1) for index, item in enumerate(enriched)]

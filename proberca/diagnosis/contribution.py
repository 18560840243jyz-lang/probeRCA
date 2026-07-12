"""Uniform weighted fitted-contribution measurements for all root types."""

from __future__ import annotations

import math
import numpy as np

from .contracts import CandidateContributionError


def contribution_and_energy(matrix, indices, values, W):
    vector = np.asarray(matrix[:, indices] @ np.asarray(values, dtype=float)).reshape(-1)
    if not np.isfinite(vector).all():
        raise CandidateContributionError("candidate contribution is non-finite")
    energy_squared = float(vector @ (W @ vector))
    if not math.isfinite(energy_squared) or energy_squared < -1e-12:
        raise CandidateContributionError("candidate weighted energy is invalid")
    return vector, math.sqrt(max(energy_squared, 0.0))


def member_diagnostics(matrix, indices, values, variable_ids, W):
    output = []
    for index, value, variable_id in zip(indices, values, variable_ids):
        vector, energy = contribution_and_energy(matrix, [index], [value], W)
        output.append({"variable_id": variable_id, "raw_value": float(value),
                       "energy": energy, "contribution_vector": vector.tolist()})
    return sorted(output, key=lambda item: item["variable_id"])

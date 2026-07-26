"""Final-scheme Burst normalization performed before evidence is archived."""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable


class BurstNormalizationError(ValueError):
    """Burst samples or calibration parameters are invalid."""


def _finite(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BurstNormalizationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise BurstNormalizationError(f"{name} must be finite")
    return result


def _unit_interval(name: str, value: float) -> float:
    result = _finite(name, value)
    if not 0.0 <= result <= 1.0:
        raise BurstNormalizationError(f"{name} must be in [0,1]")
    return result


def rare_event_strength(
    event_count: int, exposure: float, threshold: float, *, epsilon: float = 1.0e-12,
) -> float:
    """Normalize an OOM/RTO/timeout/failure event rate into [0,1]."""
    if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
        raise BurstNormalizationError("event_count must be a non-negative integer")
    exposure_value = _finite("exposure", exposure)
    threshold_value = _finite("threshold", threshold)
    epsilon_value = _finite("epsilon", epsilon)
    if exposure_value < 0 or threshold_value <= 0 or epsilon_value <= 0:
        raise BurstNormalizationError("exposure and rare-event calibration are invalid")
    rate = event_count / (exposure_value + epsilon_value)
    return min(max(rate / threshold_value, 0.0), 1.0)


def continuous_burst_strength(
    value: float,
    healthy_values: Iterable[float],
    *,
    polarity: int = 1,
    transform: str = "identity",
    z_cap: float = 5.0,
    minimum_healthy_samples: int = 5,
    minimum_scale: float = 1.0e-6,
) -> float:
    """Normalize a continuous Burst metric; no reliable healthy reference means zero."""
    if polarity not in {-1, 1}:
        raise BurstNormalizationError("polarity must be -1 or +1")
    if transform not in {"identity", "log1p"}:
        raise BurstNormalizationError("unsupported Burst transform")
    if isinstance(minimum_healthy_samples, bool) \
            or not isinstance(minimum_healthy_samples, int) \
            or minimum_healthy_samples <= 0:
        raise BurstNormalizationError("minimum_healthy_samples must be positive")
    cap = _finite("z_cap", z_cap)
    floor = _finite("minimum_scale", minimum_scale)
    if cap <= 0 or floor <= 0:
        raise BurstNormalizationError("continuous Burst calibration must be positive")

    def converted(item: float) -> float:
        raw = _finite("Burst value", item)
        if transform == "log1p":
            if raw < 0:
                raise BurstNormalizationError("log1p Burst values must be non-negative")
            return math.log1p(raw)
        return raw

    healthy = tuple(converted(item) for item in healthy_values)
    if len(healthy) < minimum_healthy_samples:
        return 0.0
    current = converted(value)
    median = float(statistics.median(healthy))
    mad = float(statistics.median(abs(item - median) for item in healthy))
    scale = max(1.4826 * mad, floor)
    signed_z = polarity * (current - median) / scale
    return min(max(max(signed_z, 0.0) / cap, 0.0), 1.0)


def burst_observation_quality(
    *, coverage: float, event_loss_rate: float, mapping_quality: float,
) -> float:
    """Combine Burst window completeness, event loss, and identity mapping quality."""
    coverage_value = _unit_interval("coverage", coverage)
    loss_value = _unit_interval("event_loss_rate", event_loss_rate)
    mapping_value = _unit_interval("mapping_quality", mapping_quality)
    return coverage_value * (1.0 - loss_value) * mapping_value

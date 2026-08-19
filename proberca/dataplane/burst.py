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


def _convert_continuous_value(value: float, transform: str) -> float:
    raw = _finite("Burst value", value)
    if transform == "log1p":
        if raw < 0:
            raise BurstNormalizationError(
                "log1p Burst values must be non-negative"
            )
        return math.log1p(raw)
    return raw


def fit_continuous_burst_reference(
    healthy_values: Iterable[float],
    *,
    transform: str = "identity",
    minimum_healthy_samples: int = 5,
    minimum_scale: float = 1.0e-6,
) -> tuple[float, float] | None:
    """Fit the immutable Healthy center/scale once for repeated normalization."""
    if transform not in {"identity", "log1p"}:
        raise BurstNormalizationError("unsupported Burst transform")
    if isinstance(minimum_healthy_samples, bool) \
            or not isinstance(minimum_healthy_samples, int) \
            or minimum_healthy_samples <= 0:
        raise BurstNormalizationError("minimum_healthy_samples must be positive")
    floor = _finite("minimum_scale", minimum_scale)
    if floor <= 0:
        raise BurstNormalizationError(
            "continuous Burst calibration must be positive"
        )
    healthy = tuple(
        _convert_continuous_value(item, transform)
        for item in healthy_values
    )
    if len(healthy) < minimum_healthy_samples:
        return None
    median = float(statistics.median(healthy))
    mad = float(statistics.median(abs(item - median) for item in healthy))
    return median, max(1.4826 * mad, floor)


def continuous_burst_strength_from_reference(
    value: float,
    reference: tuple[float, float] | None,
    *,
    polarity: int = 1,
    transform: str = "identity",
    z_cap: float = 5.0,
) -> float:
    """Apply a previously fitted Healthy reference without refitting it."""
    if polarity not in {-1, 1}:
        raise BurstNormalizationError("polarity must be -1 or +1")
    if transform not in {"identity", "log1p"}:
        raise BurstNormalizationError("unsupported Burst transform")
    cap = _finite("z_cap", z_cap)
    if cap <= 0:
        raise BurstNormalizationError(
            "continuous Burst calibration must be positive"
        )
    if reference is None:
        return 0.0
    if (
        not isinstance(reference, tuple)
        or len(reference) != 2
    ):
        raise BurstNormalizationError("continuous Burst reference is invalid")
    center = _finite("Burst reference center", reference[0])
    scale = _finite("Burst reference scale", reference[1])
    if scale <= 0:
        raise BurstNormalizationError("Burst reference scale must be positive")
    current = _convert_continuous_value(value, transform)
    signed_z = polarity * (current - center) / scale
    return min(max(max(signed_z, 0.0) / cap, 0.0), 1.0)


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
    reference = fit_continuous_burst_reference(
        healthy_values,
        transform=transform,
        minimum_healthy_samples=minimum_healthy_samples,
        minimum_scale=minimum_scale,
    )
    return continuous_burst_strength_from_reference(
        value,
        reference,
        polarity=polarity,
        transform=transform,
        z_cap=z_cap,
    )


def burst_observation_quality(
    *, coverage: float, event_loss_rate: float, mapping_quality: float,
) -> float:
    """Combine Burst window completeness, event loss, and identity mapping quality."""
    coverage_value = _unit_interval("coverage", coverage)
    loss_value = _unit_interval("event_loss_rate", event_loss_rate)
    mapping_value = _unit_interval("mapping_quality", mapping_quality)
    return coverage_value * (1.0 - loss_value) * mapping_value

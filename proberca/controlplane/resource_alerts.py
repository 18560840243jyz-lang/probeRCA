"""Healthy-calibrated resource alert channel for formal service and host roots."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import statistics

from proberca.dataplane.contracts import fingerprint

from .config import FinalControlConfig


@dataclass(frozen=True)
class ResourceAlertScores:
    """Resource scores emitted for the common per-entity alert state machine."""

    service_scores: dict[str, float]
    host_scores: dict[str, float]
    metric_scores: dict[str, dict[str, float | int | str]]


class ResourceAlertChannel:
    """Detect sustained changes in configured formal root metrics.

    Baseline z-scores alone are unsuitable for direct resource alerting because
    family floors can make harmless level drift look large.  This channel uses
    the change from a short, strictly prior rolling median and learns a
    per-metric-family Hard threshold from the calibration segment.  A small
    persistence prefilter rejects a three-sample transient while the existing
    common Soft/Hard state machine and its thresholds remain unchanged.  The
    observation validity and exposure gates run before this channel, so sparse
    missing windows are never converted into zero-valued alert evidence.
    """

    def __init__(self, config: FinalControlConfig):
        self.config = config
        self._history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=config.resource_alert_history_windows)
        )
        self._last_seen_sequence: dict[str, int] = {}
        self._calibration_runs: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=config.hard_consecutive_windows)
        )
        self._calibration_worst: dict[str, float] = {}
        self._coordinate_metric_names: dict[str, str] = {}
        self._thresholds: dict[str, float] | None = None
        self._prefilter_counts: dict[str, int] = {}

    def reset(self) -> None:
        """Discard both learned thresholds and short-term runtime state."""
        self._history.clear()
        self._last_seen_sequence.clear()
        self._calibration_runs.clear()
        self._calibration_worst.clear()
        self._coordinate_metric_names.clear()
        self._thresholds = None
        self._prefilter_counts.clear()

    def begin_observation_session(self) -> None:
        """Start a sealed archive after an unobserved gap without label input.

        Learned Healthy thresholds remain frozen.  Only the rolling change
        history is cleared, so the archive's own normal prefix establishes the
        short-term level instead of comparing against a stale process lifetime.
        """
        self._history.clear()
        self._last_seen_sequence.clear()
        self._calibration_runs.clear()
        self._prefilter_counts.clear()

    @property
    def frozen(self) -> bool:
        return self._thresholds is not None

    @property
    def thresholds(self) -> dict[str, float]:
        return dict(sorted((self._thresholds or {}).items()))

    @property
    def threshold_fingerprint(self) -> str | None:
        if self._thresholds is None:
            return None
        return fingerprint({
            "coordinate_metric_names": dict(sorted(
                self._coordinate_metric_names.items()
            )),
            "history_windows": self.config.resource_alert_history_windows,
            "metric_names": list(self.config.resource_alert_metric_names),
            "prefilter_windows": self.config.resource_alert_prefilter_windows,
            "thresholds": self.thresholds,
        })

    def freeze(self) -> None:
        """Freeze thresholds learned only from the calibration segment."""
        if self._thresholds is not None:
            return
        if not self._coordinate_metric_names:
            raise RuntimeError(
                "resource alert channel has no calibrated formal coordinates"
            )
        self._thresholds = {
            node_id: max(
                self.config.hard_threshold,
                self._calibration_worst.get(node_id, 0.0)
                + self.config.resource_alert_calibration_margin,
            )
            for node_id in sorted(self._coordinate_metric_names)
        }
        self._prefilter_counts.clear()

    def _in_channel_scope(self, observation) -> bool:
        metric = observation.metric
        return (
            metric.root_eligible
            and metric.entity_type in {"service", "host"}
            and metric.metric_name in self.config.resource_alert_metric_names
            and self.config.entity_is_in_formal_scope(metric.entity_id)
        )

    def _eligible(self, observation) -> bool:
        return (
            observation.alert_eligible
            and self._in_channel_scope(observation)
        )

    def observe(
        self, *, sequence: int, observations: dict, learn: bool,
    ) -> ResourceAlertScores:
        """Consume one ordered window and return label-blind resource scores."""
        service_scores: dict[str, float] = {}
        host_scores: dict[str, float] = {}
        details: dict[str, dict[str, float | int | str]] = {}
        seen: set[str] = set()
        for node_id, observation in sorted(observations.items()):
            if not self._in_channel_scope(observation):
                continue
            metric = observation.metric
            name = metric.metric_name
            known_name = self._coordinate_metric_names.setdefault(node_id, name)
            if known_name != name:
                raise RuntimeError("resource alert coordinate identity changed")
            if not self._eligible(observation):
                continue
            seen.add(node_id)
            previous_sequence = self._last_seen_sequence.get(node_id)
            if previous_sequence is not None and previous_sequence != sequence - 1:
                self._calibration_runs[node_id].clear()
                self._prefilter_counts[node_id] = 0
            history = self._history[node_id]
            delta: float | None = None
            if len(history) >= self.config.resource_alert_history_windows:
                delta = max(
                    0.0,
                    float(observation.anomaly) - float(statistics.median(history)),
                )
            history.append(float(observation.anomaly))
            self._last_seen_sequence[node_id] = sequence
            if delta is None:
                self._prefilter_counts[node_id] = 0
                continue
            if learn:
                run = self._calibration_runs[node_id]
                run.append(delta)
                if len(run) == self.config.hard_consecutive_windows:
                    self._calibration_worst[node_id] = max(
                        self._calibration_worst.get(node_id, 0.0), min(run),
                    )
            if self._thresholds is None:
                continue
            if node_id not in self._thresholds:
                raise RuntimeError(
                    "resource alert coordinate was not present during calibration: "
                    f"{node_id}"
                )
            normalized = (
                self.config.hard_threshold * delta / self._thresholds[node_id]
            )
            count = (
                self._prefilter_counts.get(node_id, 0) + 1
                if normalized >= self.config.soft_threshold else 0
            )
            self._prefilter_counts[node_id] = count
            details[node_id] = {
                "metric_name": name,
                "rolling_delta": delta,
                "learned_hard_delta": self._thresholds[node_id],
                "normalized_score": normalized,
                "prefilter_count": count,
            }
            if count < self.config.resource_alert_prefilter_windows:
                continue
            target = (
                service_scores if metric.entity_type == "service"
                else host_scores
            )
            target[metric.entity_id] = max(
                target.get(metric.entity_id, 0.0), normalized,
            )
        for node_id in set(self._prefilter_counts) - seen:
            self._prefilter_counts[node_id] = 0
            self._calibration_runs[node_id].clear()
        return ResourceAlertScores(
            service_scores=dict(sorted(service_scores.items())),
            host_scores=dict(sorted(host_scores.items())),
            metric_scores=dict(sorted(details.items())),
        )

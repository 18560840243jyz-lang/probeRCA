"""Canonical P2 event-time aggregation and counter semantics."""

from .core import (
    AggregationBatch,
    AggregationIssue,
    AggregationPlan,
    CounterDeltaTracker,
    LateRecordError,
    RejectedWindowError,
    WindowAggregator,
)

__all__ = [
    "AggregationBatch",
    "AggregationIssue",
    "AggregationPlan",
    "CounterDeltaTracker",
    "LateRecordError",
    "RejectedWindowError",
    "WindowAggregator",
]

"""Bounded, clean-attempt collection retry state machine."""
from __future__ import annotations

from enum import Enum
import math
import time


class CollectionOutcome(str, Enum):
    SUCCESS = "success"
    ALLOW_EMPTY = "allow_empty"
    TRANSIENT_EMPTY = "transient_empty"
    TRANSIENT_ERROR = "transient_error"
    PERMANENT_ERROR = "permanent_error"


class ControlledTransientCollectionEmpty(RuntimeError):
    """Generic test-overlay fault; production defaults keep it disabled."""

    reason_code = "no_samples"
    retryable = True


class CollectionExhaustedError(RuntimeError):
    def __init__(self, sequence, attempts, outcome):
        self.sequence = int(sequence)
        self.attempts = int(attempts)
        self.outcome = outcome
        super().__init__(
            f"live collection exhausted sequence={sequence} "
            f"attempts={attempts} outcome={outcome.value}",
        )


class WindowCollectionRetrier:
    """Retries a whole window attempt without carrying sample state forward."""

    def __init__(self, *, max_attempts, initial_backoff_sec,
                 max_backoff_sec, sleep=time.sleep, on_retry=None):
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise TypeError("max_attempts must be an integer")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        for name, value in (
            ("initial_backoff_sec", initial_backoff_sec),
            ("max_backoff_sec", max_backoff_sec),
        ):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be finite and positive")
        if initial_backoff_sec > max_backoff_sec:
            raise ValueError("initial retry backoff exceeds maximum")
        self.max_attempts = max_attempts
        self.initial_backoff_sec = float(initial_backoff_sec)
        self.max_backoff_sec = float(max_backoff_sec)
        self.sleep = sleep
        self.on_retry = on_retry

    def run(self, *, sequence, new_context, collect, cleanup):
        last_outcome = CollectionOutcome.TRANSIENT_ERROR
        for attempt in range(1, self.max_attempts + 1):
            context = new_context(attempt)
            try:
                outcome, value = collect(context, attempt)
                if not isinstance(outcome, CollectionOutcome):
                    raise TypeError("collector must return CollectionOutcome")
                if outcome in {
                    CollectionOutcome.SUCCESS,
                    CollectionOutcome.ALLOW_EMPTY,
                }:
                    return value
                last_outcome = outcome
                if outcome is CollectionOutcome.PERMANENT_ERROR:
                    raise CollectionExhaustedError(sequence, attempt, outcome)
            finally:
                if 'outcome' not in locals() or outcome not in {
                    CollectionOutcome.SUCCESS,
                    CollectionOutcome.ALLOW_EMPTY,
                }:
                    cleanup(context)
            if attempt < self.max_attempts:
                delay = min(
                    self.max_backoff_sec,
                    self.initial_backoff_sec * (2 ** (attempt - 1)),
                )
                if self.on_retry is not None:
                    self.on_retry(attempt, last_outcome, delay)
                self.sleep(delay)
        raise CollectionExhaustedError(
            sequence, self.max_attempts, last_outcome,
        )

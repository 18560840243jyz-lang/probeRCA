"""Bounded execution for every canonical live-window stage."""
from __future__ import annotations

import contextlib
import math
import queue
import threading
import time

from .progress import LiveStage, StageProgressTracker


class LiveStageExecutionError(RuntimeError):
    """A stage failed before it could produce an accepted result."""


class LiveStageTimeoutError(TimeoutError):
    """A stage exceeded its configured finite deadline."""

    def __init__(self, stage, sequence, timeout_sec):
        self.stage = stage
        self.sequence = sequence
        self.timeout_sec = timeout_sec
        super().__init__(
            f"live stage {stage.value} sequence={sequence} exceeded "
            f"timeout={timeout_sec} sec",
        )


class LiveStageLockTimeout(TimeoutError):
    """A named live-path lock exceeded its finite acquisition deadline."""

    def __init__(self, lock_name, holder_thread, wait_duration_sec):
        self.lock_name = str(lock_name)
        self.holder_thread = str(holder_thread or "unknown")
        self.wait_duration_sec = float(wait_duration_sec)
        super().__init__(
            f"live lock timeout lock={self.lock_name} "
            f"holder={self.holder_thread} "
            f"wait_sec={self.wait_duration_sec:.6f}"
        )


def _result_count(result):
    if result is None:
        return 0
    try:
        return len(result)
    except (TypeError, AttributeError):
        return 1


class StageExecutor:
    """Runs one stage at a time and never accepts an unverified late result."""

    def __init__(
        self, tracker: StageProgressTracker, *, on_progress=None,
        lock_timeout_sec=5.0, lock_name="live-transaction",
    ):
        if not all(hasattr(tracker, name) for name in ("enter", "snapshot")):
            raise TypeError("tracker must provide enter and snapshot")
        self.tracker = tracker
        self.on_progress = on_progress
        if lock_timeout_sec <= 0 or not lock_name:
            raise ValueError("stage lock configuration is invalid")
        self.lock_timeout_sec = float(lock_timeout_sec)
        self.lock_name = str(lock_name)
        self._state_lock = threading.Lock()
        self._lock_holder = None
        self._active_thread = None

    @contextlib.contextmanager
    def _locked(self):
        started = time.monotonic()
        acquired = self._state_lock.acquire(timeout=self.lock_timeout_sec)
        waited = time.monotonic() - started
        if not acquired:
            raise LiveStageLockTimeout(
                self.lock_name, self._lock_holder, waited,
            )
        self._lock_holder = threading.current_thread().name
        try:
            yield
        finally:
            self._lock_holder = None
            self._state_lock.release()

    @property
    def active_worker_count(self):
        with self._locked():
            return int(
                self._active_thread is not None
                and self._active_thread.is_alive()
            )

    def run_stage(self, stage, *, sequence, timeout_sec, operation,
                  attempt=1, input_count=None, context=None):
        if not isinstance(stage, LiveStage):
            raise TypeError("stage must be LiveStage")
        if (isinstance(timeout_sec, bool) or not isinstance(timeout_sec, (int, float))
                or not math.isfinite(timeout_sec) or timeout_sec <= 0):
            raise ValueError("stage timeout must be finite and positive")
        if not callable(operation):
            raise TypeError("stage operation must be callable")
        with self._locked():
            if self._active_thread is not None and self._active_thread.is_alive():
                raise RuntimeError("a live window stage is already active")
            result_queue = queue.Queue(maxsize=1)

            def invoke():
                try:
                    result_queue.put((True, operation()))
                except BaseException as error:
                    result_queue.put((False, error))

            worker = threading.Thread(
                target=invoke,
                name=f"proberca-stage-{stage.value.lower()}",
                daemon=True,
            )
            self._active_thread = worker
        stage_context = dict(context or {})
        self.tracker.enter(
            stage,
            sequence=sequence,
            attempt=attempt,
            input_count=input_count,
            **stage_context,
        )
        worker.start()
        if self.on_progress is not None:
            self.on_progress(self.tracker.snapshot())
        worker.join(timeout_sec)
        if worker.is_alive():
            if hasattr(self.tracker, "timeout"):
                self.tracker.timeout(reason_code="stage_deadline_exceeded")
            if self.on_progress is not None:
                self.on_progress(self.tracker.snapshot())
            raise LiveStageTimeoutError(stage, sequence, timeout_sec)
        with self._locked():
            self._active_thread = None
        succeeded, value = result_queue.get_nowait()
        if not succeeded:
            if hasattr(self.tracker, "record_error"):
                self.tracker.record_error(value, "stage_operation_failed")
            if self.on_progress is not None:
                self.on_progress(self.tracker.snapshot())
            raise value
        if hasattr(self.tracker, "exit"):
            self.tracker.exit(output_count=_result_count(value))
        if self.on_progress is not None:
            self.on_progress(self.tracker.snapshot())
        return value

"""Transactional working-Engine isolation with a finite supervisor wait."""
from __future__ import annotations

import contextlib
import math
import queue
import threading
import time

from .executor import LiveStageLockTimeout
from .progress import LiveStage, StageProgressTracker


class EngineStageTimeout(TimeoutError):
    def __init__(self, sequence, timeout_sec):
        self.sequence = int(sequence)
        self.timeout_sec = float(timeout_sec)
        super().__init__(
            f"Engine sequence={sequence} exceeded timeout={timeout_sec} sec",
        )


class WorkingEngineExecutor:
    """Never mutates active Engine until the caller durably commits working."""

    def __init__(self, tracker: StageProgressTracker, *, lock_timeout_sec=5.0):
        if lock_timeout_sec <= 0:
            raise ValueError("Engine staging lock timeout must be positive")
        self.tracker = tracker
        self.lock_timeout_sec = float(lock_timeout_sec)
        self._lock = threading.Lock()
        self._lock_holder = None
        self._worker = None

    @contextlib.contextmanager
    def _locked(self):
        started = time.monotonic()
        acquired = self._lock.acquire(timeout=self.lock_timeout_sec)
        waited = time.monotonic() - started
        if not acquired:
            raise LiveStageLockTimeout(
                "engine-staging", self._lock_holder, waited,
            )
        self._lock_holder = threading.current_thread().name
        try:
            yield
        finally:
            self._lock_holder = None
            self._lock.release()

    @property
    def unrecoverable_worker_alive(self):
        with self._locked():
            return self._worker is not None and self._worker.is_alive()

    def run(self, *, active_engine, engine_input, sequence, timeout_sec,
            operation=None):
        if (isinstance(timeout_sec, bool) or not isinstance(timeout_sec, (int, float))
                or not math.isfinite(timeout_sec) or timeout_sec <= 0):
            raise ValueError("Engine timeout must be finite and positive")
        with self._locked():
            if self._worker is not None and self._worker.is_alive():
                raise RuntimeError("Engine worker is still active")
            working = active_engine.fork_for_window()
            if self.tracker.snapshot().get("working_engine_fingerprint"):
                self.tracker.materialize_working_engine()
            result_queue = queue.Queue(maxsize=1)

            def invoke():
                try:
                    result = (
                        operation(working, engine_input)
                        if operation is not None
                        else working.process_window(engine_input)
                    )
                    result_queue.put((True, result))
                except BaseException as error:
                    result_queue.put((False, error))

            self._worker = threading.Thread(
                target=invoke,
                name="proberca-working-engine",
                daemon=True,
            )
            worker = self._worker
        self.tracker.enter(LiveStage.ENGINE_PROCESS, sequence=sequence)
        worker.start()
        worker.join(timeout_sec)
        if worker.is_alive():
            self.tracker.timeout(reason_code="engine_process_timeout")
            raise EngineStageTimeout(sequence, timeout_sec)
        with self._locked():
            self._worker = None
        succeeded, value = result_queue.get_nowait()
        if not succeeded:
            self.tracker.record_error(value, "engine_process_failed")
            raise value
        self.tracker.exit(output_count=1)
        return working, value

"""Monotonic live-window stall detection and bounded fail-stop supervision."""
from __future__ import annotations

import faulthandler
import os
import threading
import time
from enum import Enum

from .health import LiveHealthState
from .progress import LiveStage


class WatchdogDecision(str, Enum):
    HEALTHY = "healthy"
    STALLED = "stalled"
    INACTIVE = "inactive"


class LiveWindowWatchdog:
    def __init__(
        self,
        tracker,
        health=None,
        *,
        progress_timeout_sec,
        stage_timeouts,
        backlog_fatal_threshold=None,
        dump_grace_sec=5.0,
        exit_grace_sec=15.0,
        poll_interval_sec=1.0,
        stack_dump=None,
        abort_transaction=None,
        stop_lease_renew=None,
        exit_process=None,
        state_provider=None,
        clock=time.monotonic,
    ):
        if progress_timeout_sec <= 0:
            raise ValueError("progress_timeout_sec must be positive")
        if not (0 < dump_grace_sec < exit_grace_sec):
            raise ValueError("watchdog grace intervals are invalid")
        if poll_interval_sec <= 0:
            raise ValueError("watchdog poll interval must be positive")
        normalized = {}
        for stage, timeout in stage_timeouts.items():
            if not isinstance(stage, LiveStage) or timeout <= 0:
                raise ValueError("stage timeout is invalid")
            normalized[stage] = float(timeout)
        self.tracker = tracker
        self.health = health or LiveHealthState()
        self.progress_timeout_sec = float(progress_timeout_sec)
        self.stage_timeouts = normalized
        if backlog_fatal_threshold is not None and backlog_fatal_threshold <= 0:
            raise ValueError("backlog fatal threshold must be positive")
        self.backlog_fatal_threshold = (
            int(backlog_fatal_threshold)
            if backlog_fatal_threshold is not None else None
        )
        self.dump_grace_sec = float(dump_grace_sec)
        self.exit_grace_sec = float(exit_grace_sec)
        self.poll_interval_sec = float(poll_interval_sec)
        self.stack_dump = stack_dump or (
            lambda: faulthandler.dump_traceback(all_threads=True)
        )
        self.abort_transaction = abort_transaction or (lambda: None)
        self.stop_lease_renew = stop_lease_renew or (lambda: None)
        self.exit_process = exit_process or (lambda code: os._exit(code))
        self.state_provider = state_provider
        self.clock = clock
        self.last_commit_monotonic = self.clock()
        self.last_committed_sequence = 0
        self._stalled = False
        self._fail_stopped = False
        self._stop = threading.Event()
        self._thread = None

    @property
    def stalled(self):
        return self._stalled

    def note_commit(self, *, sequence: int) -> None:
        self.last_committed_sequence = int(sequence)
        self.last_commit_monotonic = self.clock()
        self._stalled = False
        self.health.update(
            progress_stalled=False,
            stage_timed_out=False,
            collection_retrying=False,
        )

    def _mark_stalled(self, snapshot, *, reason_code):
        if self._stalled:
            return
        self._stalled = True
        self.health.update(progress_stalled=True)
        self.health.update_progress(snapshot | {"stalled_reason": reason_code})
        self.health.increment("live_watchdog_stall_total")
        try:
            self.stack_dump()
        except BaseException as error:
            snapshot["last_structured_error"] = {
                "error_type": type(error).__name__,
                "reason_code": "watchdog_stack_dump_failed",
            }
            self.health.update_progress(snapshot)
        try:
            self.abort_transaction()
        except BaseException as error:
            snapshot["last_structured_error"] = {
                "error_type": type(error).__name__,
                "reason_code": "watchdog_abort_failed",
            }
            self.health.update_progress(snapshot)

    def evaluate(self, *, leader_active: bool, backlog_count: int,
                 active_transaction: bool, working_engine_count=0) -> WatchdogDecision:
        if self._stalled:
            return WatchdogDecision.STALLED
        snapshot = self.tracker.snapshot()
        now = self.clock()
        snapshot["backlog_count"] = int(backlog_count)
        snapshot["last_committed_sequence"] = self.last_committed_sequence
        snapshot["last_commit_age_sec"] = max(
            0.0, now - self.last_commit_monotonic,
        )
        snapshot["active_transaction"] = bool(active_transaction)
        snapshot["working_engine_count"] = int(working_engine_count)
        self.health.update_progress(snapshot)
        if not leader_active or backlog_count <= 0:
            self.health.update(progress_stalled=False, stage_timed_out=False)
            return WatchdogDecision.INACTIVE
        stage = LiveStage(snapshot["stage"])
        stage_timeout = self.stage_timeouts.get(stage)
        stage_stalled = (
            active_transaction
            and stage_timeout is not None
            and snapshot["stage_age_sec"] > stage_timeout
        )
        progress_stalled = (
            snapshot["last_commit_age_sec"] > self.progress_timeout_sec
        )
        backlog_fatal = (
            self.backlog_fatal_threshold is not None
            and backlog_count >= self.backlog_fatal_threshold
        )
        if stage_stalled or progress_stalled or backlog_fatal:
            if stage_stalled:
                self.health.increment("live_stage_timeout_total")
            if backlog_fatal:
                reason_code = "backlog_fatal_threshold"
            elif stage_stalled:
                reason_code = "stage_deadline_exceeded"
            else:
                reason_code = "commit_progress_timeout"
            self._mark_stalled(snapshot, reason_code=reason_code)
            return WatchdogDecision.STALLED
        self.health.update(progress_stalled=False, stage_timed_out=False)
        return WatchdogDecision.HEALTHY

    def poll(self, *, backlog_count, last_commit_monotonic,
             leader_active, active_transaction=True, working_engine_count=0):
        if self._stalled:
            return True
        self.last_commit_monotonic = float(last_commit_monotonic)
        return self.evaluate(
            leader_active=leader_active,
            backlog_count=backlog_count,
            active_transaction=active_transaction,
            working_engine_count=working_engine_count,
        ) is WatchdogDecision.STALLED

    def enforce_grace(self):
        if not self._stalled or self._fail_stopped:
            return False
        self._fail_stopped = True
        self.stop_lease_renew()
        self.health.increment("live_fail_stop_total")
        self.health.update(fatal_error="live_stage_stalled")
        self.exit_process(8)
        return True

    def _run(self):
        while not self._stop.wait(self.poll_interval_sec):
            if self.state_provider is None:
                continue
            try:
                state = self.state_provider()
                stalled = self.poll(**state)
            except BaseException as error:
                snapshot = self.tracker.snapshot()
                snapshot["last_structured_error"] = {
                    "error_type": type(error).__name__,
                    "reason_code": "watchdog_state_error",
                }
                self._mark_stalled(
                    snapshot, reason_code="watchdog_state_error",
                )
                stalled = True
            if stalled:
                if self._stop.wait(self.dump_grace_sec):
                    return
                remaining = self.exit_grace_sec - self.dump_grace_sec
                if self._stop.wait(remaining):
                    return
                if not self._stalled:
                    continue
                self.enforce_grace()
                return

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("live watchdog is already running")
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="proberca-live-watchdog",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._stop.set()

    def join(self, timeout_sec):
        if timeout_sec <= 0:
            raise ValueError("watchdog join timeout must be positive")
        if self._thread is not None:
            self._thread.join(timeout_sec)
            if self._thread.is_alive():
                raise RuntimeError("live watchdog did not stop within timeout")

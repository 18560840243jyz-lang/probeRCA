from proberca.live.health import LiveHealthState
from proberca.live.progress import LiveStage, StageProgressTracker
from proberca.live.watchdog import LiveWindowWatchdog, WatchdogDecision


def test_watchdog_detects_backlog_without_commit_and_marks_not_ready():
    now = [100.0]
    tracker = StageProgressTracker(clock=lambda: now[0])
    tracker.enter(
        LiveStage.COLLECT_CALL_EDGES,
        sequence=24,
        window_start_ns=100,
        window_end_ns=200,
        transaction_id="tx",
        leadership_epoch_fingerprint="epoch",
        backlog_count=8,
        retry_attempt=1,
    )
    health = LiveHealthState()
    health.update(
        kubernetes_connected=True,
        watchers_synchronized=True,
        prometheus_healthy=True,
        leader=True,
        checkpoint_writable=True,
        output_writable=True,
        engine_available=True,
        inventory_stale=False,
    )
    watchdog = LiveWindowWatchdog(
        tracker,
        health,
        progress_timeout_sec=5.0,
        stage_timeouts={LiveStage.COLLECT_CALL_EDGES: 3.0},
        clock=lambda: now[0],
    )

    watchdog.note_commit(sequence=23)
    now[0] = 106.0
    decision = watchdog.evaluate(
        leader_active=True,
        backlog_count=8,
        active_transaction=True,
    )

    assert decision is WatchdogDecision.STALLED
    assert not health.ready
    assert "live_progress_stalled" in health.reason_codes()
    assert health.status()["progress"]["stage"] == "COLLECT_CALL_EDGES"


def test_watchdog_allows_backlog_while_progress_is_recent():
    now = [100.0]
    tracker = StageProgressTracker(clock=lambda: now[0])
    tracker.enter(
        LiveStage.BEGIN_WINDOW,
        sequence=24,
        window_start_ns=100,
        window_end_ns=200,
        transaction_id="tx",
        leadership_epoch_fingerprint="epoch",
        backlog_count=2,
        retry_attempt=1,
    )
    health = LiveHealthState()
    watchdog = LiveWindowWatchdog(
        tracker,
        health,
        progress_timeout_sec=5.0,
        stage_timeouts={LiveStage.BEGIN_WINDOW: 4.0},
        clock=lambda: now[0],
    )
    watchdog.note_commit(sequence=23)
    now[0] = 102.0
    decision = watchdog.evaluate(
        leader_active=True,
        backlog_count=2,
        active_transaction=True,
    )
    assert decision is WatchdogDecision.HEALTHY
    assert not watchdog.stalled


def test_watchdog_fatal_backlog_stalls_even_when_commit_is_recent():
    now = [100.0]
    tracker = StageProgressTracker(clock=lambda: now[0])
    tracker.enter(
        LiveStage.BEGIN_WINDOW,
        sequence=24,
        window_start_ns=100,
        window_end_ns=200,
        transaction_id="tx",
        leadership_epoch_fingerprint="epoch",
        backlog_count=10,
        retry_attempt=1,
    )
    dumps = []
    watchdog = LiveWindowWatchdog(
        tracker,
        LiveHealthState(),
        progress_timeout_sec=30.0,
        stage_timeouts={LiveStage.BEGIN_WINDOW: 20.0},
        backlog_fatal_threshold=10,
        stack_dump=lambda: dumps.append("dump"),
        clock=lambda: now[0],
    )
    watchdog.note_commit(sequence=23)
    decision = watchdog.evaluate(
        leader_active=True,
        backlog_count=10,
        active_transaction=True,
    )
    assert decision is WatchdogDecision.STALLED
    assert dumps == ["dump"]
    assert watchdog.health.status()["progress"]["stalled_reason"] == "backlog_fatal_threshold"


def test_watchdog_waits_full_exit_grace_before_fail_stop():
    import threading
    import time

    tracker = StageProgressTracker()
    tracker.enter(
        LiveStage.ENGINE_PROCESS, sequence=24,
        window_start_ns=100, window_end_ns=200,
        transaction_id="tx", leadership_epoch_fingerprint="epoch",
        backlog_count=1, retry_attempt=1,
    )
    exited = threading.Event()
    exit_times = []
    started = time.monotonic()
    watchdog = LiveWindowWatchdog(
        tracker, LiveHealthState(), progress_timeout_sec=0.01,
        stage_timeouts={LiveStage.ENGINE_PROCESS: 0.01},
        dump_grace_sec=0.01, exit_grace_sec=0.06,
        poll_interval_sec=0.005, stack_dump=lambda: None,
        exit_process=lambda code: (
            exit_times.append((code, time.monotonic())), exited.set(),
        ),
        state_provider=lambda: {
            "backlog_count": 1,
            "last_commit_monotonic": started - 1.0,
            "leader_active": True, "active_transaction": True,
        },
    )
    watchdog.start()
    try:
        assert exited.wait(0.5)
        assert exit_times[0][0] == 8
        assert exit_times[0][1] - started >= 0.055
    finally:
        watchdog.stop()
        watchdog.join(0.5)


def test_watchdog_state_provider_failure_is_fail_stopped_not_silent():
    import threading

    tracker = StageProgressTracker(maximum_history=8)
    tracker.enter(LiveStage.BEGIN_WINDOW, sequence=24, backlog_count=1)
    dumps = []
    exits = []
    exited = threading.Event()

    def broken_state():
        raise RuntimeError("state lock timeout")

    watchdog = LiveWindowWatchdog(
        tracker, LiveHealthState(),
        progress_timeout_sec=1.0,
        stage_timeouts={LiveStage.BEGIN_WINDOW: 1.0},
        dump_grace_sec=0.01, exit_grace_sec=0.03,
        poll_interval_sec=0.005,
        stack_dump=lambda: dumps.append("dump"),
        exit_process=lambda code: (exits.append(code), exited.set()),
        state_provider=broken_state,
    )
    watchdog.start()
    try:
        assert exited.wait(0.3)
        assert dumps == ["dump"]
        assert exits == [8]
        assert not watchdog.health.ready
        assert watchdog.health.status()["progress"]["stalled_reason"] == "watchdog_state_error"
    finally:
        watchdog.stop()
        watchdog.join(0.5)

def test_watchdog_abort_callback_failure_still_reaches_fail_stop():
    import threading

    tracker = StageProgressTracker(maximum_history=8)
    tracker.enter(LiveStage.ENGINE_PROCESS, sequence=24, backlog_count=1)
    exited = threading.Event()
    exits = []

    def abort_fails():
        raise RuntimeError("worker still active")

    watchdog = LiveWindowWatchdog(
        tracker, LiveHealthState(),
        progress_timeout_sec=0.01,
        stage_timeouts={LiveStage.ENGINE_PROCESS: 0.01},
        dump_grace_sec=0.01, exit_grace_sec=0.03,
        poll_interval_sec=0.005,
        stack_dump=lambda: None,
        abort_transaction=abort_fails,
        exit_process=lambda code: (exits.append(code), exited.set()),
        state_provider=lambda: {
            "backlog_count": 1,
            "last_commit_monotonic": 0.0,
            "leader_active": True,
            "active_transaction": True,
        },
    )
    watchdog.start()
    try:
        assert exited.wait(0.3)
        assert exits == [8]
        error = watchdog.health.status()["progress"]["last_structured_error"]
        assert error["reason_code"] == "watchdog_abort_failed"
    finally:
        watchdog.stop()
        watchdog.join(0.5)


def test_watchdog_status_reports_active_working_worker_count():
    tracker = StageProgressTracker(maximum_history=8)
    tracker.enter(LiveStage.ENGINE_PROCESS, sequence=24, backlog_count=1)
    health = LiveHealthState()
    watchdog = LiveWindowWatchdog(
        tracker, health,
        progress_timeout_sec=10.0,
        stage_timeouts={LiveStage.ENGINE_PROCESS: 10.0},
        clock=lambda: 1.0,
    )
    watchdog.last_commit_monotonic = 1.0
    watchdog.poll(
        backlog_count=1,
        last_commit_monotonic=1.0,
        leader_active=True,
        active_transaction=True,
        working_engine_count=1,
    )
    assert health.status()["progress"]["working_engine_count"] == 1

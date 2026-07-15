from proberca.live.health import LiveHealthState
from proberca.live.progress import LiveStage, StageProgressTracker
from proberca.live.watchdog import LiveWindowWatchdog


def test_watchdog_stall_dumps_once_marks_not_ready_and_fail_stops():
    ticks = iter((0.0, 6.0, 7.0, 9.0, 11.0))
    tracker = StageProgressTracker(clock=lambda: next(ticks), maximum_history=16)
    tracker.enter(LiveStage.ENGINE_PROCESS, sequence=24, backlog_count=5)
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
    dumps, exits, aborts, renew = [], [], [], []
    watchdog = LiveWindowWatchdog(
        tracker,
        health=health,
        stage_timeouts={LiveStage.ENGINE_PROCESS: 5.0},
        progress_timeout_sec=10.0,
        dump_grace_sec=1.0,
        exit_grace_sec=2.0,
        stack_dump=lambda: dumps.append("dump"),
        abort_transaction=lambda: aborts.append(24),
        stop_lease_renew=lambda: renew.append("stopped"),
        exit_process=lambda code: exits.append(code),
        clock=lambda: next(ticks),
    )

    assert watchdog.poll(backlog_count=5, last_commit_monotonic=0.0, leader_active=True)
    assert not health.ready
    assert health.counter("live_watchdog_stall_total") == 1
    assert dumps == ["dump"]
    assert aborts == [24]
    watchdog.poll(backlog_count=5, last_commit_monotonic=0.0, leader_active=True)
    assert dumps == ["dump"]
    watchdog.enforce_grace()
    assert renew == ["stopped"]
    assert exits == [8]


def test_watchdog_does_not_stall_without_backlog_or_active_leader():
    tracker = StageProgressTracker(clock=lambda: 0.0, maximum_history=4)
    tracker.enter(LiveStage.IDLE, sequence=24, backlog_count=0)
    watchdog = LiveWindowWatchdog(
        tracker,
        stage_timeouts={LiveStage.IDLE: 1.0},
        progress_timeout_sec=1.0,
        dump_grace_sec=0.1,
        exit_grace_sec=0.2,
        stack_dump=lambda: (_ for _ in ()).throw(AssertionError()),
        abort_transaction=lambda: None,
        stop_lease_renew=lambda: None,
        exit_process=lambda _: None,
        clock=lambda: 10.0,
    )
    assert not watchdog.poll(
        backlog_count=0,
        last_commit_monotonic=0.0,
        leader_active=True,
    )
    assert not watchdog.poll(
        backlog_count=4,
        last_commit_monotonic=0.0,
        leader_active=False,
    )


def test_live_cli_fail_stops_after_committed_output_worker_stalls():
    import inspect

    from proberca.cli import live

    source = inspect.getsource(live._run_live)
    marker = "except CommittedOutputStalledError"
    assert marker in source
    branch = source[source.index(marker):]
    assert "return 8" in branch[:1200]
    assert "dump_all_threads()" in branch[:1200]


def test_live_cli_stage_timeout_dumps_threads_and_returns_fail_stop_code():
    import inspect

    from proberca.cli import live

    source = inspect.getsource(live._run_live)
    marker = "except (LiveStageTimeoutError, EngineStageTimeout)"
    assert marker in source
    branch = source[source.index(marker):]
    generic = branch.index("except Exception")
    timeout_branch = branch[:generic]
    assert "dump_all_threads()" in timeout_branch
    assert "return 8" in timeout_branch


def test_stage_timeout_with_live_worker_bypasses_unsafe_discard():
    from types import SimpleNamespace

    import pytest

    from proberca.config import LiveLivenessConfig
    from proberca.live.executor import LiveStageTimeoutError
    from proberca.live.runner import process_window_with_retry

    class Runner:
        active_worker_count = 1

        def __init__(self):
            self.discard_calls = 0

        def process_window(self, window, *, attempt):
            raise LiveStageTimeoutError(
                LiveStage.FREEZE_REVISION,
                window.sequence,
                1.0,
            )

        def discard_uncommitted(self):
            self.discard_calls += 1
            raise AssertionError("unsafe discard reached a live worker")

    runner = Runner()
    with pytest.raises(LiveStageTimeoutError):
        process_window_with_retry(
            runner,
            SimpleNamespace(sequence=24),
            liveness_config=LiveLivenessConfig(),
            health=None,
            transient_error_types=(RuntimeError,),
            sleep=lambda _: None,
        )
    assert runner.discard_calls == 0

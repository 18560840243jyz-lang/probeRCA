import threading
import time

import pytest

from proberca.live.executor import LiveStageTimeoutError, StageExecutor
from proberca.live.progress import LiveStage, StageEventType, StageProgressTracker


def test_stage_executor_records_paired_enter_exit_and_result_count():
    tracker = StageProgressTracker(maximum_history=16)
    executor = StageExecutor(tracker)

    result = executor.run_stage(
        LiveStage.COLLECT_NODE_METRICS,
        sequence=24,
        timeout_sec=1.0,
        operation=lambda: [1, 2, 3],
    )

    assert result == [1, 2, 3]
    events = tracker.events()
    assert [item.event_type for item in events] == [
        StageEventType.ENTER, StageEventType.EXIT,
    ]
    assert events[-1].output_count == 3
    assert events[-1].sequence == 24


def test_stage_executor_timeout_is_bounded_and_does_not_return_candidate():
    release = threading.Event()
    tracker = StageProgressTracker(maximum_history=16)
    executor = StageExecutor(tracker)

    with pytest.raises(LiveStageTimeoutError) as caught:
        executor.run_stage(
            LiveStage.ENGINE_PROCESS,
            sequence=24,
            timeout_sec=0.02,
            operation=lambda: release.wait(5.0),
        )
    release.set()

    assert caught.value.stage is LiveStage.ENGINE_PROCESS
    assert caught.value.sequence == 24
    assert tracker.events()[-1].event_type is StageEventType.TIMEOUT


def test_stage_executor_rejects_concurrent_window_stage():
    entered = threading.Event()
    release = threading.Event()
    executor = StageExecutor(StageProgressTracker(maximum_history=16))

    worker = threading.Thread(target=lambda: executor.run_stage(
        LiveStage.FREEZE_REVISION,
        sequence=24,
        timeout_sec=1.0,
        operation=lambda: (entered.set(), release.wait(1.0)),
    ))
    worker.start()
    assert entered.wait(0.5)
    with pytest.raises(RuntimeError, match="already active"):
        executor.run_stage(
            LiveStage.BUILD_TOPOLOGY,
            sequence=24,
            timeout_sec=0.1,
            operation=lambda: None,
        )
    release.set()
    worker.join(1.0)
    assert not worker.is_alive()


def test_stage_executor_timeout_values_must_be_finite_positive():
    executor = StageExecutor(StageProgressTracker(maximum_history=4))
    for value in (0, -1, float("inf"), float("nan"), None):
        with pytest.raises((TypeError, ValueError)):
            executor.run_stage(
                LiveStage.BEGIN_WINDOW,
                sequence=1,
                timeout_sec=value,
                operation=lambda: None,
            )


def test_stage_executor_internal_lock_wait_is_bounded():
    from proberca.live.executor import LiveStageLockTimeout

    executor = StageExecutor(
        StageProgressTracker(maximum_history=4),
        lock_timeout_sec=0.02,
        lock_name="live-transaction",
    )
    executor._state_lock.acquire()
    try:
        with pytest.raises(LiveStageLockTimeout) as caught:
            _ = executor.active_worker_count
        assert caught.value.lock_name == "live-transaction"
        assert caught.value.wait_duration_sec >= 0.02
    finally:
        executor._state_lock.release()


def test_engine_staging_internal_lock_wait_is_bounded():
    from proberca.live.engine_worker import WorkingEngineExecutor
    from proberca.live.executor import LiveStageLockTimeout

    executor = WorkingEngineExecutor(
        StageProgressTracker(maximum_history=4),
        lock_timeout_sec=0.02,
    )
    executor._lock.acquire()
    try:
        with pytest.raises(LiveStageLockTimeout) as caught:
            _ = executor.unrecoverable_worker_alive
        assert caught.value.lock_name == "engine-staging"
    finally:
        executor._lock.release()

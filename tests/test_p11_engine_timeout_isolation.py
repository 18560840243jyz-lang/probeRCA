import threading

import pytest

from proberca.live.engine_worker import EngineStageTimeout, WorkingEngineExecutor
from proberca.live.progress import StageProgressTracker


class Engine:
    def __init__(self, state):
        self.state = list(state)

    def fork_for_window(self):
        return Engine(self.state)

    def process_window(self, value):
        self.state.append(value)
        return tuple(self.state)

    def adopt_committed_working_engine(self, working):
        self.state = list(working.state)


def test_engine_timeout_does_not_mutate_active_engine():
    active = Engine(["baseline", "alert", "p4", "p5", "pending"])
    release = threading.Event()
    executor = WorkingEngineExecutor(StageProgressTracker(maximum_history=16))

    def blocked(working, _value):
        working.state.append("uncommitted")
        release.wait(5.0)

    with pytest.raises(EngineStageTimeout):
        executor.run(
            active_engine=active,
            engine_input="window-24",
            sequence=24,
            timeout_sec=0.02,
            operation=blocked,
        )
    release.set()

    assert active.state == ["baseline", "alert", "p4", "p5", "pending"]
    assert executor.unrecoverable_worker_alive


def test_engine_success_requires_explicit_commit_to_replace_active():
    active = Engine(["committed"])
    executor = WorkingEngineExecutor(StageProgressTracker(maximum_history=16))
    working, result = executor.run(
        active_engine=active,
        engine_input="window-24",
        sequence=24,
        timeout_sec=1.0,
    )

    assert result == ("committed", "window-24")
    assert active.state == ["committed"]
    active.adopt_committed_working_engine(working)
    assert active.state == ["committed", "window-24"]


def test_second_engine_worker_is_rejected_while_timed_out_worker_lives():
    active = Engine([])
    release = threading.Event()
    executor = WorkingEngineExecutor(StageProgressTracker(maximum_history=16))
    with pytest.raises(EngineStageTimeout):
        executor.run(
            active_engine=active,
            engine_input=1,
            sequence=24,
            timeout_sec=0.01,
            operation=lambda *_: release.wait(5.0),
        )
    with pytest.raises(RuntimeError, match="worker is still active"):
        executor.run(
            active_engine=active,
            engine_input=2,
            sequence=24,
            timeout_sec=0.01,
        )
    release.set()

from proberca.live.transaction import CommitPhase, LiveWindowTransactionState


def test_pre_commit_abort_does_not_advance_durable_sequence():
    state = LiveWindowTransactionState(
        expected_sequence=24,
        committed_sequence=23,
    )
    state.enter_phase(CommitPhase.PRE_COMMIT)
    state.abort("engine_timeout")
    assert state.committed_sequence == 23
    assert state.next_sequence == 24
    assert state.aborted


def test_run_state_cas_is_the_commit_point():
    state = LiveWindowTransactionState(
        expected_sequence=24,
        committed_sequence=23,
    )
    state.mark_run_state_committed()
    assert state.phase is CommitPhase.COMMITTED
    assert state.committed_sequence == 24
    assert state.next_sequence == 25


def test_output_stall_after_cas_is_degraded_and_not_replayed():
    state = LiveWindowTransactionState(
        expected_sequence=24,
        committed_sequence=23,
    )
    state.mark_run_state_committed()
    state.mark_output_degraded("output_timeout")
    assert state.phase is CommitPhase.OUTPUT_DEGRADED
    assert state.committed_sequence == 24
    assert state.next_sequence == 25
    assert not state.should_replay_sequence(24)


def test_gap_or_duplicate_commit_is_rejected():
    state = LiveWindowTransactionState(
        expected_sequence=24,
        committed_sequence=23,
    )
    state.mark_run_state_committed()
    try:
        state.mark_run_state_committed()
    except RuntimeError as error:
        assert "already committed" in str(error)
    else:
        raise AssertionError("duplicate commit was accepted")


def test_runner_output_timeout_after_cas_requires_fail_stop_without_replay():
    import threading
    from types import SimpleNamespace

    import pytest

    from proberca.config import LiveLivenessConfig
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import (
        CommittedOutputStalledError, ProbeRCALiveRunner,
    )

    release = threading.Event()
    from proberca.live.health import LiveHealthState
    health = LiveHealthState()
    health.update(leader=True)

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def __init__(self):
            self.committed = []

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **_payload):
            return object()

        def commit_run_state(self, context, _generation):
            self.committed.append(context.sequence)

        def project_output(self, _context, _generation):
            release.wait(5.0)

        def apply_retention(self, _generation):
            raise AssertionError("retention must not run after output stall")

    coordinator = Coordinator()
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        health=health,
        liveness_config=LiveLivenessConfig(
            output_projection_timeout_sec=0.02,
        ),
    )
    with pytest.raises(CommittedOutputStalledError):
        runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    assert coordinator.committed == [24]
    assert runner.active_worker_count == 1
    assert health.counter("live_stage_timeout_total") == 1
    release.set()

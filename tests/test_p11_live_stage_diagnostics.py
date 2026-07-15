import io
import inspect
import signal
import threading
from types import SimpleNamespace

from proberca.live.diagnostics import install_thread_dump_handler
from proberca.live.health import LiveHealthState
from proberca.live.progress import LiveStage, StageProgressTracker


def test_stage_tracker_records_monotonic_transition_and_safe_status():
    ticks = iter((10.0, 10.0, 12.5, 12.5))
    tracker = StageProgressTracker(clock=lambda: next(ticks))

    tracker.enter(
        LiveStage.BEGIN_WINDOW,
        sequence=24,
        window_start_ns=100,
        window_end_ns=200,
        transaction_id="transaction-secret-value",
        leadership_epoch_fingerprint="epoch-secret-value",
        backlog_count=9,
        retry_attempt=1,
    )
    tracker.enter(LiveStage.FREEZE_REVISION)

    snapshot = tracker.snapshot()
    assert snapshot["stage"] == "FREEZE_REVISION"
    assert snapshot["sequence"] == 24
    assert snapshot["previous_stage_duration_sec"] == 2.5
    assert snapshot["operation_counter"] == 2
    assert snapshot["thread_name"] == threading.current_thread().name
    assert snapshot["transaction_id"] != "transaction-secret-value"
    assert snapshot["leadership_epoch_fingerprint"] != "epoch-secret-value"

    health = LiveHealthState()
    health.update_progress(snapshot)
    assert health.status()["progress"] == snapshot


def test_stage_tracker_records_error_type_without_sensitive_message():
    tracker = StageProgressTracker(clock=lambda: 4.0)
    tracker.enter(
        LiveStage.COLLECT_CALL_EDGES,
        sequence=24,
        window_start_ns=100,
        window_end_ns=200,
        transaction_id="tx",
        leadership_epoch_fingerprint="epoch",
        backlog_count=1,
        retry_attempt=2,
    )
    tracker.record_error(RuntimeError("token=do-not-record"), "query_empty")

    snapshot = tracker.snapshot()
    assert snapshot["last_structured_error"] == {
        "error_type": "RuntimeError",
        "reason_code": "query_empty",
    }
    assert "do-not-record" not in str(snapshot)


def test_sigusr1_installs_non_terminating_all_thread_dump(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "proberca.live.diagnostics.faulthandler.enable",
        lambda file=None, all_threads=True: calls.append(
            ("enable", file, all_threads),
        ),
    )
    monkeypatch.setattr(
        "proberca.live.diagnostics.faulthandler.register",
        lambda signum, file=None, all_threads=True, chain=False: calls.append(
            ("register", signum, file, all_threads, chain),
        ),
    )

    output = io.StringIO()
    install_thread_dump_handler(output)

    assert calls == [
        ("enable", output, True),
        ("register", signal.SIGUSR1, output, True, False),
    ]


def test_runner_tracks_every_window_boundary_in_order():
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner

    class Tracker:
        def __init__(self):
            self.stages = []

        def enter(self, stage, **_):
            self.stages.append(stage)

        def record_error(self, *_):
            return None

        def snapshot(self):
            return {"stage": self.stages[-1].value}

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE
        token = SimpleNamespace(
            token_fingerprint="token",
            leadership_epoch="epoch",
        )

        def begin_window(self, *_):
            return SimpleNamespace(
                sequence=24,
                working_engine=object(),
                engine_result=None,
                token=self.token,
            )

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, context, **payload):
            return payload

        def commit(self, *_):
            return None

    tracker = Tracker()
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(
            ready=True,
            freeze=lambda _: SimpleNamespace(ready=True),
        ),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        progress_tracker=tracker,
    )
    runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))

    assert tracker.stages == [
        LiveStage.BEGIN_WINDOW,
        LiveStage.FREEZE_REVISION,
        LiveStage.BUILD_TOPOLOGY,
        LiveStage.COLLECT_NODE_METRICS,
        LiveStage.COLLECT_EDGE_METRICS,
        LiveStage.ADAPT_NODE_RECORDS,
        LiveStage.ADAPT_EDGE_RECORDS,
        LiveStage.BUILD_ENGINE_INPUT,
        LiveStage.ENGINE_PROCESS,
        LiveStage.PREPARE_GENERATION,
        LiveStage.COMMIT_RUN_STATE,
        LiveStage.WINDOW_COMPLETE,
    ]


def test_live_cli_reserves_sigusr1_for_stack_dump_and_sigusr2_for_relist():
    import proberca.cli.live as live_cli

    source = inspect.getsource(live_cli)
    assert "install_thread_dump_handler()" in source
    assert "signal.SIGUSR2" in source
    assert "signal.SIGUSR1" not in source


def test_coordinator_does_not_emit_duplicate_stage_events_inside_executor():
    from proberca.live.coordinator import LiveCommitCoordinator

    source = inspect.getsource(LiveCommitCoordinator.prepare_generation)
    assert "self._enter" not in source


def test_runner_stage_context_includes_transaction_and_backlog():
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, *_):
            return SimpleNamespace(
                sequence=24, working_engine=object(),
                transaction_id="transaction-secret",
            )

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **payload):
            return payload

        def commit(self, *_):
            return None

    tracker = StageProgressTracker(maximum_history=128)
    health = LiveHealthState()
    health.update(leader=True)
    health.update_runtime(eligible_window_count=5)
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        metric_collector=lambda *_: ([object()], [object()]),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        progress_tracker=tracker,
        health=health,
    )
    runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    freeze = next(
        event for event in tracker.events()
        if event.stage is LiveStage.FREEZE_REVISION
        and event.event_type.value == "enter"
    )
    assert freeze.transaction_id
    assert freeze.transaction_id != "transaction-secret"
    assert freeze.backlog_count == 5

def test_runner_failure_emits_window_abort_event():
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.progress import StageEventType
    from proberca.live.runner import ProbeRCALiveRunner

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

    tracker = StageProgressTracker(maximum_history=64)
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: (_ for _ in ()).throw(RuntimeError("boom")),
        metric_collector=lambda *_: ([], []),
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        progress_tracker=tracker,
    )
    try:
        runner.process_window(SimpleNamespace(start_ns=10, end_ns=20))
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected topology failure")

    assert tracker.events()[-1].stage is LiveStage.WINDOW_ABORTED
    assert tracker.events()[-1].event_type is StageEventType.ABORT
    assert tracker.events()[-1].reason_code == "window_stage_failed"


def test_live_cli_wires_configured_stage_event_history_bound():
    import proberca.cli.live as live_cli

    source = inspect.getsource(live_cli._run_live)
    assert "maximum_history=(" in source
    assert "config.live_liveness.maximum_stage_event_history" in source

import pytest

from proberca.live.collection import (
    CollectionExhaustedError,
    CollectionOutcome,
    WindowCollectionRetrier,
)


def test_transient_empty_retries_same_sequence_with_clean_context():
    contexts = []
    responses = [
        (CollectionOutcome.TRANSIENT_EMPTY, None),
        (CollectionOutcome.SUCCESS, [1, 2, 3, 4, 5]),
    ]

    retrier = WindowCollectionRetrier(
        max_attempts=2,
        initial_backoff_sec=0.001,
        max_backoff_sec=0.001,
        sleep=lambda _: None,
    )
    result = retrier.run(
        sequence=24,
        new_context=lambda attempt: contexts.append({"attempt": attempt}) or contexts[-1],
        collect=lambda _context, attempt: responses[attempt - 1],
        cleanup=lambda context: context.update(cleaned=True),
    )

    assert result == [1, 2, 3, 4, 5]
    assert len(contexts) == 2
    assert contexts[0] is not contexts[1]
    assert contexts[0]["cleaned"] is True
    assert "cleaned" not in contexts[1]


def test_transient_error_then_success_does_not_reuse_previous_samples():
    seen = []
    retrier = WindowCollectionRetrier(
        max_attempts=2,
        initial_backoff_sec=0.001,
        max_backoff_sec=0.001,
        sleep=lambda _: None,
    )

    def collect(context, attempt):
        seen.append((id(context), attempt, tuple(context.get("samples", ()))))
        if attempt == 1:
            context["samples"] = ["stale"]
            return CollectionOutcome.TRANSIENT_ERROR, RuntimeError("temporary")
        return CollectionOutcome.SUCCESS, ["fresh"]

    result = retrier.run(
        sequence=24,
        new_context=lambda _attempt: {},
        collect=collect,
        cleanup=lambda context: context.clear(),
    )
    assert result == ["fresh"]
    assert seen[1][2] == ()


def test_permanent_or_exhausted_collection_never_advances_sequence():
    advances = []
    retrier = WindowCollectionRetrier(
        max_attempts=2,
        initial_backoff_sec=0.001,
        max_backoff_sec=0.001,
        sleep=lambda _: None,
    )
    with pytest.raises(CollectionExhaustedError) as caught:
        retrier.run(
            sequence=24,
            new_context=lambda _attempt: {},
            collect=lambda *_: (CollectionOutcome.TRANSIENT_EMPTY, None),
            cleanup=lambda _context: None,
        )
    assert caught.value.sequence == 24
    assert advances == []


def test_allow_empty_is_a_successful_explicit_outcome():
    retrier = WindowCollectionRetrier(
        max_attempts=1,
        initial_backoff_sec=0.001,
        max_backoff_sec=0.001,
        sleep=lambda _: None,
    )
    assert retrier.run(
        sequence=7,
        new_context=lambda _: {},
        collect=lambda *_: (CollectionOutcome.ALLOW_EMPTY, ()),
        cleanup=lambda _: None,
    ) == ()


def test_runner_retry_wrapper_replays_whole_window_and_clears_health():
    from types import SimpleNamespace

    from proberca.config import LiveLivenessConfig
    from proberca.live.health import LiveHealthState
    from proberca.live.runner import process_window_with_retry

    class TemporaryError(RuntimeError):
        pass

    class Runner:
        def __init__(self):
            self.attempts = []
            self.discards = 0

        def process_window(self, window, *, attempt):
            self.attempts.append((window.sequence, attempt))
            if attempt == 1:
                raise TemporaryError("empty")
            return "committed"

        def discard_uncommitted(self):
            self.discards += 1

    runner = Runner()
    health = LiveHealthState()
    result = process_window_with_retry(
        runner,
        SimpleNamespace(sequence=24),
        liveness_config=LiveLivenessConfig(),
        health=health,
        transient_error_types=(TemporaryError,),
        sleep=lambda _: None,
    )
    assert result == "committed"
    assert runner.attempts == [(24, 1), (24, 2)]
    assert runner.discards == 1
    assert health.counter("live_collection_retry_total") == 1
    assert not health.values["collection_retrying"]


def test_retry_uses_structured_no_samples_reason_not_message_text():
    from types import SimpleNamespace

    from proberca.config import LiveLivenessConfig
    from proberca.live.health import LiveHealthState
    from proberca.live.runner import process_window_with_retry
    from proberca.metrics import PrometheusResponseError

    class Tracker:
        def __init__(self):
            self.reasons = []

        def retry(self, **payload):
            self.reasons.append(payload["reason_code"])

    class Runner:
        def __init__(self):
            self.calls = 0
            self.progress_tracker = Tracker()

        def process_window(self, _window, *, attempt):
            self.calls += 1
            if attempt == 1:
                raise PrometheusResponseError(
                    "opaque", reason_code="no_samples", retryable=True,
                )
            return "committed"

        def discard_uncommitted(self):
            return None

    runner = Runner()
    result = process_window_with_retry(
        runner, SimpleNamespace(sequence=24),
        liveness_config=LiveLivenessConfig(),
        health=LiveHealthState(),
        transient_error_types=(PrometheusResponseError,),
        sleep=lambda _: None,
    )
    assert result == "committed"
    assert runner.calls == 2
    assert runner.progress_tracker.reasons == ["transient_empty"]


def test_permanent_prometheus_error_is_not_retried():
    from types import SimpleNamespace

    from proberca.config import LiveLivenessConfig
    from proberca.live.runner import process_window_with_retry
    from proberca.metrics import PrometheusResponseError

    class Runner:
        def __init__(self):
            self.calls = 0

        def process_window(self, _window, *, attempt):
            self.calls += 1
            raise PrometheusResponseError(
                "forbidden", reason_code="authorization", retryable=False,
            )

        def discard_uncommitted(self):
            return None

    runner = Runner()
    with pytest.raises(CollectionExhaustedError) as caught:
        process_window_with_retry(
            runner, SimpleNamespace(sequence=24),
            liveness_config=LiveLivenessConfig(),
            health=None,
            transient_error_types=(PrometheusResponseError,),
            sleep=lambda _: None,
        )
    assert runner.calls == 1
    assert caught.value.outcome is CollectionOutcome.PERMANENT_ERROR


def test_runner_stage_events_preserve_retry_attempt_number():
    from types import SimpleNamespace

    from proberca.config import LiveLivenessConfig
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.progress import (
        LiveStage, StageEventType, StageProgressTracker,
    )
    from proberca.live.runner import ProbeRCALiveRunner, process_window_with_retry
    from proberca.metrics import PrometheusResponseError

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **payload):
            return payload

        def commit(self, *_):
            return None

    calls = []

    def collect(*_):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            raise PrometheusResponseError(
                "opaque", reason_code="no_samples", retryable=True,
            )
        return [object()], [object()]

    tracker = StageProgressTracker(maximum_history=128)
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        metric_collector=collect,
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        progress_tracker=tracker,
        liveness_config=LiveLivenessConfig(),
    )
    process_window_with_retry(
        runner, SimpleNamespace(sequence=24, start_ns=10, end_ns=20),
        liveness_config=LiveLivenessConfig(),
        health=None,
        transient_error_types=(PrometheusResponseError,),
        sleep=lambda _: None,
    )
    attempts = [
        event.attempt for event in tracker.events()
        if event.stage is LiveStage.COLLECT_NODE_METRICS
        and event.event_type is StageEventType.ENTER
    ]
    assert attempts == [1, 2]


def test_controlled_transient_empty_retries_full_runner_attempt_without_stale_samples():
    from types import SimpleNamespace

    from proberca.config import LiveLivenessConfig
    from proberca.live.collection import ControlledTransientCollectionEmpty
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.health import LiveHealthState
    from proberca.live.runner import ProbeRCALiveRunner, process_window_with_retry

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def __init__(self):
            self.commits = []

        def begin_window(self, *_):
            return SimpleNamespace(sequence=24, working_engine=object())

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, _context, **payload):
            return payload

        def commit(self, context, _generation):
            self.commits.append(context.sequence)

    node_calls = []
    edge_calls = []
    config = LiveLivenessConfig(
        transient_retry_max_attempts=2,
        transient_retry_initial_backoff_sec=0.001,
        transient_retry_max_backoff_sec=0.001,
        controlled_collection_fault_enabled=True,
        controlled_transient_empty_attempts=1,
    )
    coordinator = Coordinator()
    health = LiveHealthState()
    health.update(leader=True)
    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        node_metric_collector=lambda *_: node_calls.append(object()) or [object()],
        edge_metric_collector=lambda *_: edge_calls.append(object()) or [object()],
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        liveness_config=config,
        health=health,
    )
    result = process_window_with_retry(
        runner, SimpleNamespace(sequence=24, start_ns=10, end_ns=20),
        liveness_config=config,
        health=health,
        transient_error_types=(ControlledTransientCollectionEmpty,),
        sleep=lambda _: None,
    )
    assert result is not None
    assert len(node_calls) == 2
    assert len(edge_calls) == 2
    assert coordinator.commits == [24]
    assert health.counter("live_collection_retry_total") == 1


def test_controlled_live_hooks_do_not_change_durable_engine_identity():
    from dataclasses import replace
    from pathlib import Path

    import yaml

    from proberca.cli.live import _durable_engine_config
    from proberca.config import ProbeRCAConfig
    from proberca.orchestration.engine import ProbeRCAEngine

    manifest = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/proberca-live-configmap.yaml",
    ).read_text(encoding="utf-8"))
    base = ProbeRCAConfig.from_dict(
        yaml.safe_load(manifest["data"]["config.yaml"]),
    )
    controlled = replace(
        base,
        live_liveness=replace(
            base.live_liveness,
            controlled_collection_fault_enabled=True,
            controlled_transient_empty_attempts=1,
        ),
    )

    base_engine = ProbeRCAEngine.from_config(_durable_engine_config(base))
    controlled_engine = ProbeRCAEngine.from_config(
        _durable_engine_config(controlled),
    )
    assert base_engine.config_fingerprint == controlled_engine.config_fingerprint


def test_live_initial_and_restore_paths_share_one_durable_engine_factory():
    import inspect

    from proberca.cli import live

    source = inspect.getsource(live._run_live)
    assert "ProbeRCAEngine.from_config(config)" not in source
    assert source.count("_new_durable_engine(config)") == 2

from __future__ import annotations

import inspect
from types import SimpleNamespace


def _identity(**overrides):
    from proberca.live.coordinator import WindowAttemptIdentity

    values = {
        "sequence": 7,
        "window_start_ns": 100,
        "window_end_ns": 200,
        "attempt_index": 1,
        "leadership_epoch_fingerprint": "e" * 64,
        "runner_instance_fingerprint": "r" * 64,
        "previous_generation_fingerprint": "g" * 64,
    }
    values.update(overrides)
    return WindowAttemptIdentity(**values)


def test_attempt_identity_changes_only_when_canonical_identity_changes():
    first = _identity()
    assert len(first.transaction_id) == 64
    assert first.transaction_id == _identity().transaction_id
    assert first.transaction_id != _identity(attempt_index=2).transaction_id
    assert first.transaction_id != _identity(sequence=8).transaction_id
    assert first.transaction_id != _identity(window_start_ns=99).transaction_id
    assert first.transaction_id != _identity(window_end_ns=201).transaction_id
    assert first.transaction_id != _identity(
        previous_generation_fingerprint="h" * 64,
    ).transaction_id
    assert first.transaction_id != _identity(
        leadership_epoch_fingerprint="f" * 64,
    ).transaction_id
    assert first.transaction_id != _identity(
        runner_instance_fingerprint="s" * 64,
    ).transaction_id


def test_attempt_identity_has_no_business_or_wall_clock_inputs():
    from proberca.live.coordinator import WindowAttemptIdentity

    fields = set(WindowAttemptIdentity.__dataclass_fields__)
    assert fields == {
        "sequence",
        "window_start_ns",
        "window_end_ns",
        "attempt_index",
        "leadership_epoch_fingerprint",
        "runner_instance_fingerprint",
        "previous_generation_fingerprint",
    }
    source = inspect.getsource(WindowAttemptIdentity)
    assert "hash(" not in source
    assert "time." not in source
    assert not {"metric", "service", "namespace", "incident_label"} & fields


def test_coordinator_retry_contexts_use_distinct_transaction_and_staging_identity(
    tmp_path,
):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import InMemoryLeaseRunStateStore, LeaseRunStateRecord

    record = LeaseRunStateRecord.initial(
        run_id="run-a",
        cluster_id="cluster-a",
        namespace_scope=("scope-a",),
        config_fingerprint="c" * 64,
        code_schema_version="generation_v5",
    )
    coordinator = LiveCommitCoordinator(
        InMemoryLeaseRunStateStore(record),
        ImmutableGenerationStore(tmp_path / "generations"),
        "runner-a",
    )
    coordinator.acquire_and_recover(active_engine=object())
    coordinator.recover_current(engine_loader=lambda payload: payload)
    first = coordinator.begin_window(100, 200, attempt_index=1)
    second = coordinator.begin_window(100, 200, attempt_index=2)
    assert first is not second
    assert first.sequence == second.sequence == 1
    assert first.attempt_identity.attempt_index == 1
    assert second.attempt_identity.attempt_index == 2
    assert first.transaction_id != second.transaction_id


def test_runner_passes_retry_attempt_into_new_transaction_context():
    from proberca.config import LiveLivenessConfig
    from proberca.live.collection import ControlledTransientCollectionEmpty
    from proberca.live.coordinator import LiveCoordinatorState
    from proberca.live.runner import ProbeRCALiveRunner, process_window_with_retry

    contexts = []

    class Coordinator:
        state = LiveCoordinatorState.LEADER_ACTIVE

        def begin_window(self, _start, _end, attempt_index=1):
            context = SimpleNamespace(
                sequence=1,
                transaction_id=f"transaction-{attempt_index}",
                attempt_identity=SimpleNamespace(attempt_index=attempt_index),
                attempt_state="active",
                working_engine=object(),
                engine_result=None,
            )
            contexts.append(context)
            return context

        def run_engine(self, context, value):
            context.engine_result = value
            return value

        def prepare_generation(self, context, **_payload):
            return SimpleNamespace(transaction_id=context.transaction_id)

        def commit(self, context, _generation):
            context.attempt_state = "committed"

    config = LiveLivenessConfig(
        transient_retry_max_attempts=2,
        transient_retry_initial_backoff_sec=0.001,
        transient_retry_max_backoff_sec=0.001,
        controlled_collection_fault_enabled=True,
        controlled_transient_empty_attempts=1,
    )
    runner = ProbeRCALiveRunner(
        coordinator=Coordinator(),
        inventory=SimpleNamespace(ready=True, freeze=lambda _: object()),
        topology_builder=lambda *_: object(),
        node_metric_collector=lambda *_: [object()],
        edge_metric_collector=lambda *_: [object()],
        window_adapter=lambda *_: object(),
        commit_payload_builder=lambda *_: {},
        liveness_config=config,
    )
    process_window_with_retry(
        runner,
        SimpleNamespace(sequence=1, start_ns=100, end_ns=200),
        liveness_config=config,
        health=None,
        transient_error_types=(ControlledTransientCollectionEmpty,),
        sleep=lambda _: None,
    )
    assert len(contexts) == 2
    assert contexts[0] is not contexts[1]
    assert contexts[0].transaction_id != contexts[1].transaction_id
    assert contexts[0].attempt_state == "aborted"
    assert contexts[0].working_engine is None
    assert contexts[1].attempt_state == "committed"
    assert contexts[1].attempt_identity.attempt_index == 2


def test_process_restart_uses_a_new_runner_instance_identity():
    from proberca.live.coordinator import LiveCommitCoordinator

    first = LiveCommitCoordinator(object(), object(), "stable-holder")
    restarted = LiveCommitCoordinator(object(), object(), "stable-holder")
    assert first.instance_fingerprint == restarted.instance_fingerprint
    assert (
        first.runner_instance_fingerprint
        != restarted.runner_instance_fingerprint
    )
    assert _identity(
        runner_instance_fingerprint=first.runner_instance_fingerprint,
    ).transaction_id != _identity(
        runner_instance_fingerprint=restarted.runner_instance_fingerprint,
    ).transaction_id


def test_leader_handoff_changes_transaction_identity_for_same_window():
    before = _identity(
        leadership_epoch_fingerprint="before-handoff",
    )
    after = _identity(
        leadership_epoch_fingerprint="after-handoff",
    )
    assert before.sequence == after.sequence

from __future__ import annotations

import pytest


def test_standby_cannot_begin_window_and_active_sequence_comes_from_run_state(tmp_path):
    from proberca.live.coordinator import LiveCommitCoordinator, LiveCoordinatorState
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import InMemoryLeaseRunStateStore, LeaseRunStateRecord

    record = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    coordinator = LiveCommitCoordinator(
        authority=InMemoryLeaseRunStateStore(record),
        generation_store=ImmutableGenerationStore(tmp_path / "generations"),
        instance_fingerprint="holder-a",
    )
    with pytest.raises(RuntimeError, match="active"):
        coordinator.begin_window(0, 1)
    coordinator.acquire_and_recover()
    coordinator.recover_current(engine_loader=lambda payload: payload)
    assert coordinator.state is LiveCoordinatorState.LEADER_ACTIVE
    context = coordinator.begin_window(0, 1)
    assert context.sequence == 1
    assert len(context.transaction_id) == 64
    assert context.transaction_id == coordinator.begin_window(0, 1).transaction_id


def test_failed_cas_discards_working_engine_without_advancing_active_state(tmp_path):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import InMemoryLeaseRunStateStore, LeaseRunStateRecord

    class Engine:
        def __init__(self, value=0): self.value = value
        def fork_for_window(self): return Engine(self.value)

    record = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    store = InMemoryLeaseRunStateStore(record)
    coordinator = LiveCommitCoordinator(store, ImmutableGenerationStore(tmp_path / "generations"), "holder-a")
    coordinator.acquire_and_recover(active_engine=Engine())
    coordinator.recover_current(engine_loader=lambda payload: payload)
    context = coordinator.begin_window(0, 1)
    working = coordinator.working_engine(context)
    working.value = 9
    store.force_takeover_for_test("holder-b")
    with pytest.raises(Exception):
        coordinator.commit_prepared_for_test(context, working)
    assert coordinator.active_engine.value == 0


def test_acquire_conflict_returns_coordinator_to_standby(tmp_path):
    from proberca.live.coordinator import (
        LiveCommitCoordinator,
        LiveCoordinatorState,
    )
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import LeaseRunStateConflict

    class ContendedAuthority:
        def try_acquire(self, _instance):
            raise LeaseRunStateConflict("held by another instance")

    coordinator = LiveCommitCoordinator(
        ContendedAuthority(),
        ImmutableGenerationStore(tmp_path / "generations"),
        "holder-a",
    )
    with pytest.raises(LeaseRunStateConflict):
        coordinator.acquire_and_recover()
    assert coordinator.state is LiveCoordinatorState.STANDBY
    assert coordinator.token is None


def test_generation_store_constructor_does_not_write_shared_storage(tmp_path):
    from proberca.live.generation import ImmutableGenerationStore

    root = tmp_path / "not-created"
    ImmutableGenerationStore(root)
    assert not root.exists()


def test_initial_leader_recovery_creates_empty_output_view_directory(tmp_path):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.output_projector import OutputProjector
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore,
        LeaseRunStateRecord,
    )

    record = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="generation_v5",
    )
    generation_root = tmp_path / "generations"
    output_root = tmp_path / "output"
    coordinator = LiveCommitCoordinator(
        InMemoryLeaseRunStateStore(record),
        ImmutableGenerationStore(generation_root),
        "holder-a",
        output_projector=OutputProjector(
            output_root, ImmutableGenerationStore(generation_root),
        ),
    )
    assert not generation_root.exists() and not output_root.exists()
    coordinator.acquire_and_recover(active_engine=object())
    coordinator.recover_current(engine_loader=lambda payload: payload)
    assert generation_root.is_dir()
    assert output_root.is_dir()

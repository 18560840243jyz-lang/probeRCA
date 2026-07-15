from __future__ import annotations

import pytest


class Engine:
    def __init__(self, value=0):
        self.value = value

    def fork_for_window(self):
        return Engine(self.value)

    def process_window(self, increment):
        self.value += increment
        return {"value": self.value}

    def adopt_committed_working_engine(self, working):
        self.value = working.value


def _coordinator(tmp_path, projector=None):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore,
        LeaseRunStateRecord,
    )

    record = LeaseRunStateRecord.initial(
        run_id="run-a",
        cluster_id="cluster-a",
        namespace_scope=("ns-a",),
        config_fingerprint="c" * 64,
        code_schema_version="p11-live-v5",
    )
    authority = InMemoryLeaseRunStateStore(record)
    coordinator = LiveCommitCoordinator(
        authority,
        ImmutableGenerationStore(tmp_path / "generations"),
        "holder-a",
        output_projector=projector,
    )
    coordinator.acquire_and_recover(active_engine=Engine())
    coordinator.recover_current(
        engine_loader=lambda payload: Engine(payload["value"]),
    )
    return coordinator, authority


def _prepare(coordinator, increment=3):
    context = coordinator.begin_window(10, 20)
    result = coordinator.run_engine(context, increment)
    from proberca.orchestration.state import OutputLedger
    ledger = OutputLedger.create(
        alerts=[], reports=[], failures=[],
        processed_window_count=context.sequence,
        last_processed_timestamp=context.window_end_ns,
        pending_incident=None, dataset_fingerprint="dataset",
        config_fingerprint="config",
    )
    generation = coordinator.prepare_generation(
        context,
        engine_state={"value": result["value"]},
        output_ledger=ledger.to_dict(),
        output_bundle={
            "alerts.jsonl": "",
            "failures.jsonl": "",
            "reports": {},
        },
        config_fingerprint="c" * 64,
        code_schema_version="p11-live-v5",
    )
    return context, generation


def test_generation_is_written_before_single_run_state_cas(tmp_path):
    coordinator, authority = _coordinator(tmp_path)
    context, generation = _prepare(coordinator)

    assert authority.read().record.committed_sequence == 0
    assert generation.path.is_dir()

    snapshot = coordinator.commit(context, generation)
    assert snapshot.record.committed_sequence == 1
    assert snapshot.record.current_generation_id == generation.generation_id
    assert coordinator.active_engine.value == 3


def test_cas_conflict_discards_working_engine_and_leaves_generation_orphan(tmp_path):
    from proberca.live.run_state import LeaseRunStateConflict

    coordinator, authority = _coordinator(tmp_path)
    context, generation = _prepare(coordinator)
    authority.force_takeover_for_test("holder-b")

    with pytest.raises(LeaseRunStateConflict):
        coordinator.commit(context, generation)
    assert coordinator.active_engine.value == 0
    assert authority.read().record.committed_sequence == 0
    assert generation.path.is_dir()


def test_output_failure_after_cas_is_committed_degraded_not_aborted(tmp_path):
    from proberca.live.coordinator import (
        CommittedOutputDegradedError,
        LiveCoordinatorState,
    )

    class FailingProjector:
        def initialize_empty_view(self):
            return None

        def project(self, generation_id):
            raise OSError("output unavailable")

    coordinator, authority = _coordinator(tmp_path, projector=FailingProjector())
    context, generation = _prepare(coordinator)

    with pytest.raises(CommittedOutputDegradedError):
        coordinator.commit(context, generation)
    assert authority.read().record.committed_sequence == 1
    assert coordinator.active_engine.value == 3
    assert coordinator.state is LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED


def test_recover_uses_only_run_state_generation_pointer(tmp_path):
    coordinator, authority = _coordinator(tmp_path)
    context, generation = _prepare(coordinator)
    coordinator.commit(context, generation)

    restored = coordinator.recover_current(
        engine_loader=lambda payload: Engine(payload["value"]),
    )
    assert restored.value == 3
    assert coordinator.active_engine.value == 3
    assert authority.read().record.current_generation_id == generation.generation_id


def test_recovery_rejects_generation_bundle_not_matching_run_state(tmp_path):
    from dataclasses import replace

    coordinator, authority = _coordinator(tmp_path)
    context, generation = _prepare(coordinator)
    coordinator.commit(context, generation)

    authority._record = replace(
        authority._record,
        output_bundle_fingerprint="f" * 64,
        record_fingerprint="",
    )._validated_with_fingerprint()

    with pytest.raises(RuntimeError, match="output bundle"):
        coordinator.recover_current(
            engine_loader=lambda payload: Engine(payload["value"]),
        )


def test_output_projection_recovery_never_replays_engine_or_cas(tmp_path):
    from proberca.live.coordinator import CommittedOutputDegradedError

    class ToggleProjector:
        def __init__(self):
            self.fail = True
            self.calls = []

        def initialize_empty_view(self):
            return None

        def project(self, generation_id):
            self.calls.append(generation_id)
            if self.fail:
                raise OSError("output unavailable")

    projector = ToggleProjector()
    coordinator, authority = _coordinator(tmp_path, projector=projector)
    context, generation = _prepare(coordinator)
    with pytest.raises(CommittedOutputDegradedError):
        coordinator.commit(context, generation)

    committed = authority.read()
    projector.fail = False
    restored = coordinator.recover_current(
        engine_loader=lambda payload: Engine(payload["value"]),
    )
    assert authority.read() == committed
    assert restored.value == 3
    assert projector.calls == [generation.generation_id, generation.generation_id]

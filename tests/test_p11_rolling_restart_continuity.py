from __future__ import annotations

import json

import pytest


class Engine:
    def __init__(self, value=0):
        self.value = value

    def fork_for_window(self):
        return Engine(self.value)

    def process_window(self, value):
        self.value += value
        return {"value": self.value}

    def adopt_committed_working_engine(self, working):
        self.value = working.value


def _setup(tmp_path, holder="holder-a", authority=None):
    from proberca.live.coordinator import LiveCommitCoordinator
    from proberca.live.generation import ImmutableGenerationStore
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore,
        LeaseRunStateRecord,
    )

    if authority is None:
        authority = InMemoryLeaseRunStateStore(
            LeaseRunStateRecord.initial(
                run_id="run-a",
                cluster_id="cluster-a",
                namespace_scope=("ns-a",),
                config_fingerprint="c" * 64,
                code_schema_version="generation_v5",
            ),
        )
    coordinator = LiveCommitCoordinator(
        authority,
        ImmutableGenerationStore(tmp_path / "generations"),
        holder,
    )
    coordinator.acquire_and_recover(active_engine=Engine())
    coordinator.recover_current(
        engine_loader=lambda payload: Engine(payload["value"]),
    )
    return coordinator, authority


def _prepare(coordinator, start, end, increment=1):
    from proberca.orchestration.state import OutputLedger

    context = coordinator.begin_window(start, end)
    result = coordinator.run_engine(context, increment)
    ledger = OutputLedger.create(
        alerts=[],
        reports=[],
        failures=[],
        processed_window_count=context.sequence,
        last_processed_timestamp=end,
        pending_incident=None,
        dataset_fingerprint="dataset",
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
        code_schema_version="generation_v5",
    )
    return context, generation


def test_engine_completion_without_cas_never_advances_durable_sequence(tmp_path):
    coordinator, authority = _setup(tmp_path)
    context = coordinator.begin_window(0, 1)
    coordinator.run_engine(context, 5)

    assert authority.read().record.committed_sequence == 0
    assert coordinator.active_engine.value == 0


def test_generation_publish_without_lease_cas_is_an_uncommitted_orphan(tmp_path):
    from proberca.live.run_state import LeaseRunStateConflict

    coordinator, authority = _setup(tmp_path)
    context, generation = _prepare(coordinator, 0, 1)
    authority.force_takeover_for_test("holder-b")

    with pytest.raises(LeaseRunStateConflict):
        coordinator.commit(context, generation)
    assert authority.read().record.committed_sequence == 0
    assert generation.path.is_dir()
    assert coordinator.active_engine.value == 0


def test_handoff_recovers_next_sequence_only_from_lease_run_state(tmp_path):
    first, authority = _setup(tmp_path, "holder-a")
    context, generation = _prepare(first, 0, 1)
    first.commit(context, generation)

    authority.force_takeover_for_test("holder-b")
    second, _ = _setup(tmp_path, "holder-b", authority)
    next_context = second.begin_window(1, 2)

    assert next_context.sequence == 2
    assert second.active_engine.value == 1


def test_generation_chain_is_the_durable_sequence_history_across_handoff(
    tmp_path,
):
    first, authority = _setup(tmp_path, "holder-a")
    for sequence in range(1, 3):
        context, generation = _prepare(
            first,
            sequence - 1,
            sequence,
        )
        first.commit(context, generation)

    authority.force_takeover_for_test("holder-b")
    second, _ = _setup(tmp_path, "holder-b", authority)
    for sequence in range(3, 6):
        context, generation = _prepare(
            second,
            sequence - 1,
            sequence,
        )
        second.commit(context, generation)

    record = authority.read().record
    assert record.committed_sequence == 5
    observed = []
    generation_id = record.current_generation_id
    store = second.generation_store
    while generation_id:
        generation = store.load(generation_id)
        observed.append(generation.manifest["proposed_sequence"])
        generation_id = generation.manifest["previous_generation_id"]
    assert observed == [5, 4, 3, 2, 1]


def test_live_path_has_no_external_sequence_journal_or_current_pointer():
    import inspect

    import proberca.cli.live as live_cli
    import proberca.live.runner as runner

    source = inspect.getsource(live_cli) + inspect.getsource(runner)
    assert "LiveSequenceJournal" not in source
    assert "sequence_commits.jsonl" not in source
    assert '"CURRENT"' not in source
    assert "scheduler.commit" not in source

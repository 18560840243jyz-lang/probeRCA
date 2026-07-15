from __future__ import annotations

import pytest


def test_initial_run_state_has_no_committed_generation_and_canonical_fingerprint():
    from proberca.live.run_state import LeaseRunStateRecord

    state = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    assert state.committed_sequence == 0
    assert state.current_generation_id is None
    assert state.commit_fingerprint is None
    assert len(state.record_fingerprint) == 64
    assert LeaseRunStateRecord.from_dict(state.to_dict()) == state


def test_committed_state_requires_next_sequence_and_complete_identity():
    from proberca.live.run_state import LeaseRunStateError, LeaseRunStateRecord

    initial = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    with pytest.raises(LeaseRunStateError, match="sequence"):
        initial.with_commit(
            holder_fingerprint="h" * 24, leadership_epoch=1, sequence=2,
            generation_id="generation-a", generation_fingerprint="g" * 64,
            output_ledger_fingerprint="l" * 64, output_bundle_fingerprint="b" * 64,
            engine_state_fingerprint="e" * 64, window_start_ns=1, window_end_ns=2,
        )


def test_in_memory_authority_preserves_run_state_on_stale_epoch():
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore, LeaseRunStateConflict, LeaseRunStateRecord,
    )

    initial = LeaseRunStateRecord.initial(
        run_id="run-a", cluster_id="cluster-a", namespace_scope=("ns-a",),
        config_fingerprint="c" * 64, code_schema_version="p11-live-v5",
    )
    store = InMemoryLeaseRunStateStore(initial)
    first = store.try_acquire("holder-a")
    context = store.prepare_commit(first, expected_sequence=1, expected_generation_id=None)
    committed = store.commit_generation(context, store.read().record.with_commit(
        holder_fingerprint=first.holder_fingerprint, leadership_epoch=first.leadership_epoch,
        sequence=1, generation_id="generation-a", generation_fingerprint="a" * 64,
        output_ledger_fingerprint="b" * 64, output_bundle_fingerprint="c" * 64,
        engine_state_fingerprint="d" * 64, window_start_ns=1, window_end_ns=2,
    ))
    assert committed.record.committed_sequence == 1
    replacement = store.force_takeover_for_test("holder-b")
    with pytest.raises(LeaseRunStateConflict):
        store.prepare_commit(first, expected_sequence=2, expected_generation_id="generation-a")
    assert replacement.leadership_epoch == first.leadership_epoch + 1
    assert store.read().record.committed_sequence == 1

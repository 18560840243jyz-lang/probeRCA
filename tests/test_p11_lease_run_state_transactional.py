from __future__ import annotations

import pytest


def _initial():
    from proberca.live.run_state import LeaseRunStateRecord

    return LeaseRunStateRecord.initial(
        run_id="run-a",
        cluster_id="cluster-a",
        namespace_scope=("ns-a",),
        config_fingerprint="c" * 64,
        code_schema_version="p11-live-v5",
    )


def _candidate(record, token, sequence=1, generation_id=None):
    return record.with_commit(
        holder_fingerprint=token.holder_fingerprint,
        leadership_epoch=token.leadership_epoch,
        sequence=sequence,
        generation_id=generation_id or f"generation-{sequence}",
        generation_fingerprint="a" * 64,
        output_ledger_fingerprint="b" * 64,
        output_bundle_fingerprint="c" * 64,
        engine_state_fingerprint="d" * 64,
        window_start_ns=sequence,
        window_end_ns=sequence + 1,
    )


def test_duplicate_identical_commit_is_idempotent():
    from proberca.live.run_state import InMemoryLeaseRunStateStore

    store = InMemoryLeaseRunStateStore(_initial())
    token = store.try_acquire("holder-a")
    context = store.prepare_commit(
        token, expected_sequence=1, expected_generation_id=None,
    )
    candidate = _candidate(store.read().record, token)
    first = store.commit_generation(context, candidate)
    second = store.commit_generation(context, candidate)
    assert second == first


def test_conflicting_duplicate_commit_fails():
    from proberca.live.run_state import (
        InMemoryLeaseRunStateStore,
        LeaseRunStateConflict,
    )

    store = InMemoryLeaseRunStateStore(_initial())
    token = store.try_acquire("holder-a")
    context = store.prepare_commit(
        token, expected_sequence=1, expected_generation_id=None,
    )
    candidate = _candidate(store.read().record, token)
    conflicting = _candidate(store.read().record, token, generation_id="other-generation")
    store.commit_generation(context, candidate)
    with pytest.raises(LeaseRunStateConflict):
        store.commit_generation(context, conflicting)


def test_run_state_annotation_round_trip_and_limit():
    from proberca.live.run_state import (
        LeaseRunStateError,
        decode_run_state_annotation,
        encode_run_state_annotation,
    )

    record = _initial()
    encoded = encode_run_state_annotation(record, max_bytes=4096)
    assert decode_run_state_annotation(encoded, max_bytes=4096) == record
    with pytest.raises(LeaseRunStateError, match="limit"):
        encode_run_state_annotation(record, max_bytes=8)


def test_renew_preserves_committed_run_state_and_epoch():
    from proberca.live.run_state import InMemoryLeaseRunStateStore

    store = InMemoryLeaseRunStateStore(_initial())
    token = store.try_acquire("holder-a")
    context = store.prepare_commit(
        token, expected_sequence=1, expected_generation_id=None,
    )
    committed = store.commit_generation(
        context, _candidate(store.read().record, token),
    )
    renewed = store.renew(token)
    after = store.read()
    assert after.record == committed.record
    assert renewed.leadership_epoch == token.leadership_epoch


def test_kubernetes_release_uses_valid_positive_lease_duration():
    from proberca.live.leader import InMemoryLeaseAPI
    from proberca.live.run_state import KubernetesLeaseRunStateStore

    class StrictLeaseAPI(InMemoryLeaseAPI):
        def create_or_replace(self, namespace, name, value, expected_version=None):
            if expected_version is not None and value.duration <= 0:
                raise RuntimeError("Kubernetes rejects non-positive Lease duration")
            return super().create_or_replace(
                namespace, name, value, expected_version,
            )

    clock = [100.0]
    store = KubernetesLeaseRunStateStore(
        StrictLeaseAPI(), namespace="ns-a", name="lease-a",
        initial_record=_initial(), lease_duration_sec=15.0,
        clock=lambda: clock[0], annotation_max_bytes=4096,
    )
    token = store.try_acquire("holder-a")
    store.release(token)
    clock[0] += 0.1
    next_token = store.try_acquire("holder-b")
    assert next_token.holder_fingerprint != token.holder_fingerprint

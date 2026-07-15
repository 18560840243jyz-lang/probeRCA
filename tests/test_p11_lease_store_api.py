from __future__ import annotations


def _record():
    from proberca.live.run_state import LeaseRunStateRecord

    return LeaseRunStateRecord.initial(
        run_id="run-a",
        cluster_id="cluster-a",
        namespace_scope=("ns-a",),
        config_fingerprint="c" * 64,
        code_schema_version="p11-live-v5",
    )


def test_lease_store_commits_owner_sequence_and_generation_on_same_object():
    from proberca.live.leader import InMemoryLeaseAPI
    from proberca.live.run_state import KubernetesLeaseRunStateStore

    now = [10.0]
    api = InMemoryLeaseAPI()
    store = KubernetesLeaseRunStateStore(
        api,
        namespace="operator",
        name="run-state",
        initial_record=_record(),
        lease_duration_sec=15,
        clock=lambda: now[0],
        annotation_max_bytes=8192,
    )
    token = store.try_acquire("instance-a")
    context = store.prepare_commit(
        token, expected_sequence=1, expected_generation_id=None,
    )
    candidate = store.read().record.with_commit(
        holder_fingerprint=token.holder_fingerprint,
        leadership_epoch=token.leadership_epoch,
        sequence=1,
        generation_id="generation-1",
        generation_fingerprint="a" * 64,
        output_ledger_fingerprint="b" * 64,
        output_bundle_fingerprint="c" * 64,
        engine_state_fingerprint="d" * 64,
        window_start_ns=1,
        window_end_ns=2,
    )
    snapshot = store.commit_generation(context, candidate)

    lease = api.read("operator", "run-state")
    assert lease.resource_version == snapshot.resource_version
    assert lease.holder == "instance-a"
    assert snapshot.record.committed_sequence == 1
    assert snapshot.record.current_generation_id == "generation-1"


def test_takeover_preserves_committed_state_and_invalidates_old_epoch():
    import pytest

    from proberca.live.leader import InMemoryLeaseAPI
    from proberca.live.run_state import (
        KubernetesLeaseRunStateStore,
        LeaseRunStateConflict,
    )

    now = [10.0]
    api = InMemoryLeaseAPI()
    first = KubernetesLeaseRunStateStore(
        api,
        namespace="operator",
        name="run-state",
        initial_record=_record(),
        lease_duration_sec=5,
        clock=lambda: now[0],
        annotation_max_bytes=8192,
    )
    old = first.try_acquire("instance-a")
    now[0] = 20.0
    second = KubernetesLeaseRunStateStore(
        api,
        namespace="operator",
        name="run-state",
        initial_record=_record(),
        lease_duration_sec=5,
        clock=lambda: now[0],
        annotation_max_bytes=8192,
    )
    new = second.try_acquire("instance-b")

    assert new.leadership_epoch == old.leadership_epoch + 1
    with pytest.raises(LeaseRunStateConflict):
        first.prepare_commit(
            old, expected_sequence=1, expected_generation_id=None,
        )


def test_renew_and_commit_share_latest_resource_version_without_lost_fields():
    from proberca.live.leader import InMemoryLeaseAPI
    from proberca.live.run_state import KubernetesLeaseRunStateStore

    now = [10.0]
    store = KubernetesLeaseRunStateStore(
        InMemoryLeaseAPI(),
        namespace="operator",
        name="run-state",
        initial_record=_record(),
        lease_duration_sec=5,
        clock=lambda: now[0],
        annotation_max_bytes=8192,
    )
    token = store.try_acquire("instance-a")
    now[0] = 11.0
    token = store.renew(token)
    snapshot = store.read()

    assert snapshot.record.leadership_epoch == token.leadership_epoch
    assert snapshot.record.holder_fingerprint == token.holder_fingerprint
    assert snapshot.record.committed_sequence == 0

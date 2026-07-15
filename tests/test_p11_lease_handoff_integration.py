from __future__ import annotations

from types import SimpleNamespace

import pytest

from proberca.live.leader import InMemoryLeaseAPI, LeaseCoordinator


class Clock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


def _config():
    return SimpleNamespace(
        enabled=True, lease_namespace="test-ns", lease_name="probe-lease",
        lease_duration_sec=5.0, validate=lambda: None)


def test_two_instances_handoff_without_duplicate_committed_sequence():
    api = InMemoryLeaseAPI()
    clock = Clock()
    first = LeaseCoordinator(api, _config(), "pod-uid-a", clock=clock)
    second = LeaseCoordinator(api, _config(), "pod-uid-b", clock=clock)
    committed = []

    assert first.try_acquire()
    assert not second.try_acquire()
    committed.extend((1, 2, 3))
    first.lose()
    clock.value += 6.0
    assert second.try_acquire()
    next_sequence = max(committed) + 1
    committed.extend((next_sequence, next_sequence + 1))
    assert committed == [1, 2, 3, 4, 5]
    assert len(committed) == len(set(committed))
    assert api.value.holder == "pod-uid-b"


def test_follower_cannot_process_or_commit_before_lease_expiry():
    api = InMemoryLeaseAPI()
    clock = Clock()
    first = LeaseCoordinator(api, _config(), "pod-uid-a", clock=clock)
    second = LeaseCoordinator(api, _config(), "pod-uid-b", clock=clock)
    assert first.try_acquire()
    assert not second.try_acquire()
    assert not second.is_leader
    assert not second.can_commit


def test_empty_holder_identity_is_rejected():
    import pytest
    with pytest.raises(ValueError, match="identity"):
        LeaseCoordinator(InMemoryLeaseAPI(), _config(), "", clock=Clock())


@pytest.mark.parametrize("advance,expected", [(0.0, False), (4.9, False), (5.0, True), (8.0, True)])
def test_takeover_occurs_only_after_lease_expiry(advance, expected):
    api, clock = InMemoryLeaseAPI(), Clock()
    first = LeaseCoordinator(api, _config(), "pod-uid-a", clock=clock)
    second = LeaseCoordinator(api, _config(), "pod-uid-b", clock=clock)
    assert first.try_acquire()
    clock.value += advance
    assert second.try_acquire() is expected

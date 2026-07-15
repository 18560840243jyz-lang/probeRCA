from __future__ import annotations

import threading
import time

import pytest

from proberca.k8s.inventory import KubernetesInventory
from proberca.k8s.watch import KubernetesListWatcher, WatchExpiredError


def _object(kind: str, name: str, uid: str, rv: str, **extra):
    value = {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {
            "name": name,
            "namespace": "test-ns" if kind != "Node" else None,
            "uid": uid,
            "resourceVersion": rv,
        },
    }
    value.update(extra)
    return value


class ManagedWatcher:
    def __init__(self, kind, inventory, initial):
        self.resource_kind = kind
        self.inventory = inventory
        self.initial = initial
        self.list_calls = 0
        self.stop_seen = False

    def run(self, stop_event, state_callback):
        self.list_calls += 1
        self.inventory.replace_kind(
            self.resource_kind, self.initial, "1", time.time_ns(), f"{self.resource_kind}-watch")
        state_callback(self.resource_kind, "synchronized", None)
        stop_event.wait()
        self.stop_seen = True


def test_supervisor_lists_once_and_freezes_multiple_windows():
    from proberca.k8s.supervisor import KubernetesWatchSupervisor

    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    watcher = ManagedWatcher("Pod", inventory, [_object("Pod", "pod-a", "pod-1", "1")])
    supervisor = KubernetesWatchSupervisor(inventory, [watcher])
    supervisor.start()
    assert supervisor.wait_until_synchronized(1.0)
    revisions = [supervisor.freeze_revision(time.time_ns()) for _ in range(3)]
    supervisor.stop()
    supervisor.join(1.0)
    assert watcher.list_calls == 1
    assert all(revision.object_counts["Pod"] == 1 for revision in revisions)
    assert watcher.stop_seen
    assert supervisor.health_snapshot()["thread_count"] == 0


def test_supervisor_relisting_and_fatal_state_block_freeze():
    from proberca.k8s.supervisor import KubernetesWatchSupervisor, WatchSupervisorError

    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    watcher = ManagedWatcher("Pod", inventory, [_object("Pod", "pod-a", "pod-1", "1")])
    supervisor = KubernetesWatchSupervisor(inventory, [watcher])
    supervisor.start()
    assert supervisor.wait_until_synchronized(1.0)
    supervisor.update_watcher_state("Pod", "relisting", None)
    with pytest.raises(WatchSupervisorError, match="relisting"):
        supervisor.freeze_revision(time.time_ns())
    supervisor.update_watcher_state("Pod", "fatal", RuntimeError("watch failed"))
    with pytest.raises(WatchSupervisorError, match="fatal"):
        supervisor.freeze_revision(time.time_ns())
    supervisor.stop()
    supervisor.join(1.0)


def test_list_watcher_modified_event_updates_without_second_list():
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    calls = []
    initial = _object("Pod", "pod-a", "pod-1", "1", spec={"nodeName": "node-1"})
    changed = _object("Pod", "pod-a", "pod-1", "2", spec={"nodeName": "node-2"})
    watcher = KubernetesListWatcher(
        "Pod", inventory, lambda: (calls.append(1) or [initial], "1"),
        lambda rv: [{"type": "MODIFIED", "object": changed}])
    watcher.synchronize(1)
    watcher.consume_once(2)
    revision = inventory.freeze(2)
    assert len(calls) == 1
    assert revision.objects_by_kind["Pod"]["pod-1"]["spec"]["nodeName"] == "node-2"


def test_expired_watch_marks_relist_and_lists_again():
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    calls = []
    watcher = KubernetesListWatcher(
        "Pod", inventory,
        lambda: (calls.append(1) or [_object("Pod", "pod-a", "pod-1", str(len(calls)))], str(len(calls))),
        lambda rv: (_ for _ in ()).throw(WatchExpiredError("gone")))
    watcher.synchronize(1)
    watcher.consume_once(2)
    assert len(calls) == 2
    assert watcher.relist_generation == 1
    assert inventory.synchronized


def test_frozen_revision_does_not_share_mutable_cache():
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    first = _object("Pod", "pod-a", "pod-1", "1", spec={"nodeName": "node-1"})
    inventory.replace_kind("Pod", [first], "1", 1)
    frozen = inventory.freeze(1)
    first["spec"]["nodeName"] = "mutated"
    inventory.replace_kind("Pod", [first], "2", 2)
    assert frozen.objects_by_kind["Pod"]["pod-1"]["spec"]["nodeName"] == "node-1"


@pytest.mark.parametrize("state", ["starting", "relisting", "reconnecting", "fatal", "stopped"])
def test_health_snapshot_exposes_each_managed_watcher_state(state):
    from proberca.k8s.supervisor import KubernetesWatchSupervisor
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    watcher = ManagedWatcher("Pod", inventory, [])
    supervisor = KubernetesWatchSupervisor(inventory, [watcher])
    supervisor.update_watcher_state("Pod", state, RuntimeError("fatal") if state == "fatal" else None)
    assert supervisor.health_snapshot()["states"]["Pod"] == state


def test_supervisor_rejects_unknown_watcher_state():
    from proberca.k8s.supervisor import KubernetesWatchSupervisor, WatchSupervisorError
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    supervisor = KubernetesWatchSupervisor(inventory, [ManagedWatcher("Pod", inventory, [])])
    with pytest.raises(WatchSupervisorError, match="invalid watcher state"):
        supervisor.update_watcher_state("Pod", "silently_ignored", None)


def test_endpoint_slice_initial_list_accepts_empty_raw_endpoints():
    from proberca.k8s.client import _decode_raw_collection

    class Response:
        data = (
            b'{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList",'
            b'"metadata":{"resourceVersion":"29"},"items":['
            b'{"apiVersion":"discovery.k8s.io/v1","metadata":'
            b'{"name":"empty","namespace":"test-ns","uid":"slice-1"},'
            b'"addressType":"IPv4","endpoints":null,"ports":null}]}'
        )

    objects, resource_version = _decode_raw_collection("EndpointSlice", Response())
    assert resource_version == "29"
    assert objects[0]["kind"] == "EndpointSlice"
    assert objects[0]["endpoints"] is None


def test_bookmark_advances_resource_version_without_requiring_object_identity():
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Pod",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    initial = _object("Pod", "pod-a", "pod-1", "1")
    bookmark = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"resourceVersion": "2"},
    }
    watcher = KubernetesListWatcher(
        "Pod", inventory, lambda: ([initial], "1"),
        lambda rv: [{"type": "BOOKMARK", "object": bookmark}])

    watcher.synchronize(1)
    watcher.consume_once(2)

    revision = inventory.freeze(2)
    assert revision.object_counts["Pod"] == 1
    assert revision.resource_versions[0].resource_version == "2"

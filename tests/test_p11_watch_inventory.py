from __future__ import annotations

import pytest

from proberca.k8s.contracts import KubernetesWatchEvent
from proberca.k8s.inventory import KubernetesInventory, InventoryConflictError
from proberca.k8s.watch import KubernetesListWatcher, WatchExpiredError


def obj(kind, name, uid, rv, namespace="observability", **extra):
    value = {
        "apiVersion": "v1", "kind": kind,
        "metadata": {"name": name, "namespace": namespace, "uid": uid,
                     "resourceVersion": rv},
    }
    value.update(extra)
    return value


def test_initial_list_uses_collection_rv_and_watch_events_keep_rv_opaque():
    inventory = KubernetesInventory("cluster-a", required_kinds=("Pod",), stale_after_sec=10)
    calls = []

    def list_call():
        return [obj("Pod", "pod-a", "pod-1", "9")], "rv-z"

    def watch_call(rv):
        calls.append(rv)
        return [
            {"type": "BOOKMARK", "object": obj("Pod", "bookmark", "b", "rv-a")},
            {"type": "MODIFIED", "object": obj("Pod", "pod-a", "pod-1", "2")},
        ]

    watcher = KubernetesListWatcher("Pod", inventory, list_call, watch_call)
    watcher.synchronize(observed_at_ns=1)
    watcher.consume_once(observed_at_ns=2)
    assert calls == ["rv-z"]
    assert inventory.resource_versions["Pod"].resource_version == "2"
    assert inventory.object_count("Pod") == 1


def test_deleted_uid_is_tombstoned_and_recreated_name_is_new_identity():
    inventory = KubernetesInventory("cluster-a", required_kinds=("Pod",), stale_after_sec=10)
    inventory.replace_kind("Pod", [obj("Pod", "pod-a", "old", "1")], "1", 1)
    inventory.apply_event(KubernetesWatchEvent.from_raw(
        "DELETED", obj("Pod", "pod-a", "old", "2"), observed_at_ns=2,
        watch_stream_id="watch-1", relist_generation=0))
    inventory.apply_event(KubernetesWatchEvent.from_raw(
        "ADDED", obj("Pod", "pod-a", "new", "3"), observed_at_ns=3,
        watch_stream_id="watch-1", relist_generation=0))
    assert inventory.uid_for_name("Pod", "observability", "pod-a") == "new"
    assert inventory.tombstone("old").uid == "old"


def test_410_relist_marks_not_ready_then_atomically_replaces_cache():
    inventory = KubernetesInventory("cluster-a", required_kinds=("Pod",), stale_after_sec=10)
    responses = iter([
        ([obj("Pod", "pod-a", "a", "1")], "one"),
        ([obj("Pod", "pod-b", "b", "2")], "two"),
    ])
    watcher = KubernetesListWatcher(
        "Pod", inventory, lambda: next(responses),
        lambda rv: (_ for _ in ()).throw(WatchExpiredError("410 Gone")))
    watcher.synchronize(1)
    assert inventory.synchronized
    watcher.consume_once(2)
    assert inventory.synchronized
    assert inventory.uid_for_name("Pod", "observability", "pod-b") == "b"
    assert inventory.uid_for_name("Pod", "observability", "pod-a") is None


def test_ip_conflict_is_explicit_and_deletion_cleans_reverse_index():
    inventory = KubernetesInventory("cluster-a", required_kinds=("Pod",), stale_after_sec=10)
    pods = [
        obj("Pod", "a", "a", "1", status={"podIP": "10.0.0.1"}),
        obj("Pod", "b", "b", "1", status={"podIP": "10.0.0.1"}),
    ]
    inventory.replace_kind("Pod", pods, "1", 1)
    with pytest.raises(InventoryConflictError):
        inventory.resolve_unique_pod_ip("10.0.0.1")
    inventory.apply_event(KubernetesWatchEvent.from_raw(
        "DELETED", pods[1], 2, "watch", 0))
    assert inventory.resolve_unique_pod_ip("10.0.0.1") == "a"


def test_inventory_sync_barrier_and_staleness_are_per_kind():
    inventory = KubernetesInventory(
        "cluster-a", required_kinds=("Pod", "Service"), stale_after_sec=5)
    inventory.replace_kind("Pod", [], "pod-rv", 1)
    assert not inventory.synchronized
    inventory.replace_kind("Service", [], "svc-rv", 2)
    assert inventory.freeze(3).synchronized
    assert inventory.freeze(8_000_000_003).stale



def test_reindex_issues_describe_current_state_without_unbounded_duplicates():
    inventory = KubernetesInventory(
        "cluster-a",
        required_kinds=("Pod", "EndpointSlice"),
        stale_after_sec=10,
    )
    endpoint_slice = obj(
        "EndpointSlice",
        "orphaned-backend",
        "slice-1",
        "1",
        apiVersion="discovery.k8s.io/v1",
        metadata={
            "name": "orphaned-backend",
            "namespace": "observability",
            "uid": "slice-1",
            "resourceVersion": "1",
            "labels": {"kubernetes.io/service-name": "missing-service"},
        },
        endpoints=[],
    )
    pod = obj("Pod", "pod-a", "pod-1", "1")
    inventory.replace_kind("EndpointSlice", [endpoint_slice], "1", 1)
    inventory.replace_kind("Pod", [pod], "1", 1)

    for resource_version in range(2, 102):
        changed = obj("Pod", "pod-a", "pod-1", str(resource_version))
        inventory.apply_event(KubernetesWatchEvent.from_raw(
            "MODIFIED",
            changed,
            observed_at_ns=resource_version,
            watch_stream_id="pod-watch",
            relist_generation=0,
        ))

    revision = inventory.freeze(102)
    missing_service_issues = [
        issue for issue in revision.issues
        if issue["reason_code"] == "endpoint_service_missing"
    ]
    assert missing_service_issues == [{
        "reason_code": "endpoint_service_missing",
        "object_id": "slice-1",
    }]



def test_freeze_is_atomic_against_background_watch_updates():
    import threading

    started = threading.Event()
    completed = threading.Event()

    class CoordinatedVersions(dict):
        def values(self):
            iterator = iter(super().values())
            yield next(iterator)
            started.set()
            completed.wait(0.05)
            yield from iterator

    inventory = KubernetesInventory(
        "cluster-a", required_kinds=("Service", "Node"), stale_after_sec=10,
    )
    inventory.replace_kind("Service", [], "1", 1)
    inventory._versions = CoordinatedVersions(inventory._versions)

    def update_from_watch():
        started.wait(1)
        try:
            inventory.apply_bookmark("Node", "2", 2, "node-watch")
        finally:
            completed.set()

    writer = threading.Thread(target=update_from_watch)
    writer.start()
    revision = inventory.freeze(3)
    writer.join(1)

    assert not writer.is_alive()
    assert revision.object_counts == {"Service": 0}
    assert inventory.resource_versions["Node"].resource_version == "2"

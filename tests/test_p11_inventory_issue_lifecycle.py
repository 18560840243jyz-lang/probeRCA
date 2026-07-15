from __future__ import annotations

import json
import threading
import time

from proberca.k8s.contracts import KubernetesWatchEvent, canonical_hash
from proberca.k8s.inventory import (
    KubernetesInventory,
    _canonicalize_structural_issues,
)
from proberca.k8s.supervisor import KubernetesWatchSupervisor


def _object(kind, name, uid, resource_version="1", namespace="observability", **extra):
    value = {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {
            "name": name,
            "namespace": namespace if kind != "Node" else None,
            "uid": uid,
            "resourceVersion": str(resource_version),
        },
    }
    value.update(extra)
    return value


def _service(name="service-a", uid="service-1", resource_version="1"):
    return _object("Service", name, uid, resource_version)


def _pod(resource_version="1"):
    return _object("Pod", "pod-a", "pod-1", resource_version)


def _slice(name="slice-a", uid="slice-1", service_name="missing-service",
           resource_version="1"):
    return {
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": name,
            "namespace": "observability",
            "uid": uid,
            "resourceVersion": str(resource_version),
            "labels": {"kubernetes.io/service-name": service_name},
        },
        "addressType": "IPv4",
        "ports": [],
        "endpoints": [],
    }


def _event(event_type, raw, observed_at_ns=2):
    return KubernetesWatchEvent.from_raw(
        event_type,
        raw,
        observed_at_ns=observed_at_ns,
        watch_stream_id=f"{raw['kind']}-watch",
        relist_generation=0,
    )


def _inventory(endpoint_slices=None):
    inventory = KubernetesInventory(
        "cluster-a",
        required_kinds=("Pod", "Service", "EndpointSlice"),
        stale_after_sec=3600,
    )
    inventory.replace_kind("Pod", [_pod()], "pod-rv", 1)
    inventory.replace_kind("Service", [], "service-rv", 1)
    inventory.replace_kind(
        "EndpointSlice",
        list(endpoint_slices or [_slice()]),
        "slice-rv",
        1,
    )
    return inventory


def _missing_issues(revision):
    return tuple(
        issue for issue in revision.issues
        if issue["reason_code"] == "endpoint_service_missing"
    )


def _issue_payload_size(revision):
    return len(json.dumps(
        revision.issues,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8"))


def test_one_missing_service_slice_produces_one_structural_issue():
    issues = _missing_issues(_inventory().freeze(2))
    assert issues == ({
        "reason_code": "endpoint_service_missing",
        "object_id": "slice-1",
    },)


def test_unrelated_pod_updates_do_not_accumulate_structural_issue():
    inventory = _inventory()
    for resource_version in range(2, 102):
        inventory.apply_event(_event(
            "MODIFIED",
            _pod(resource_version),
            observed_at_ns=resource_version,
        ))
    assert len(_missing_issues(inventory.freeze(102))) == 1


def test_repeated_reindex_does_not_accumulate_structural_issue():
    inventory = _inventory()
    for _ in range(100):
        inventory._reindex()
    assert len(_missing_issues(inventory.freeze(2))) == 1


def test_resolved_missing_service_issue_disappears_from_current_revision():
    inventory = _inventory()
    inventory.replace_kind(
        "Service",
        [_service("missing-service", "service-1")],
        "service-rv-2",
        2,
    )
    assert _missing_issues(inventory.freeze(2)) == ()


def test_deleting_resolved_service_reintroduces_exactly_one_issue():
    inventory = _inventory()
    service = _service("missing-service", "service-1")
    inventory.replace_kind("Service", [service], "service-rv-2", 2)
    inventory.apply_event(_event("DELETED", service, observed_at_ns=3))
    assert _missing_issues(inventory.freeze(3)) == ({
        "reason_code": "endpoint_service_missing",
        "object_id": "slice-1",
    },)


def test_distinct_missing_endpoint_slices_remain_distinct():
    inventory = _inventory([
        _slice("slice-b", "slice-2", "missing-b"),
        _slice("slice-a", "slice-1", "missing-a"),
    ])
    assert _missing_issues(inventory.freeze(2)) == (
        {
            "reason_code": "endpoint_service_missing",
            "object_id": "slice-1",
        },
        {
            "reason_code": "endpoint_service_missing",
            "object_id": "slice-2",
        },
    )


def test_update_order_produces_identical_canonical_issue_output():
    slices = [
        _slice("slice-a", "slice-1", "missing-a"),
        _slice("slice-b", "slice-2", "missing-b"),
    ]
    left = _inventory(slices).freeze(2)
    right = _inventory(list(reversed(slices))).freeze(2)
    assert left.issues == right.issues
    assert canonical_hash(left.issues) == canonical_hash(right.issues)


def test_watcher_input_order_preserves_issue_and_revision_fingerprints():
    first = _inventory([])
    second = _inventory([])
    slices = [
        _slice("slice-a", "slice-1", "missing-a"),
        _slice("slice-b", "slice-2", "missing-b"),
    ]
    for raw in slices:
        first.apply_event(_event("ADDED", raw))
    for raw in reversed(slices):
        second.apply_event(_event("ADDED", raw))
    left = first.freeze(2)
    right = second.freeze(2)
    assert left.issues == right.issues
    assert left.fingerprint == right.fingerprint
    assert left.revision_id == right.revision_id


def test_resolved_issue_is_absent_from_following_frozen_revision():
    inventory = _inventory()
    unresolved = inventory.freeze(2)
    inventory.replace_kind(
        "Service",
        [_service("missing-service", "service-1")],
        "service-rv-2",
        2,
    )
    resolved = inventory.freeze(3)
    assert _missing_issues(unresolved)
    assert _missing_issues(resolved) == ()
    assert unresolved.issues is not resolved.issues


def test_watcher_diagnostic_state_does_not_enter_structural_issues_or_fingerprint():
    inventory = _inventory()
    before = inventory.freeze(2)
    supervisor = KubernetesWatchSupervisor(inventory, [])
    supervisor.update_watcher_state("Pod", "reconnecting", RuntimeError("temporary"))
    after = inventory.freeze(2)
    assert after.issues == before.issues
    assert after.fingerprint == before.fingerprint


def test_thousand_updates_keep_issue_payload_bounded():
    inventory = _inventory()
    repeated = _pod("2")
    inventory.apply_event(_event("MODIFIED", repeated, observed_at_ns=2))
    initial = inventory.freeze(2)
    for _ in range(999):
        inventory.apply_event(_event("MODIFIED", repeated, observed_at_ns=2))
    final = inventory.freeze(2)
    assert len(_missing_issues(initial)) == len(_missing_issues(final)) == 1
    assert _issue_payload_size(final) == _issue_payload_size(initial)
    assert final.fingerprint == initial.fingerprint


def test_freeze_after_thousand_updates_completes_without_timeout_inflation():
    inventory = _inventory()
    repeated = _pod("2")
    for _ in range(1000):
        inventory.apply_event(_event("MODIFIED", repeated, observed_at_ns=2))
    started = time.monotonic()
    revision = inventory.freeze(2)
    elapsed = time.monotonic() - started
    assert len(_missing_issues(revision)) == 1
    assert elapsed < 2.0
    assert inventory.lock_timeout_sec == 10.0


def test_concurrent_watch_updates_and_freeze_publish_coherent_issues():
    inventory = _inventory()
    errors = []

    def update():
        try:
            for resource_version in range(2, 202):
                inventory.apply_event(_event(
                    "MODIFIED",
                    _pod(resource_version),
                    observed_at_ns=resource_version,
                ))
        except Exception as error:
            errors.append(error)

    writer = threading.Thread(target=update)
    writer.start()
    revisions = [inventory.freeze(202) for _ in range(100)]
    writer.join(2.0)
    assert not writer.is_alive()
    assert errors == []
    assert all(len(_missing_issues(revision)) == 1 for revision in revisions)


def test_endpoint_slice_event_lifecycle_matches_current_objects():
    inventory = _inventory([])
    added = _slice(resource_version="1")
    modified = _slice(resource_version="2")
    readded = _slice(resource_version="4")
    inventory.apply_event(_event("ADDED", added, 1))
    assert len(_missing_issues(inventory.freeze(1))) == 1
    inventory.apply_event(_event("MODIFIED", modified, 2))
    assert len(_missing_issues(inventory.freeze(2))) == 1
    inventory.apply_event(_event("DELETED", modified, 3))
    assert _missing_issues(inventory.freeze(3)) == ()
    inventory.apply_event(_event("ADDED", readded, 4))
    assert len(_missing_issues(inventory.freeze(4))) == 1


def test_stable_issue_identity_is_deduplicated_and_sorted():
    duplicate = {
        "reason_code": "endpoint_service_missing",
        "object_id": "slice-a",
        "related_object_ids": ["service-b", "service-a"],
        "details": {"namespace": "observability", "kind": "EndpointSlice"},
    }
    reordered = {
        "details": {"kind": "EndpointSlice", "namespace": "observability"},
        "related_object_ids": ["service-a", "service-b"],
        "object_id": "slice-a",
        "reason_code": "endpoint_service_missing",
    }
    canonical = _canonicalize_structural_issues([duplicate, reordered])
    assert canonical == ({
        "reason_code": "endpoint_service_missing",
        "object_id": "slice-a",
        "related_object_ids": ["service-a", "service-b"],
        "details": {"namespace": "observability", "kind": "EndpointSlice"},
    },)


    inventory = _inventory([
        _slice("slice-z", "slice-z", "missing-z"),
        _slice("slice-a", "slice-a", "missing-a"),
    ])
    for _ in range(20):
        inventory._reindex()
    issues = _missing_issues(inventory.freeze(2))
    identities = [
        (
            issue["reason_code"],
            issue["object_id"],
            tuple(issue.get("related_object_ids", ())),
            canonical_hash(issue.get("details", {})),
        )
        for issue in issues
    ]
    assert identities == sorted(set(identities))


def test_real_kubernetes_object_changes_remain_visible_in_revision_identity():
    changes = ("pod_placement", "endpoint_backend", "service_uid")
    for change in changes:
        inventory = _inventory()
        inventory.replace_kind(
            "Service",
            [_service("missing-service", "service-1")],
            "service-rv-2",
            2,
        )
        before = inventory.freeze(2)
        if change == "pod_placement":
            changed = _pod("2")
            changed["spec"] = {"nodeName": "node-b"}
            inventory.replace_kind("Pod", [changed], "pod-rv-2", 3)
        elif change == "endpoint_backend":
            changed = _slice(service_name="missing-service", resource_version="2")
            changed["endpoints"] = [{
                "addresses": ["192.0.2.10"],
                "conditions": {"ready": True},
                "targetRef": {"kind": "Pod", "uid": "pod-1"},
            }]
            inventory.replace_kind(
                "EndpointSlice",
                [changed],
                "slice-rv-2",
                3,
            )
        else:
            inventory.replace_kind(
                "Service",
                [_service("missing-service", "service-2", "2")],
                "service-rv-3",
                3,
            )
        after = inventory.freeze(3)
        assert after.fingerprint != before.fingerprint
        assert after.revision_id != before.revision_id


def test_frozen_revision_remains_immutable_after_watcher_update():
    inventory = _inventory()
    frozen = inventory.freeze(2)
    changed = _pod("2")
    changed["spec"] = {"nodeName": "node-b"}
    inventory.apply_event(_event("MODIFIED", changed, observed_at_ns=3))
    current = inventory.freeze(3)
    assert (frozen.objects_by_kind["Pod"]["pod-1"].get("spec") or {}).get(
        "nodeName"
    ) is None
    assert current.objects_by_kind["Pod"]["pod-1"]["spec"]["nodeName"] == "node-b"
    assert frozen.fingerprint != current.fingerprint

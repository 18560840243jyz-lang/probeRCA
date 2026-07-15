from __future__ import annotations

import os

from proberca.live.health import LiveHealthState


def _ready_state():
    state = LiveHealthState()
    state.update(
        kubernetes_connected=True, watchers_synchronized=True,
        inventory_stale=False, watcher_relisting=False, watcher_fatal=False,
        prometheus_healthy=True, leader=True, checkpoint_writable=True,
        output_writable=True, engine_available=True, catchup_exceeded=False,
        fatal_error=None)
    return state


def test_readiness_reason_codes_transition_and_recover():
    state = _ready_state()
    assert state.ready and state.status()["reason_codes"] == []
    state.update(prometheus_healthy=False)
    assert not state.ready and "prometheus_unavailable" in state.status()["reason_codes"]
    state.update(prometheus_healthy=True, watcher_relisting=True)
    assert not state.ready and "watcher_relisting" in state.status()["reason_codes"]
    state.update(watcher_relisting=False, checkpoint_writable=False)
    assert "checkpoint_not_writable" in state.status()["reason_codes"]
    state.update(checkpoint_writable=True, output_writable=False)
    assert "output_not_writable" in state.status()["reason_codes"]
    state.update(output_writable=True)
    assert state.ready


def test_writable_probe_performs_real_fsync_and_cleans_up(tmp_path):
    from proberca.live.health import probe_writable_directory

    assert probe_writable_directory(tmp_path)
    assert list(tmp_path.iterdir()) == []
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    assert not probe_writable_directory(blocked)
    assert blocked.read_text(encoding="utf-8") == "not a directory"


def test_supervisor_request_relist_uses_managed_watcher():
    from proberca.k8s.supervisor import KubernetesWatchSupervisor

    class Watcher:
        resource_kind = "Pod"
        def __init__(self): self.reasons = []
        def request_relist(self, reason): self.reasons.append(reason)
    watcher = Watcher()
    supervisor = KubernetesWatchSupervisor(object(), [watcher])
    supervisor.request_relist("Pod", "operator_requested")
    assert watcher.reasons == ["operator_requested"]
    assert supervisor.health_snapshot()["relist_count"] == 1


def test_resume_tolerates_only_concurrent_internal_writable_probe(tmp_path):
    import pytest

    from proberca.orchestration.state import OutputLedger
    from proberca.replay.output import ReplayOutputError, ReplayOutputWriter

    ledger = OutputLedger.create(
        alerts=[], reports=[], failures=[], processed_window_count=0,
        last_processed_timestamp=None, pending_incident=None,
        dataset_fingerprint="live", config_fingerprint="config")
    probe = tmp_path / (".proberca-write-probe-" + "a" * 32 + ".tmp")
    probe.write_text("probe", encoding="utf-8")
    ReplayOutputWriter(tmp_path, resume_ledger=ledger)
    assert probe.read_text(encoding="utf-8") == "probe"

    (tmp_path / ".proberca-write-probe-not-a-uuid.tmp").write_text(
        "conflict", encoding="utf-8")
    with pytest.raises(ReplayOutputError, match="unknown files"):
        ReplayOutputWriter(tmp_path, resume_ledger=ledger)


def test_status_exposes_non_sensitive_scheduler_cursor():
    state = _ready_state()
    state.update_runtime(
        coordinator_state="LEADER_ACTIVE", committed_sequence=22,
        next_sequence=23, next_start_ns=123, last_now_ns=456,
        eligible_window_count=3,
    )
    runtime = state.status()["runtime"]
    assert runtime == {
        "committed_sequence": 22,
        "coordinator_state": "LEADER_ACTIVE",
        "eligible_window_count": 3,
        "last_now_ns": 456,
        "next_sequence": 23,
        "next_start_ns": 123,
    }
    assert "token" not in str(runtime).lower()

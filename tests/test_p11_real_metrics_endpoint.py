from __future__ import annotations

from proberca.live.health import LiveHealthState


REQUIRED = (
    "processed_windows_total", "topology_builds_total", "metric_queries_total",
    "metric_query_failures_total", "watch_reconnects_total", "watch_relists_total",
    "engine_failures_total", "alerts_total", "reports_total", "failures_total",
    "leader_transitions_total", "checkpoint_saves_total",
    "checkpoint_failures_total", "output_conflicts_total",
)


def test_metrics_exposition_is_nonempty_typed_and_complete():
    state = LiveHealthState()
    text = state.prometheus_metrics()
    for name in REQUIRED:
        metric = "proberca_" + name
        assert f"# HELP {metric} " in text
        assert f"# TYPE {metric} counter" in text
        assert f"{metric} 0" in text


def test_counters_are_monotonic_and_follower_does_not_process():
    state = LiveHealthState()
    state.increment("processed_windows_total", 3)
    assert state.counter("processed_windows_total") == 3
    import pytest
    with pytest.raises(ValueError):
        state.increment("processed_windows_total", -1)
    state.update(leader=False)
    with pytest.raises(RuntimeError):
        state.record_processed_window()


def test_metrics_do_not_expose_sensitive_labels():
    state = LiveHealthState(code_revision="abc", source_fingerprint="f" * 64)
    text = state.prometheus_metrics().lower()
    for forbidden in ("pod_uid", "token", "kubeconfig", "10.0.0."):
        assert forbidden not in text

from proberca.live.health import LiveHealthState
from proberca.live.progress import LiveStage, StageProgressTracker


def _healthy():
    health = LiveHealthState()
    health.update(
        kubernetes_connected=True,
        watchers_synchronized=True,
        prometheus_healthy=True,
        leader=True,
        checkpoint_writable=True,
        output_writable=True,
        engine_available=True,
        inventory_stale=False,
    )
    return health


def test_backlog_without_recent_commit_is_not_ready():
    health = _healthy()
    health.update_progress_health(
        backlog_count=4,
        last_commit_age_sec=31.0,
        current_stage=LiveStage.IDLE,
        current_stage_age_sec=0.0,
        current_stage_timeout_sec=10.0,
        progress_timeout_sec=30.0,
        backlog_not_ready_threshold=10,
        working_engine_count=0,
        stalled=False,
    )
    assert not health.ready
    assert "live_progress_stalled" in health.reason_codes()


def test_stage_timeout_and_backlog_threshold_are_readiness_inputs():
    health = _healthy()
    health.update_progress_health(
        backlog_count=10,
        last_commit_age_sec=1.0,
        current_stage=LiveStage.ENGINE_PROCESS,
        current_stage_age_sec=11.0,
        current_stage_timeout_sec=10.0,
        progress_timeout_sec=30.0,
        backlog_not_ready_threshold=10,
        working_engine_count=1,
        stalled=False,
    )
    assert not health.ready
    reasons = health.reason_codes()
    assert "live_stage_timeout" in reasons
    assert "live_backlog_threshold" in reasons


def test_commit_progress_restores_readiness_and_status_fields():
    health = _healthy()
    health.update_progress_health(
        backlog_count=1,
        last_commit_age_sec=0.1,
        current_stage=LiveStage.WINDOW_COMPLETE,
        current_stage_age_sec=0.1,
        current_stage_timeout_sec=10.0,
        progress_timeout_sec=30.0,
        backlog_not_ready_threshold=10,
        working_engine_count=0,
        stalled=False,
        committed_sequence=24,
        next_sequence=25,
        attempt=2,
        active_transaction_state="committed",
    )
    assert health.ready
    progress = health.status()["progress"]
    assert progress["committed_sequence"] == 24
    assert progress["next_sequence"] == 25
    assert progress["attempt"] == 2
    metrics = health.prometheus_metrics()
    assert "proberca_live_backlog_current 1" in metrics
    assert "proberca_live_last_commit_age_seconds 0.1" in metrics

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path


class _FlushingTextStream(io.StringIO):
    def __init__(self):
        super().__init__()
        self.flush_count = 0

    def flush(self):
        self.flush_count += 1
        return super().flush()


def test_canonical_stage_audit_is_streamed_and_flushed_per_event():
    from proberca.live.progress import (
        CanonicalStageAuditWriter,
        LiveStage,
        StageProgressTracker,
    )

    stream = _FlushingTextStream()
    writer = CanonicalStageAuditWriter(stream)
    tracker = StageProgressTracker(
        event_sink=writer,
        clock=lambda: 1.0,
        wall_clock=lambda: 2,
    )
    tracker.enter(
        LiveStage.FREEZE_REVISION,
        sequence=1,
        window_start_ns=10,
        window_end_ns=20,
        transaction_id="transaction-a",
        attempt=1,
    )
    tracker.abort(reason_code="transient_collection_retry")

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    assert stream.flush_count == 2
    for line in lines:
        payload = json.loads(line)
        assert line == json.dumps(payload, sort_keys=True, separators=(",", ":"))
        assert payload["schema_version"] == "p11-live-stage-audit-v1"
        assert "transaction_id" not in payload["event"]
        assert payload["event"]["transaction_id_fingerprint"] == hashlib.sha256(
            b"transaction-a"
        ).hexdigest()


def test_attempt_binding_clears_stale_identity_and_records_distinct_retries():
    from proberca.live.progress import LiveStage, StageProgressTracker

    tracker = StageProgressTracker(clock=lambda: 1.0)
    tracker.enter(
        LiveStage.BEGIN_WINDOW,
        sequence=1,
        attempt=1,
        transaction_id=None,
    )
    tracker.bind_attempt(
        sequence=1,
        window_start_ns=10,
        window_end_ns=20,
        attempt=1,
        transaction_id="transaction-a",
        working_engine_fingerprint="working-a",
        generation_staging_fingerprint="staging-a",
    )
    tracker.abort(reason_code="transient_collection_retry")
    tracker.enter(
        LiveStage.BEGIN_WINDOW,
        sequence=1,
        attempt=2,
        transaction_id=None,
        working_engine_fingerprint=None,
        generation_staging_fingerprint=None,
    )
    assert tracker.snapshot()["transaction_id"] is None
    tracker.bind_attempt(
        sequence=1,
        window_start_ns=10,
        window_end_ns=20,
        attempt=2,
        transaction_id="transaction-b",
        working_engine_fingerprint="working-b",
        generation_staging_fingerprint="staging-b",
    )

    bound = [event for event in tracker.events() if event.event_type.value == "attempt_enter"]
    assert [event.attempt for event in bound] == [1, 2]
    assert bound[0].transaction_id != bound[1].transaction_id
    assert bound[0].working_engine_fingerprint != bound[1].working_engine_fingerprint
    assert bound[0].generation_staging_fingerprint != bound[1].generation_staging_fingerprint


def test_attempt_resource_materialization_is_explicit_not_inferred():
    from proberca.live.progress import StageProgressTracker

    tracker = StageProgressTracker(clock=lambda: 1.0)
    tracker.bind_attempt(
        sequence=1,
        window_start_ns=10,
        window_end_ns=20,
        attempt=1,
        transaction_id="transaction-a",
        working_engine_fingerprint="working-a",
        generation_staging_fingerprint="staging-a",
    )
    assert tracker.snapshot()["working_engine_materialized"] is False
    assert tracker.snapshot()["generation_staging_materialized"] is False
    tracker.materialize_working_engine()
    tracker.materialize_generation_staging()
    assert tracker.snapshot()["working_engine_materialized"] is True
    assert tracker.snapshot()["generation_staging_materialized"] is True


def test_attempt_identity_reserves_stage_independent_resource_identities():
    from proberca.live.coordinator import WindowAttemptIdentity

    def identity(attempt):
        return WindowAttemptIdentity(
            sequence=1,
            window_start_ns=10,
            window_end_ns=20,
            attempt_index=attempt,
            leadership_epoch_fingerprint="leader",
            runner_instance_fingerprint="runner",
            previous_generation_fingerprint=None,
        )

    first = identity(1)
    second = identity(2)
    assert first.working_engine_fingerprint != second.working_engine_fingerprint
    assert first.generation_staging_fingerprint != second.generation_staging_fingerprint


def _bound_tracker(writer, attempt=1):
    from proberca.live.progress import LiveStage, StageProgressTracker

    tracker = StageProgressTracker(
        event_sink=writer, clock=lambda: 1.0, wall_clock=lambda: 2,
    )
    tracker.enter(
        LiveStage.BEGIN_WINDOW, sequence=1, window_start_ns=10,
        window_end_ns=20, transaction_id=None, attempt=attempt,
        working_engine_fingerprint=None,
        generation_staging_fingerprint=None,
    )
    tracker.bind_attempt(
        sequence=1, window_start_ns=10, window_end_ns=20,
        attempt=attempt, transaction_id=f"transaction-{attempt}",
        working_engine_fingerprint=f"working-{attempt}",
        generation_staging_fingerprint=f"staging-{attempt}",
    )
    return tracker


def test_bounded_attempt_audit_is_identical_on_stdout_and_durable_storage(tmp_path):
    from proberca.live.audit import BoundedAttemptAuditWriter

    stream = _FlushingTextStream()
    path = tmp_path / "live_attempt_audit.jsonl"
    writer = BoundedAttemptAuditWriter(stream, path)
    tracker = _bound_tracker(writer)
    tracker.classify_attempt("TRANSIENT_EMPTY", reason_code="no_samples")
    tracker.abort(reason_code="no_samples", classification="TRANSIENT_EMPTY")

    stdout_lines = stream.getvalue().splitlines()
    durable_lines = path.read_text().splitlines()
    assert stdout_lines == durable_lines
    assert writer.failed is False
    payloads = [json.loads(line) for line in durable_lines]
    assert all(item["schema_version"] == "p11-live-attempt-audit-v1" for item in payloads)
    assert payloads[-1]["transaction_state"] == "aborted"
    assert payloads[-1]["classification"] == "TRANSIENT_EMPTY"
    assert payloads[-1]["attempt_index"] == 1
    assert payloads[-1]["working_engine_materialized"] is False
    assert payloads[-1]["generation_staging_materialized"] is False


def test_attempt_two_records_materialization_commit_and_distinct_resources(tmp_path):
    from proberca.live.audit import BoundedAttemptAuditWriter

    stream = _FlushingTextStream()
    path = tmp_path / "live_attempt_audit.jsonl"
    writer = BoundedAttemptAuditWriter(stream, path)
    first = _bound_tracker(writer, attempt=1)
    first.abort(reason_code="no_samples", classification="TRANSIENT_EMPTY")
    second = _bound_tracker(writer, attempt=2)
    second.materialize_working_engine()
    second.materialize_generation_staging()
    second.commit_attempt(1)

    payloads = [json.loads(line) for line in path.read_text().splitlines()]
    bound = [item for item in payloads if item["event_type"] == "attempt_enter"]
    assert len(bound) == 2
    assert bound[0]["transaction_id_fingerprint"] != bound[1]["transaction_id_fingerprint"]
    assert bound[0]["working_engine_fingerprint"] != bound[1]["working_engine_fingerprint"]
    assert bound[0]["staging_path_fingerprint"] != bound[1]["staging_path_fingerprint"]
    commit = [item for item in payloads if item["event_type"] == "attempt_commit"]
    assert len(commit) == 1
    assert commit[0]["attempt_index"] == 2
    assert commit[0]["transaction_state"] == "committed"
    assert commit[0]["committed_sequence"] == 1


def test_attempt_audit_rotation_is_bounded_and_preserves_complete_json(tmp_path):
    from proberca.live.audit import BoundedAttemptAuditWriter

    writer = BoundedAttemptAuditWriter(
        _FlushingTextStream(), tmp_path / "live_attempt_audit.jsonl",
        max_bytes=4096, backup_count=2,
    )
    tracker = _bound_tracker(writer)
    for index in range(40):
        tracker.progress(input_count=index, output_count=index, result_code="ok")

    files = sorted(tmp_path.glob("live_attempt_audit.jsonl*"))
    assert 1 < len(files) <= 3
    assert all(path.stat().st_size <= 4096 for path in files)
    for path in files:
        lines = path.read_text().splitlines()
        assert lines
        assert all(json.loads(line)["schema_version"] == "p11-live-attempt-audit-v1" for line in lines)


def test_attempt_audit_failure_is_non_throwing_and_degrades_readiness(tmp_path):
    from proberca.live.audit import BoundedAttemptAuditWriter
    from proberca.live.health import LiveHealthState

    health = LiveHealthState()
    blocking_file = tmp_path / "not-a-directory"
    blocking_file.write_text("x")

    def fail(reason_code, _error_type):
        assert reason_code == "audit_write_failed"
        health.update(audit_write_failed=True)
        health.increment("live_attempt_audit_write_failures_total")

    writer = BoundedAttemptAuditWriter(
        _FlushingTextStream(), blocking_file / "audit.jsonl", on_failure=fail,
    )
    tracker = _bound_tracker(writer)
    tracker.abort(reason_code="no_samples", classification="TRANSIENT_EMPTY")
    assert writer.failed is True
    assert writer.failure_reason == "audit_write_failed"
    assert "audit_write_failed" in health.reason_codes()
    assert health.counter("live_attempt_audit_write_failures_total") >= 1


def test_attempt_audit_allowlist_does_not_serialize_sensitive_values(tmp_path):
    from proberca.live.audit import BoundedAttemptAuditWriter

    stream = _FlushingTextStream()
    path = tmp_path / "live_attempt_audit.jsonl"
    writer = BoundedAttemptAuditWriter(stream, path)
    tracker = _bound_tracker(writer)
    tracker.abort(reason_code="no_samples", classification="TRANSIENT_EMPTY")
    text = path.read_text().lower()
    for forbidden in ("kubeconfig", "bearer", "promql", "service_name", "metric_name", "pod_uid"):
        assert forbidden not in text


def test_cli_wires_bounded_attempt_audit_to_stdout_and_checkpoint_pvc():
    source = Path("proberca/cli/live.py").read_text()
    assert "BoundedAttemptAuditWriter" in source
    assert 'Path(checkpoint_dir) / "live_attempt_audit.jsonl"' in source
    assert "sys.stdout" in source
    assert "audit_write_failed" in source

"""Credential-free monotonic progress state for one live window."""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import hashlib
import json
import threading
import time
from enum import Enum


class LiveStage(str, Enum):
    IDLE = "IDLE"
    BEGIN_WINDOW = "BEGIN_WINDOW"
    FREEZE_REVISION = "FREEZE_REVISION"
    BUILD_TOPOLOGY = "BUILD_TOPOLOGY"
    COLLECT_CALL_EDGES = "COLLECT_CALL_EDGES"
    COLLECT_NODE_METRICS = "COLLECT_NODE_METRICS"
    COLLECT_EDGE_METRICS = "COLLECT_EDGE_METRICS"
    ADAPT_RECORDS = "ADAPT_RECORDS"
    ADAPT_NODE_RECORDS = "ADAPT_NODE_RECORDS"
    ADAPT_EDGE_RECORDS = "ADAPT_EDGE_RECORDS"
    BUILD_ENGINE_INPUT = "BUILD_ENGINE_INPUT"
    ENGINE_PROCESS = "ENGINE_PROCESS"
    PREPARE_GENERATION = "PREPARE_GENERATION"
    COMMIT_RUN_STATE = "COMMIT_RUN_STATE"
    PROJECT_OUTPUT = "PROJECT_OUTPUT"
    RETENTION = "RETENTION"
    WINDOW_COMPLETE = "WINDOW_COMPLETE"
    WINDOW_ABORTED = "WINDOW_ABORTED"
    STALLED = "STALLED"
    FATAL = "FATAL"


class StageEventType(str, Enum):
    ENTER = "enter"
    PROGRESS = "progress"
    RETRY = "retry"
    EXIT = "exit"
    TIMEOUT = "timeout"
    ERROR = "error"
    ABORT = "abort"
    ATTEMPT_ENTER = "attempt_enter"
    ATTEMPT_RESOURCE = "attempt_resource"
    ATTEMPT_CLASSIFIED = "attempt_classified"
    ATTEMPT_COMMIT = "attempt_commit"


def _fingerprint(value) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()


@dataclass(frozen=True)
class StageEvent:
    event_index: int
    sequence: int | None
    window_start_ns: int | None
    window_end_ns: int | None
    transaction_id: str | None
    leadership_epoch_fingerprint: str | None
    stage: LiveStage
    event_type: StageEventType
    monotonic_ns: int
    wall_timestamp_ns: int
    stage_age_sec: float
    previous_stage_duration_sec: float
    thread_name: str
    thread_ident_fingerprint: str
    attempt: int
    input_count: int | None
    output_count: int | None
    result_code: str | None
    exception_type: str | None
    reason_code: str | None
    backlog_count: int
    working_engine_fingerprint: str | None
    generation_staging_fingerprint: str | None
    working_engine_materialized: bool
    generation_staging_materialized: bool
    transaction_state: str
    classification: str | None
    raw_sample_count: int | None
    normalized_sample_count: int | None
    adapted_record_count: int | None
    retry_backoff_sec: float | None
    committed_sequence: int | None

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["stage"] = self.stage.value
        payload["event_type"] = self.event_type.value
        return payload


class CanonicalStageAuditWriter:
    """Flush one credential-free canonical event per live transition."""

    schema_version = "p11-live-stage-audit-v1"

    def __init__(self, stream):
        if not all(hasattr(stream, name) for name in ("write", "flush")):
            raise TypeError("stage audit stream must provide write and flush")
        self._stream = stream
        self._lock = threading.Lock()

    def __call__(self, event: StageEvent) -> None:
        if not isinstance(event, StageEvent):
            raise TypeError("stage audit writer requires StageEvent")
        item = event.to_dict()
        item["transaction_id_fingerprint"] = item.pop("transaction_id")
        line = json.dumps(
            {"schema_version": self.schema_version, "event": item},
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()


_UNSET = object()


class StageProgressTracker:
    """Tracks bounded status and streams every transition to an audit sink."""

    def __init__(self, *, clock=time.monotonic, wall_clock=time.time_ns,
                 maximum_history=256, event_sink=None):
        if isinstance(maximum_history, bool) or not isinstance(maximum_history, int):
            raise TypeError("maximum_history must be an integer")
        if maximum_history <= 0:
            raise ValueError("maximum_history must be positive")
        if event_sink is not None and not callable(event_sink):
            raise TypeError("event_sink must be callable")
        self._clock = clock
        self._wall_clock = wall_clock
        self._event_sink = event_sink
        self._lock = threading.RLock()
        self._events = deque(maxlen=maximum_history)
        self._stage = LiveStage.IDLE
        self._stage_started = self._clock()
        self._last_progress = self._stage_started
        self._previous_duration = 0.0
        self._sequence = None
        self._window_start_ns = None
        self._window_end_ns = None
        self._transaction_id = None
        self._leadership_epoch = None
        self._operation_counter = 0
        self._retry_attempt = 0
        self._last_error = None
        self._backlog_count = 0
        self._working_engine_fingerprint = None
        self._generation_staging_fingerprint = None
        self._working_engine_materialized = False
        self._generation_staging_materialized = False
        self._transaction_state = "idle"
        self._classification = None
        self._raw_sample_count = None
        self._normalized_sample_count = None
        self._adapted_record_count = None
        self._retry_backoff_sec = None
        self._committed_sequence = None
        self._thread_name = threading.current_thread().name
        self._thread_ident_fingerprint = _fingerprint(
            threading.current_thread().ident,
        )

    def _append(self, event_type, *, now, input_count=None, output_count=None,
                result_code=None, exception_type=None, reason_code=None):
        self._operation_counter += 1
        thread = threading.current_thread()
        self._thread_name = thread.name
        self._thread_ident_fingerprint = _fingerprint(thread.ident)
        event = StageEvent(
            event_index=self._operation_counter,
            sequence=self._sequence,
            window_start_ns=self._window_start_ns,
            window_end_ns=self._window_end_ns,
            transaction_id=self._transaction_id,
            leadership_epoch_fingerprint=self._leadership_epoch,
            stage=self._stage,
            event_type=event_type,
            monotonic_ns=int(now * 1_000_000_000),
            wall_timestamp_ns=int(self._wall_clock()),
            stage_age_sec=max(0.0, now - self._stage_started),
            previous_stage_duration_sec=self._previous_duration,
            thread_name=self._thread_name,
            thread_ident_fingerprint=self._thread_ident_fingerprint,
            attempt=self._retry_attempt,
            input_count=input_count,
            output_count=output_count,
            result_code=result_code,
            exception_type=exception_type,
            reason_code=reason_code,
            backlog_count=self._backlog_count,
            working_engine_fingerprint=self._working_engine_fingerprint,
            generation_staging_fingerprint=(
                self._generation_staging_fingerprint
            ),
            working_engine_materialized=self._working_engine_materialized,
            generation_staging_materialized=(
                self._generation_staging_materialized
            ),
            transaction_state=self._transaction_state,
            classification=self._classification,
            raw_sample_count=self._raw_sample_count,
            normalized_sample_count=self._normalized_sample_count,
            adapted_record_count=self._adapted_record_count,
            retry_backoff_sec=self._retry_backoff_sec,
            committed_sequence=self._committed_sequence,
        )
        self._events.append(event)
        self._last_progress = now
        if self._event_sink is not None:
            self._event_sink(event)
        return event

    def enter(self, stage: LiveStage, *, sequence=None, window_start_ns=None,
              window_end_ns=None, transaction_id=_UNSET,
              leadership_epoch_fingerprint=None, backlog_count=None,
              retry_attempt=None, attempt=None, input_count=None,
              working_engine_fingerprint=_UNSET,
              generation_staging_fingerprint=_UNSET) -> StageEvent:
        if not isinstance(stage, LiveStage):
            raise TypeError("stage must be a LiveStage")
        now = self._clock()
        with self._lock:
            self._previous_duration = max(0.0, now - self._stage_started)
            self._stage = stage
            self._stage_started = now
            if sequence is not None:
                self._sequence = int(sequence)
            if window_start_ns is not None:
                self._window_start_ns = int(window_start_ns)
            if window_end_ns is not None:
                self._window_end_ns = int(window_end_ns)
            if transaction_id is not _UNSET:
                self._transaction_id = _fingerprint(transaction_id)
            if working_engine_fingerprint is not _UNSET:
                if working_engine_fingerprint != self._working_engine_fingerprint:
                    self._working_engine_materialized = False
                self._working_engine_fingerprint = working_engine_fingerprint
            if generation_staging_fingerprint is not _UNSET:
                if (generation_staging_fingerprint
                        != self._generation_staging_fingerprint):
                    self._generation_staging_materialized = False
                self._generation_staging_fingerprint = (
                    generation_staging_fingerprint
                )
            if leadership_epoch_fingerprint is not None:
                self._leadership_epoch = _fingerprint(
                    leadership_epoch_fingerprint,
                )
            if backlog_count is not None:
                self._backlog_count = int(backlog_count)
            selected_attempt = attempt if attempt is not None else retry_attempt
            if selected_attempt is not None:
                self._retry_attempt = int(selected_attempt)
            self._last_error = None
            return self._append(
                StageEventType.ENTER, now=now, input_count=input_count,
            )

    def bind_attempt(
        self, *, sequence, window_start_ns, window_end_ns, attempt,
        transaction_id, working_engine_fingerprint,
        generation_staging_fingerprint,
    ) -> StageEvent:
        if not transaction_id:
            raise ValueError("attempt transaction_id is required")
        if not working_engine_fingerprint or not generation_staging_fingerprint:
            raise ValueError("attempt resource fingerprints are required")
        now = self._clock()
        with self._lock:
            self._sequence = int(sequence)
            self._window_start_ns = int(window_start_ns)
            self._window_end_ns = int(window_end_ns)
            self._retry_attempt = int(attempt)
            self._transaction_id = _fingerprint(transaction_id)
            self._working_engine_fingerprint = str(working_engine_fingerprint)
            self._generation_staging_fingerprint = str(
                generation_staging_fingerprint
            )
            self._working_engine_materialized = False
            self._generation_staging_materialized = False
            self._transaction_state = "active"
            self._classification = None
            self._raw_sample_count = None
            self._normalized_sample_count = None
            self._adapted_record_count = None
            self._retry_backoff_sec = None
            self._committed_sequence = None
            return self._append(StageEventType.ATTEMPT_ENTER, now=now)

    def materialize_working_engine(self) -> StageEvent:
        now = self._clock()
        with self._lock:
            if not self._working_engine_fingerprint:
                raise RuntimeError("working Engine attempt identity is not bound")
            if self._working_engine_materialized:
                raise RuntimeError("working Engine is already materialized")
            self._working_engine_materialized = True
            return self._append(
                StageEventType.ATTEMPT_RESOURCE,
                now=now,
                result_code="working_engine_materialized",
            )

    def materialize_generation_staging(self) -> StageEvent:
        now = self._clock()
        with self._lock:
            if not self._generation_staging_fingerprint:
                raise RuntimeError("generation staging identity is not bound")
            if self._generation_staging_materialized:
                raise RuntimeError("generation staging is already materialized")
            self._generation_staging_materialized = True
            return self._append(
                StageEventType.ATTEMPT_RESOURCE,
                now=now,
                result_code="generation_staging_materialized",
            )

    def record_collection_counts(
        self, *, raw_sample_count, normalized_sample_count,
        adapted_record_count,
    ) -> StageEvent:
        values = (
            raw_sample_count, normalized_sample_count, adapted_record_count,
        )
        if any(isinstance(value, bool) or int(value) < 0 for value in values):
            raise ValueError("collection counts must be non-negative integers")
        now = self._clock()
        with self._lock:
            self._raw_sample_count = int(raw_sample_count)
            self._normalized_sample_count = int(normalized_sample_count)
            self._adapted_record_count = int(adapted_record_count)
            return self._append(
                StageEventType.PROGRESS,
                now=now,
                input_count=self._raw_sample_count,
                output_count=self._adapted_record_count,
                result_code="collection_counts",
            )

    def classify_attempt(self, classification, *, reason_code=None) -> StageEvent:
        if not classification:
            raise ValueError("attempt classification is required")
        now = self._clock()
        with self._lock:
            self._classification = str(classification)
            return self._append(
                StageEventType.ATTEMPT_CLASSIFIED,
                now=now,
                reason_code=(str(reason_code) if reason_code else None),
            )

    def commit_attempt(self, committed_sequence) -> StageEvent:
        now = self._clock()
        with self._lock:
            self._transaction_state = "committed"
            self._classification = "SUCCESS"
            self._committed_sequence = int(committed_sequence)
            return self._append(StageEventType.ATTEMPT_COMMIT, now=now)

    def progress(self, *, input_count=None, output_count=None,
                 result_code=None) -> StageEvent:
        now = self._clock()
        with self._lock:
            return self._append(
                StageEventType.PROGRESS,
                now=now,
                input_count=input_count,
                output_count=output_count,
                result_code=result_code,
            )

    def exit(self, *, output_count=None, result_code="ok") -> StageEvent:
        now = self._clock()
        with self._lock:
            return self._append(
                StageEventType.EXIT,
                now=now,
                output_count=output_count,
                result_code=result_code,
            )

    def retry(self, *, attempt, reason_code, backoff_sec=None) -> StageEvent:
        now = self._clock()
        with self._lock:
            self._retry_attempt = int(attempt)
            self._retry_backoff_sec = (
                float(backoff_sec) if backoff_sec is not None else None
            )
            return self._append(
                StageEventType.RETRY, now=now, reason_code=str(reason_code),
            )

    def timeout(self, *, reason_code="stage_timeout") -> StageEvent:
        now = self._clock()
        with self._lock:
            self._last_error = {
                "error_type": "LiveStageTimeoutError",
                "reason_code": str(reason_code),
            }
            return self._append(
                StageEventType.TIMEOUT,
                now=now,
                exception_type="LiveStageTimeoutError",
                reason_code=str(reason_code),
            )

    def record_error(self, error: BaseException, reason_code: str) -> StageEvent:
        if not reason_code:
            raise ValueError("reason_code is required")
        now = self._clock()
        with self._lock:
            self._last_error = {
                "error_type": type(error).__name__,
                "reason_code": str(reason_code),
            }
            return self._append(
                StageEventType.ERROR,
                now=now,
                exception_type=type(error).__name__,
                reason_code=str(reason_code),
            )

    def abort(self, *, reason_code, classification=None) -> StageEvent:
        now = self._clock()
        with self._lock:
            self._stage = LiveStage.WINDOW_ABORTED
            self._stage_started = now
            self._transaction_state = "aborted"
            if classification is not None:
                self._classification = str(classification)
            return self._append(
                StageEventType.ABORT, now=now, reason_code=str(reason_code),
            )

    def events(self) -> tuple[StageEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def snapshot(self) -> dict:
        now = self._clock()
        with self._lock:
            return {
                "stage": self._stage.value,
                "stage_age_sec": max(0.0, now - self._stage_started),
                "stage_started_monotonic": self._stage_started,
                "previous_stage_duration_sec": self._previous_duration,
                "last_progress_monotonic": self._last_progress,
                "sequence": self._sequence,
                "window_start_ns": self._window_start_ns,
                "window_end_ns": self._window_end_ns,
                "transaction_id": self._transaction_id,
                "leadership_epoch_fingerprint": self._leadership_epoch,
                "thread_name": self._thread_name,
                "thread_ident_fingerprint": self._thread_ident_fingerprint,
                "operation_counter": self._operation_counter,
                "retry_attempt": self._retry_attempt,
                "attempt": self._retry_attempt,
                "last_structured_error": (
                    dict(self._last_error) if self._last_error else None
                ),
                "backlog_count": self._backlog_count,
                "working_engine_fingerprint": (
                    self._working_engine_fingerprint
                ),
                "generation_staging_fingerprint": (
                    self._generation_staging_fingerprint
                ),
                "working_engine_materialized": (
                    self._working_engine_materialized
                ),
                "generation_staging_materialized": (
                    self._generation_staging_materialized
                ),
                "transaction_state": self._transaction_state,
                "classification": self._classification,
                "raw_sample_count": self._raw_sample_count,
                "normalized_sample_count": self._normalized_sample_count,
                "adapted_record_count": self._adapted_record_count,
                "retry_backoff_sec": self._retry_backoff_sec,
                "committed_sequence": self._committed_sequence,
                "recent_events": [event.to_dict() for event in self._events],
            }

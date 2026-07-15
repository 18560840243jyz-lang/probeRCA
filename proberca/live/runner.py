"""Canonical transactional live orchestrator for P11."""
from __future__ import annotations

from dataclasses import dataclass
import time

from proberca.config import LiveLivenessConfig

from .collection import (
    CollectionOutcome, ControlledTransientCollectionEmpty,
    WindowCollectionRetrier,
)

from .coordinator import (
    CommittedOutputDegradedError,
    LiveCommitCoordinator,
    LiveCoordinatorState,
)
from .engine_worker import EngineStageTimeout, WorkingEngineExecutor
from .executor import LiveStageTimeoutError, StageExecutor
from .progress import LiveStage, StageProgressTracker


class LiveRunnerError(RuntimeError):
    """A live window cannot be safely processed or committed."""


class CommittedOutputStalledError(CommittedOutputDegradedError):
    """RunState committed, but an uninterruptible output worker is alive."""

    exit_code = 8


@dataclass(frozen=True)
class _TransactionalWindow:
    source: object
    sequence: int

    def __getattr__(self, name):
        return getattr(self.source, name)


class ProbeRCALiveRunner:
    """Run every live boundary with one finite deadline and one commit path."""

    def __init__(
        self,
        *,
        coordinator: LiveCommitCoordinator,
        topology_builder,
        window_adapter,
        commit_payload_builder,
        metric_collector=None,
        node_metric_collector=None,
        edge_metric_collector=None,
        call_edge_collector=None,
        health=None,
        inventory=None,
        watch_supervisor=None,
        progress_tracker=None,
        liveness_config=None,
    ):
        self.coordinator = coordinator
        self.inventory = inventory
        self.watch_supervisor = watch_supervisor
        self.topology_builder = topology_builder
        self.metric_collector = metric_collector
        self.node_metric_collector = node_metric_collector
        self.edge_metric_collector = edge_metric_collector
        self.call_edge_collector = call_edge_collector
        self.window_adapter = window_adapter
        self.commit_payload_builder = commit_payload_builder
        self.health = health
        self.progress_tracker = progress_tracker or StageProgressTracker()
        self.liveness_config = liveness_config or LiveLivenessConfig()
        self.liveness_config.validate()
        callback = self.health.update_progress if self.health is not None else None
        self.stage_executor = StageExecutor(
            self.progress_tracker,
            on_progress=callback,
            lock_timeout_sec=self.liveness_config.record_adaptation_timeout_sec,
            lock_name="live-transaction",
        )
        self.retention_executor = StageExecutor(
            self.progress_tracker,
            on_progress=callback,
            lock_timeout_sec=self.liveness_config.retention_timeout_sec,
            lock_name="retention",
        )
        self.engine_executor = WorkingEngineExecutor(
            self.progress_tracker,
            lock_timeout_sec=self.liveness_config.engine_process_timeout_sec,
        )
        self.active_context = None
        if self.inventory is None and self.watch_supervisor is None:
            raise ValueError("inventory or watch_supervisor is required")
        if (
            self.metric_collector is None
            and (self.node_metric_collector is None or self.edge_metric_collector is None)
        ):
            raise ValueError(
                "combined or separate node/edge metric collectors are required",
            )

    @property
    def active_worker_count(self):
        return (
            self.stage_executor.active_worker_count
            + self.retention_executor.active_worker_count
            + int(self.engine_executor.unrecoverable_worker_alive)
        )

    def start(self, *, sync_timeout_sec):
        if self.watch_supervisor is None:
            return
        self.watch_supervisor.start()
        if not self.watch_supervisor.wait_until_synchronized(sync_timeout_sec):
            self.watch_supervisor.stop()
            self.watch_supervisor.join(sync_timeout_sec)
            raise LiveRunnerError("Kubernetes watch synchronization timed out")
        if self.health is not None:
            self.health.update(
                kubernetes_connected=True,
                watchers_synchronized=True,
                inventory_stale=False,
            )

    def stop(self, *, join_timeout_sec):
        if self.watch_supervisor is not None:
            self.watch_supervisor.stop()
            self.watch_supervisor.join(join_timeout_sec)
        if self.health is not None:
            self.health.update(watchers_synchronized=False)

    def _revision(self, end_ns):
        if self.watch_supervisor is not None:
            if self.health is not None:
                snapshot = self.watch_supervisor.health_snapshot()
                self.health.update(
                    watchers_synchronized=snapshot["synchronized"],
                    watcher_relisting=any(
                        value == "relisting"
                        for value in snapshot["states"].values()
                    ),
                    watcher_fatal=snapshot["fatal"],
                )
                reconnects = snapshot.get("reconnect_count", 0)
                relists = snapshot.get("relist_count", 0)
                current_reconnects = self.health.counter(
                    "watch_reconnects_total",
                )
                current_relists = self.health.counter("watch_relists_total")
                if reconnects > current_reconnects:
                    self.health.increment(
                        "watch_reconnects_total",
                        reconnects - current_reconnects,
                    )
                if relists > current_relists:
                    self.health.increment(
                        "watch_relists_total",
                        relists - current_relists,
                    )
            return self.watch_supervisor.freeze_revision(end_ns)
        if not self.inventory.ready:
            raise LiveRunnerError("Kubernetes inventory is not ready")
        return self.inventory.freeze(end_ns)

    def _enter(self, stage, **context):
        self.progress_tracker.enter(stage, **context)
        if self.health is not None:
            self.health.update_progress(self.progress_tracker.snapshot())

    def _execute_stage_operation(self, stage, operation):
        config = self.liveness_config
        if (
            config.controlled_stage_delay_enabled
            and config.controlled_stage_delay_stage == stage.value
        ):
            time.sleep(config.controlled_stage_delay_sec)
        return operation()

    def _run_stage(self, stage, sequence, timeout_sec, operation, *, window,
                   attempt=1, input_count=None, executor=None):
        return (executor or self.stage_executor).run_stage(
            stage,
            sequence=sequence,
            timeout_sec=timeout_sec,
            operation=lambda: self._execute_stage_operation(stage, operation),
            attempt=attempt,
            input_count=input_count,
            context={
                "window_start_ns": window.start_ns,
                "window_end_ns": window.end_ns,
                "transaction_id": getattr(
                    self.active_context, "transaction_id", None,
                ),
                "working_engine_fingerprint": getattr(
                    self.active_context, "working_engine_fingerprint", None,
                ),
                "generation_staging_fingerprint": getattr(
                    self.active_context,
                    "generation_staging_fingerprint",
                    None,
                ),
                "backlog_count": (
                    int(self.health.status()["runtime"].get(
                        "eligible_window_count", 0,
                    ))
                    if self.health is not None else 0
                ),
                "leadership_epoch_fingerprint": getattr(
                    getattr(self.coordinator, "token", None),
                    "token_fingerprint",
                    None,
                ),
            },
        )

    def _collect_metrics(
        self, transactional_window, revision, sequence, attempt,
    ):
        if self.metric_collector is not None:
            node_records, edge_records = self._run_stage(
                LiveStage.COLLECT_NODE_METRICS,
                sequence,
                self.liveness_config.node_metric_collection_timeout_sec,
                lambda: self.metric_collector(transactional_window, revision),
                window=transactional_window,
                attempt=attempt,
            )
            edge_records = self._run_stage(
                LiveStage.COLLECT_EDGE_METRICS,
                sequence,
                self.liveness_config.edge_metric_collection_timeout_sec,
                lambda: edge_records,
                window=transactional_window,
                attempt=attempt,
            )
            total_records = len(node_records) + len(edge_records)
            if hasattr(self.progress_tracker, "record_collection_counts"):
                self.progress_tracker.record_collection_counts(
                    raw_sample_count=total_records,
                    normalized_sample_count=total_records,
                    adapted_record_count=total_records,
                )
            self._raise_controlled_collection_fault(attempt)
            return node_records, edge_records
        node_records = self._run_stage(
            LiveStage.COLLECT_NODE_METRICS,
            sequence,
            self.liveness_config.node_metric_collection_timeout_sec,
            lambda: self.node_metric_collector(transactional_window, revision),
            window=transactional_window,
            attempt=attempt,
        )
        edge_records = self._run_stage(
            LiveStage.COLLECT_EDGE_METRICS,
            sequence,
            self.liveness_config.edge_metric_collection_timeout_sec,
            lambda: self.edge_metric_collector(transactional_window, revision),
            window=transactional_window,
            attempt=attempt,
        )
        total_records = len(node_records) + len(edge_records)
        if hasattr(self.progress_tracker, "record_collection_counts"):
            self.progress_tracker.record_collection_counts(
                raw_sample_count=total_records,
                normalized_sample_count=total_records,
                adapted_record_count=total_records,
            )
        self._raise_controlled_collection_fault(attempt)
        return node_records, edge_records

    def _raise_controlled_collection_fault(self, attempt):
        config = self.liveness_config
        if (
            config.controlled_collection_fault_enabled
            and attempt <= config.controlled_transient_empty_attempts
        ):
            raise ControlledTransientCollectionEmpty(
                "controlled transient empty collection attempt",
            )

    def process_window(self, window, *, attempt=1):
        if self.coordinator.state is not LiveCoordinatorState.LEADER_ACTIVE:
            raise LiveRunnerError("instance does not hold transactional leadership")
        initial_sequence = getattr(window, "sequence", None)
        try:
            context = self._run_stage(
                LiveStage.BEGIN_WINDOW,
                initial_sequence,
                self.liveness_config.record_adaptation_timeout_sec,
                lambda: self.coordinator.begin_window(
                    window.start_ns,
                    window.end_ns,
                    attempt,
                ),
                window=window,
                attempt=attempt,
            )
            self.active_context = context
            attempt_identity = getattr(context, "attempt_identity", None)
            attempt_resources = (
                getattr(context, "transaction_id", None),
                getattr(context, "working_engine_fingerprint", None),
                getattr(context, "generation_staging_fingerprint", None),
            )
            if attempt_identity is not None and all(attempt_resources):
                self.progress_tracker.bind_attempt(
                    sequence=context.sequence,
                    window_start_ns=getattr(
                        context, "window_start_ns", window.start_ns,
                    ),
                    window_end_ns=getattr(
                        context, "window_end_ns", window.end_ns,
                    ),
                    attempt=attempt,
                    transaction_id=attempt_resources[0],
                    working_engine_fingerprint=attempt_resources[1],
                    generation_staging_fingerprint=attempt_resources[2],
                )
                if self.health is not None:
                    self.health.update_progress(
                        self.progress_tracker.snapshot()
                    )
            transactional_window = _TransactionalWindow(
                window, context.sequence,
            )
            revision = self._run_stage(
                LiveStage.FREEZE_REVISION,
                context.sequence,
                self.liveness_config.freeze_revision_timeout_sec,
                lambda: self._revision(window.end_ns),
                window=transactional_window,
                attempt=attempt,
            )
            calls = ()
            if self.call_edge_collector is not None:
                calls = self._run_stage(
                    LiveStage.COLLECT_CALL_EDGES,
                    context.sequence,
                    self.liveness_config.call_edge_collection_timeout_sec,
                    lambda: self.call_edge_collector(
                        transactional_window, revision,
                    ),
                    window=transactional_window,
                    attempt=attempt,
                )
            topology = self._run_stage(
                LiveStage.BUILD_TOPOLOGY,
                context.sequence,
                self.liveness_config.topology_build_timeout_sec,
                lambda: (
                    self.topology_builder(transactional_window, revision, calls)
                    if self.call_edge_collector is not None
                    else self.topology_builder(transactional_window, revision)
                ),
                window=transactional_window,
                attempt=attempt,
                input_count=len(calls),
            )
            if self.health is not None:
                self.health.increment("topology_builds_total")
            node_records, edge_records = self._collect_metrics(
                transactional_window, revision, context.sequence, attempt,
            )
            if self.health is not None:
                self.health.update(prometheus_healthy=True)
            node_records = self._run_stage(
                LiveStage.ADAPT_NODE_RECORDS,
                context.sequence,
                self.liveness_config.record_adaptation_timeout_sec,
                lambda: list(node_records),
                window=transactional_window,
                attempt=attempt,
                input_count=len(node_records),
            )
            edge_records = self._run_stage(
                LiveStage.ADAPT_EDGE_RECORDS,
                context.sequence,
                self.liveness_config.record_adaptation_timeout_sec,
                lambda: list(edge_records),
                window=transactional_window,
                attempt=attempt,
                input_count=len(edge_records),
            )
            engine_input = self._run_stage(
                LiveStage.BUILD_ENGINE_INPUT,
                context.sequence,
                self.liveness_config.record_adaptation_timeout_sec,
                lambda: self.window_adapter(
                    transactional_window,
                    topology,
                    node_records,
                    edge_records,
                ),
                window=transactional_window,
                attempt=attempt,
                input_count=len(node_records) + len(edge_records),
            )
            if hasattr(self.coordinator, "active_engine"):
                working, result = self.engine_executor.run(
                    active_engine=self.coordinator.active_engine,
                    engine_input=engine_input,
                    sequence=context.sequence,
                    timeout_sec=self.liveness_config.engine_process_timeout_sec,
                    operation=lambda working_engine, value: (
                        self._execute_stage_operation(
                            LiveStage.ENGINE_PROCESS,
                            lambda: working_engine.process_window(value),
                        )
                    ),
                )
                context.working_engine = working
                context.engine_result = result
                if self.health is not None:
                    self.health.update_progress(self.progress_tracker.snapshot())
            else:
                result = self._run_stage(
                    LiveStage.ENGINE_PROCESS,
                    context.sequence,
                    self.liveness_config.engine_process_timeout_sec,
                    lambda: self.coordinator.run_engine(context, engine_input),
                    window=transactional_window,
                    attempt=attempt,
                )
            def prepare_generation():
                payload = self.commit_payload_builder(
                    context, result, context.working_engine,
                )
                return self.coordinator.prepare_generation(
                    context, **payload,
                )

            generation = self._run_stage(
                LiveStage.PREPARE_GENERATION,
                context.sequence,
                self.liveness_config.generation_prepare_timeout_sec,
                prepare_generation,
                window=transactional_window,
                attempt=attempt,
            )
            if hasattr(self.coordinator, "commit_run_state"):
                self._run_stage(
                    LiveStage.COMMIT_RUN_STATE,
                    context.sequence,
                    self.liveness_config.run_state_commit_timeout_sec,
                    lambda: self.coordinator.commit_run_state(
                        context, generation,
                    ),
                    window=transactional_window,
                    attempt=attempt,
                )
                self._record_committed_result(result)
                try:
                    self._run_stage(
                        LiveStage.PROJECT_OUTPUT,
                        context.sequence,
                        self.liveness_config.output_projection_timeout_sec,
                        lambda: self.coordinator.project_output(
                            context, generation,
                        ),
                        window=transactional_window,
                        attempt=attempt,
                    )
                except LiveStageTimeoutError as error:
                    self.coordinator.state = (
                        LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED
                    )
                    if self.health is not None:
                        self.health.increment("live_stage_timeout_total")
                        self.health.update(progress_stalled=True)
                    raise CommittedOutputStalledError(
                        "RunState committed but output projection timed out",
                    ) from error
                try:
                    self._run_stage(
                        LiveStage.RETENTION,
                        context.sequence,
                        self.liveness_config.retention_timeout_sec,
                        lambda: self.coordinator.apply_retention(generation),
                        window=transactional_window,
                        attempt=attempt,
                        executor=self.retention_executor,
                    )
                except (LiveStageTimeoutError, RuntimeError) as error:
                    self.coordinator.retention_issues.append({
                        "reason_code": "retention_timeout",
                        "error_type": type(error).__name__,
                    })
            else:
                try:
                    self._run_stage(
                        LiveStage.COMMIT_RUN_STATE,
                        context.sequence,
                        self.liveness_config.run_state_commit_timeout_sec,
                        lambda: self.coordinator.commit(context, generation),
                        window=transactional_window,
                        attempt=attempt,
                    )
                except CommittedOutputDegradedError:
                    self._record_committed_result(result)
                    raise
                self._record_committed_result(result)
            self._enter(LiveStage.WINDOW_COMPLETE, sequence=context.sequence)
            self.active_context = None
            return result
        except CommittedOutputDegradedError:
            self.active_context = None
            if self.health is not None:
                self.health.update(output_writable=False)
            raise
        except (LiveStageTimeoutError, EngineStageTimeout) as error:
            # A surviving worker forces fail-stop; its working state is never adopted.
            if self.health is not None:
                self.health.increment("live_stage_timeout_total")
                self.health.update(progress_stalled=True)
            self.progress_tracker.record_error(error, "window_stage_timeout")
            raise
        except Exception as error:
            self.progress_tracker.record_error(error, "window_stage_failed")
            if (not getattr(error, "retryable", False)
                    and hasattr(self.progress_tracker, "abort")):
                self.progress_tracker.abort(reason_code="window_stage_failed")
            if self.health is not None:
                self.health.update_progress(self.progress_tracker.snapshot())
            raise

    def discard_uncommitted(self):
        if self.active_worker_count:
            raise LiveRunnerError("cannot discard while a live worker is active")
        if self.active_context is not None:
            if getattr(self.active_context, "attempt_state", None) != "committed":
                self.active_context.attempt_state = "aborted"
                self.active_context.abort_reason = "transient_collection_retry"
            self.active_context.working_engine = None
            self.active_context.engine_result = None
        self.active_context = None

    def _record_committed_result(self, result):
        if self.health is None:
            return
        self.health.record_processed_window()
        self.health.increment("checkpoint_saves_total")
        self.health.increment("alerts_total", len(getattr(result, "alerts", ())))
        self.health.increment(
            "reports_total", len(getattr(result, "reports", ())),
        )
        self.health.increment(
            "failures_total", len(getattr(result, "failures", ())),
        )

    def run_forever(self, scheduler, *, now_ns, stop, max_windows=None):
        processed = 0
        while not stop.is_set() and (
            max_windows is None or processed < max_windows
        ):
            windows = scheduler.eligible_windows(now_ns())
            if not windows:
                if stop.wait(0.05):
                    break
                continue
            for window in windows:
                self.process_window(window)
                scheduler.advance(window)
                processed += 1
                if max_windows is not None and processed >= max_windows:
                    break
        return processed


def process_window_with_retry(
    runner,
    window,
    *,
    liveness_config,
    health,
    transient_error_types,
    sleep=None,
):
    """Retry a complete, clean window attempt for transient collection errors."""
    import time

    if not isinstance(liveness_config, LiveLivenessConfig):
        raise TypeError("liveness_config must be LiveLivenessConfig")
    if not transient_error_types:
        raise ValueError("transient error types are required")

    def on_retry(attempt, outcome, delay):
        if health is not None:
            health.increment("live_collection_retry_total")
            health.update(collection_retrying=True, prometheus_healthy=False)
        if hasattr(runner, "progress_tracker"):
            runner.progress_tracker.retry(
                attempt=attempt,
                reason_code=outcome.value,
                backoff_sec=delay,
            )

    retrier = WindowCollectionRetrier(
        max_attempts=liveness_config.transient_retry_max_attempts,
        initial_backoff_sec=(
            liveness_config.transient_retry_initial_backoff_sec
        ),
        max_backoff_sec=liveness_config.transient_retry_max_backoff_sec,
        sleep=sleep or time.sleep,
        on_retry=on_retry,
    )

    def collect(_context, attempt):
        try:
            result = runner.process_window(window, attempt=attempt)
        except transient_error_types as error:
            if not getattr(error, "retryable", True):
                outcome = CollectionOutcome.PERMANENT_ERROR
            elif getattr(error, "reason_code", None) == "no_samples":
                outcome = CollectionOutcome.TRANSIENT_EMPTY
            else:
                outcome = CollectionOutcome.TRANSIENT_ERROR
            tracker = getattr(runner, "progress_tracker", None)
            if tracker is not None:
                reason_code = getattr(error, "reason_code", outcome.value)
                if hasattr(tracker, "classify_attempt"):
                    tracker.classify_attempt(
                        outcome.name, reason_code=reason_code,
                    )
                if hasattr(tracker, "abort"):
                    tracker.abort(
                        reason_code=reason_code, classification=outcome.name,
                    )
            if (health is not None and tracker is not None
                    and hasattr(tracker, "snapshot")):
                health.update_progress(tracker.snapshot())
            return outcome, error
        return CollectionOutcome.SUCCESS, result

    def cleanup_attempt(_context):
        # A timed-out daemon worker cannot be cancelled safely. Its state is
        # isolated, so fail-stop without touching the active transaction.
        if getattr(runner, "active_worker_count", 0):
            return
        runner.discard_uncommitted()

    result = retrier.run(
        sequence=window.sequence,
        new_context=lambda attempt: {"attempt": attempt},
        collect=collect,
        cleanup=cleanup_attempt,
    )
    if health is not None:
        health.update(collection_retrying=False, prometheus_healthy=True)
    return result

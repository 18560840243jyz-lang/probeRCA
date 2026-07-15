"""CLI for Kubernetes discovery and Prometheus-backed live windows."""
from __future__ import annotations

import argparse
import json
import hashlib
import os
import signal
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path

from proberca.config import load_config_yaml
from proberca.k8s.client import KubernetesDiscoveryClient, KubernetesDiscoveryError
from proberca.k8s.supervisor import WatchSupervisorError
from proberca.k8s.topology_builder import LiveTopologyBuilder
from proberca.live.commit_authority import KubernetesLeaseCommitAuthority
from proberca.live.collection import (
    CollectionExhaustedError, ControlledTransientCollectionEmpty,
)
from proberca.live.coordinator import (
    CommittedOutputDegradedError,
    LiveCommitCoordinator,
    LiveCoordinatorState,
)
from proberca.live.diagnostics import (
    dump_all_threads, install_thread_dump_handler,
)
from proberca.live.audit import BoundedAttemptAuditWriter
from proberca.live.progress import LiveStage, StageProgressTracker
from proberca.live.executor import LiveStageTimeoutError
from proberca.live.engine_worker import EngineStageTimeout
from proberca.live.engine_state import (
    build_output_ledger,
    output_bundle_from_ledger,
    restore_live_engine_state,
    write_live_engine_state,
)
from proberca.live.generation import (
    GENERATION_SCHEMA_VERSION,
    ImmutableGenerationStore,
)
from proberca.live.leader import KubernetesLeaseAPI
from proberca.live.output_projector import OutputProjector
from proberca.live.run_state import LeaseRunStateConflict, LeaseRunStateRecord
from proberca.live.runner import (
    CommittedOutputStalledError, ProbeRCALiveRunner,
    process_window_with_retry,
)
from proberca.live.watchdog import LiveWindowWatchdog
from proberca.live.health import LiveHealthState, probe_writable_directory, serve_health
from proberca.live.identity import verify_runtime_identity
from proberca.live.scheduler import LiveWindowScheduler
from proberca.metrics import (
    PrometheusClient, PrometheusResponseError, call_edges_from_samples,
    records_from_samples,
)
from proberca.orchestration.engine import ProbeRCAEngine
from proberca.orchestration.state import EngineWindowInput


def _parser():
    parser = argparse.ArgumentParser(prog="proberca-live")
    parser.add_argument("--config", required=True)
    parser.add_argument("--in-cluster", action="store_true")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--context")
    parser.add_argument("--namespace", action="append")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-windows", type=int)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run-discovery", action="store_true")
    parser.add_argument("--dry-run-metrics", action="store_true")
    parser.add_argument("--status-bind")
    parser.add_argument("--log-level", default="INFO")
    return parser


def _effective_config(args):
    config = load_config_yaml(args.config)
    if config.kubernetes is None:
        raise ValueError("configuration has no kubernetes section")
    kubernetes = config.kubernetes
    if args.in_cluster:
        kubernetes = replace(kubernetes, in_cluster=True)
    if args.kubeconfig:
        kubernetes = replace(kubernetes, kubeconfig_path=args.kubeconfig)
    if args.context:
        kubernetes = replace(kubernetes, context=args.context)
    if args.namespace:
        kubernetes = replace(kubernetes, namespaces=tuple(args.namespace))
    kubernetes.validate()
    return replace(config, kubernetes=kubernetes)


def _discovery(config, windows):
    now = time.time_ns()
    supervisor = KubernetesDiscoveryClient(config.kubernetes).create_supervisor()
    supervisor.start()
    if not supervisor.wait_until_synchronized(config.kubernetes.resync_timeout_sec):
        supervisor.stop()
        supervisor.join(config.kubernetes.resync_timeout_sec)
        raise KubernetesDiscoveryError("Kubernetes initial synchronization timed out")
    revision = supervisor.freeze_revision(now)
    snapshots = []
    window_ns = config.window_sec * 1_000_000_000
    end = (now // window_ns) * window_ns
    for index in range(windows):
        finish = end - (windows - index - 1) * window_ns
        snapshots.append(LiveTopologyBuilder(config.kubernetes.cluster_id).build(
            finish, finish + window_ns, revision, ()))
    supervisor.stop()
    supervisor.join(config.kubernetes.resync_timeout_sec)
    return supervisor.inventory, revision, snapshots


def _metrics_for_record_type(
    config, revision, start_ns, end_ns, record_type, health=None,
):
    if record_type not in {"node_metric", "edge_metric"}:
        raise ValueError("record_type must be node_metric or edge_metric")
    if config.prometheus is None or not config.prometheus.enabled:
        raise PrometheusResponseError("Prometheus is not enabled")
    settings = config.prometheus
    client = PrometheusClient(
        settings.base_url, token_file=settings.token_file, ca_file=settings.ca_file,
        client_cert_file=settings.client_cert_file,
        client_key_file=settings.client_key_file, timeout_sec=settings.timeout_sec,
        max_retries=settings.max_retries,
        retry_initial_sec=settings.retry_initial_sec,
        retry_max_sec=settings.retry_max_sec,
        reject_partial_response=settings.reject_partial_response,
    )
    records, queries = [], 0
    for spec in settings.query_specs:
        if not spec.enabled or spec.record_type != record_type:
            continue
        if health is not None:
            health.increment("metric_queries_total")
        try:
            samples, _ = client.query_window(spec, start_ns, end_ns)
        except Exception:
            if health is not None:
                health.increment("metric_query_failures_total")
                health.update(prometheus_healthy=False)
            raise
        records.extend(records_from_samples(
            spec, samples, revision, config.window_sec,
        ))
        queries += 1
    if health is not None:
        health.update(prometheus_healthy=True)
    return records, queries


def _metrics(config, revision, start_ns, end_ns, health=None):
    node, node_queries = _metrics_for_record_type(
        config, revision, start_ns, end_ns, "node_metric", health,
    )
    edge, edge_queries = _metrics_for_record_type(
        config, revision, start_ns, end_ns, "edge_metric", health,
    )
    return node, edge, node_queries + edge_queries


def _call_edges(config, revision, start_ns, end_ns, health=None):
    if config.prometheus is None or not config.prometheus.enabled:
        raise PrometheusResponseError("Prometheus is not enabled")
    settings = config.prometheus
    client = PrometheusClient(
        settings.base_url, token_file=settings.token_file, ca_file=settings.ca_file,
        client_cert_file=settings.client_cert_file,
        client_key_file=settings.client_key_file, timeout_sec=settings.timeout_sec,
        max_retries=settings.max_retries, retry_initial_sec=settings.retry_initial_sec,
        retry_max_sec=settings.retry_max_sec,
        reject_partial_response=settings.reject_partial_response)
    observations = []
    for spec in (*settings.query_specs, *settings.call_edge_query_specs):
        if not spec.enabled or spec.record_type != "call_edge":
            continue
        if health is not None:
            health.increment("metric_queries_total")
        try:
            samples, _ = client.query_window(spec, start_ns, end_ns)
        except Exception:
            if health is not None:
                health.increment("metric_query_failures_total")
                health.update(prometheus_healthy=False)
            raise
        observations.extend(call_edges_from_samples(
            spec, samples, revision, start_ns, end_ns))
    if health is not None:
        health.update(prometheus_healthy=True)
    return tuple(sorted(observations, key=lambda item: item.observation_id))


def _source_record_id(record):
    payload = record.to_dict()
    digest = hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()
    return f"{payload.get('record_type', 'record')}:{digest}"


def _fingerprint(config):
    payload = config.to_dict()
    liveness = dict(payload.get("live_liveness") or {})
    payload["live_liveness"] = {
        key: value for key, value in liveness.items()
        if not key.startswith("controlled_")
    }
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()


def _durable_engine_config(config):
    """Remove non-durable controlled hooks from the Engine identity."""
    liveness = replace(
        config.live_liveness,
        controlled_stage_delay_enabled=False,
        controlled_stage_delay_stage="",
        controlled_stage_delay_sec=0.0,
        controlled_collection_fault_enabled=False,
        controlled_transient_empty_attempts=0,
    )
    return replace(config, live_liveness=liveness)


def _new_durable_engine(config):
    return ProbeRCAEngine.from_config(_durable_engine_config(config))


def _run_live(config, args):
    install_thread_dump_handler()
    if config.live is None or config.prometheus is None or config.retention is None:
        print(
            "full live mode requires live, prometheus, and retention configuration",
            file=sys.stderr,
        )
        return 2
    if not config.leader_election or not config.leader_election.enabled:
        print("live mode requires Kubernetes Lease RunState", file=sys.stderr)
        return 5
    output_dir = os.environ.get("PROBERCA_OUTPUT_DIR")
    checkpoint_dir = os.environ.get("PROBERCA_CHECKPOINT_DIR")
    identity = os.environ.get("POD_UID") or os.environ.get("PROBERCA_INSTANCE_ID")
    if not output_dir or not checkpoint_dir or not identity:
        print(
            "live output/checkpoint directories and instance identity are required",
            file=sys.stderr,
        )
        return 5
    config_hash = _fingerprint(config)
    source_fingerprint = os.environ.get("PROBERCA_SOURCE_FINGERPRINT", "unknown")
    expected_fingerprint = os.environ.get("PROBERCA_EXPECTED_SOURCE_FINGERPRINT")
    if expected_fingerprint:
        try:
            verify_runtime_identity(expected_fingerprint, source_fingerprint)
        except Exception as error:
            print(str(error), file=sys.stderr)
            return 5
    health = LiveHealthState(
        code_revision=os.environ.get("PROBERCA_CODE_REVISION", "unknown"),
        source_fingerprint=source_fingerprint,
        schema_version=os.environ.get("PROBERCA_SCHEMA_VERSION", "1.0"),
        image_digest=os.environ.get("PROBERCA_IMAGE_DIGEST"),
    )
    def report_audit_failure(reason_code, error_type):
        health.update(audit_write_failed=True)
        health.increment("live_attempt_audit_write_failures_total")
        print(json.dumps(
            {
                "event_type": "audit_failure",
                "reason_code": str(reason_code),
                "error_type": str(error_type),
            },
            sort_keys=True,
            separators=(",", ":"),
        ), file=sys.stderr, flush=True)

    attempt_audit = BoundedAttemptAuditWriter(
        sys.stdout,
        Path(checkpoint_dir) / "live_attempt_audit.jsonl",
        max_bytes=config.live_liveness.attempt_audit_max_bytes,
        backup_count=config.live_liveness.attempt_audit_backup_count,
        on_failure=report_audit_failure,
    )
    progress_tracker = StageProgressTracker(
        maximum_history=(
            config.live_liveness.maximum_stage_event_history
        ),
        event_sink=attempt_audit,
    )
    health.update(leader=False, engine_available=False)
    health_server = serve_health(
        health,
        args.status_bind or config.live.health_bind,
    )
    health_thread = threading.Thread(
        target=health_server.serve_forever,
        name="proberca-health",
        daemon=True,
    )
    health_thread.start()
    stop_event = threading.Event()
    supervisor = KubernetesDiscoveryClient(config.kubernetes).create_supervisor()
    initial = LeaseRunStateRecord.initial(
        run_id=hashlib.sha256(json.dumps(
            {
                "cluster": config.kubernetes.cluster_id,
                "namespaces": sorted(config.kubernetes.namespaces),
                "config": config_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()).hexdigest(),
        cluster_id=config.kubernetes.cluster_id,
        namespace_scope=config.kubernetes.namespaces,
        config_fingerprint=config_hash,
        code_schema_version=GENERATION_SCHEMA_VERSION,
    )
    try:
        authority = KubernetesLeaseCommitAuthority(
            KubernetesLeaseAPI.from_kubernetes_config(config.kubernetes),
            namespace=config.leader_election.lease_namespace,
            name=config.leader_election.lease_name,
            initial_record=initial,
            lease_duration_sec=config.leader_election.lease_duration_sec,
            clock=time.time,
            annotation_max_bytes=(
                config.leader_election.run_state_annotation_max_bytes
            ),
            lock_timeout_sec=(
                config.live_liveness.run_state_commit_timeout_sec),
        )
        generation_store = ImmutableGenerationStore(
            Path(checkpoint_dir) / "live-generations-v5",
        )
        projector = OutputProjector(output_dir, generation_store)
        coordinator = LiveCommitCoordinator(
            authority,
            generation_store,
            hashlib.sha256(identity.encode()).hexdigest(),
            output_projector=projector,
            retention_config=config.retention,
            clock=time.time,
            progress_tracker=progress_tracker,
        )
        initial_engine = _new_durable_engine(config)
    except Exception as error:
        print(str(error), file=sys.stderr)
        health_server.shutdown()
        health_server.server_close()
        health_thread.join(config.live.graceful_shutdown_timeout_sec)
        return 5

    def watch_state_changed(_kind, _state):
        snapshot = supervisor.health_snapshot()
        health.update(
            watchers_synchronized=snapshot["synchronized"],
            watcher_relisting=any(
                value == "relisting"
                for value in snapshot["states"].values()
            ),
            watcher_fatal=snapshot["fatal"],
        )

    supervisor.add_state_listener(watch_state_changed)

    def call_edge_collector(window, revision):
        return _call_edges(
            config,
            revision,
            window.start_ns,
            window.end_ns,
            health=health,
        )

    def topology_builder(window, revision, calls):
        window_ns = config.window_sec * 1_000_000_000
        return LiveTopologyBuilder(config.kubernetes.cluster_id).build(
            window.end_ns,
            window.end_ns + window_ns,
            revision,
            calls,
        )

    def node_metric_collector(window, revision):
        records, _ = _metrics_for_record_type(
            config,
            revision,
            window.start_ns,
            window.end_ns,
            "node_metric",
            health=health,
        )
        return records

    def edge_metric_collector(window, revision):
        records, _ = _metrics_for_record_type(
            config,
            revision,
            window.start_ns,
            window.end_ns,
            "edge_metric",
            health=health,
        )
        return records

    def adapter(window, topology, nodes, edges):
        source_ids = [
            _source_record_id(item) for item in [*nodes, *edges]
        ]
        return EngineWindowInput(
            window.end_ns,
            window.start_ns,
            window.end_ns,
            list(nodes),
            list(edges),
            [topology],
            [],
            source_ids,
            window.sequence,
            [],
        )

    def commit_payload(context, _result, working_engine):
        ledger = build_output_ledger(
            working_engine,
            sequence=context.sequence,
            dataset_fingerprint=config_hash,
        )
        return {
            "engine_state": lambda directory: write_live_engine_state(
                working_engine,
                directory,
            ),
            "output_ledger": ledger.to_dict(),
            "output_bundle": output_bundle_from_ledger(ledger),
            "config_fingerprint": config_hash,
            "code_schema_version": GENERATION_SCHEMA_VERSION,
        }

    runner = ProbeRCALiveRunner(
        coordinator=coordinator,
        watch_supervisor=supervisor,
        topology_builder=topology_builder,
        call_edge_collector=call_edge_collector,
        node_metric_collector=node_metric_collector,
        edge_metric_collector=edge_metric_collector,
        window_adapter=adapter,
        commit_payload_builder=commit_payload,
        health=health,
        progress_tracker=progress_tracker,
        liveness_config=config.live_liveness,
    )
    scheduler = None
    processed = 0
    target_windows = args.max_windows or (1 if args.once else None)

    def watchdog_state():
        return {
            "backlog_count": int(health.runtime.get("eligible_window_count", 0)),
            "last_commit_monotonic": watchdog.last_commit_monotonic,
            "leader_active": coordinator.state is LiveCoordinatorState.LEADER_ACTIVE,
            "active_transaction": runner.active_context is not None,
            "working_engine_count": runner.active_worker_count,
        }

    def stop_lease_for_stall():
        coordinator.state = LiveCoordinatorState.LOST
        coordinator.token = None
        health.update(leader=False, engine_available=False)
        stop_event.set()

    watchdog = LiveWindowWatchdog(
        progress_tracker,
        health,
        progress_timeout_sec=config.live_liveness.progress_timeout_sec,
        stage_timeouts=config.live_liveness.stage_timeouts(),
        backlog_fatal_threshold=config.live_liveness.backlog_fatal_threshold,
        dump_grace_sec=config.live_liveness.watchdog_dump_grace_sec,
        exit_grace_sec=config.live_liveness.watchdog_exit_grace_sec,
        poll_interval_sec=config.live_liveness.watchdog_poll_interval_sec,
        abort_transaction=lambda: (
            runner.discard_uncommitted()
            if runner.active_worker_count == 0 else None
        ),
        stop_lease_renew=stop_lease_for_stall,
        state_provider=watchdog_state,
    )

    def restore_engine(source):
        engine = _new_durable_engine(config)
        return restore_live_engine_state(engine, source)

    def sigterm_handler(*_):
        coordinator.drain()
        health.update(leader=False)
        stop_event.set()

    try:
        runner.start(sync_timeout_sec=config.kubernetes.resync_timeout_sec)
        watchdog.start()
        if hasattr(signal, "SIGUSR2"):
            signal.signal(
                signal.SIGUSR2,
                lambda *_: supervisor.request_relist(
                    "Pod",
                    "operator_requested",
                ),
            )
        signal.signal(signal.SIGTERM, sigterm_handler)
        while not stop_event.is_set() and (
            target_windows is None or processed < target_windows
        ):
            if coordinator.state in {
                LiveCoordinatorState.STANDBY,
                LiveCoordinatorState.LOST,
            }:
                try:
                    coordinator.acquire_and_recover(active_engine=initial_engine)
                    coordinator.recover_current(engine_loader=restore_engine)
                    snapshot = authority.read()
                    if args.resume and snapshot.record.committed_sequence == 0:
                        raise RuntimeError(
                            "--resume requires committed Lease RunState",
                        )
                    scheduler = LiveWindowScheduler.from_run_state(
                        config.live, snapshot.record,
                    )
                    health.update_runtime(
                        coordinator_state=coordinator.state.value,
                        committed_sequence=snapshot.record.committed_sequence,
                        next_sequence=scheduler.next_sequence,
                        next_start_ns=scheduler.next_start_ns,
                        eligible_window_count=0,
                    )
                    health.update(leader=True, engine_available=True)
                    health.increment("leader_transitions_total")
                    watchdog.note_commit(
                        sequence=snapshot.record.committed_sequence,
                    )
                except LeaseRunStateConflict:
                    health.update(leader=False, engine_available=False)
                    time.sleep(config.leader_election.retry_period_sec)
                    continue
            elif coordinator.state is LiveCoordinatorState.COMMITTED_OUTPUT_DEGRADED:
                try:
                    coordinator.recover_current(engine_loader=restore_engine)
                    health.update(output_writable=True)
                except Exception as error:
                    health.update(output_writable=False)
                    print(str(error), file=sys.stderr)
                    time.sleep(config.leader_election.retry_period_sec)
                    continue
            else:
                try:
                    coordinator.renew()
                except LeaseRunStateConflict:
                    health.increment("leader_transitions_total")
                    health.update(leader=False, engine_available=False)
                    scheduler = None
                    continue
            checkpoint_ok = probe_writable_directory(
                Path(checkpoint_dir) / "live-generations-v5",
            )
            output_ok = probe_writable_directory(output_dir)
            health.update(
                checkpoint_writable=checkpoint_ok,
                output_writable=output_ok,
            )
            if not checkpoint_ok or not output_ok:
                if not checkpoint_ok:
                    health.increment("checkpoint_failures_total")
                if not output_ok:
                    health.increment("output_conflicts_total")
                time.sleep(0.1)
                continue
            try:
                current_now_ns = time.time_ns()
                health.update_runtime(
                    coordinator_state=coordinator.state.value,
                    last_now_ns=current_now_ns,
                    next_sequence=scheduler.next_sequence,
                    next_start_ns=scheduler.next_start_ns,
                )
                windows = scheduler.eligible_windows(current_now_ns)
                health.update_runtime(eligible_window_count=len(windows))
                progress = progress_tracker.snapshot()
                stage = LiveStage(progress["stage"])
                stage_timeout = config.live_liveness.stage_timeouts().get(
                    stage, config.live_liveness.progress_timeout_sec,
                )
                health.update_progress_health(
                    backlog_count=len(windows),
                    last_commit_age_sec=max(
                        0.0, time.monotonic() - watchdog.last_commit_monotonic,
                    ),
                    current_stage=stage,
                    current_stage_age_sec=progress["stage_age_sec"],
                    current_stage_timeout_sec=stage_timeout,
                    progress_timeout_sec=config.live_liveness.progress_timeout_sec,
                    backlog_not_ready_threshold=(
                        config.live_liveness.backlog_not_ready_threshold
                    ),
                    working_engine_count=runner.active_worker_count,
                    stalled=watchdog.stalled,
                    committed_sequence=health.runtime.get("committed_sequence"),
                    next_sequence=scheduler.next_sequence,
                    attempt=progress.get("attempt", 0),
                    active_transaction_state=(
                        "active" if runner.active_context is not None else "idle"
                    ),
                )
            except Exception as error:
                print(str(error), file=sys.stderr)
                return 7
            if not windows:
                time.sleep(0.05)
                continue
            for window in windows:
                try:
                    result = process_window_with_retry(
                        runner,
                        window,
                        liveness_config=config.live_liveness,
                        health=health,
                        transient_error_types=(
                            PrometheusResponseError,
                            ControlledTransientCollectionEmpty,
                        ),
                    )
                except CommittedOutputStalledError as error:
                    scheduler.advance(window)
                    processed += 1
                    health.update_runtime(
                        committed_sequence=window.sequence,
                        next_sequence=scheduler.next_sequence,
                        next_start_ns=scheduler.next_start_ns,
                    )
                    watchdog.note_commit(sequence=window.sequence)
                    health.update(
                        output_writable=False,
                        progress_stalled=True,
                        fatal_error="output_projection_stalled",
                    )
                    dump_all_threads()
                    print(str(error), file=sys.stderr)
                    return 8
                except CommittedOutputDegradedError as error:
                    scheduler.advance(window)
                    processed += 1
                    health.update_runtime(
                        committed_sequence=window.sequence,
                        next_sequence=scheduler.next_sequence,
                        next_start_ns=scheduler.next_start_ns,
                    )
                    watchdog.note_commit(sequence=window.sequence)
                    health.update(output_writable=False)
                    print(str(error), file=sys.stderr)
                    break
                except CollectionExhaustedError as error:
                    health.increment("live_collection_exhausted_total")
                    health.update(
                        prometheus_healthy=False,
                        collection_retrying=True,
                        progress_stalled=True,
                    )
                    print(str(error), file=sys.stderr)
                    return 8
                except PrometheusResponseError as error:
                    print(str(error), file=sys.stderr)
                    health.update(prometheus_healthy=False)
                    time.sleep(config.prometheus.retry_initial_sec)
                    break
                except WatchSupervisorError as error:
                    snapshot = supervisor.health_snapshot()
                    health.update(
                        watchers_synchronized=snapshot["synchronized"],
                        watcher_relisting=any(
                            value == "relisting"
                            for value in snapshot["states"].values()
                        ),
                        watcher_fatal=snapshot["fatal"],
                    )
                    if snapshot["fatal"]:
                        health.increment("engine_failures_total")
                        print(str(error), file=sys.stderr)
                        return 6
                    time.sleep(0.05)
                    break
                except LeaseRunStateConflict:
                    health.update(leader=False, engine_available=False)
                    scheduler = None
                    break
                except (LiveStageTimeoutError, EngineStageTimeout) as error:
                    health.update(
                        progress_stalled=True,
                        engine_available=False,
                        fatal_error="live_stage_stalled",
                    )
                    dump_all_threads()
                    print(str(error), file=sys.stderr)
                    return 8
                except Exception as error:
                    health.increment("engine_failures_total")
                    print(str(error), file=sys.stderr)
                    return 6
                scheduler.advance(window)
                processed += 1
                health.update_runtime(
                    committed_sequence=window.sequence,
                    next_sequence=scheduler.next_sequence,
                    next_start_ns=scheduler.next_start_ns,
                )
                watchdog.note_commit(sequence=window.sequence)
                if result.failures:
                    return 6
                if target_windows is not None and processed >= target_windows:
                    return 0
        return 0
    finally:
        watchdog.stop()
        watchdog.join(config.live.graceful_shutdown_timeout_sec)
        coordinator.drain()
        health.update(leader=False)
        try:
            coordinator.release()
        except Exception as error:
            health.update(fatal_error="lease_release_failed")
            print(
                f"lease release failed: {type(error).__name__}",
                file=sys.stderr,
            )
        runner.stop(join_timeout_sec=config.live.graceful_shutdown_timeout_sec)
        health_server.shutdown()
        health_server.server_close()
        health_thread.join(config.live.graceful_shutdown_timeout_sec)


def main(argv=None):
    args = _parser().parse_args(argv)
    if args.max_windows is not None and args.max_windows <= 0:
        print("--max-windows must be positive", file=sys.stderr)
        return 2
    windows = args.max_windows or (1 if args.once else 1)
    try:
        config = _effective_config(args)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 2
    if not args.dry_run_discovery and not args.dry_run_metrics:
        return _run_live(config, args)
    try:
        inventory, revision, snapshots = _discovery(config, windows)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 3
    if args.dry_run_discovery:
        print(json.dumps({
            "mode": "dry_run_discovery", "windows": len(snapshots),
            "synchronized": revision.synchronized, "stale": revision.stale,
            "object_counts": revision.object_counts,
            "resource_kinds": sorted(item.resource_kind for item in revision.resource_versions),
        }, sort_keys=True))
        return 0 if revision.ready else 7
    try:
        node, edge, queries = _metrics(
            config, revision, snapshots[-1].valid_from_ns, snapshots[-1].valid_to_ns)
        calls = _call_edges(
            config, revision, snapshots[-1].valid_from_ns, snapshots[-1].valid_to_ns)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 4
    if args.dry_run_metrics:
        print(json.dumps({"mode": "dry_run_metrics", "queries": queries + 1,
                          "node_records": len(node), "edge_records": len(edge),
                          "call_edges": len(calls)},
                         sort_keys=True))
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Crash-safe generation checkpoints selected only through an atomic CURRENT pointer."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import asdict
from pathlib import Path

from proberca.aggregation import WindowAggregator
from proberca.alerting import AlertStateMachine
from proberca.baseline import RobustBaselineStore
from proberca.data.schema import (
    AlertEvent, CandidateSubgraph, EdgeAnomalyRecord, EvidenceObservationRecord,
    NodeAnomalyRecord, RCAReport,
)
from proberca.propagation.metric_history import (
    MetricHealthyHistoryStore, MetricRuntimeHistoryStore,
)
from proberca.propagation.metric_model import (
    MetricPropagationContribution, MetricPropagationPrediction,
)
from proberca.propagation.metric_ridge import MetricPropagationLearner
from proberca.propagation.service_rls import ServicePropagationLearner
from proberca.topology import TopologyStore
from proberca.live.sequence import validate_sequence_continuity

from .state import OutputLedger, PendingIncident, ReplayIncidentFailure


CHECKPOINT_FORMAT_VERSION = "4"
CHECKPOINT_SCHEMA_VERSION = "1.0"
CODE_FINGERPRINT = "p11.3-fenced-checkpoint-schema-4"


def _safe_temp_component(value, field):
    text = str(value)
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    if not text or Path(text).name != text or any(
            character not in allowed for character in text):
        raise ReplayCheckpointError(f"invalid checkpoint {field}")
    return text


def temporary_generation_path(generations, generation_id,
                              instance_fingerprint, transaction_id):
    generation = _safe_temp_component(generation_id, "generation ID")
    instance = _safe_temp_component(instance_fingerprint, "instance fingerprint")
    transaction = _safe_temp_component(transaction_id, "transaction ID")
    return Path(generations) / f".{generation}.{instance}.{transaction}.tmp"


def temporary_current_path(root, instance_fingerprint, transaction_id):
    instance = _safe_temp_component(instance_fingerprint, "instance fingerprint")
    transaction = _safe_temp_component(transaction_id, "transaction ID")
    return Path(root) / f".CURRENT.{instance}.{transaction}.tmp"


def cleanup_owned_temporaries(generations, instance_fingerprint,
                              active_transaction_ids=()):
    generations = Path(generations)
    instance = _safe_temp_component(instance_fingerprint, "instance fingerprint")
    active = {str(value) for value in active_transaction_ids}
    removed = []
    marker = f".{instance}."
    for path in sorted(generations.iterdir()):
        if not path.is_dir() or not path.name.startswith(".") or \
                not path.name.endswith(".tmp") or marker not in path.name:
            continue
        transaction = path.name[:-4].rsplit(".", 1)[-1]
        if transaction in active:
            continue
        shutil.rmtree(path)
        removed.append(path.name)
    if removed:
        _fsync_directory(generations)
    return removed


def apply_checkpoint_retention(root, config, *, now_ns,
                               instance_fingerprint=None,
                               active_transaction_ids=()):
    """Delete only old, unselected generations after CURRENT is committed."""
    root = Path(root)
    generations = root / "generations"
    issues = []
    try:
        current = json.loads((root / "CURRENT").read_text(encoding="utf-8"))
        current_id = current["generation_id"]
    except Exception as error:
        raise ReplayCheckpointError(f"retention cannot read CURRENT: {error}") from error
    entries = []
    for path in sorted(generations.iterdir()):
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            if instance_fingerprint is not None:
                continue
            try:
                shutil.rmtree(path)
            except OSError as error:
                issues.append({"reason_code": "checkpoint_orphan_cleanup_failed",
                               "object_id": path.name, "detail": str(error)})
            continue
        if not path.is_dir():
            continue
        try:
            metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
            created = int(metadata["created_at_ns"])
        except Exception:
            continue
        entries.append((created, path.name, path))
    if instance_fingerprint is not None:
        try:
            cleanup_owned_temporaries(
                generations, instance_fingerprint, active_transaction_ids)
        except OSError as error:
            issues.append({"reason_code": "checkpoint_orphan_cleanup_failed",
                           "object_id": str(instance_fingerprint),
                           "detail": str(error)})
    entries.sort(reverse=True)
    keep = {current_id}
    keep.update(name for _, name, _ in entries
                if name != current_id and len(keep) < config.checkpoint_generations)
    minimum_age_ns = int(config.checkpoint_min_age_sec * 1_000_000_000)
    for created, name, path in entries:
        if name in keep or now_ns - created < minimum_age_ns:
            continue
        try:
            shutil.rmtree(path)
        except OSError as error:
            issues.append({"reason_code": "checkpoint_retention_failed",
                           "object_id": name, "detail": str(error)})
    return issues


class ReplayCheckpointError(ValueError):
    """Checkpoint is corrupt or belongs to another run."""


def _canonical(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_fsync(path: Path, payload) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_tree(root: Path) -> None:
    files = sorted(item for item in root.rglob("*") if item.is_file())
    directories = sorted(
        (item for item in root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts), reverse=True,
    )
    for path in files:
        _fsync_file(path)
    for path in directories:
        _fsync_directory(path)
    _fsync_directory(root)


def _cleanup_orphan_temporaries(generations: Path) -> None:
    for path in sorted(generations.iterdir()):
        if path.is_dir() and path.name.startswith(".") and path.name.endswith(".tmp"):
            shutil.rmtree(path)
    _fsync_directory(generations)


def _component_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _sha_file(path)
        for path in sorted(item for item in root.rglob("*") if item.is_file())
        if path.name != "metadata.json"
    }


def _metadata_fingerprint(metadata: dict) -> str:
    payload = dict(metadata)
    payload.pop("checkpoint_fingerprint", None)
    return _sha_bytes(_canonical(payload))


def _validate_generation(path: Path, expected_fingerprint: str | None = None) -> dict:
    try:
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
    except Exception as error:
        raise ReplayCheckpointError(f"checkpoint metadata is unreadable: {error}") from error
    required = {
        "format_version", "schema_version", "code_fingerprint", "manifest_hash",
        "config_fingerprint", "replay_sequence", "last_timestamp",
        "has_metric_model", "latest_candidate", "pending", "hard_alert",
        "alerts", "reports", "failures", "output_ledger",
        "previous_output_ledger", "component_sha256",
        "sequence_journal", "created_at_ns", "checkpoint_fingerprint",
    }
    if set(metadata) != required:
        raise ReplayCheckpointError("checkpoint metadata fields mismatch")
    if metadata["format_version"] != CHECKPOINT_FORMAT_VERSION or \
            metadata["schema_version"] != CHECKPOINT_SCHEMA_VERSION or \
            metadata["code_fingerprint"] != CODE_FINGERPRINT:
        raise ReplayCheckpointError("checkpoint version mismatch")
    fingerprint = _metadata_fingerprint(metadata)
    if fingerprint != metadata["checkpoint_fingerprint"] or \
            (expected_fingerprint is not None and fingerprint != expected_fingerprint):
        raise ReplayCheckpointError("checkpoint fingerprint mismatch")
    if _component_hashes(path) != metadata["component_sha256"]:
        raise ReplayCheckpointError("checkpoint component hash mismatch")
    OutputLedger.from_dict(metadata["output_ledger"])
    if metadata["previous_output_ledger"] is not None:
        OutputLedger.from_dict(metadata["previous_output_ledger"])
    continuity = validate_sequence_continuity(metadata["sequence_journal"])
    if continuity.gap_count or continuity.duplicate_count or \
            continuity.max_holders_per_sequence > 1:
        raise ReplayCheckpointError("checkpoint sequence journal is invalid")
    if metadata["sequence_journal"] and \
            int(metadata["sequence_journal"][-1]["sequence"]) != \
            int(metadata["replay_sequence"]):
        raise ReplayCheckpointError("checkpoint sequence journal is not current")
    return metadata


def _prediction(payload):
    values = dict(payload)
    values["contributions"] = [
        MetricPropagationContribution(**item) for item in values["contributions"]]
    return MetricPropagationPrediction(**values)


def _generation_id(engine, replay_sequence: int, created_at_ns: int) -> str:
    seed = {
        "sequence": replay_sequence, "timestamp": engine._last_timestamp,
        "created_at_ns": created_at_ns, "config": engine.config_fingerprint,
    }
    return f"{replay_sequence:020d}-{_sha_bytes(_canonical(seed))[:20]}"


def save_engine_checkpoint(engine, directory, *, manifest_hash, replay_sequence,
                           fence_token=None, fence_validator=None,
                           instance_fingerprint=None, transaction_id=None,
                           sequence_entry_factory=None):
    if (fence_token is None) != (fence_validator is None):
        raise ReplayCheckpointError(
            "checkpoint fence token and validator must be provided together")
    if fence_validator is not None and (not instance_fingerprint or not transaction_id):
        raise ReplayCheckpointError(
            "fenced checkpoint requires instance and transaction identities")

    def validate_fence(operation):
        if fence_validator is not None:
            fence_validator(operation, fence_token)

    root = Path(directory)
    generations = root / "generations"
    root.mkdir(parents=True, exist_ok=True)
    generations.mkdir(exist_ok=True)
    _fsync_directory(root)
    _fsync_directory(generations)
    created_at_ns = time.time_ns()
    generation_id = _generation_id(engine, replay_sequence, created_at_ns)
    final_generation = generations / generation_id
    temp_generation = (
        temporary_generation_path(
            generations, generation_id, instance_fingerprint, transaction_id)
        if fence_validator is not None else generations / f".{generation_id}.tmp")
    if final_generation.exists() or temp_generation.exists():
        raise ReplayCheckpointError("checkpoint generation ID collision")
    validate_fence("generation_prepare")
    temp_generation.mkdir(exist_ok=False)

    engine.aggregator.save_json(temp_generation / "aggregator.json")
    engine.baseline.save_json(temp_generation / "baseline.json")
    engine.alert_machine.save_json(temp_generation / "alert.json")
    engine.topology_store.save_json(temp_generation / "topology.json")
    engine.service_learner.snapshot(temp_generation / "service_model")
    engine.metric_learner.training_history.snapshot(
        temp_generation / "metric_training_history")
    engine.metric_learner.runtime_history.snapshot(
        temp_generation / "metric_runtime_history")
    candidate = engine._latest_candidate
    has_metric_model = bool(candidate is not None and engine.metric_learner.cached_model_infos())
    if has_metric_model:
        engine.metric_learner.snapshot(temp_generation / "metric_model")

    previous_ledger = getattr(engine, "_output_ledger", None)
    run_manifest_payload = (previous_ledger.run_manifest_payload
                            if isinstance(previous_ledger, OutputLedger) else None)
    ledger = OutputLedger.create(
        alerts=engine._alerts, reports=engine._reports, failures=engine._failures,
        processed_window_count=replay_sequence,
        last_processed_timestamp=engine._last_timestamp,
        pending_incident=engine.pending_incident,
        dataset_fingerprint=manifest_hash,
        config_fingerprint=engine.config_fingerprint,
        run_manifest_payload=run_manifest_payload,
    )
    sequence_journal = [
        dict(item) for item in getattr(engine, "_sequence_journal", ())]
    if sequence_entry_factory is not None:
        entry = dict(sequence_entry_factory(generation_id, ledger))
        if sequence_journal and int(entry["sequence"]) != \
                int(sequence_journal[-1]["sequence"]) + 1:
            raise ReplayCheckpointError("checkpoint sequence is not continuous")
        if int(entry["sequence"]) != int(replay_sequence):
            raise ReplayCheckpointError("checkpoint sequence entry mismatch")
        sequence_journal.append(entry)
    metadata = {
        "format_version": CHECKPOINT_FORMAT_VERSION,
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "code_fingerprint": CODE_FINGERPRINT,
        "manifest_hash": manifest_hash,
        "config_fingerprint": engine.config_fingerprint,
        "replay_sequence": replay_sequence,
        "last_timestamp": engine._last_timestamp,
        "has_metric_model": has_metric_model,
        "latest_candidate": candidate.to_dict() if candidate else None,
        "pending": asdict(engine.pending_incident) if engine.pending_incident else None,
        "hard_alert": engine._hard_alert.to_dict() if hasattr(engine, "_hard_alert") else None,
        "alerts": [item.to_dict() for item in engine._alerts],
        "reports": [item.to_dict() for item in engine._reports],
        "failures": [item.to_dict() for item in engine._failures],
        "output_ledger": ledger.to_dict(),
        "previous_output_ledger": (
            previous_ledger.to_dict()
            if isinstance(previous_ledger, OutputLedger) else None),
        "sequence_journal": sequence_journal,
        "component_sha256": _component_hashes(temp_generation),
        "created_at_ns": created_at_ns,
    }
    metadata["checkpoint_fingerprint"] = _metadata_fingerprint(metadata)
    _write_json_fsync(temp_generation / "metadata.json", metadata)
    _fsync_tree(temp_generation)
    _validate_generation(temp_generation, metadata["checkpoint_fingerprint"])

    validate_fence("generation_publish")
    os.replace(temp_generation, final_generation)
    _fsync_directory(generations)
    current_payload = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "generation_id": generation_id,
        "checkpoint_fingerprint": metadata["checkpoint_fingerprint"],
        "created_at_ns": created_at_ns,
    }
    current_temp = (
        temporary_current_path(root, instance_fingerprint, transaction_id)
        if fence_validator is not None else root / "CURRENT.tmp")
    _write_json_fsync(current_temp, current_payload)
    validate_fence("current_replace")
    os.replace(current_temp, root / "CURRENT")
    _fsync_directory(root)
    engine._output_ledger = ledger
    engine._previous_output_ledger = (
        previous_ledger if isinstance(previous_ledger, OutputLedger) else None)
    engine._sequence_journal = sequence_journal
    try:
        if fence_validator is None:
            _cleanup_orphan_temporaries(generations)
        else:
            cleanup_owned_temporaries(
                generations, instance_fingerprint,
                active_transaction_ids=(transaction_id,))
    except OSError as error:
        engine._checkpoint_issues = [{
            "reason_code": "checkpoint_cleanup_failed",
            "detail": str(error),
        }]
    else:
        engine._checkpoint_issues = []
    return {
        "generation_id": generation_id,
        "checkpoint_fingerprint": metadata["checkpoint_fingerprint"],
        "output_ledger": ledger,
    }


def _selected_generation(root: Path) -> tuple[Path, dict]:
    current_path = root / "CURRENT"
    try:
        current = json.loads(current_path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ReplayCheckpointError(f"CURRENT is missing or corrupt: {error}") from error
    if set(current) != {
        "schema_version", "generation_id", "checkpoint_fingerprint", "created_at_ns",
    } or current["schema_version"] != CHECKPOINT_SCHEMA_VERSION:
        raise ReplayCheckpointError("CURRENT fields or version are invalid")
    generation_id = current["generation_id"]
    if not isinstance(generation_id, str) or not generation_id or \
            Path(generation_id).name != generation_id:
        raise ReplayCheckpointError("CURRENT generation ID is invalid")
    generation = root / "generations" / generation_id
    if not generation.is_dir():
        raise ReplayCheckpointError("CURRENT selected generation is missing")
    metadata = _validate_generation(generation, current["checkpoint_fingerprint"])
    return generation, metadata


def restore_engine_checkpoint(engine, directory, *, manifest_hash):
    root = Path(directory)
    try:
        path, metadata = _selected_generation(root)
        if metadata["manifest_hash"] != manifest_hash:
            raise ReplayCheckpointError("checkpoint dataset mismatch")
        if metadata["config_fingerprint"] != engine.config_fingerprint:
            raise ReplayCheckpointError("checkpoint config mismatch")
        engine.aggregator = WindowAggregator.load_json(
            path / "aggregator.json", engine.aggregation_plan)
        engine.baseline = RobustBaselineStore.load_json(
            path / "baseline.json", engine.baseline_config, engine.config.window_sec)
        state_config = engine.alert_machine.config
        engine.alert_machine = AlertStateMachine.load_json(
            path / "alert.json", state_config, engine.config.window_sec,
            engine.config.composite_alert_rules)
        engine.topology_store = TopologyStore.load_json(path / "topology.json")
        engine.service_learner = ServicePropagationLearner.restore(
            path / "service_model", engine.config.propagation, engine.config.window_sec,
            engine.config.impact_derivation_rules,
            engine.config.candidate_graph.allow_cross_namespace)
        candidate = (CandidateSubgraph.from_dict(metadata["latest_candidate"])
                     if metadata["latest_candidate"] else None)
        engine._latest_candidate = candidate
        pending = metadata["pending"]
        if pending:
            values = dict(pending)
            values["candidate_subgraph"] = CandidateSubgraph.from_dict(
                values["candidate_subgraph"])
            values["hard_node_anomalies"] = [
                NodeAnomalyRecord.from_dict(item) for item in values["hard_node_anomalies"]]
            values["hard_edge_anomalies"] = [
                EdgeAnomalyRecord.from_dict(item) for item in values["hard_edge_anomalies"]]
            values["hard_metric_predictions"] = [
                _prediction(item) for item in values["hard_metric_predictions"]]
            values["normalized_evidence"] = [
                EvidenceObservationRecord.from_dict(item)
                for item in values["normalized_evidence"]]
            engine.pending_incident = PendingIncident(**values)
        if metadata["has_metric_model"]:
            if candidate is None:
                raise ReplayCheckpointError("metric model checkpoint lacks candidate")
            engine.metric_learner = MetricPropagationLearner.restore(
                path / "metric_model", engine.config.propagation,
                engine.config.window_sec, candidate)
        else:
            engine.metric_learner = MetricPropagationLearner(
                engine.config.propagation, engine.config.window_sec)
            engine.metric_learner.training_history = MetricHealthyHistoryStore.restore(
                path / "metric_training_history", engine.config.propagation,
                engine.config.window_sec)
            engine.metric_learner.runtime_history = MetricRuntimeHistoryStore.restore(
                path / "metric_runtime_history", engine.config.propagation,
                engine.config.window_sec)
        if metadata["hard_alert"]:
            engine._hard_alert = AlertEvent.from_dict(metadata["hard_alert"])
        engine._last_timestamp = metadata["last_timestamp"]
        engine._alerts = [AlertEvent.from_dict(item) for item in metadata["alerts"]]
        engine._reports = [RCAReport.from_dict(item) for item in metadata["reports"]]
        engine._failures = [ReplayIncidentFailure(**item) for item in metadata["failures"]]
        engine._output_ledger = OutputLedger.from_dict(metadata["output_ledger"])
        engine._previous_output_ledger = (
            OutputLedger.from_dict(metadata["previous_output_ledger"])
            if metadata["previous_output_ledger"] is not None else None)
        engine._sequence_journal = [dict(item) for item in metadata["sequence_journal"]]
        return metadata["replay_sequence"]
    except ReplayCheckpointError:
        raise
    except Exception as error:
        raise ReplayCheckpointError(f"checkpoint restore failed: {error}") from error

"""Component-level ProbeRCAEngine state codec for immutable live generations."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from proberca.aggregation import WindowAggregator
from proberca.alerting import AlertStateMachine
from proberca.baseline import RobustBaselineStore
from proberca.data.schema import (
    AlertEvent,
    CandidateSubgraph,
    EdgeAnomalyRecord,
    EvidenceObservationRecord,
    NodeAnomalyRecord,
    RCAReport,
)
from proberca.orchestration.state import (
    OutputLedger,
    PendingIncident,
    ReplayIncidentFailure,
)
from proberca.propagation.metric_history import (
    MetricHealthyHistoryStore,
    MetricRuntimeHistoryStore,
)
from proberca.propagation.metric_model import (
    MetricPropagationContribution,
    MetricPropagationPrediction,
)
from proberca.propagation.metric_ridge import MetricPropagationLearner
from proberca.propagation.service_rls import ServicePropagationLearner
from proberca.topology import TopologyStore


LIVE_ENGINE_STATE_SCHEMA = "live_engine_state_v1"


class LiveEngineStateError(ValueError):
    """An immutable generation contains incompatible Engine state."""


def _prediction(payload):
    values = dict(payload)
    values["contributions"] = [
        MetricPropagationContribution(**item)
        for item in values["contributions"]
    ]
    return MetricPropagationPrediction(**values)


def _metadata(engine) -> dict:
    candidate = engine._latest_candidate
    pending = engine.pending_incident
    return {
        "schema_version": LIVE_ENGINE_STATE_SCHEMA,
        "config_fingerprint": engine.config_fingerprint,
        "last_timestamp": engine._last_timestamp,
        "has_metric_model": bool(
            candidate is not None and engine.metric_learner.cached_model_infos()
        ),
        "latest_candidate": candidate.to_dict() if candidate else None,
        "pending": asdict(pending) if pending else None,
        "hard_alert": (
            engine._hard_alert.to_dict()
            if hasattr(engine, "_hard_alert")
            else None
        ),
        "alerts": [item.to_dict() for item in engine._alerts],
        "reports": [item.to_dict() for item in engine._reports],
        "failures": [item.to_dict() for item in engine._failures],
    }


def write_live_engine_state(engine, directory) -> None:
    path = Path(directory)
    if path.exists() and any(path.iterdir()):
        raise LiveEngineStateError("engine state directory must be empty")
    path.mkdir(parents=True, exist_ok=True)
    engine.aggregator.save_json(path / "aggregator.json")
    engine.baseline.save_json(path / "baseline.json")
    engine.alert_machine.save_json(path / "alert.json")
    engine.topology_store.save_json(path / "topology.json")
    engine.service_learner.snapshot(path / "service_model")
    engine.metric_learner.training_history.snapshot(
        path / "metric_training_history",
    )
    engine.metric_learner.runtime_history.snapshot(
        path / "metric_runtime_history",
    )
    metadata = _metadata(engine)
    if metadata["has_metric_model"]:
        engine.metric_learner.snapshot(path / "metric_model")
    (path / "metadata.json").write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def restore_live_engine_state(engine, directory):
    path = Path(directory)
    try:
        metadata = json.loads(
            (path / "metadata.json").read_text(encoding="utf-8"),
        )
        required = {
            "schema_version",
            "config_fingerprint",
            "last_timestamp",
            "has_metric_model",
            "latest_candidate",
            "pending",
            "hard_alert",
            "alerts",
            "reports",
            "failures",
        }
        if set(metadata) != required:
            raise LiveEngineStateError("engine state metadata fields mismatch")
        if metadata["schema_version"] != LIVE_ENGINE_STATE_SCHEMA:
            raise LiveEngineStateError("engine state schema mismatch")
        if metadata["config_fingerprint"] != engine.config_fingerprint:
            raise LiveEngineStateError("engine state config mismatch")
        engine.aggregator = WindowAggregator.load_json(
            path / "aggregator.json",
            engine.aggregation_plan,
        )
        engine.baseline = RobustBaselineStore.load_json(
            path / "baseline.json",
            engine.baseline_config,
            engine.config.window_sec,
        )
        engine.alert_machine = AlertStateMachine.load_json(
            path / "alert.json",
            engine.alert_machine.config,
            engine.config.window_sec,
            engine.config.composite_alert_rules,
        )
        engine.topology_store = TopologyStore.load_json(path / "topology.json")
        engine.service_learner = ServicePropagationLearner.restore(
            path / "service_model",
            engine.config.propagation,
            engine.config.window_sec,
            engine.config.impact_derivation_rules,
            engine.config.candidate_graph.allow_cross_namespace,
        )
        candidate = (
            CandidateSubgraph.from_dict(metadata["latest_candidate"])
            if metadata["latest_candidate"]
            else None
        )
        engine._latest_candidate = candidate
        if metadata["has_metric_model"]:
            if candidate is None:
                raise LiveEngineStateError(
                    "metric model state lacks a candidate",
                )
            engine.metric_learner = MetricPropagationLearner.restore(
                path / "metric_model",
                engine.config.propagation,
                engine.config.window_sec,
                candidate,
            )
        else:
            engine.metric_learner = MetricPropagationLearner(
                engine.config.propagation,
                engine.config.window_sec,
            )
            engine.metric_learner.training_history = (
                MetricHealthyHistoryStore.restore(
                    path / "metric_training_history",
                    engine.config.propagation,
                    engine.config.window_sec,
                )
            )
            engine.metric_learner.runtime_history = (
                MetricRuntimeHistoryStore.restore(
                    path / "metric_runtime_history",
                    engine.config.propagation,
                    engine.config.window_sec,
                )
            )
        pending = metadata["pending"]
        engine.pending_incident = None
        if pending:
            values = dict(pending)
            values["candidate_subgraph"] = CandidateSubgraph.from_dict(
                values["candidate_subgraph"],
            )
            values["hard_node_anomalies"] = [
                NodeAnomalyRecord.from_dict(item)
                for item in values["hard_node_anomalies"]
            ]
            values["hard_edge_anomalies"] = [
                EdgeAnomalyRecord.from_dict(item)
                for item in values["hard_edge_anomalies"]
            ]
            values["hard_metric_predictions"] = [
                _prediction(item)
                for item in values["hard_metric_predictions"]
            ]
            values["normalized_evidence"] = [
                EvidenceObservationRecord.from_dict(item)
                for item in values["normalized_evidence"]
            ]
            engine.pending_incident = PendingIncident(**values)
        if metadata["hard_alert"]:
            engine._hard_alert = AlertEvent.from_dict(metadata["hard_alert"])
        elif hasattr(engine, "_hard_alert"):
            delattr(engine, "_hard_alert")
        engine._last_timestamp = metadata["last_timestamp"]
        engine._alerts = [
            AlertEvent.from_dict(item) for item in metadata["alerts"]
        ]
        engine._reports = [
            RCAReport.from_dict(item) for item in metadata["reports"]
        ]
        engine._failures = [
            ReplayIncidentFailure(**item) for item in metadata["failures"]
        ]
        engine._output_ledger = None
        engine._previous_output_ledger = None
        engine._sequence_journal = []
        return engine
    except LiveEngineStateError:
        raise
    except Exception as error:
        raise LiveEngineStateError(
            f"engine state restore failed: {error}",
        ) from error


def build_output_ledger(engine, *, sequence, dataset_fingerprint) -> OutputLedger:
    previous = getattr(engine, "_output_ledger", None)
    ledger = OutputLedger.create(
        alerts=engine._alerts,
        reports=engine._reports,
        failures=engine._failures,
        processed_window_count=sequence,
        last_processed_timestamp=engine._last_timestamp,
        pending_incident=engine.pending_incident,
        dataset_fingerprint=dataset_fingerprint,
        config_fingerprint=engine.config_fingerprint,
        run_manifest_payload=(
            previous.run_manifest_payload
            if isinstance(previous, OutputLedger)
            else None
        ),
    )
    engine._previous_output_ledger = (
        previous if isinstance(previous, OutputLedger) else None
    )
    engine._output_ledger = ledger
    return ledger


def output_bundle_from_ledger(ledger: OutputLedger) -> dict:
    def lines(entries):
        return "".join(
            json.dumps(
                item["payload"],
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for item in entries
        )

    return {
        "alerts.jsonl": lines(ledger.alert_entries),
        "failures.jsonl": lines(ledger.failure_entries),
        "reports": {
            item["object_id"]: item["payload"]
            for item in ledger.report_entries
        },
    }

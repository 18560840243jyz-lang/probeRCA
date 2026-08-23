from __future__ import annotations

import hashlib
import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
import proberca.dataplane.archive as archive_module

from proberca.aggregation import CounterDeltaTracker
from proberca.baseline import RobustBaselineStore as LegacyRobustBaselineStore
from proberca.cli.analyze_collection import main as analyze_main
from proberca.cli.seal_collection import main as seal_main
from proberca.controlplane import FinalControlConfig, FinalControlPlane
from proberca.controlplane.config import (
    EXPERIMENTAL_DNS_BURST_CHANNEL_IDS,
    experimental_dns_metric_roles,
)
from proberca.controlplane import (
    CalibrationNotReadyError,
    load_ready_calibration_report,
)
from proberca.controlplane.metric_model import fit_metric_propagation
from proberca.controlplane.evidence import aggregate_burst_evidence
from proberca.controlplane.model import (
    CandidateEntityGraph,
    MetricNode,
    MetricPropagationModel,
    MetricTargetReadiness,
    NormalizedObservation,
)
from proberca.controlplane.observations import (
    MetricResolver,
    RobustBaselineStore,
    quantile_required_samples,
)
from proberca.controlplane.pipeline import ControlPlaneError
from proberca.controlplane.resource_alerts import ResourceAlertChannel
from proberca.controlplane.service_model import (
    AllowedServiceGraph,
    ServiceRLS,
    allowed_service_graph,
    build_candidate_graph,
    formal_service_graph,
)
from proberca.config import (
    BaselineConfig,
    MetricSignalSpec,
    MonotonicCounterPolicy,
)
from proberca.data.schema import (
    EdgeMetricRecord,
    EvidenceObservationRecord,
    METRIC_RECORD_SCHEMA_VERSION,
    NodeMetricRecord,
    ServiceNodePlacement,
    ServiceResourceBinding,
    TopologyEdge,
    TopologySnapshot,
)
from proberca.dataplane.contracts import (
    GroundTruthFieldError,
    canonical_json,
    fingerprint,
)
from proberca.dataplane.adapters import from_engine_window
from proberca.dataplane import (
    CollectedWindow,
    CollectionArchive,
    CollectionArchiveError,
    CollectionArchiveNotSealedError,
    CollectionArchiveWriter,
    burst_observation_quality,
    continuous_burst_strength,
    rare_event_strength,
)


_NS = 1_000_000_000
_BUILD_FINGERPRINT = fingerprint({"build": "final-test-collector"})
_DATASET_ID = fingerprint({"dataset": "final-synthetic"})
_METRICS = (
    ("request_rate", "request"),
    ("request_failure_rate", "request"),
    ("request_latency_p95", "request"),
    ("cpu_usage_rate", "cpu"),
    ("cpu_throttle_ratio", "cpu"),
    ("memory_working_set_ratio", "memory"),
    ("io_psi", "io"),
    ("futex_wait_time_rate", "lock"),
    ("local_socket_failure_rate", "net_local"),
)
_HOST_METRICS = (
    ("cpu_psi", "cpu"),
    ("memory_psi", "memory"),
    ("io_psi", "io"),
    ("nic_drop_error_rate", "net_local"),
)
_HEALTHY_PATTERNS = (
    (-1, 1, 0, 1, -1),
    (0, -1, 1, -1, 1),
    (1, 0, -1, 1, 0),
)


def _healthy_variation(sequence: int, pattern: int) -> int:
    if sequence <= 3:
        return (-1, 0, 1)[sequence - 1]
    values = _HEALTHY_PATTERNS[pattern]
    return values[(sequence - 4) % len(values)]


def _required_root_coordinates() -> tuple[str, ...]:
    output = []
    for spec in FinalControlConfig().metric_roles:
        if not spec.root_eligible:
            continue
        if spec.entity_type == "service":
            entity_id = "cluster::ns::payment"
        elif spec.entity_type == "host":
            entity_id = "cluster::host::node-a"
        else:
            continue
        output.append(f"{entity_id}::{spec.metric_name}")
    return tuple(sorted(output))


def _config() -> FinalControlConfig:
    return FinalControlConfig(
        baseline_min_windows=3,
        baseline_min_scale=1.0e-12,
        baseline_family_min_scales={
            "count": 0.5,
            "latency": 0.5,
            "psi": 0.0005,
            "ratio": 0.0005,
        },
        latency_min_samples=1,
        failure_min_requests=1,
        service_min_training_updates=1,
        service_lags=(1,),
        metric_lags=(1,),
        metric_min_training_rows=2,
        metric_rows_per_feature=0.5,
        calibration_learning_windows=6,
        calibration_validation_windows=1,
        calibration_required_root_coordinates=(
            _required_root_coordinates()
        ),
        load_profile_id="test-healthy-profile",
        load_profile_fingerprint=fingerprint({
            "profile": "test-healthy-profile",
        }),
        soft_threshold=2.0,
        soft_consecutive_windows=1,
        hard_threshold=4.0,
        hard_candidate_windows=1,
        hard_consecutive_windows=2,
        recovery_threshold=0.5,
        recovery_windows=2,
        burst_window_count=1,
        l1_penalty=0.05,
        fista_tolerance=1.0e-9,
    )


def _topology(
    *,
    snapshot_name: str = "topology-1",
    valid_from_ns: int = 0,
    valid_to_ns: int = 100 * _NS,
) -> TopologySnapshot:
    return TopologySnapshot(
        schema_version="1.0",
        snapshot_id=fingerprint({"topology": snapshot_name}),
        valid_from_ns=valid_from_ns,
        valid_to_ns=valid_to_ns,
        cluster_id="cluster",
        services=["ns::payment"],
        call_edges=[],
        host_edges=[],
        resource_edges=[],
        service_nodes=[ServiceNodePlacement(
            namespace="ns",
            service_name="payment",
            node_name="node-a",
            pod_uid=None,
        )],
        structure_fingerprint=fingerprint({
            "cluster": "cluster",
            "services": ["ns::payment"],
            "calls": [],
            "hosts": [],
            "bindings": [],
        }),
    )


def _values(sequence: int) -> dict[str, float]:
    variation = _healthy_variation(sequence, 0)
    cpu_variation = _healthy_variation(sequence, 1)
    io_variation = _healthy_variation(sequence, 1)
    values = {
        "request_rate": 100.0 + 2.0 * variation,
        "request_failure_rate": 0.01 + 0.001 * variation,
        "request_latency_p95": 10.0 + variation,
        "cpu_usage_rate": 0.30 + 0.01 * cpu_variation,
        "cpu_throttle_ratio": 0.02 + 0.002 * cpu_variation,
        "memory_working_set_ratio": 0.40 + 0.01 * variation,
        "io_psi": 0.02 + 0.002 * io_variation,
        "futex_wait_time_rate": 0.01 + 0.001 * variation,
        "local_socket_failure_rate": 0.01 + 0.001 * variation,
    }
    if sequence == 10:
        values.update({
            "request_latency_p95": 20.0,
            "request_failure_rate": 0.03,
            "cpu_usage_rate": 0.75,
            "cpu_throttle_ratio": 0.20,
        })
    elif sequence >= 11:
        values.update({
            "request_latency_p95": 30.0,
            "request_failure_rate": 0.04,
            "cpu_usage_rate": 0.90,
            "cpu_throttle_ratio": 0.25,
        })
    return values


def _node_records(sequence: int) -> tuple[NodeMetricRecord, ...]:
    timestamp = (sequence - 1) * _NS
    values = _values(sequence)

    roles = {
        (item.entity_type, item.metric_name): item
        for item in FinalControlConfig().metric_roles
    }

    def record(*, name: str, family: str, value: float, scope: str, service: str):
        entity_type = "host" if scope == "node" else "service"
        spec = roles[(entity_type, name)]
        return NodeMetricRecord(
            schema_version=METRIC_RECORD_SCHEMA_VERSION,
            timestamp_ns=timestamp,
            window_sec=1,
            cluster_id="cluster",
            node_name="node-a",
            namespace="ns",
            service_name=service,
            pod_uid=None,
            container_id=None,
            metric_family=family,
            metric_name=name,
            value=value,
            valid=True,
            invalid_reason=None,
            unit=spec.unit,
            sample_count=10,
            coverage=1.0,
            event_loss_rate=0.0,
            mapping_quality=1.0,
            source="final_window_aggregation",
            metric_kind=spec.metric_kind,
            scope=scope,
            histogram_upper_bound=None,
            histogram_is_inf_bucket=False,
            histogram_is_cumulative=None,
            quantile=spec.quantile,
        )

    services = tuple(record(
        name=name, family=family, value=values[name],
        scope="service", service="payment",
    ) for name, family in _METRICS)
    host_patterns = {
        "cpu_psi": 2,
        "memory_psi": 1,
        "io_psi": 2,
        "nic_drop_error_rate": 0,
    }
    hosts = tuple(record(
        name=name,
        family=family,
        value=0.02 + 0.001 * _healthy_variation(
            sequence, host_patterns[name],
        ),
        scope="node",
        service="host-metrics",
    ) for name, family in _HOST_METRICS)
    return services + hosts


def _evidence() -> EvidenceObservationRecord:
    evidence_id = fingerprint({"evidence": "burst-cpu-1"})
    source_record_ids = [
        "source:" + fingerprint({"source": "burst-record-1"})
    ]
    return EvidenceObservationRecord(
        schema_version="1.0",
        evidence_id=evidence_id,
        timestamp_ns=11 * _NS + _NS // 2,
        evidence_window_start_ns=11 * _NS,
        evidence_window_end_ns=12 * _NS,
        analysis_cutoff_ns=12 * _NS,
        cluster_id="cluster",
        namespace="ns",
        target_type="node",
        # BurstEvidenceCollector emits an entity-level target.  The frozen
        # channel role selects the unique root-cause category on that entity.
        target_id="cluster::ns::payment",
        channel_id="sched.runqueue_wait_p95",
        source_type="burst_event",
        normalized_strength=0.9,
        observation_quality=1.0,
        reliability_weight=1.0,
        source_record_ids=source_record_ids,
        source_object_ids=[],
        independent_from_residual=True,
        provenance={
            "calibration_id": fingerprint({"calibration": "healthy-burst-v1"}),
            "collector_build_fingerprint": _BUILD_FINGERPRINT,
            "source_set_fingerprint": fingerprint(sorted(source_record_ids)),
        },
        config_fingerprint=FinalControlConfig().collection_contract[
            "burst_config_fingerprint"
        ],
    )


def _unknown_evidence() -> EvidenceObservationRecord:
    source_record_ids = [
        "source:" + fingerprint({"source": "burst-record-unknown"})
    ]
    return replace(
        _evidence(),
        evidence_id=fingerprint({"evidence": "burst-unknown-1"}),
        channel_id="unmapped.mystery_signal",
        normalized_strength=0.8,
        source_record_ids=source_record_ids,
        provenance={
            **_evidence().provenance,
            "source_set_fingerprint": fingerprint(sorted(source_record_ids)),
        },
    )


def test_burst_evidence_outside_candidate_scope_is_ignored():
    entity_id = "cluster::ns::payment"
    metric = MetricNode(
        node_id=f"{entity_id}::cpu_usage_rate",
        entity_id=entity_id,
        entity_type="service",
        metric_name="cpu_usage_rate",
        role="service_cpu_usage",
        root_category="CPU",
        root_eligible=True,
    )
    outside = replace(
        _evidence(),
        evidence_id=fingerprint({"evidence": "outside-candidate"}),
        target_id="cluster::ns::other-service",
    )
    strengths, identifiers = aggregate_burst_evidence(
        [outside], {(entity_id, "CPU"): (metric,)},
    )
    assert strengths[(entity_id, "CPU")] == 0.0
    assert identifiers[(entity_id, "CPU")] == ()


def _collection_metadata(config: FinalControlConfig | None = None) -> dict[str, str]:
    contract = (config or FinalControlConfig()).collection_contract
    return {
        "collector_build_fingerprint": _BUILD_FINGERPRINT,
        "aggregation_config_fingerprint": contract[
            "aggregation_config_fingerprint"
        ],
        "burst_config_fingerprint": contract["burst_config_fingerprint"],
    }


def _residual_source_ids(sequence: int) -> tuple[str, ...]:
    return (
        "source:" + fingerprint({"residual_window_sequence": sequence}),
    )


def _window(
    sequence: int, *, complete: bool = True,
    evidence: tuple[EvidenceObservationRecord, ...] | None = None,
    topology: TopologySnapshot | None = None,
) -> CollectedWindow:
    records = _node_records(sequence)
    if not complete:
        records = tuple(
            item for item in records
            if item.metric_name != "local_socket_failure_rate"
        )
    return CollectedWindow.create(
        sequence=sequence,
        window_start_ns=(sequence - 1) * _NS,
        window_end_ns=sequence * _NS,
        node_metrics=records,
        topology_events=((topology or _topology()),) if sequence == 1 else (),
        burst_evidence=(
            evidence if evidence is not None
            else ((_evidence(),) if sequence == 12 else ())
        ),
        residual_source_record_ids=_residual_source_ids(sequence),
        collection_metadata=_collection_metadata(),
    )


def _healthy_node_records(sequence: int) -> tuple[NodeMetricRecord, ...]:
    """Repeat only the synthetic healthy patterns with the real window time."""
    template_sequence = ((sequence - 1) % 8) + 1
    timestamp_ns = (sequence - 1) * _NS
    return tuple(
        replace(record, timestamp_ns=timestamp_ns)
        for record in _node_records(template_sequence)
    )


def _per_window_topology(
    sequence: int, *, changed_structure: bool = False,
    runtime_identity: str = "runtime-a",
) -> TopologySnapshot:
    snapshot = _topology(
        snapshot_name=f"window-snapshot-{sequence}",
        valid_from_ns=(sequence - 1) * _NS,
        valid_to_ns=sequence * _NS,
    )
    resources = (
        [ServiceResourceBinding(
            namespace="ns",
            service_name="payment",
            resource_type="database",
            resource_id="cluster/ns/database/payment-db",
        )]
        if changed_structure else []
    )
    structure_fingerprint = fingerprint({
        "cluster": snapshot.cluster_id,
        "services": snapshot.services,
        "calls": [item.to_dict() for item in snapshot.call_edges],
        "hosts": [item.to_dict() for item in snapshot.host_edges],
        "bindings": [item.to_dict() for item in resources],
    })
    return replace(
        snapshot,
        structure_fingerprint=structure_fingerprint,
        inventory_revision_id=fingerprint({"inventory": sequence}),
        resource_version_vector={
            "Pod": fingerprint({"pod-resource-version": sequence}),
        },
        runtime_identity_fingerprints=[
            fingerprint({"runtime": runtime_identity}),
        ],
        call_edge_provider_fingerprint=fingerprint({
            "provider-window": sequence,
        }),
        service_resources=resources,
    )


def _healthy_archive_with_window_snapshots(
    root: Path, *, window_count: int = 120,
    topology_change_at: int | None = None,
    runtime_change_at: int | None = None,
) -> CollectionArchive:
    config = _config()
    writer = CollectionArchiveWriter(
        root,
        dataset_id=fingerprint({
            "dataset": root.name,
            "window_count": window_count,
            "topology_change_at": topology_change_at,
            "runtime_change_at": runtime_change_at,
        }),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, window_count + 1):
        topology = _per_window_topology(
            sequence,
            changed_structure=(
                topology_change_at is not None
                and sequence >= topology_change_at
            ),
            runtime_identity=(
                "runtime-b"
                if runtime_change_at is not None
                and sequence >= runtime_change_at
                else "runtime-a"
            ),
        )
        writer.append(CollectedWindow.create(
            sequence=sequence,
            window_start_ns=(sequence - 1) * _NS,
            window_end_ns=sequence * _NS,
            node_metrics=_healthy_node_records(sequence),
            topology_events=(topology,),
            residual_source_record_ids=_residual_source_ids(sequence),
            collection_metadata=_collection_metadata(config),
        ))
    return writer.seal()


def _file_hash(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_collection_must_be_complete_and_sealed_before_control(tmp_path):
    config = _config()
    archive_dir = tmp_path / "archive"
    writer = CollectionArchiveWriter(
        archive_dir,
        dataset_id=_DATASET_ID,
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 13):
        writer.append(_window(sequence))

    with pytest.raises(CollectionArchiveNotSealedError):
        CollectionArchive.load(archive_dir)

    archive = writer.seal()
    manifest_hash = _file_hash(archive_dir / "collection-manifest.json")
    windows_hash = _file_hash(archive_dir / "collected-windows.jsonl")
    run = FinalControlPlane(config).run(CollectionArchive.load(archive_dir))

    assert run.processed_window_count == 12
    assert run.calibration_readiness["ready"] is True
    assert run.calibration_readiness["Av_required_count"] > 0
    assert not any(
        "::dns::" in coordinate
        for coordinate in run.calibration_readiness[
            "required_root_coordinates"
        ]
    )
    assert len(run.results) == 1
    result = run.results[0]
    assert result.top_k[0].entity_id == "cluster::ns::payment"
    assert result.top_k[0].root_category == "CPU"
    assert result.top_k[0].score > 0.0
    assert result.top_k[0].burst_evidence_strength == pytest.approx(0.9)
    assert result.top_k[0].burst_evidence_ids == (_evidence().evidence_id,)
    assert result.top_k[0].burst_evidence[0]["channel_id"] \
        == "sched.runqueue_wait_p95"
    assert result.top_k[0].effective_group_penalty == pytest.approx(
        result.top_k[0].base_group_penalty / (1.0 + config.burst_eta * 0.9)
    )
    assert result.model_metadata["self_history_subtracted_from_residual"] is False
    assert result.model_metadata["burst_role"] == "candidate_group_penalty_only"
    assert result.model_metadata["counterfactual_resolve"] is False
    assert _file_hash(archive_dir / "collection-manifest.json") == manifest_hash
    assert _file_hash(archive_dir / "collected-windows.jsonl") == windows_hash


def test_control_retains_only_hard_interval_burst_evidence(tmp_path):
    config = _config()
    archive_dir = tmp_path / "archive"
    writer = CollectionArchiveWriter(
        archive_dir,
        dataset_id=_DATASET_ID,
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    early_source_ids = [
        "source:" + fingerprint({"source": "healthy-burst-record"})
    ]
    early = replace(
        _evidence(),
        evidence_id=fingerprint({"evidence": "healthy-burst"}),
        timestamp_ns=_NS // 2,
        evidence_window_start_ns=0,
        evidence_window_end_ns=_NS,
        analysis_cutoff_ns=_NS,
        source_record_ids=early_source_ids,
        provenance={
            **_evidence().provenance,
            "source_set_fingerprint": fingerprint(sorted(early_source_ids)),
        },
    )
    for sequence in range(1, 13):
        evidence = (early,) if sequence == 1 else None
        writer.append(_window(sequence, evidence=evidence))
    archive = writer.seal()
    control = FinalControlPlane(config)

    run = control.run(archive)

    assert len(run.results) == 1
    assert tuple(item.evidence_id for item in control._evidence) == (
        _evidence().evidence_id,
    )
    assert early.evidence_id not in run.results[0].top_k[0].burst_evidence_ids


def test_data_plane_rejects_incomplete_final_metric_set(tmp_path):
    config = _config()
    writer = CollectionArchiveWriter(
        tmp_path / "archive",
        dataset_id=fingerprint({"dataset": "incomplete"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    with pytest.raises(CollectionArchiveError, match="incomplete service metric set"):
        writer.append(_window(1, complete=False))


def test_ground_truth_cannot_cross_collection_boundary(tmp_path):
    config = _config()
    with pytest.raises(GroundTruthFieldError):
        CollectionArchiveWriter(
            tmp_path / "archive",
            dataset_id=fingerprint({"dataset": "unsafe"}),
            collection_contract=config.collection_contract,
            source_description=config.collection_contract["source_description"],
            collection_metadata={"ground_truth": "payment::CPU"},
        )


def test_checked_in_contract_and_plane_imports_are_separate():
    contract = yaml.safe_load(
        Path("configs/final_collection_contract.yaml").read_text(encoding="utf-8")
    )
    checked_in_control = FinalControlConfig.from_dict(yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    ))
    assert fingerprint(contract) \
        == checked_in_control.collection_contract_fingerprint
    control_payload = yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    )
    assert set(control_payload) == set(FinalControlConfig.__dataclass_fields__)
    formal = FinalControlConfig.from_dict(control_payload)
    assert formal.calibration_learning_windows == 600
    assert formal.calibration_validation_windows == 300
    assert len(formal.calibration_required_root_coordinates) == 100
    assert len(formal.formal_service_entity_ids) == 11
    assert len(formal.formal_host_entity_ids) == 1
    assert len(formal.formal_tcp_edge_entity_ids) == 15
    assert all(
        "::dns::" not in coordinate
        for coordinate in formal.calibration_required_root_coordinates
    )
    assert all(
        value is not None
        for value in formal.baseline_family_min_scales.values()
    )
    scope = yaml.safe_load(Path(
        "configs/final_single_vm_scope.yaml"
    ).read_text(encoding="utf-8"))
    assert scope["required_root_coordinate_count"] \
        == len(formal.calibration_required_root_coordinates)
    assert scope["exclusion_policy"] \
        == "exclusions_are_fixed_before_fault_selection"
    for path in Path("proberca/dataplane").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "proberca.controlplane" not in source
    pipeline = Path("proberca/controlplane/pipeline.py").read_text(encoding="utf-8")
    assert "proberca.collectors" not in pipeline
    assert "proberca.experiments" not in pipeline


def test_separate_cli_phases(tmp_path, capsys):
    input_path = tmp_path / "input.jsonl"
    input_path.write_text(
        "".join(canonical_json(_window(sequence).to_dict()) + "\n"
                for sequence in range(1, 13)),
        encoding="utf-8",
    )
    config_path = tmp_path / "control.yaml"
    config_path.write_text(
        yaml.safe_dump(_config().to_dict(), sort_keys=False), encoding="utf-8",
    )
    metadata_path = tmp_path / "collection-metadata.yaml"
    metadata_path.write_text(
        yaml.safe_dump(_collection_metadata(_config()), sort_keys=False),
        encoding="utf-8",
    )
    contract_path = tmp_path / "collection-contract.yaml"
    contract_path.write_text(
        yaml.safe_dump(_config().collection_contract, sort_keys=False),
        encoding="utf-8",
    )
    archive_dir = tmp_path / "sealed"
    assert seal_main([
        "--windows-jsonl", str(input_path),
        "--collection-contract", str(contract_path),
        "--dataset-id", fingerprint({"dataset": "cli-two-phase"}),
        "--source-description", _config().collection_contract["source_description"],
        "--collection-metadata", str(metadata_path),
        "--output", str(archive_dir),
    ]) == 0
    collection_output = capsys.readouterr().out
    assert '"phase":"collection_sealed"' in collection_output
    control_dir = tmp_path / "control"
    assert analyze_main([
        "--archive", str(archive_dir),
        "--config", str(config_path),
        "--output", str(control_dir),
    ]) == 0
    control_output = capsys.readouterr().out
    assert '"phase":"control_complete"' in control_output
    assert (control_dir / "control-run.json").is_file()
    readiness_path = control_dir / "calibration-readiness.json"
    assert readiness_path.is_file()
    report = load_ready_calibration_report(readiness_path)
    assert report["ready"] is True
    report["ready"] = False
    readiness_path.write_text(
        json.dumps(report), encoding="utf-8",
    )
    with pytest.raises(
        CalibrationNotReadyError, match="fingerprint mismatch",
    ):
        load_ready_calibration_report(readiness_path)
    assert (control_dir / "rca-results.jsonl").is_file()


def test_burst_normalization_is_bounded_and_quality_aware():
    assert rare_event_strength(1, 10.0, 0.05) == 1.0
    assert continuous_burst_strength(10.0, [1.0, 1.1]) == 0.0
    assert continuous_burst_strength(10.0, [1.0, 1.1, 0.9, 1.2, 0.8]) == 1.0
    assert burst_observation_quality(
        coverage=0.8, event_loss_rate=0.1, mapping_quality=0.5,
    ) == pytest.approx(0.36)


def test_edge_identity_and_cross_metric_only_prediction():
    edge = EdgeMetricRecord(
        schema_version=METRIC_RECORD_SCHEMA_VERSION,
        timestamp_ns=0, window_sec=1,
        cluster_id="cluster", namespace="ns",
        src_service="checkout", dst_service="payment",
        src_pod_uid=None, dst_pod_uid=None, src_node="node-a", dst_node="node-b",
        protocol="tcp", metric_name="edge_latency_p95", value=10.0,
        valid=True, invalid_reason=None,
        unit="milliseconds", sample_count=10, coverage=1.0, event_loss_rate=0.0,
        mapping_quality=1.0,
        source="final_window_aggregation", metric_kind="quantile", scope="service_pair",
        histogram_upper_bound=None, histogram_is_inf_bucket=False,
        histogram_is_cumulative=None, quantile=0.95,
    )
    metric, spec = MetricResolver(FinalControlConfig()).resolve(edge)
    assert metric.entity_id == "cluster::ns::checkout->payment::tcp"
    assert metric.root_category == "TCP"
    assert spec.role == "edge_latency"


def test_quantile_required_samples_is_derived_from_the_tail_probability():
    assert quantile_required_samples(0.95) == 20
    assert quantile_required_samples(0.9) == 10
    assert quantile_required_samples(0.99) == 100
    for invalid in (0.0, 1.0, float("nan"), float("inf"), True):
        with pytest.raises(ValueError, match="quantile"):
            quantile_required_samples(invalid)


def test_local_socket_ratio_separates_modeling_from_root_exposure():
    config = replace(
        FinalControlConfig(),
        baseline_min_windows=3,
        failure_min_requests=5,
        baseline_family_min_scales={
            "count": 0.5,
            "latency": 0.5,
            "psi": 0.0005,
            "ratio": 0.0005,
        },
    )
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    local = next(
        item for item in _node_records(1)
        if item.metric_name == "local_socket_failure_rate"
    )
    metric, spec = resolver.resolve(local)
    for _ in range(3):
        baseline.update(metric.node_id, baseline.transform(0.0, spec), spec)

    sparse = replace(local, value=1.0, sample_count=1)
    observations, raw = resolver.normalize_window(
        SimpleNamespace(node_metrics=(sparse,), edge_metrics=()), baseline,
    )
    assert metric.node_id in observations
    assert metric.node_id in raw
    validity = resolver.last_validity[metric.node_id]
    assert validity["data_plane_invalid_reason"] is None
    assert validity["control_plane_invalid_reason"] is None
    assert validity["model_valid"] is True
    assert validity["alert_eligible"] is False
    assert validity["root_evidence_eligible"] is False
    assert observations[metric.node_id].alert_eligible is False

    exposed = replace(local, value=0.2, sample_count=5)
    observations, raw = resolver.normalize_window(
        SimpleNamespace(node_metrics=(exposed,), edge_metrics=()), baseline,
    )
    assert metric.node_id in observations
    assert metric.node_id in raw
    assert resolver.last_validity[metric.node_id]["model_valid"] is True
    assert resolver.last_validity[metric.node_id]["alert_eligible"] is True
    assert resolver.last_validity[metric.node_id][
        "root_evidence_eligible"
    ] is True


def test_service_quantile_reliability_filters_alerts_without_hiding_exposure():
    config = replace(
        FinalControlConfig(),
        baseline_min_windows=3,
        baseline_family_min_scales={
            "count": 0.5,
            "latency": 0.5,
            "psi": 0.0005,
            "ratio": 0.0005,
        },
        latency_min_samples=5,
    )
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    records = _node_records(1)
    rate = next(item for item in records if item.metric_name == "request_rate")
    failure = next(
        item for item in records
        if item.metric_name == "request_failure_rate"
    )
    latency = next(
        item for item in records
        if item.metric_name == "request_latency_p95"
    )
    metric, spec = resolver.resolve(latency)
    for _ in range(3):
        baseline.update(metric.node_id, baseline.transform(10.0, spec), spec)

    too_sparse_latency = replace(latency, value=1000.0, sample_count=4)
    sparse_observations, sparse_raw = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(rate, failure, too_sparse_latency), edge_metrics=(),
        ),
        baseline,
    )
    rate_id = resolver.resolve(rate)[0].node_id
    failure_id = resolver.resolve(failure)[0].node_id
    assert metric.node_id not in sparse_observations
    assert metric.node_id not in sparse_raw
    assert {rate_id, failure_id} <= set(sparse_raw)
    validity = resolver.last_validity[metric.node_id]
    assert validity["invalid_reason"] == "insufficient_sample_count"
    assert validity["model_valid"] is False
    assert validity["alert_eligible"] is False
    assert validity["quantile"] == 0.95
    assert validity["quantile_required_samples"] == 20
    assert validity["model_latency_min_samples"] == 5
    assert validity["alert_latency_min_samples"] == 20

    service = metric.entity_id
    graph = AllowedServiceGraph(
        services=(service,), relations=(), physical_edges=(), placements=(),
        snapshot_id="snapshot",
    )
    control = FinalControlPlane(config)
    for sample_count in (5, 19):
        low_latency = replace(latency, value=1000.0, sample_count=sample_count)
        low_observations, low_raw = resolver.normalize_window(
            SimpleNamespace(
                node_metrics=(rate, failure, low_latency), edge_metrics=(),
            ),
            baseline,
        )
        assert metric.node_id in low_observations
        assert {rate_id, failure_id, metric.node_id} <= set(low_raw)
        validity = resolver.last_validity[metric.node_id]
        assert validity["invalid_reason"] is None
        assert validity["model_valid"] is True
        assert validity["alert_eligible"] is False
        assert low_observations[metric.node_id].alert_eligible is False
        service_scores, edge_scores = control._scores(
            low_observations, graph,
        )
        assert service_scores == {}
        assert edge_scores == {}
        for _ in range(config.soft_consecutive_windows):
            soft, _candidate, hard = control._advance_alert_counters(
                service_scores, edge_scores,
            )
            assert soft == set()
            assert hard == set()

    high_latency = replace(latency, value=1000.0, sample_count=20)
    high_observations, high_raw = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(rate, failure, high_latency), edge_metrics=(),
        ),
        baseline,
    )
    assert metric.node_id in high_observations
    assert {rate_id, failure_id, metric.node_id} <= set(high_raw)
    validity = resolver.last_validity[metric.node_id]
    assert validity["model_valid"] is True
    assert validity["alert_eligible"] is True
    assert high_observations[metric.node_id].alert_eligible is True
    service_scores, edge_scores = control._scores(high_observations, graph)
    assert service_scores[service] >= config.soft_threshold
    for _ in range(config.soft_consecutive_windows - 1):
        soft, _candidate, hard = control._advance_alert_counters(
            service_scores, edge_scores,
        )
        assert ("service", service) not in soft
        assert ("service", service) not in hard
    soft, _candidate, hard = control._advance_alert_counters(
        service_scores, edge_scores,
    )
    assert ("service", service) in soft
    assert ("service", service) not in hard


def test_edge_quantile_reliability_uses_the_same_rule_as_service_latency():
    config = replace(
        FinalControlConfig(),
        baseline_min_windows=3,
        baseline_family_min_scales={
            "count": 0.5,
            "latency": 0.5,
            "psi": 0.0005,
            "ratio": 0.0005,
        },
        latency_min_samples=5,
    )
    specs = {
        spec.metric_name: spec
        for spec in config.metric_roles
        if spec.entity_type == "edge"
    }

    def record(name: str, value: float, sample_count: int) -> EdgeMetricRecord:
        spec = specs[name]
        return EdgeMetricRecord(
            schema_version=METRIC_RECORD_SCHEMA_VERSION,
            timestamp_ns=0,
            window_sec=1,
            cluster_id="cluster",
            namespace="ns",
            src_service="checkout",
            dst_service="payment",
            src_pod_uid=None,
            dst_pod_uid=None,
            src_node="node-a",
            dst_node="node-b",
            protocol="tcp",
            metric_name=name,
            value=value,
            valid=True,
            invalid_reason=None,
            unit=spec.unit,
            sample_count=sample_count,
            coverage=1.0,
            event_loss_rate=0.0,
            mapping_quality=1.0,
            source="final_window_aggregation",
            metric_kind=spec.metric_kind,
            scope="service_pair",
            histogram_upper_bound=None,
            histogram_is_inf_bucket=False,
            histogram_is_cumulative=None,
            quantile=spec.quantile,
        )

    count = record("edge_request_count", 25.0, 1)
    failure = record("edge_failure_rate", 0.0, 25)
    latency = record("edge_latency_p95", 50.0, 4)
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    latency_metric, latency_spec = resolver.resolve(latency)
    for _ in range(3):
        baseline.update(
            latency_metric.node_id,
            baseline.transform(10.0, latency_spec),
            latency_spec,
        )

    sparse_observations, sparse_raw = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(), edge_metrics=(count, failure, latency),
        ),
        baseline,
    )
    count_id = resolver.resolve(count)[0].node_id
    failure_id = resolver.resolve(failure)[0].node_id
    assert latency_metric.node_id not in sparse_observations
    assert latency_metric.node_id not in sparse_raw
    assert {count_id, failure_id} <= set(sparse_raw)
    validity = resolver.last_validity[latency_metric.node_id]
    assert validity["invalid_reason"] == "insufficient_sample_count"
    assert validity["model_valid"] is False
    assert validity["alert_eligible"] is False

    edge = latency_metric.entity_id
    source = "cluster::ns::checkout"
    target = "cluster::ns::payment"
    graph = AllowedServiceGraph(
        services=(source, target),
        relations=((source, target, "call"), (target, source, "call")),
        physical_edges=((edge, source, target, "tcp"),),
        placements=(),
        snapshot_id="snapshot",
    )
    control = FinalControlPlane(config)
    for sample_count in (5, 19):
        low_latency = replace(latency, sample_count=sample_count)
        low_observations, low_raw = resolver.normalize_window(
            SimpleNamespace(
                node_metrics=(),
                edge_metrics=(count, failure, low_latency),
            ),
            baseline,
        )
        assert latency_metric.node_id in low_observations
        assert {count_id, failure_id, latency_metric.node_id} <= set(low_raw)
        validity = resolver.last_validity[latency_metric.node_id]
        assert validity["invalid_reason"] is None
        assert validity["model_valid"] is True
        assert validity["alert_eligible"] is False
        assert low_observations[latency_metric.node_id].alert_eligible is False
        _, edge_scores = control._scores(low_observations, graph)
        assert edge_scores == {edge: 0.0}
        for _ in range(config.soft_consecutive_windows):
            soft, _candidate, hard = control._advance_alert_counters(
                {}, edge_scores,
            )
            assert soft == set()
            assert hard == set()

    failure_metric, failure_spec = resolver.resolve(failure)
    for _ in range(3):
        baseline.update(
            failure_metric.node_id,
            baseline.transform(0.0, failure_spec),
            failure_spec,
        )
    failing = replace(failure, value=0.01)
    low_latency = replace(latency, sample_count=19)
    failure_observations, _ = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(), edge_metrics=(count, failing, low_latency),
        ),
        baseline,
    )
    assert failure_observations[latency_metric.node_id].alert_eligible is False
    assert failure_observations[failure_metric.node_id].alert_eligible is True
    failure_control = FinalControlPlane(config)
    _, failure_scores = failure_control._scores(failure_observations, graph)
    assert failure_scores[edge] >= config.hard_threshold
    soft, _candidate, hard = failure_control._advance_alert_counters(
        {}, failure_scores,
    )
    assert soft == set()
    assert hard == set()
    _, candidate, hard = failure_control._advance_alert_counters(
        {}, failure_scores,
    )
    assert ("edge", edge) in candidate
    assert hard == set()
    _, candidate, hard = failure_control._advance_alert_counters(
        {}, failure_scores,
    )
    assert ("edge", edge) in hard

    high_latency = replace(latency, sample_count=20)
    high_observations, high_raw = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(),
            edge_metrics=(count, failure, high_latency),
        ),
        baseline,
    )
    assert latency_metric.node_id in high_observations
    assert {count_id, failure_id, latency_metric.node_id} <= set(high_raw)
    validity = resolver.last_validity[latency_metric.node_id]
    assert validity["model_valid"] is True
    assert validity["alert_eligible"] is True
    assert high_observations[latency_metric.node_id].alert_eligible is True
    _, edge_scores = control._scores(high_observations, graph)
    assert edge_scores[edge] >= config.soft_threshold
    for _ in range(config.soft_consecutive_windows - 1):
        soft, _candidate, hard = control._advance_alert_counters(
            {}, edge_scores,
        )
        assert ("edge", edge) not in soft
        assert ("edge", edge) not in hard
    soft, _candidate, hard = control._advance_alert_counters({}, edge_scores)
    assert ("edge", edge) in soft
    assert ("edge", edge) not in hard


def test_data_plane_invalid_record_does_not_enter_healthy_baseline():
    config = _config()
    record = replace(
        _node_records(1)[0],
        value=None,
        valid=False,
        invalid_reason="zero_coverage",
        sample_count=0,
        coverage=0.0,
    )
    window = SimpleNamespace(node_metrics=(record,), edge_metrics=())
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    normalized, raw = resolver.normalize_window(window, baseline)

    assert normalized == {}
    assert raw == {}
    assert baseline.snapshot() == {}
    validity = next(iter(resolver.last_validity.values()))
    assert validity == {
        "valid": False,
        "invalid_reason": "zero_coverage",
        "data_plane_invalid_reason": "zero_coverage",
        "control_plane_invalid_reason": None,
        "raw_value": None,
        "coverage": 0.0,
        "mapping_quality": 1.0,
        "sample_count": 0,
        "request_count": None,
        "quality": 0.0,
        "formal_scope": "included",
        "model_valid": False,
        "alert_eligible": False,
        "root_evidence_eligible": False,
        "root_eligible": False,
        "readiness_required": False,
    }

    model = MetricPropagationModel(
        node_ids=("a", "b"), lags=(1,),
        coefficients={("a", "a", 1): 100.0, ("a", "b", 1): 2.0},
        semantic_mask=(("a", "a"), ("a", "b")),
        training_rows=4, healthy_cutoff_ns=10,
    )
    assert model.cross_prediction("a", {4: {"a": 7.0, "b": 3.0}}, 5) == 6.0


def test_data_plane_reason_is_preserved_before_control_plane_thresholds():
    config = _config()
    records = _node_records(1)
    request_count = next(
        item for item in records if item.metric_name == "request_rate"
    )
    missing_latency = replace(
        next(
            item for item in records
            if item.metric_name == "request_latency_p95"
        ),
        value=None,
        valid=False,
        invalid_reason="no_exposure",
        sample_count=0,
    )
    resolver = MetricResolver(config)
    normalized, raw = resolver.normalize_window(
        SimpleNamespace(
            node_metrics=(request_count, missing_latency),
            edge_metrics=(),
        ),
        RobustBaselineStore(config),
    )
    node_id = (
        "cluster::ns::payment::request_latency_p95"
    )
    assert node_id not in normalized
    assert node_id not in raw
    validity = resolver.last_validity[node_id]
    assert validity["valid"] is False
    assert validity["invalid_reason"] == "no_exposure"
    assert validity["data_plane_invalid_reason"] == "no_exposure"
    assert validity["control_plane_invalid_reason"] is None
    assert validity["raw_value"] is None


@pytest.mark.parametrize(
    "data_plane_reason",
    ("inconsistent_histogram", "series_lifecycle_transition"),
)
def test_invalid_data_plane_observation_never_enters_control_plane_math(
    data_plane_reason,
):
    config = _config()
    record = replace(
        next(
            item for item in _node_records(1)
            if item.metric_name == "request_latency_p95"
        ),
        value=None,
        valid=False,
        invalid_reason=data_plane_reason,
        sample_count=5,
    )
    resolver = MetricResolver(config)
    normalized, raw = resolver.normalize_window(
        SimpleNamespace(node_metrics=(record,), edge_metrics=()),
        RobustBaselineStore(config),
    )
    assert normalized == {}
    assert raw == {}
    validity = resolver.last_validity[record.stable_id]
    assert validity["data_plane_invalid_reason"] == data_plane_reason
    assert validity["control_plane_invalid_reason"] is None


def test_legacy_control_and_counter_paths_also_fail_closed_on_invalid_record():
    record = replace(
        _node_records(1)[0],
        value=None,
        valid=False,
        invalid_reason="no_exposure",
    )
    spec = MetricSignalSpec(
        record_type="node_metric",
        metric_family=record.metric_family,
        metric_name=record.metric_name,
        protocol=None,
        transform="identity",
        polarity="increase_bad",
        rare_event_threshold=None,
        direct_hard=False,
        z_cap=6.0,
        aggregation_output_id=record.stable_id,
    )
    baseline = LegacyRobustBaselineStore(
        BaselineConfig(
            healthy_history_sec=10,
            min_healthy_windows=1,
            min_scale=0.1,
            z_cap=6.0,
        ),
        window_sec=1,
    )
    assert baseline.update(record, spec, state="healthy") is False
    scored = baseline.score(record, spec, 0, _NS)
    assert scored.score is None
    assert [item.reason_code for item in scored.issues] == ["no_exposure"]

    counter = replace(record, metric_kind="monotonic_counter")
    tracker = CounterDeltaTracker(MonotonicCounterPolicy(
        delta_before_cross_series_sum=True,
        value_decrease_means_reset=True,
        reset_policy="mark_missing",
    ))
    output, issues = tracker.process(counter)
    assert output is None
    assert [item.reason_code for item in issues] == ["no_exposure"]


def test_zero_mad_uses_metric_family_floor_instead_of_numeric_epsilon():
    config = replace(
        _config(),
        baseline_family_min_scales={
            "count": 1.0,
            "latency": 1.0,
            "psi": 0.1,
            "ratio": 0.5,
        },
    )
    spec = next(
        item for item in config.metric_roles
        if item.metric_name == "cpu_usage_rate"
    )
    baseline = RobustBaselineStore(config)
    for _ in range(config.baseline_min_windows):
        baseline.update("service::cpu_usage_rate", 10.0, spec)

    score, scale = baseline.score(
        "service::cpu_usage_rate", 11.0, 1, spec,
    )

    assert score == pytest.approx(2.0)
    assert scale.mad_scale == 0.0
    assert scale.iqr_scale == 0.0
    assert scale.final_scale == 0.5
    assert scale.scale_source == "family_floor"


def test_zero_mad_prefers_iqr_when_iqr_exceeds_family_floor():
    config = replace(
        _config(),
        baseline_min_windows=4,
        baseline_family_min_scales={
            "count": 1.0,
            "latency": 1.0,
            "psi": 0.1,
            "ratio": 0.5,
        },
    )
    spec = next(
        item for item in config.metric_roles
        if item.metric_name == "cpu_usage_rate"
    )
    baseline = RobustBaselineStore(config)
    for value in (0.0, 0.0, 0.0, 10.0):
        baseline.update("service::cpu_usage_rate", value, spec)

    scale = baseline.scale("service::cpu_usage_rate", spec)

    assert scale is not None
    assert scale.mad_scale == 0.0
    assert scale.iqr_scale > scale.family_floor
    assert scale.final_scale == scale.iqr_scale
    assert scale.scale_source == "iqr"


def test_metric_ridge_is_fitted_per_target_not_by_global_complete_rows():
    service_a = "cluster::ns::a"
    service_b = "cluster::ns::b"
    rate_a = MetricNode(
        node_id=f"{service_a}::request_rate",
        entity_id=service_a,
        entity_type="service",
        metric_name="request_rate",
        role="request_rate",
        root_category=None,
        root_eligible=False,
    )
    cpu_a = MetricNode(
        node_id=f"{service_a}::cpu_usage_rate",
        entity_id=service_a,
        entity_type="service",
        metric_name="cpu_usage_rate",
        role="service_cpu_usage",
        root_category="CPU",
        root_eligible=True,
    )
    rate_b = replace(
        rate_a, node_id=f"{service_b}::request_rate", entity_id=service_b,
    )
    cpu_b = replace(
        cpu_a, node_id=f"{service_b}::cpu_usage_rate", entity_id=service_b,
    )
    candidate = CandidateEntityGraph(
        seed_services=(service_a, service_b),
        seed_edges=(),
        services=(service_a, service_b),
        hosts=(),
        edges=(),
        strong_service_relations=(),
        topology_snapshot_id="topology",
    )
    graph = AllowedServiceGraph(
        services=(service_a, service_b),
        relations=(),
        physical_edges=(),
        placements=(),
        snapshot_id="topology",
    )
    history = {
        1: {
            rate_a.node_id: 1.0, cpu_a.node_id: 1.0,
            rate_b.node_id: 1.0, cpu_b.node_id: 1.0,
        },
        2: {rate_a.node_id: 2.0, cpu_a.node_id: 2.0},
        3: {rate_a.node_id: 3.0, cpu_a.node_id: 3.0},
        4: {rate_a.node_id: 4.0, cpu_a.node_id: 4.0},
    }

    model = fit_metric_propagation(
        metrics={
            item.node_id: item
            for item in (rate_a, cpu_a, rate_b, cpu_b)
        },
        healthy_history=history,
        candidate=candidate,
        service_graph=graph,
        healthy_cutoff_ns=5 * _NS,
        config=replace(
            _config(),
            metric_min_training_rows=2,
            metric_rows_per_feature=2.0,
        ),
    )

    ready = model.target_readiness[cpu_a.node_id]
    sparse = model.target_readiness[cpu_b.node_id]
    assert ready.ready is True
    assert ready.valid_training_rows == 3
    assert (cpu_a.node_id, rate_a.node_id, 1) in model.coefficients
    assert all(target != parent for target, parent in model.semantic_mask)
    assert model.target_readiness[rate_a.node_id].ready is True
    assert model.target_readiness[rate_a.node_id].allowed_feature_count == 0
    assert sparse.ready is False
    assert sparse.valid_training_rows == 0
    assert sparse.not_ready_reason == "insufficient_valid_history"
    assert model.ready is False


def test_metric_target_without_cross_metric_parents_is_ready_without_ridge():
    service = "cluster::ns::payment"
    metric = MetricNode(
        node_id=f"{service}::local_socket_failure_rate",
        entity_id=service,
        entity_type="service",
        metric_name="local_socket_failure_rate",
        role="service_localnet",
        root_category="LocalNet",
        root_eligible=True,
    )
    candidate = CandidateEntityGraph(
        seed_services=(service,),
        seed_edges=(),
        services=(service,),
        hosts=(),
        edges=(),
        strong_service_relations=(),
        topology_snapshot_id="topology",
    )
    graph = AllowedServiceGraph(
        services=(service,),
        relations=(),
        physical_edges=(),
        placements=(),
        snapshot_id="topology",
    )
    history = {1: {metric.node_id: 0.0}, 6: {metric.node_id: 1.0}}

    model = fit_metric_propagation(
        metrics={metric.node_id: metric},
        healthy_history=history,
        candidate=candidate,
        service_graph=graph,
        healthy_cutoff_ns=9 * _NS,
        config=replace(
            _config(),
            metric_lags=(1, 2),
            metric_min_training_rows=4,
            metric_rows_per_feature=2.0,
        ),
    )

    readiness = model.target_readiness[metric.node_id]
    assert readiness.ready is True
    assert readiness.allowed_feature_count == 0
    assert readiness.valid_training_rows == 0
    assert readiness.minimum_training_rows == 0
    assert readiness.effective_rank == 0
    assert readiness.raw_design_rank_ratio == 0.0
    assert readiness.regularized_gram_condition_number is None
    assert readiness.not_ready_reason is None
    assert model.semantic_mask == ()
    assert model.coefficients == {}
    assert model.ready is True


def test_metric_ridge_uses_cross_metric_lags_and_drops_incomplete_rows():
    service = "cluster::ns::payment"
    rate = MetricNode(
        node_id=f"{service}::request_rate",
        entity_id=service,
        entity_type="service",
        metric_name="request_rate",
        role="request_rate",
        root_category=None,
        root_eligible=False,
    )
    cpu = MetricNode(
        node_id=f"{service}::cpu_usage_rate",
        entity_id=service,
        entity_type="service",
        metric_name="cpu_usage_rate",
        role="service_cpu_usage",
        root_category="CPU",
        root_eligible=True,
    )
    candidate = CandidateEntityGraph(
        seed_services=(service,),
        seed_edges=(),
        services=(service,),
        hosts=(),
        edges=(),
        strong_service_relations=(),
        topology_snapshot_id="topology",
    )
    graph = AllowedServiceGraph(
        services=(service,),
        relations=(),
        physical_edges=(),
        placements=(),
        snapshot_id="topology",
    )
    history = {
        sequence: {
            **({rate.node_id: float(sequence)} if sequence != 3 else {}),
            cpu.node_id: float(sequence),
        }
        for sequence in range(1, 8)
    }

    model = fit_metric_propagation(
        metrics={rate.node_id: rate, cpu.node_id: cpu},
        healthy_history=history,
        candidate=candidate,
        service_graph=graph,
        healthy_cutoff_ns=8 * _NS,
        config=replace(
            _config(),
            metric_lags=(1, 2),
            metric_min_training_rows=2,
            metric_rows_per_feature=1.0,
        ),
    )

    readiness = model.target_readiness[cpu.node_id]
    assert model.semantic_mask == ((cpu.node_id, rate.node_id),)
    assert all(target != parent for target, parent in model.semantic_mask)
    assert readiness.ready is True
    assert readiness.allowed_feature_count == 2
    assert readiness.valid_training_rows == 3
    assert readiness.minimum_training_rows == 2
    assert set(model.coefficients) == {
        (cpu.node_id, rate.node_id, 1),
        (cpu.node_id, rate.node_id, 2),
    }
    assert model.target_readiness[rate.node_id].ready is True
    assert model.target_readiness[rate.node_id].allowed_feature_count == 0


def test_fully_missing_service_symptom_is_not_treated_as_healthy_zero():
    service = "cluster::ns::payment"
    graph = AllowedServiceGraph(
        services=(service,),
        relations=(),
        physical_edges=(),
        placements=(),
        snapshot_id="topology",
    )

    service_scores, edge_scores = FinalControlPlane(_config())._scores(
        {}, graph,
    )

    assert service_scores == {}
    assert edge_scores == {}


def test_unfrozen_family_floors_keep_run_in_calibration(tmp_path):
    config = replace(
        _config(),
        baseline_family_min_scales={
            "count": None,
            "latency": None,
            "psi": None,
            "ratio": None,
        },
    )
    writer = CollectionArchiveWriter(
        tmp_path / "not-ready",
        dataset_id=fingerprint({"dataset": "not-ready"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 13):
        writer.append(_window(sequence))

    run = FinalControlPlane(config).run(writer.seal())

    assert run.results == ()
    assert run.rca_not_ready_events == ()
    assert run.calibration_readiness["ready"] is False
    assert run.calibration_readiness["baseline_ready"] is False
    assert all(
        item["not_ready_reason"] == "family_floor_not_frozen"
        for item in run.calibration_readiness["baseline_status"].values()
    )
    assert {item["state"] for item in run.state_timeline} == {"calibrating"}

    output = tmp_path / "control-output"
    from proberca.controlplane import save_control_run
    save_control_run(output, run)
    with pytest.raises(
        CalibrationNotReadyError, match="calibration is not READY",
    ):
        load_ready_calibration_report(
            output / "calibration-readiness.json"
        )


def test_calibration_and_healthy_validation_are_independent_and_frozen(
    tmp_path,
):
    archive = _healthy_archive_with_window_snapshots(
        tmp_path / "independent-validation",
        window_count=9,
    )
    control = FinalControlPlane(_config())

    run = control.run(archive)
    report = run.calibration_readiness

    assert run.state_timeline[5]["state"] == "healthy_validating"
    assert run.state_timeline[5]["baseline_frozen"] is True
    assert run.state_timeline[6]["state"] == "ready"
    assert run.state_timeline[6]["baseline_frozen"] is True
    # Once independent Healthy Validation passes, the calibrated model is a
    # frozen experiment input.  Extra healthy windows may be retained as raw
    # data but cannot silently change Baseline, A_s, or A_v provenance.
    assert set(map(len, control.baseline.snapshot().values())) == {6}
    assert report["ready"] is True
    assert report["healthy_validation_result"] == "passed"
    assert report["healthy_validation_windows"] == 1
    assert report["healthy_validation_alerts"] == []
    assert report["healthy_validation_soft_episodes"] == []
    assert report["healthy_validation_hard_candidate_episodes"] == []
    assert report["healthy_validation_confirmed_hard_episodes"] == []
    assert report["calibration_learning_complete"] is True
    assert report["baseline_ready_count"] \
        == report["baseline_required_count"]
    assert report["As_ready_count"] == report["As_required_count"]
    assert report["Av_ready_count"] == report["Av_required_count"]
    for name in (
        "collection_contract_fingerprint",
        "scale_config_fingerprint",
        "As_fingerprint",
        "Av_fingerprint",
        "required_scope_fingerprint",
        "calibration_fingerprint",
    ):
        assert len(report[name]) == 64
    baseline = next(iter(report["baseline_status"].values()))
    assert baseline["calibration_fingerprint"] \
        == report["calibration_fingerprint"]
    scale = baseline["scale"]
    assert {
        "median", "mad", "iqr", "family_floor",
        "final_scale", "scale_source",
    } <= set(scale)
    metric = next(iter(report["metric_model_status"].values()))
    assert {
        "target_coordinate", "feature_count",
        "valid_training_rows", "effective_rank",
        "raw_design_rank_ratio",
        "regularized_gram_condition_number",
        "ready", "not_ready_reason",
    } <= set(metric)


def test_sustained_alert_during_healthy_validation_blocks_ready(
    tmp_path,
):
    config = replace(
        _config(),
        calibration_validation_windows=6,
        hard_candidate_windows=2,
        hard_consecutive_windows=3,
    )
    writer = CollectionArchiveWriter(
        tmp_path / "validation-false-alarm",
        dataset_id=fingerprint({"dataset": "validation-false-alarm"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 14):
        writer.append(_window(sequence))

    control = FinalControlPlane(config)
    run = control.run(writer.seal())
    report = run.calibration_readiness

    assert run.results == ()
    assert report["ready"] is False
    assert report["state"] == "healthy_validating"
    assert report["healthy_validation_result"] == "failed"
    assert report["healthy_validation_alerts"]
    assert report["healthy_validation_hard_candidate_episodes"]
    assert report["healthy_validation_confirmed_hard_episodes"]
    assert set(map(len, control.baseline.snapshot().values())) == {6}


def test_hard_candidate_is_diagnostic_during_healthy_validation(tmp_path):
    config = replace(
        _config(),
        calibration_validation_windows=5,
        hard_candidate_windows=2,
        hard_consecutive_windows=3,
    )
    writer = CollectionArchiveWriter(
        tmp_path / "validation-hard-candidate",
        dataset_id=fingerprint({"dataset": "validation-hard-candidate"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 13):
        writer.append(_window(sequence))

    report = FinalControlPlane(config).run(writer.seal()).calibration_readiness

    assert report["ready"] is True
    assert report["healthy_validation_result"] == "passed"
    assert report["healthy_validation_hard_candidate_episodes"]
    assert report["healthy_validation_confirmed_hard_episodes"] == []


def test_hard_candidate_does_not_confirm_until_third_window():
    control = FinalControlPlane(FinalControlConfig())
    edge_id = "cluster::ns::caller->callee::tcp"

    _soft, candidates, confirmed = control._advance_alert_counters(
        {}, {edge_id: 5.1},
    )
    assert candidates == set()
    assert confirmed == set()

    _soft, candidates, confirmed = control._advance_alert_counters(
        {}, {edge_id: 5.2},
    )
    assert candidates == {("edge", edge_id)}
    assert confirmed == set()
    assert control.state == "starting"

    _soft, candidates, confirmed = control._advance_alert_counters(
        {}, {edge_id: 5.3},
    )
    assert candidates == {("edge", edge_id)}
    assert confirmed == {("edge", edge_id)}


def test_soft_episode_is_diagnostic_during_healthy_validation(tmp_path):
    config = replace(
        _config(),
        calibration_validation_windows=5,
        hard_threshold=100.0,
        hard_candidate_windows=2,
        hard_consecutive_windows=3,
    )
    writer = CollectionArchiveWriter(
        tmp_path / "validation-soft-diagnostic",
        dataset_id=fingerprint({"dataset": "validation-soft-diagnostic"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 13):
        writer.append(_window(sequence))

    control = FinalControlPlane(config)
    report = control.run(writer.seal()).calibration_readiness

    assert report["ready"] is True
    assert report["healthy_validation_result"] == "passed"
    assert report["healthy_validation_alerts"] == []
    assert report["healthy_validation_confirmed_hard_episodes"] == []
    assert report["healthy_validation_soft_episode_count"] >= 1
    episode = report["healthy_validation_soft_episodes"][0]
    assert episode["entity_type"] == "service"
    assert episode["episode_number"] == 1
    assert episode["duration_windows"] >= 1
    assert episode["maximum_score"] >= config.soft_threshold


def test_fault_runner_requires_current_readiness_fingerprint_handshake(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    payload = yaml.safe_load(
        runner.CONTROL_CONFIG.read_text(encoding="utf-8")
    )
    config = FinalControlConfig.from_dict(payload)
    snapshot = _formal_service_scope_topology(config)
    full_graph = allowed_service_graph(snapshot)
    graph = formal_service_graph(snapshot, config)
    assert full_graph.topology_fingerprint != graph.topology_fingerprint
    assert (
        full_graph.runtime_identity_fingerprint
        != graph.runtime_identity_fingerprint
    )
    readiness = {
        "control_config_fingerprint": config.config_fingerprint,
        "collection_contract_fingerprint": (
            config.collection_contract_fingerprint
        ),
        "required_scope_fingerprint": (
            config.required_scope_fingerprint
        ),
        "scale_config_fingerprint": (
            config.scale_config_fingerprint
        ),
        "load_profile_id": config.load_profile_id,
        "load_profile_fingerprint": config.load_profile_fingerprint,
        "topology_fingerprint": graph.topology_fingerprint,
        "runtime_identity_fingerprint": (
            graph.runtime_identity_fingerprint
        ),
    }
    current_snapshot = [snapshot]
    archive = SimpleNamespace(
        collection_contract_fingerprint=(
            config.collection_contract_fingerprint
        ),
        dataset_id="preflight-dataset",
        manifest_fingerprint="m" * 64,
        iter_windows=lambda: iter((
            SimpleNamespace(topology_events=(current_snapshot[0],)),
        )),
    )
    monkeypatch.setattr(
        runner, "run", lambda *_args, **_kwargs: SimpleNamespace()
    )
    monkeypatch.setattr(
        runner.CollectionArchive, "load", lambda _path: archive,
    )

    pod_binding = runner.formal_pod_binding_fingerprint(snapshot, config)
    handshake = runner.assert_current_readiness_handshake(
        readiness, tmp_path, calibration_pod_binding=pod_binding,
    )
    assert handshake["topology_fingerprint"] \
        == graph.topology_fingerprint
    assert handshake["runtime_identity_fingerprint"] \
        == graph.runtime_identity_fingerprint
    assert handshake["runtime_identity_rebound"] is False
    assert handshake["calibration_pod_binding_fingerprint"] \
        == pod_binding

    changed_runtime = dict(snapshot.service_runtime_identity_fingerprints)
    formal_id = sorted(config.formal_service_entity_ids)[0]
    changed_runtime[formal_id] = [fingerprint({"runtime": "replacement"})]
    restarted = replace(
        snapshot,
        runtime_identity_fingerprints=sorted({
            identity
            for identities in changed_runtime.values()
            for identity in identities
        }),
        service_runtime_identity_fingerprints=changed_runtime,
    )
    current_snapshot[0] = restarted
    with pytest.raises(
        runner.ExperimentError,
        match="runtime identity differs from the frozen Healthy calibration",
    ):
        runner.assert_current_readiness_handshake(
            readiness, tmp_path, calibration_pod_binding=pod_binding,
        )

    changed_placements = [
        replace(item, pod_uid="replacement-pod")
        if (
            f"{snapshot.cluster_id}::{item.namespace}::"
            f"{item.service_name}"
        ) == formal_id
        else item
        for item in snapshot.service_nodes
    ]
    current_snapshot[0] = replace(
        restarted, service_nodes=changed_placements,
    )
    with pytest.raises(
        runner.ExperimentError,
        match="runtime identity differs from the frozen Healthy calibration",
    ):
        runner.assert_current_readiness_handshake(
            readiness, tmp_path,
            calibration_pod_binding=pod_binding,
        )

    current_snapshot[0] = snapshot

    bad = dict(readiness)
    bad["scale_config_fingerprint"] = "bad"
    with pytest.raises(
        runner.ExperimentError,
        match="readiness/config fingerprint mismatch",
    ):
        runner.assert_current_readiness_handshake(
            bad, tmp_path, calibration_pod_binding=pod_binding,
        )

def test_fault_runner_subprocesses_use_frozen_kubeconfig(monkeypatch):
    import scripts.run_final_fault_matrix as runner

    observed = {}

    def fake_subprocess_run(arguments, **kwargs):
        observed["arguments"] = arguments
        observed["environment"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(runner.subprocess, "run", fake_subprocess_run)

    runner.run(["kubectl", "version"], check=False)

    assert observed["arguments"] == ["kubectl", "version"]
    assert observed["environment"]["KUBECONFIG"] == str(runner.KUBECONFIG)

def test_fault_runner_validates_frozen_formal_burst_channels(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    formal_channels = runner.formal_burst_channel_ids()
    assert len(formal_channels) == 26
    assert {
        "tcp.retrans_rate",
        "tcp.rto_rate",
        "tcp.rtt_p95",
        "tcp.connect_failure_rate",
        "tcp.rst_rate",
    } <= formal_channels
    assert formal_channels.isdisjoint(
        EXPERIMENTAL_DNS_BURST_CHANNEL_IDS
    )
    assert set(runner.BURST_CHANNEL_MODES) - formal_channels == set(
        EXPERIMENTAL_DNS_BURST_CHANNEL_IDS
    )

    normal_root = tmp_path / "normal"
    burst_root = tmp_path / "burst"
    normal_root.mkdir()
    burst_root.mkdir()
    (normal_root / "collection-manifest.json").write_text(
        "normal", encoding="utf-8",
    )
    (burst_root / "burst-manifest.json").write_text(
        "burst", encoding="utf-8",
    )
    left = SimpleNamespace(
        sequence=1,
        window_start_ns=0,
        window_end_ns=_NS,
        burst_evidence=(),
        node_metrics=(
            SimpleNamespace(scope="service", service_name="checkoutservice"),
        ),
        topology_events=(object(),),
    )

    def sample(channel_id):
        return SimpleNamespace(
            channel_id=channel_id,
            mapping_quality=1.0,
            entity_type="service",
            entity_id="cluster::namespace::checkoutservice",
        )

    right = SimpleNamespace(
        sequence=1,
        window_start_ns=0,
        window_end_ns=_NS,
        samples=tuple(sample(item) for item in sorted(formal_channels)),
        event_loss_rate=0.0,
    )
    normal = SimpleNamespace(
        dataset_id="dataset",
        manifest_fingerprint="normal-manifest",
        iter_windows=lambda: iter((left,)),
    )
    burst = SimpleNamespace(
        dataset_id="dataset",
        manifest_fingerprint="burst-manifest",
        iter_windows=lambda: iter((right,)),
    )
    monkeypatch.setattr(runner.CollectionArchive, "load", lambda _path: normal)
    monkeypatch.setattr(runner.BurstArchive, "load", lambda _path: burst)
    monkeypatch.setattr(
        runner, "formal_service_graph",
        lambda _snapshot, _config: SimpleNamespace(
            topology_fingerprint="topology",
            runtime_identity_fingerprint="runtime",
        ),
    )

    result = runner.validate_archives(normal_root, burst_root, 1)
    assert result["window_count"] == 1

    right.samples += (sample("dns.timeout_rate"),)
    with pytest.raises(runner.ExperimentError, match="channel coverage"):
        runner.validate_archives(normal_root, burst_root, 1)

    right.samples = tuple(
        sample(item) for item in sorted(formal_channels - {"tcp.rtt_p95"})
    )
    with pytest.raises(runner.ExperimentError, match="channel coverage"):
        runner.validate_archives(normal_root, burst_root, 1)



def test_fault_runner_observes_exporter_after_bounded_recovery_restart(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    clock = {"now": 0.0}
    restarted = {"value": False}
    events = []

    def fake_run(arguments, *args, **kwargs):
        del args, kwargs
        if arguments[:2] == ["systemctl", "restart"]:
            restarted["value"] = True
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if "http://127.0.0.1:9477/metrics" in arguments:
            if not restarted["value"]:
                # Model a failed exporter probe consuming its HTTP timeout.
                clock["now"] += 8.0
                return SimpleNamespace(returncode=22, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="metrics", stderr="")
        if "http://127.0.0.1:9090/api/v1/query" in arguments:
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps({
                    "data": {"result": [{"value": [clock["now"], "1"]}]},
                }),
                stderr="",
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(runner, "run", fake_run)
    monkeypatch.setattr(
        runner.time, "monotonic", lambda: clock["now"],
    )
    monkeypatch.setattr(
        runner.time, "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )
    monkeypatch.setattr(
        runner, "log_event",
        lambda _root, event, **_fields: events.append(event),
    )

    runner.wait_data_plane(tmp_path)

    assert restarted["value"] is True
    assert events == ["primitive_exporter_restarted"]
    assert clock["now"] >= runner.DATA_PLANE_READY_TIMEOUT_SEC
    assert clock["now"] < 2 * runner.DATA_PLANE_READY_TIMEOUT_SEC



def test_call_identity_stays_directed_but_service_mask_is_bidirectional():
    topology = TopologySnapshot(
        schema_version="1.0",
        snapshot_id="topology-two-services",
        valid_from_ns=0,
        valid_to_ns=100 * _NS,
        cluster_id="cluster",
        services=["ns::checkout", "ns::payment"],
        call_edges=[TopologyEdge(
            src_service="checkout",
            dst_service="payment",
            relation_type="call",
            src_namespace="ns",
            dst_namespace="ns",
            protocol="tcp",
            directed=True,
        )],
        host_edges=[],
        resource_edges=[],
        service_nodes=[
            ServiceNodePlacement("ns", "checkout", "node-a", None),
            ServiceNodePlacement("ns", "payment", "node-b", None),
        ],
    )
    graph = allowed_service_graph(topology)
    checkout = "cluster::ns::checkout"
    payment = "cluster::ns::payment"
    assert ("cluster::ns::checkout->payment::tcp", checkout, payment, "tcp") \
        in graph.physical_edges
    assert all(edge[0] != "cluster::ns::payment->checkout::tcp"
               for edge in graph.physical_edges)
    assert (checkout, payment, "call") in graph.relations
    assert (payment, checkout, "call") in graph.relations
    learner = ServiceRLS(FinalControlConfig(service_lags=(1,)))
    learner.update(1, {checkout: 0.1, payment: 0.2}, graph)
    learner.update(2, {checkout: 0.3, payment: 0.4}, graph)
    coefficients = learner.coefficients()
    assert (payment, checkout, 1) in coefficients
    assert (checkout, payment, 1) in coefficients


def test_formal_alert_defaults_and_per_entity_consecutive_state():
    config = FinalControlConfig()
    assert (config.soft_threshold, config.soft_consecutive_windows) == (3.0, 3)
    assert (
        config.hard_threshold,
        config.hard_candidate_windows,
        config.hard_consecutive_windows,
    ) == (5.0, 2, 3)
    assert config.calibration_required_root_coordinates == ()
    assert all(
        value is None
        for value in config.baseline_family_min_scales.values()
    )
    control = FinalControlPlane(config)
    service_a = "cluster::ns::a"
    service_b = "cluster::ns::b"

    soft, _candidate, hard = control._advance_alert_counters(
        {service_a: 4.0}, {},
    )
    assert not soft and not hard
    soft, _candidate, hard = control._advance_alert_counters(
        {service_b: 4.0}, {},
    )
    assert not soft and not hard
    soft, _candidate, hard = control._advance_alert_counters(
        {service_a: 4.0}, {},
    )
    assert not soft and not hard

    control = FinalControlPlane(config)
    soft, candidate, hard = control._advance_alert_counters(
        {}, {"edge-a": 6.0},
    )
    assert not candidate
    assert not hard
    soft, candidate, hard = control._advance_alert_counters(
        {}, {"edge-a": 6.0},
    )
    assert ("edge", "edge-a") in candidate
    assert ("edge", "edge-a") not in hard
    soft, candidate, hard = control._advance_alert_counters(
        {}, {"edge-a": 6.0},
    )
    assert ("edge", "edge-a") in hard
    assert ("edge", "edge-a") in soft


def _resource_observation(
    *, entity_id: str, entity_type: str, metric_name: str, anomaly: float,
) -> NormalizedObservation:
    metric = MetricNode(
        node_id=f"{entity_id}::{metric_name}",
        entity_id=entity_id,
        entity_type=entity_type,
        metric_name=metric_name,
        role=metric_name,
        root_category="CPU" if "cpu" in metric_name else "Memory",
        root_eligible=True,
    )
    return NormalizedObservation(
        metric=metric,
        signed_z=anomaly,
        anomaly=anomaly,
        quality=1.0,
        source_record_id="resource-test",
        baseline_center=0.0,
        baseline_scale=1.0,
        scale_source="mad",
        alert_eligible=True,
    )


def test_resource_alert_threshold_is_healthy_learned_and_entity_generic():
    config = replace(
        FinalControlConfig(),
        resource_alert_history_windows=5,
        resource_alert_metric_names=("cpu_usage_rate",),
    )
    channel = ResourceAlertChannel(config)
    service = "cluster::ns::service-a"
    for sequence, anomaly in enumerate((0, 0, 0, 0, 0, 7, 7, 7), 1):
        observation = _resource_observation(
            entity_id=service,
            entity_type="service",
            metric_name="cpu_usage_rate",
            anomaly=float(anomaly),
        )
        channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=True,
        )
    channel.freeze()

    coordinate = f"{service}::cpu_usage_rate"
    assert channel.thresholds[coordinate] == pytest.approx(
        7.0 + config.resource_alert_calibration_margin
    )
    assert channel.threshold_fingerprint is not None


def test_resource_alert_thresholds_are_isolated_by_metric_coordinate():
    config = replace(
        FinalControlConfig(),
        resource_alert_history_windows=5,
        resource_alert_metric_names=("futex_wait_time_rate",),
    )
    channel = ResourceAlertChannel(config)
    quiet = "cluster::ns::quiet-service"
    noisy = "cluster::ns::noisy-service"
    for sequence in range(1, 11):
        observations = {}
        for service, anomaly in (
            (quiet, 0.0),
                (noisy, 100.0 if sequence >= 6 else 0.0),
        ):
            observation = _resource_observation(
                entity_id=service,
                entity_type="service",
                metric_name="futex_wait_time_rate",
                anomaly=anomaly,
            )
            observations[observation.metric.node_id] = observation
        channel.observe(
            sequence=sequence,
            observations=observations,
            learn=True,
        )
    channel.freeze()

    assert channel.thresholds[f"{quiet}::futex_wait_time_rate"] == 5.0
    assert channel.thresholds[
        f"{noisy}::futex_wait_time_rate"
    ] == pytest.approx(100.0 + config.resource_alert_calibration_margin)


def test_formal_lock_and_localnet_roots_use_the_generic_resource_channel():
    config = FinalControlConfig()

    assert "futex_wait_time_rate" in config.resource_alert_metric_names
    assert "local_socket_failure_rate" in config.resource_alert_metric_names

    channel = ResourceAlertChannel(replace(
        config,
        resource_alert_history_windows=3,
        resource_alert_metric_names=(
            "futex_wait_time_rate",
            "local_socket_failure_rate",
        ),
    ))
    service = "cluster::ns::service-a"

    for sequence in range(1, 7):
        observations = {}
        for metric_name in channel.config.resource_alert_metric_names:
            observation = _resource_observation(
                entity_id=service,
                entity_type="service",
                metric_name=metric_name,
                anomaly=0.0,
            )
            observations[observation.metric.node_id] = observation
        channel.observe(
            sequence=sequence,
            observations=observations,
            learn=True,
        )
    channel.freeze()

    for metric_name in channel.config.resource_alert_metric_names:
        channel.begin_observation_session()
        for sequence in range(1, 4):
            observation = _resource_observation(
                entity_id=service,
                entity_type="service",
                metric_name=metric_name,
                anomaly=0.0,
            )
            assert channel.observe(
                sequence=sequence,
                observations={observation.metric.node_id: observation},
                learn=False,
            ).service_scores == {}

        emitted = []
        for sequence in range(4, 6):
            observation = _resource_observation(
                entity_id=service,
                entity_type="service",
                metric_name=metric_name,
                anomaly=8.0,
            )
            emitted.append(channel.observe(
                sequence=sequence,
                observations={observation.metric.node_id: observation},
                learn=False,
            ).service_scores.get(service, 0.0))
        assert emitted == [0.0, 8.0]


def test_invalid_formal_root_observation_never_enters_resource_alert_channel():
    config = replace(
        FinalControlConfig(),
        resource_alert_history_windows=1,
        resource_alert_prefilter_windows=1,
        resource_alert_metric_names=("local_socket_failure_rate",),
    )
    channel = ResourceAlertChannel(config)
    observation = replace(
        _resource_observation(
            entity_id="cluster::ns::service-a",
            entity_type="service",
            metric_name="local_socket_failure_rate",
            anomaly=100.0,
        ),
        alert_eligible=False,
    )
    channel.observe(
        sequence=1,
        observations={observation.metric.node_id: observation},
        learn=True,
    )
    channel.freeze()
    channel.begin_observation_session()

    result = channel.observe(
        sequence=1,
        observations={observation.metric.node_id: observation},
        learn=False,
    )

    assert result.service_scores == {}
    assert result.metric_scores == {}


def test_resource_alert_prefilter_rejects_transient_but_detects_sustained_change():
    config = replace(
        FinalControlConfig(),
        resource_alert_history_windows=7,
        resource_alert_metric_names=("cpu_usage_rate",),
    )
    channel = ResourceAlertChannel(config)
    service = "cluster::ns::service-a"

    def score(sequence: int, anomaly: float) -> float:
        observation = _resource_observation(
            entity_id=service,
            entity_type="service",
            metric_name="cpu_usage_rate",
            anomaly=anomaly,
        )
        result = channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=False,
        )
        return result.service_scores.get(service, 0.0)

    for sequence in range(1, 8):
        observation = _resource_observation(
            entity_id=service,
            entity_type="service",
            metric_name="cpu_usage_rate",
            anomaly=0.0,
        )
        channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=True,
        )
    channel.freeze()
    channel.begin_observation_session()
    for sequence in range(1, 8):
        assert score(sequence, 0.0) == 0.0
    emitted = [score(sequence, 10.0) for sequence in range(8, 11)]
    assert emitted[0] == 0.0
    assert emitted[1:] == [10.0, 10.0]

    # A new sealed observation session has no label input and must warm its
    # rolling history again.  Four sustained samples yield three common Hard
    # scores; a three-sample transient yields only two and cannot confirm Hard.
    channel.begin_observation_session()
    control = FinalControlPlane(config)
    for sequence in range(1, 8):
        assert score(sequence, 0.0) == 0.0
    hard = set()
    for sequence in range(8, 12):
        value = score(sequence, 10.0)
        _soft, _candidate, hard = control._advance_alert_counters(
            {service: value} if value else {}, {}, {},
        )
    assert ("service", service) in hard


def test_resource_alert_supports_hosts_without_changing_edge_alerts():
    config = replace(
        FinalControlConfig(),
        resource_alert_history_windows=3,
        resource_alert_metric_names=("cpu_psi",),
    )
    channel = ResourceAlertChannel(config)
    host = "cluster::host::node-a"
    for sequence in range(1, 4):
        observation = _resource_observation(
            entity_id=host,
            entity_type="host",
            metric_name="cpu_psi",
            anomaly=0.0,
        )
        assert channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=True,
        ).host_scores == {}
    channel.freeze()
    channel.begin_observation_session()
    for sequence in range(1, 4):
        observation = _resource_observation(
            entity_id=host,
            entity_type="host",
            metric_name="cpu_psi",
            anomaly=0.0,
        )
        assert channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=False,
        ).host_scores == {}
    for sequence in range(4, 6):
        observation = _resource_observation(
            entity_id=host,
            entity_type="host",
            metric_name="cpu_psi",
            anomaly=8.0,
        )
        result = channel.observe(
            sequence=sequence,
            observations={observation.metric.node_id: observation},
            learn=False,
        )
    assert result.host_scores[host] == pytest.approx(8.0)

    control = FinalControlPlane(config)
    _soft, candidate, _hard = control._advance_alert_counters(
        {}, {"edge-a": 6.0}, {host: 6.0},
    )
    assert ("edge", "edge-a") not in candidate
    assert ("host", host) not in candidate


def test_metric_unit_kind_and_p95_aggregation_semantics_fail_closed(tmp_path):
    config = _config()
    contract = config.collection_contract
    bad_record = replace(_node_records(1)[0], unit="bytes")
    records = (bad_record, *_node_records(1)[1:])
    bad_window = CollectedWindow.create(
        sequence=1,
        window_start_ns=0,
        window_end_ns=_NS,
        node_metrics=records,
        topology_events=(_topology(),),
        residual_source_record_ids=_residual_source_ids(1),
        collection_metadata=_collection_metadata(config),
    )
    writer = CollectionArchiveWriter(
        tmp_path / "bad-semantics",
        dataset_id=fingerprint({"dataset": "bad-semantics"}),
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    with pytest.raises(CollectionArchiveError, match="unit or metric_kind"):
        writer.append(bad_window)

    latency = next(
        role for role in contract["normal_metric_roles"]
        if role["metric_name"] == "request_latency_p95"
    )
    assert latency["metric_kind"] == "quantile"
    assert latency["aggregation"] == "histogram_merge_quantile"
    assert latency["aggregation_formula"] \
        == "q0.95(merge_pod(request_latency_histogram))"
    assert latency["quantile"] == 0.95
    assert contract["aggregation_config_fingerprint"] == fingerprint({
        "output_source": contract["aggregation_output_source"],
        "roles": contract["normal_metric_roles"],
    })


def test_v2_contract_remains_readable_for_existing_archives(tmp_path):
    contract = dict(_config().collection_contract)
    contract["schema_version"] = "probeRCA-final-collection-contract-v2"
    contract["aggregation_config_fingerprint"] = fingerprint({
        "output_source": contract["aggregation_output_source"],
        "roles": contract["normal_metric_roles"],
    })
    metadata = _collection_metadata(_config())
    metadata["aggregation_config_fingerprint"] = contract[
        "aggregation_config_fingerprint"
    ]
    CollectionArchiveWriter(
        tmp_path / "legacy-v2",
        dataset_id=fingerprint({"dataset": "legacy-v2"}),
        collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )


def _legacy_v3_dns_contract(config: FinalControlConfig) -> dict:
    formal = config.collection_contract
    dns_roles = []
    for spec in experimental_dns_metric_roles():
        role = spec.to_dict()
        if role["metric_name"] != "dns_query_count":
            role["root_category"] = "DNS"
            role["root_eligible"] = True
        dns_roles.append(role)
    roles = sorted(
        [*formal["normal_metric_roles"], *dns_roles],
        key=lambda item: (
            item["record_type"], item["metric_name"], item["entity_type"],
            item["scopes"], item["protocols"],
        ),
    )
    burst_roles = [
        *formal["burst_channel_roles"],
        *[
            {
                "channel_id": channel_id,
                "root_category": "DNS",
                "entity_types": ["edge"],
            }
            for channel_id in sorted(EXPERIMENTAL_DNS_BURST_CHANNEL_IDS)
        ],
    ]
    policy_id = "legacy-dns-policy"
    policy_fingerprint = fingerprint({"policy": policy_id})
    return {
        **formal,
        "schema_version": "probeRCA-final-collection-contract-v3",
        "normal_metric_roles": roles,
        "dns_aggregation_policy_id": policy_id,
        "dns_aggregation_policy_fingerprint": policy_fingerprint,
        "aggregation_config_fingerprint": fingerprint({
            "output_source": formal["aggregation_output_source"],
            "roles": roles,
            "dns_aggregation_policy_id": policy_id,
            "dns_aggregation_policy_fingerprint": policy_fingerprint,
        }),
        "burst_channel_roles": burst_roles,
        "burst_config_fingerprint": fingerprint({
            "roles": burst_roles,
            "semantics": formal["burst_evidence_semantics"],
        }),
    }


def test_legacy_v3_dns_contract_projects_to_formal_v4():
    config = _config()
    legacy = _legacy_v3_dns_contract(config)

    projected = config.project_collection_contract(legacy)

    assert projected == config.collection_contract
    assert projected["schema_version"] \
        == "probeRCA-final-collection-contract-v4"
    assert all(
        "dns" not in role["protocols"]
        for role in projected["normal_metric_roles"]
    )
    assert all(
        role["root_category"] != "DNS"
        for role in projected["burst_channel_roles"]
    )


def test_v4_contract_rejects_dns_burst_channel(tmp_path):
    config = _config()
    contract = dict(config.collection_contract)
    contract["burst_channel_roles"] = [
        *contract["burst_channel_roles"],
        {
            "channel_id": "dns.timeout_rate",
            "root_category": "DNS",
            "entity_types": ["edge"],
        },
    ]
    contract["burst_config_fingerprint"] = fingerprint({
        "roles": contract["burst_channel_roles"],
        "semantics": contract["burst_evidence_semantics"],
    })
    metadata = _collection_metadata(config)
    metadata["burst_config_fingerprint"] = contract[
        "burst_config_fingerprint"
    ]

    with pytest.raises(CollectionArchiveError, match="root category"):
        CollectionArchiveWriter(
            tmp_path / "formal-dns-burst",
            dataset_id=fingerprint({"dataset": "formal-dns-burst"}),
            collection_contract=contract,
            source_description=contract["source_description"],
            collection_metadata=metadata,
        )


def test_topology_must_cover_the_entire_half_open_window(tmp_path):
    config = _config()
    partial = replace(_topology(), valid_to_ns=_NS // 2)
    window = _window(1, topology=partial)
    writer = CollectionArchiveWriter(
        tmp_path / "partial-topology",
        dataset_id=fingerprint({"dataset": "partial-topology"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    with pytest.raises(CollectionArchiveError, match="active topology snapshots"):
        writer.append(window)


def test_topology_validation_is_incremental_for_1100_window_versions(
    monkeypatch,
):
    validated = 0
    original = archive_module._validate_topology_snapshot

    def counted(snapshot):
        nonlocal validated
        validated += 1
        return original(snapshot)

    monkeypatch.setattr(
        archive_module, "_validate_topology_snapshot", counted,
    )
    tracker = archive_module._TopologyVersionTracker()
    for sequence in range(1, 1101):
        additions = tracker.prepare((
            _per_window_topology(sequence),
        ))
        window = SimpleNamespace(
            sequence=sequence,
            window_start_ns=(sequence - 1) * _NS,
            window_end_ns=sequence * _NS,
        )
        assert tracker.active_for(window, additions) is additions[0]
        tracker.commit(additions)

    assert validated == 1100


def test_unique_window_snapshots_keep_one_topology_epoch_and_full_history(
    tmp_path,
):
    archive = _healthy_archive_with_window_snapshots(
        tmp_path / "stable-topology",
    )
    control = FinalControlPlane(_config())

    run = control.run(archive)

    assert run.processed_window_count == 120
    assert len({item["snapshot_id"] for item in run.state_timeline}) == 120
    assert len({
        item["topology_fingerprint"] for item in run.state_timeline
    }) == 1
    assert len({
        item["runtime_identity_fingerprint"]
        for item in run.state_timeline
    }) == 1
    assert {
        item["topology_epoch"] for item in run.state_timeline
    } == {1}
    assert control._topology_epoch == 1
    assert control._topology_reset_count == 0
    assert control._baseline_reset_count == 0
    assert control.service_rls.reset_count == 0
    assert control._metric_history_reset_count == 0
    assert control.service_rls.configuration_count == 1
    # The topology identity remains stable, while the formal model freezes at
    # the completed independent-validation handshake instead of drifting over
    # later healthy windows.
    assert set(map(len, control.baseline.snapshot().values())) == {6}
    assert len(control._healthy_history) == 3


def test_real_semantic_topology_change_resets_once_at_window_61(tmp_path):
    archive = _healthy_archive_with_window_snapshots(
        tmp_path / "topology-change",
        topology_change_at=61,
    )
    control = FinalControlPlane(_config())

    run = control.run(archive)

    assert run.processed_window_count == 120
    assert [
        item["topology_epoch"] for item in run.state_timeline[:60]
    ] == [1] * 60
    assert [
        item["topology_epoch"] for item in run.state_timeline[60:]
    ] == [2] * 60
    assert control._topology_epoch == 2
    assert control._topology_reset_count == 1
    assert control._baseline_reset_count == 1
    assert control.service_rls.reset_count == 1
    assert control._metric_history_reset_count == 1
    assert control.service_rls.configuration_count == 2
    # The real topology change creates exactly one fresh calibration epoch;
    # that epoch then freezes at the same deterministic readiness boundary.
    assert set(map(len, control.baseline.snapshot().values())) == {6}
    assert len(control._healthy_history) == 3
    assert control._last_calibration_reset_reason \
        == "topology_fingerprint_changed"


def test_service_rls_ignores_snapshot_identity_when_semantics_are_equal():
    topology_a = TopologySnapshot(
        schema_version="1.0",
        snapshot_id="snapshot-a",
        valid_from_ns=0,
        valid_to_ns=_NS,
        cluster_id="cluster",
        services=["ns::caller", "ns::callee"],
        call_edges=[TopologyEdge(
            src_service="caller",
            dst_service="callee",
            relation_type="call",
            src_namespace="ns",
            dst_namespace="ns",
            protocol="tcp",
            directed=True,
        )],
        host_edges=[],
        resource_edges=[],
        service_nodes=[],
        runtime_identity_fingerprints=["runtime-a"],
    )
    topology_b = replace(
        topology_a,
        snapshot_id="snapshot-b",
        valid_from_ns=_NS,
        valid_to_ns=2 * _NS,
        services=list(reversed(topology_a.services)),
        call_edges=list(reversed(topology_a.call_edges)),
        inventory_revision_id="different-non-semantic-revision",
        call_edge_provider_fingerprint="different-window-provider",
    )
    graph_a = allowed_service_graph(topology_a)
    graph_b = allowed_service_graph(topology_b)
    rls = ServiceRLS(replace(
        _config(),
        service_lags=(1,),
        service_min_training_updates=1,
    ))
    state_a = {
        "cluster::ns::caller": 1.0,
        "cluster::ns::callee": 2.0,
    }
    state_b = {
        "cluster::ns::caller": 1.5,
        "cluster::ns::callee": 2.5,
    }

    rls.update(1, state_a, graph_a)
    rls.update(2, state_b, graph_b)

    assert graph_a.snapshot_id != graph_b.snapshot_id
    assert graph_a.topology_fingerprint == graph_b.topology_fingerprint
    assert rls.reset_count == 0
    assert rls.configuration_count == 1
    assert {
        item["valid_training_rows"]
        for item in rls.readiness().values()
    } == {1}


def test_runtime_identity_change_recalibrates_without_new_topology_epoch(
    tmp_path,
):
    archive = _healthy_archive_with_window_snapshots(
        tmp_path / "runtime-change",
        window_count=8,
        runtime_change_at=5,
    )
    control = FinalControlPlane(_config())

    run = control.run(archive)

    assert run.processed_window_count == 8
    assert {
        item["topology_epoch"] for item in run.state_timeline
    } == {1}
    assert control._topology_reset_count == 0
    assert control._runtime_identity_reset_count == 1
    assert control._baseline_reset_count == 1
    assert control.service_rls.reset_count == 1
    assert control._metric_history_reset_count == 1
    assert set(map(len, control.baseline.snapshot().values())) == {4}
    assert control._last_calibration_reset_reason \
        == "runtime_identity_fingerprint_changed"


def test_soft_context_keeps_using_its_frozen_topology_version(tmp_path):
    config = _config()
    old = _topology(snapshot_name="old-layout", valid_to_ns=10 * _NS)
    new = _topology(
        snapshot_name="new-layout",
        valid_from_ns=10 * _NS,
        valid_to_ns=100 * _NS,
    )
    writer = CollectionArchiveWriter(
        tmp_path / "frozen-topology",
        dataset_id=fingerprint({"dataset": "frozen-topology"}),
        collection_contract=config.collection_contract,
        source_description=config.collection_contract["source_description"],
        collection_metadata=_collection_metadata(config),
    )
    for sequence in range(1, 13):
        if sequence == 1:
            window = _window(sequence, topology=old)
        elif sequence == 11:
            window = CollectedWindow.create(
                sequence=sequence,
                window_start_ns=(sequence - 1) * _NS,
                window_end_ns=sequence * _NS,
                node_metrics=_node_records(sequence),
                topology_events=(new,),
                residual_source_record_ids=_residual_source_ids(sequence),
                collection_metadata=_collection_metadata(config),
            )
        else:
            window = _window(sequence)
        writer.append(window)
    run = FinalControlPlane(config).run(writer.seal())
    after_change = next(
        item for item in run.state_timeline
        if item["timestamp_ns"] == 11 * _NS
    )
    assert after_change["topology_snapshot_id"] == old.snapshot_id
    assert run.results[0].candidate_graph.topology_snapshot_id == old.snapshot_id


@pytest.mark.parametrize("metadata", [
    {"Incident-ID": "cpu-payment-001"},
    {"labels": {"service": "payment", "fault": "CPU"}},
    {"safe": {"Root-Service": "payment"}},
    {"safe": "target_service=payment"},
    {"note": "payment"},
])
def test_adversarial_labels_and_string_injection_are_rejected(metadata):
    with pytest.raises(GroundTruthFieldError):
        CollectedWindow.create(
            sequence=1,
            window_start_ns=0,
            window_end_ns=_NS,
            node_metrics=_node_records(1),
            topology_events=(_topology(),),
            residual_source_record_ids=_residual_source_ids(1),
            collection_metadata=metadata,
        )


def test_dataset_and_free_text_source_cannot_carry_incident_labels(tmp_path):
    config = _config()
    with pytest.raises(CollectionArchiveError, match="dataset_id"):
        CollectionArchiveWriter(
            tmp_path / "unsafe-dataset",
            dataset_id="cpu-payment-001",
            collection_contract=config.collection_contract,
            source_description=config.collection_contract["source_description"],
            collection_metadata=_collection_metadata(config),
        )
    with pytest.raises(CollectionArchiveError, match="source_description"):
        CollectionArchiveWriter(
            tmp_path / "unsafe-source",
            dataset_id=fingerprint({"dataset": "safe"}),
            collection_contract=config.collection_contract,
            source_description="target_service=payment",
            collection_metadata=_collection_metadata(config),
        )


def test_legacy_engine_window_without_opaque_residual_lineage_cannot_cross():
    legacy = SimpleNamespace(
        window_start_ns=0,
        window_end_ns=_NS,
        node_metric_records=_node_records(1),
        edge_metric_records=(),
        topology_snapshot_events=(_topology(),),
        evidence_observations_available_by_cutoff=(),
        source_record_ids=("incident-cpu-payment-001",),
        replay_sequence_number=1,
    )
    with pytest.raises(TypeError, match="residual_source_record_ids"):
        from_engine_window(legacy, collection_metadata=_collection_metadata())


def test_unknown_wrong_target_and_overlapping_burst_sources_fail_closed(tmp_path):
    config = _config()

    def writer(name: str) -> CollectionArchiveWriter:
        return CollectionArchiveWriter(
            tmp_path / name,
            dataset_id=fingerprint({"dataset": name}),
            collection_contract=config.collection_contract,
            source_description=config.collection_contract["source_description"],
            collection_metadata=_collection_metadata(config),
        )

    unknown_writer = writer("unknown-burst")
    for sequence in range(1, 12):
        unknown_writer.append(_window(sequence))
    with pytest.raises(CollectionArchiveError, match="unknown Burst channel"):
        unknown_writer.append(_window(12, evidence=(_unknown_evidence(),)))

    wrong_target = replace(
        _evidence(),
        target_id="cluster::host::node-a::cpu_psi",
    )
    wrong_writer = writer("wrong-burst-target")
    for sequence in range(1, 12):
        wrong_writer.append(_window(sequence))
    with pytest.raises(CollectionArchiveError, match="entity type mismatch"):
        wrong_writer.append(_window(12, evidence=(wrong_target,)))

    residual_source_id = _residual_source_ids(11)[0]
    overlap_sources = [residual_source_id]
    overlap = replace(
        _evidence(),
        source_record_ids=overlap_sources,
        provenance={
            **_evidence().provenance,
            "source_set_fingerprint": fingerprint(sorted(overlap_sources)),
        },
    )
    overlap_writer = writer("overlapping-burst-source")
    for sequence in range(1, 12):
        overlap_writer.append(_window(sequence))
    with pytest.raises(CollectionArchiveError, match="overlap across collected windows"):
        overlap_writer.append(_window(12, evidence=(overlap,)))


def _dns_edge_records(sequence: int) -> tuple[EdgeMetricRecord, ...]:
    timestamp_ns = (sequence - 1) * _NS
    values = {
        "dns_query_count": (20.0, 20),
        "dns_latency_p95": (5000.0, 20),
        "dns_failure_rate": (1.0, 20),
    }
    specs = {
        spec.metric_name: spec for spec in experimental_dns_metric_roles()
    }
    return tuple(
        EdgeMetricRecord(
            schema_version=METRIC_RECORD_SCHEMA_VERSION,
            timestamp_ns=timestamp_ns,
            window_sec=1,
            cluster_id="cluster",
            namespace="ns",
            src_service="payment",
            dst_service="kube-dns",
            src_pod_uid=None,
            dst_pod_uid=None,
            src_node="node-a",
            dst_node="node-a",
            protocol="dns",
            metric_name=name,
            value=value,
            valid=True,
            invalid_reason=None,
            unit=specs[name].unit,
            sample_count=sample_count,
            coverage=1.0,
            event_loss_rate=0.0,
            mapping_quality=1.0,
            source="final_window_aggregation",
            metric_kind=specs[name].metric_kind,
            scope="service_pair",
            histogram_upper_bound=None,
            histogram_is_inf_bucket=False,
            histogram_is_cumulative=None,
            quantile=specs[name].quantile,
        )
        for name, (value, sample_count) in sorted(values.items())
    )


def _legacy_dns_topology() -> TopologySnapshot:
    call = TopologyEdge(
        src_service="payment",
        dst_service="kube-dns",
        relation_type="call",
        src_namespace="ns",
        dst_namespace="ns",
        protocol="dns",
        directed=True,
    )
    base = _topology()
    services = ["ns::payment", "ns::kube-dns"]
    placements = [
        *base.service_nodes,
        ServiceNodePlacement(
            namespace="ns",
            service_name="kube-dns",
            node_name="node-a",
            pod_uid=None,
        ),
    ]
    return replace(
        base,
        services=services,
        call_edges=[call],
        service_nodes=placements,
        structure_fingerprint=fingerprint({
            "cluster": base.cluster_id,
            "services": services,
            "calls": [call.to_dict()],
            "hosts": [item.to_dict() for item in base.host_edges],
            "bindings": [
                item.to_dict() for item in base.service_resources
            ],
        }),
    )


def _legacy_dns_node_records(
    sequence: int,
) -> tuple[NodeMetricRecord, ...]:
    records = _node_records(sequence)
    service_records = tuple(
        replace(record, service_name="kube-dns")
        for record in records
        if record.scope == "service"
    )
    return (*records, *service_records)


def test_formal_scope_and_contract_are_dns_free_9_4_3():
    payload = yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    )
    config = FinalControlConfig.from_dict(payload)
    contract = config.collection_contract
    roles = contract["normal_metric_roles"]

    assert len(config.calibration_required_root_coordinates) == 100
    assert len(config.formal_service_entity_ids) == 11
    assert len(config.formal_host_entity_ids) == 1
    assert len(config.formal_tcp_edge_entity_ids) == 15
    assert not any(
        entity_id.endswith("::kube-dns")
        for entity_id in config.formal_service_entity_ids
    )
    assert not any(
        entity_id.endswith("::loadgenerator")
        for entity_id in config.formal_service_entity_ids
    )
    assert not any(
        "::dns::" in coordinate
        for coordinate in config.calibration_required_root_coordinates
    )
    assert "DNS" not in config.group_penalties
    assert sum(role["entity_type"] == "service" for role in roles) == 9
    assert sum(role["entity_type"] == "host" for role in roles) == 4
    edge_roles = [
        role for role in roles if role["entity_type"] == "edge"
    ]
    assert len(edge_roles) == 3
    assert {tuple(role["protocols"]) for role in edge_roles} == {("tcp",)}
    assert {role["metric_name"] for role in edge_roles} == {
        "edge_request_count", "edge_latency_p95", "edge_failure_rate",
    }
    assert all(
        role["root_category"] != "DNS"
        for role in contract["burst_channel_roles"]
    )


def _formal_service_scope_topology(
    config: FinalControlConfig,
) -> TopologySnapshot:
    services = [
        entity_id.split("::", 1)[1]
        for entity_id in sorted(config.formal_service_entity_ids)
    ]
    services.append("kube-system::kube-dns")
    placements = [
        ServiceNodePlacement(
            namespace=service.split("::", 1)[0],
            service_name=service.split("::", 1)[1],
            node_name="proberca-ob-control-plane",
            pod_uid=f"pod-{index}",
        )
        for index, service in enumerate(services)
    ]
    runtime_by_service = {
        entity_id: [fingerprint({"runtime": entity_id})]
        for entity_id in (
            *sorted(config.formal_service_entity_ids),
            "kind-proberca-ob::kube-system::kube-dns",
        )
    }
    return TopologySnapshot(
        schema_version="1.0",
        snapshot_id="formal-scope-snapshot",
        valid_from_ns=0,
        valid_to_ns=10 * _NS,
        cluster_id="kind-proberca-ob",
        services=services,
        call_edges=[],
        host_edges=[],
        resource_edges=[],
        service_nodes=placements,
        runtime_identity_fingerprints=sorted({
            identity
            for identities in runtime_by_service.values()
            for identity in identities
        }),
        service_runtime_identity_fingerprints=runtime_by_service,
        structure_fingerprint=fingerprint({
            "services": services,
            "placements": [
                item.to_dict() for item in placements
            ],
        }),
    )


def test_frozen_scope_excludes_infrastructure_from_models_and_alerts():
    config = FinalControlConfig.from_dict(yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    ))
    full_graph = allowed_service_graph(
        _formal_service_scope_topology(config)
    )
    graph = formal_service_graph(
        _formal_service_scope_topology(config), config,
    )
    infrastructure = "kind-proberca-ob::kube-system::kube-dns"
    frontend = "kind-proberca-ob::online-boutique::frontend"

    assert infrastructure in full_graph.services
    assert infrastructure not in graph.services
    assert len(graph.services) == 11

    snapshot = _formal_service_scope_topology(config)
    excluded_id = "kind-proberca-ob::kube-system::kube-dns"
    excluded_changed = dict(snapshot.service_runtime_identity_fingerprints)
    excluded_changed[excluded_id] = [fingerprint({"runtime": "replacement"})]
    excluded_snapshot = replace(
        snapshot,
        runtime_identity_fingerprints=sorted({
            identity
            for identities in excluded_changed.values()
            for identity in identities
        }),
        service_runtime_identity_fingerprints=excluded_changed,
    )
    assert (
        formal_service_graph(snapshot, config).runtime_identity_fingerprint
        == formal_service_graph(
            excluded_snapshot, config
        ).runtime_identity_fingerprint
    )

    formal_id = sorted(config.formal_service_entity_ids)[0]
    formal_changed = dict(snapshot.service_runtime_identity_fingerprints)
    formal_changed[formal_id] = [fingerprint({"runtime": "replacement"})]
    formal_snapshot = replace(
        snapshot,
        runtime_identity_fingerprints=sorted({
            identity
            for identities in formal_changed.values()
            for identity in identities
        }),
        service_runtime_identity_fingerprints=formal_changed,
    )
    assert (
        formal_service_graph(snapshot, config).runtime_identity_fingerprint
        != formal_service_graph(
            formal_snapshot, config
        ).runtime_identity_fingerprint
    )

    observations = {
        "infrastructure-latency": NormalizedObservation(
            metric=MetricNode(
                node_id=f"{infrastructure}::request_latency_p95",
                entity_id=infrastructure,
                entity_type="service",
                metric_name="request_latency_p95",
                role="request_latency",
                root_category=None,
                root_eligible=False,
            ),
            signed_z=1000.0,
            anomaly=1000.0,
            quality=1.0,
            source_record_id="source:" + "3" * 64,
            baseline_center=0.0,
            baseline_scale=1.0,
            scale_source="mad",
        ),
        "frontend-latency": NormalizedObservation(
            metric=MetricNode(
                node_id=f"{frontend}::request_latency_p95",
                entity_id=frontend,
                entity_type="service",
                metric_name="request_latency_p95",
                role="request_latency",
                root_category=None,
                root_eligible=False,
            ),
            signed_z=12.0,
            anomaly=12.0,
            quality=1.0,
            source_record_id="source:" + "4" * 64,
            baseline_center=0.0,
            baseline_scale=1.0,
            scale_source="mad",
        ),
    }
    control = FinalControlPlane(config)
    service_scores, edge_scores = control._scores(observations, graph)

    assert set(service_scores) == {frontend}
    assert service_scores[frontend] == pytest.approx(6.0)
    assert edge_scores == {}
    for _ in range(2):
        soft, candidate, hard = control._advance_alert_counters(
            service_scores, edge_scores,
        )
    assert ("service", frontend) in candidate
    assert ("service", frontend) not in hard
    soft, candidate, hard = control._advance_alert_counters(
        service_scores, edge_scores,
    )
    assert ("service", frontend) in hard
    assert ("service", infrastructure) not in hard
    assert ("service", frontend) in soft
    assert ("service", infrastructure) not in soft

    with pytest.raises(
        ValueError, match="at least one valid alert seed",
    ):
        build_candidate_graph(
            graph=graph,
            service_strengths={},
            seed_services={infrastructure},
            seed_edges=set(),
            config=config,
        )


def test_out_of_scope_service_records_keep_diagnostic_scope_flags():
    config = FinalControlConfig.from_dict(yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    ))
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    records = tuple(
        replace(
            record,
            cluster_id="kind-proberca-ob",
            namespace="kube-system",
            service_name="kube-dns",
        )
        for record in _node_records(1)
        if record.scope == "service"
    )

    observations, raw = resolver.normalize_window(
        SimpleNamespace(node_metrics=records, edge_metrics=()),
        baseline,
    )

    assert observations == {}
    assert raw == {}
    assert len(resolver.last_validity) == 9
    for item in resolver.last_validity.values():
        assert item["formal_scope"] == "excluded"
        assert item["alert_eligible"] is False
        assert item["root_eligible"] is False
        assert item["readiness_required"] is False
        assert item["exclusion_reason"] == "excluded_from_formal_scope"


def test_formal_fault_matrix_keeps_tcp_and_excludes_dns():
    from scripts.run_final_fault_matrix import (
        HOST_MEMORY_HIGH_BYTES,
        HOST_MEMORY_MAX_BYTES,
        HOST_MEMORY_PILOT_BYTES,
        experiment_specs,
    )

    specs = experiment_specs()
    assert any(item["fault_type"] == "tcp_edge" for item in specs)
    assert all(item["fault_type"] != "dns_edge" for item in specs)
    assert all(item["root_category"] != "DNS" for item in specs)
    assert HOST_MEMORY_PILOT_BYTES == 3 * 1024 * 1024 * 1024
    assert HOST_MEMORY_HIGH_BYTES < HOST_MEMORY_PILOT_BYTES
    assert HOST_MEMORY_PILOT_BYTES < HOST_MEMORY_MAX_BYTES


def test_dns_anomaly_is_marked_excluded_and_cannot_alert():
    config = FinalControlConfig()
    resolver = MetricResolver(config)
    baseline = RobustBaselineStore(config)
    dns_edge = "cluster::ns::payment->kube-dns::dns"
    window = SimpleNamespace(
        node_metrics=(), edge_metrics=_dns_edge_records(1),
    )

    observations, raw = resolver.normalize_window(window, baseline)

    assert observations == {}
    assert raw == {}
    assert set(resolver.last_validity) == {
        f"{dns_edge}::dns_query_count",
        f"{dns_edge}::dns_latency_p95",
        f"{dns_edge}::dns_failure_rate",
    }
    assert {
        item["exclusion_reason"]
        for item in resolver.last_validity.values()
    } == {"excluded_from_formal_rca"}
    graph = allowed_service_graph(_legacy_dns_topology())
    assert graph.physical_edges == ()
    control = FinalControlPlane(config)
    service_scores, edge_scores = control._scores(observations, graph)
    assert service_scores == {}
    assert edge_scores == {}
    soft, candidate, hard = control._advance_alert_counters(
        service_scores, edge_scores,
    )
    assert soft == set()
    assert candidate == set()
    assert hard == set()


def test_formal_candidate_graph_keeps_tcp_and_discards_dns_edges():
    caller = "cluster::ns::caller"
    callee = "cluster::ns::callee"
    tcp_edge = "cluster::ns::caller->callee::tcp"
    calls = [
        TopologyEdge(
            "caller", "callee", "call", "ns", "ns",
            protocol=protocol, directed=True,
        )
        for protocol in ("tcp", "dns")
    ]
    snapshot = TopologySnapshot(
        schema_version="1.0",
        snapshot_id="snapshot",
        valid_from_ns=0,
        valid_to_ns=_NS,
        cluster_id="cluster",
        services=["ns::caller", "ns::callee"],
        call_edges=calls,
        host_edges=[],
        resource_edges=[],
        service_nodes=[
            ServiceNodePlacement(
                namespace="ns",
                service_name=service,
                node_name="node-a",
                pod_uid=None,
            )
            for service in ("caller", "callee")
        ],
        structure_fingerprint=fingerprint({
            "cluster": "cluster",
            "services": ["ns::caller", "ns::callee"],
            "calls": [item.to_dict() for item in calls],
            "hosts": [],
            "bindings": [],
        }),
    )
    graph = allowed_service_graph(snapshot)

    candidate = build_candidate_graph(
        graph=graph,
        service_strengths={(caller, callee): 1.0},
        seed_services={caller, callee},
        seed_edges={tcp_edge},
        config=FinalControlConfig(),
    )

    assert {item[3] for item in graph.physical_edges} == {"tcp"}
    assert candidate.seed_edges == (tcp_edge,)
    assert candidate.edges == (tcp_edge,)


def test_tcp_edge_still_triggers_soft_and_hard_independently():
    control = FinalControlPlane(FinalControlConfig())
    tcp_edge = "cluster::ns::caller->callee::tcp"

    for _ in range(2):
        soft, candidate, hard = control._advance_alert_counters(
            {}, {tcp_edge: 3.5},
        )
        assert ("edge", tcp_edge) not in soft
        assert ("edge", tcp_edge) not in candidate
        assert ("edge", tcp_edge) not in hard
    soft, candidate, hard = control._advance_alert_counters(
        {}, {tcp_edge: 3.5},
    )
    assert ("edge", tcp_edge) in soft
    assert ("edge", tcp_edge) not in candidate
    assert ("edge", tcp_edge) not in hard

    control = FinalControlPlane(FinalControlConfig())
    soft, candidate, hard = control._advance_alert_counters(
        {}, {tcp_edge: 6.0},
    )
    assert ("edge", tcp_edge) not in candidate
    assert ("edge", tcp_edge) not in hard
    soft, candidate, hard = control._advance_alert_counters(
        {}, {tcp_edge: 6.0},
    )
    assert ("edge", tcp_edge) in candidate
    assert ("edge", tcp_edge) not in hard
    soft, candidate, hard = control._advance_alert_counters(
        {}, {tcp_edge: 6.0},
    )
    assert ("edge", tcp_edge) in hard


def test_tcp_edge_runs_av_residual_burst_penalty_and_fista():
    config = FinalControlConfig(
        l1_penalty=0.01,
        fista_tolerance=1.0e-10,
    )
    control = FinalControlPlane(config)
    tcp_edge = "cluster::ns::caller->callee::tcp"
    count_id = f"{tcp_edge}::edge_request_count"
    latency_id = f"{tcp_edge}::edge_latency_p95"
    count_metric = MetricNode(
        node_id=count_id,
        entity_id=tcp_edge,
        entity_type="edge",
        metric_name="edge_request_count",
        role="edge_count",
        root_category=None,
        root_eligible=False,
    )
    latency_metric = MetricNode(
        node_id=latency_id,
        entity_id=tcp_edge,
        entity_type="edge",
        metric_name="edge_latency_p95",
        role="edge_latency",
        root_category="TCP",
        root_eligible=True,
    )
    ready = MetricTargetReadiness(
        target_metric=latency_id,
        root_eligible=True,
        allowed_feature_count=1,
        valid_training_rows=20,
        minimum_training_rows=4,
        effective_rank=1,
        raw_design_rank_ratio=1.0,
        condition_number=1.0,
        regularized_gram_condition_number=1.0,
        ready=True,
        not_ready_reason=None,
    )
    model = MetricPropagationModel(
        node_ids=(count_id, latency_id),
        lags=(1,),
        coefficients={(latency_id, count_id, 1): 0.5},
        semantic_mask=((latency_id, count_id),),
        training_rows=20,
        healthy_cutoff_ns=4 * _NS,
        target_readiness={latency_id: ready},
    )
    candidate = CandidateEntityGraph(
        seed_services=(),
        seed_edges=(tcp_edge,),
        services=("cluster::ns::caller", "cluster::ns::callee"),
        hosts=(),
        edges=(tcp_edge,),
        strong_service_relations=(),
        topology_snapshot_id="snapshot",
    )
    observation = NormalizedObservation(
        metric=latency_metric,
        signed_z=6.0,
        anomaly=6.0,
        quality=1.0,
        source_record_id="source:" + "1" * 64,
        baseline_center=0.0,
        baseline_scale=1.0,
        scale_source="mad",
    )
    evidence_sources = ["source:" + "2" * 64]
    evidence = EvidenceObservationRecord(
        schema_version="1.0",
        evidence_id=fingerprint({"evidence": "tcp-rtt"}),
        timestamp_ns=5 * _NS + _NS // 2,
        evidence_window_start_ns=5 * _NS,
        evidence_window_end_ns=6 * _NS,
        analysis_cutoff_ns=6 * _NS,
        cluster_id="cluster",
        namespace="ns",
        target_type="shock",
        target_id=tcp_edge,
        channel_id="tcp.rtt_p95",
        source_type="burst_event",
        normalized_strength=0.8,
        observation_quality=1.0,
        reliability_weight=1.0,
        source_record_ids=evidence_sources,
        source_object_ids=[],
        independent_from_residual=True,
        provenance={
            "calibration_id": fingerprint({"calibration": "tcp"}),
            "collector_build_fingerprint": _BUILD_FINGERPRINT,
            "source_set_fingerprint": fingerprint(evidence_sources),
        },
        config_fingerprint=config.collection_contract[
            "burst_config_fingerprint"
        ],
    )
    control._soft = SimpleNamespace(
        metric_model=model,
        metrics={count_id: count_metric, latency_id: latency_metric},
        candidate_graph=candidate,
        seed_services=set(),
        seed_edges={tcp_edge},
    )
    control._hard = SimpleNamespace(
        sequence=5,
        timestamp_ns=5 * _NS,
        analysis_cutoff_ns=6 * _NS,
        observations={latency_id: observation},
    )
    control._signed_history = {4: {count_id: 2.0}}
    control._evidence = [evidence]
    control._dataset_fingerprint = fingerprint({"dataset": "tcp-path"})

    result = control._diagnose()

    assert len(result.candidates) == 1
    candidate_score = result.candidates[0]
    assert candidate_score.entity_id == tcp_edge
    assert candidate_score.root_category == "TCP"
    assert candidate_score.signed_residuals[latency_id] \
        == pytest.approx(5.0)
    assert candidate_score.burst_evidence_strength == pytest.approx(0.8)
    assert candidate_score.effective_group_penalty == pytest.approx(
        candidate_score.base_group_penalty
        / (1.0 + config.burst_eta * 0.8)
    )
    assert candidate_score.score > 0.0
    assert all(item.root_category != "DNS" for item in result.candidates)

    # Model-valid but exposure-ineligible root observations remain available
    # to A_v, yet cannot become an incident-time FISTA coordinate.
    control._hard = SimpleNamespace(
        sequence=5,
        timestamp_ns=5 * _NS,
        analysis_cutoff_ns=6 * _NS,
        observations={
            latency_id: replace(observation, alert_eligible=False),
        },
    )
    with pytest.raises(
        ControlPlaneError, match="candidate graph has no observed root coordinates",
    ):
        control._diagnose()


def test_diagnosis_removes_only_strictly_prior_resource_level_drift():
    config = FinalControlConfig(
        baseline_min_windows=6,
        resource_alert_history_windows=30,
        soft_consecutive_windows=3,
        l1_penalty=0.01,
        fista_tolerance=1.0e-10,
    )
    control = FinalControlPlane(config)
    service = "cluster::ns::cartservice"
    node_id = f"{service}::futex_wait_time_rate"
    metric = MetricNode(
        node_id=node_id,
        entity_id=service,
        entity_type="service",
        metric_name="futex_wait_time_rate",
        role="service_lock",
        root_category="Lock",
        root_eligible=True,
    )
    ready = MetricTargetReadiness(
        target_metric=node_id,
        root_eligible=True,
        allowed_feature_count=0,
        valid_training_rows=0,
        minimum_training_rows=0,
        effective_rank=0,
        raw_design_rank_ratio=0.0,
        condition_number=None,
        regularized_gram_condition_number=None,
        ready=True,
        not_ready_reason=None,
    )
    model = MetricPropagationModel(
        node_ids=(node_id,),
        lags=(1, 2),
        coefficients={},
        semantic_mask=(),
        training_rows=0,
        healthy_cutoff_ns=30 * _NS,
        target_readiness={node_id: ready},
    )
    candidate = CandidateEntityGraph(
        seed_services=(service,),
        seed_edges=(),
        services=(service,),
        hosts=(),
        edges=(),
        strong_service_relations=(),
        topology_snapshot_id="snapshot",
    )
    observation = NormalizedObservation(
        metric=metric,
        signed_z=108.0,
        anomaly=108.0,
        quality=1.0,
        source_record_id="source:" + "3" * 64,
        baseline_center=0.0,
        baseline_scale=1.0,
        scale_source="mad",
    )
    control._soft = SimpleNamespace(
        soft_sequence=33,
        metric_model=model,
        metrics={node_id: metric},
        candidate_graph=candidate,
        seed_services={service},
        seed_edges=set(),
    )
    control._hard = SimpleNamespace(
        sequence=34,
        timestamp_ns=34 * _NS,
        analysis_cutoff_ns=35 * _NS,
        observations={node_id: observation},
    )
    # The three Soft windows 31--33 are excluded.  Only the strictly prior
    # 30-window level is used, without changing the frozen baseline itself.
    control._signed_history = {
        sequence: {node_id: 100.0}
        for sequence in range(1, 31)
    } | {
        31: {node_id: 106.0},
        32: {node_id: 107.0},
        33: {node_id: 108.0},
    }
    control._evidence = []
    control._dataset_fingerprint = fingerprint({"dataset": "resource-drift"})

    result = control._diagnose()

    score = result.candidates[0]
    assert score.entity_id == service
    assert score.root_category == "Lock"
    assert score.signed_residuals[node_id] == pytest.approx(8.0)
    assert result.model_metadata[
        "preincident_resource_residual_offsets"
    ][node_id] == pytest.approx(100.0)
    assert result.model_metadata[
        "preincident_resource_residual_offset_samples"
    ][node_id] == 30
    assert "preincident_resource_offset" in result.residual_signal


def test_legacy_dns_archive_is_readable_but_dns_is_excluded(tmp_path):
    config = _config()
    legacy_contract = _legacy_v3_dns_contract(config)
    metadata = {
        "collector_build_fingerprint": _BUILD_FINGERPRINT,
        "aggregation_config_fingerprint": legacy_contract[
            "aggregation_config_fingerprint"
        ],
        "burst_config_fingerprint": legacy_contract[
            "burst_config_fingerprint"
        ],
    }
    writer = CollectionArchiveWriter(
        tmp_path / "legacy-dns-archive",
        dataset_id=fingerprint({"dataset": "legacy-dns-archive"}),
        collection_contract=legacy_contract,
        source_description=legacy_contract["source_description"],
        collection_metadata=metadata,
    )
    topology = _legacy_dns_topology()
    for sequence in range(1, 13):
        writer.append(CollectedWindow.create(
            sequence=sequence,
            window_start_ns=(sequence - 1) * _NS,
            window_end_ns=sequence * _NS,
            node_metrics=_legacy_dns_node_records(sequence),
            edge_metrics=_dns_edge_records(sequence),
            topology_events=(topology,) if sequence == 1 else (),
            burst_evidence=(),
            residual_source_record_ids=_residual_source_ids(sequence),
            collection_metadata=metadata,
        ))
    archive = CollectionArchive.load(writer.seal().root)
    control = FinalControlPlane(config)

    run = control.run(archive)

    assert archive.collection_contract_fingerprint \
        != config.collection_contract_fingerprint
    assert run.collection_contract_fingerprint \
        == archive.collection_contract_fingerprint
    assert run.calibration_readiness["ready"] is True
    assert run.calibration_readiness["Av_required_count"] \
        == len(_required_root_coordinates())
    assert {
        item["exclusion_reason"]
        for node_id, item in control.resolver.last_validity.items()
        if "::dns::" in node_id
    } == {"excluded_from_formal_rca"}


def test_fault_context_cleanup_is_idempotent(tmp_path):
    import scripts.run_final_fault_matrix as runner

    calls = []
    context = runner.FaultContext(tmp_path, tmp_path)
    context.add_cleanup(lambda: calls.append("cleanup"))

    context.cleanup()
    context.cleanup()

    assert calls == ["cleanup"]


def test_host_nic_fault_targets_node_side_formal_pod_veths(monkeypatch):
    import scripts.run_final_fault_matrix as runner

    commands = []
    cleanup_callbacks = []

    class Context:
        metadata = {}

        @staticmethod
        def add_cleanup(callback):
            cleanup_callbacks.append(callback)

    monkeypatch.setattr(
        runner, "formal_service_names", lambda: ("alpha", "beta")
    )
    monkeypatch.setattr(
        runner,
        "pod_peer_device",
        lambda service: {"alpha": "veth-a", "beta": "veth-b"}[service],
    )
    monkeypatch.setattr(
        runner,
        "node_command",
        lambda arguments, **kwargs: commands.append(
            (tuple(arguments), kwargs)
        ),
    )
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    runner.host_nic(Context(), 60)

    assert [item[0][4] for item in commands] == ["veth-a", "veth-b"]
    assert all(item[0][:4] == ("tc", "qdisc", "replace", "dev")
               for item in commands)
    assert Context.metadata["devices"] == ["veth-a", "veth-b"]
    assert len(cleanup_callbacks) == 1

    cleanup_callbacks[0]()
    assert [item[0][4] for item in commands[-2:]] == ["veth-a", "veth-b"]
    assert all(item[0][:4] == ("tc", "qdisc", "del", "dev")
               for item in commands[-2:])


def test_formal_service_names_reads_checked_in_control_config():
    import scripts.run_final_fault_matrix as runner

    payload = yaml.safe_load(
        runner.CONTROL_CONFIG.read_text(encoding="utf-8")
    )
    config = FinalControlConfig.from_dict(payload)
    expected = tuple(sorted(
        entity_id.rsplit("::", 1)[1]
        for entity_id in config.formal_service_entity_ids
    ))

    assert runner.formal_service_names() == expected


def test_pod_peer_device_resolves_link_inside_kind_node(monkeypatch):
    import scripts.run_final_fault_matrix as runner

    monkeypatch.setattr(
        runner, "service_info", lambda _service: {"pid": 1234}
    )
    monkeypatch.setattr(
        runner,
        "run",
        lambda _arguments: SimpleNamespace(
            stdout=json.dumps([{"ifname": "eth0", "link_index": 17}])
        ),
    )
    monkeypatch.setattr(
        runner,
        "node_command",
        lambda _arguments: SimpleNamespace(stdout=json.dumps([
            {"ifindex": 16, "ifname": "unrelated"},
            {"ifindex": 17, "ifname": "veth-target@if2"},
        ])),
    )

    assert runner.pod_peer_device("alpha") == "veth-target"


def test_fault_actor_failsafe_tracks_capture_windows_not_wall_budget():
    import scripts.run_final_fault_matrix as runner

    observed = []

    class Context:
        metadata = {}

        @staticmethod
        def start_actor(*args, **kwargs):
            observed.append((args, kwargs))

        @staticmethod
        def write_cgroup_control(*_args, **_kwargs):
            return None

    runner.actor_fault(
        "memory", service=None,
    )(Context(), 60)
    runner.service_memory(Context(), 60)

    assert [item[1]["duration"] for item in observed] == [90, 90]
    assert all(
        item[1]["duration"]
        == 60 + runner.FAULT_ACTOR_FAILSAFE_GRACE_SEC
        for item in observed
    )


def test_cgroup_control_replacement_restores_exact_value(tmp_path):
    import scripts.run_final_fault_matrix as runner

    control = tmp_path / "cpu.max"
    control.write_text("100000 100000\n", encoding="ascii")

    original, restore = runner.replace_text_file(
        control, "25000 100000"
    )

    assert original == "100000 100000"
    assert control.read_text(encoding="ascii") == "25000 100000\n"
    restore()
    restore()
    assert control.read_text(encoding="ascii") == "100000 100000\n"


def test_formal_pod_restart_or_identity_change_fails_experiment():
    import scripts.run_final_fault_matrix as runner

    before = {
        "alpha": {
            "container_id": "container-a",
            "pod_uid": "pod-a",
            "ready": True,
            "restart_count": 2,
        }
    }
    runner.assert_no_formal_pod_change(before, before)

    restarted = {
        "alpha": {**before["alpha"], "restart_count": 3},
    }
    with pytest.raises(runner.ExperimentError, match="identity/restart"):
        runner.assert_no_formal_pod_change(before, restarted)

    replaced = {
        "alpha": {**before["alpha"], "pod_uid": "pod-b"},
    }
    with pytest.raises(runner.ExperimentError, match="identity/restart"):
        runner.assert_no_formal_pod_change(before, replaced)


def test_formal_faults_act_on_real_paths_not_isolated_synthetic_signals(
    monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    specs = {item["fault_type"]: item for item in runner.experiment_specs()}
    controls = []
    actors = []

    class Context:
        metadata = {}

        @staticmethod
        def write_cgroup_control(service, control, value):
            controls.append((service, control, value))

        @staticmethod
        def start_actor(*args, **kwargs):
            actors.append((args, kwargs))

        @staticmethod
        def start_kubernetes_actor(**kwargs):
            actors.append(((), kwargs))

    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(runner, "block_device_id", lambda _path: "253:0")
    specs["service_cpu"]["activate"](Context(), 60)
    specs["service_memory"]["activate"](Context(), 60)

    assert controls[0] == (
        "frontend", "cpu.max", runner.SERVICE_CPU_QUOTA,
    )
    assert controls[1] == (
        "recommendationservice", "memory.high",
        str(runner.SERVICE_MEMORY_HIGH_BYTES),
    )
    # The pressure boundary must not sit below the actor's entire working set:
    # that configuration caused reclaim stalls and a liveness-driven restart
    # instead of a stable service-memory incident.
    assert runner.SERVICE_MEMORY_HIGH_BYTES \
        >= runner.SERVICE_MEMORY_ACTOR_BYTES
    assert runner.SERVICE_MEMORY_ACTOR_BYTES == 192 * 1024 * 1024
    assert runner.SERVICE_MEMORY_HIGH_BYTES == 224 * 1024 * 1024
    assert (
        runner.SERVICE_MEMORY_HIGH_BYTES
        - runner.SERVICE_MEMORY_ACTOR_BYTES
    ) == runner.SERVICE_MEMORY_HIGH_HEADROOM_BYTES
    assert Context.metadata["intervention_profile"] \
        == "service-memory-working-set-v2"
    assert actors[0][1]["service"] == "recommendationservice"
    specs["service_lock"]["activate"](Context(), 60)
    assert Context.metadata["intervention_profile"] \
        == "service-cgroup-futex-stall-v1"
    assert runner.SERVICE_LOCK_THREADS == 10
    assert actors[1][0] == ("futex",)
    assert actors[1][1]["service"] == "cartservice"
    assert actors[1][1]["arguments"] == [
        "--threads", str(runner.SERVICE_LOCK_THREADS),
        "--continuous-hold",
    ]
    assert actors[1][1]["ready_event"] == "futex_waiters_blocked"

    # IO actors are explicitly direct/synchronous in the formal spec.  This
    # checks the closure without duplicating implementation logic in a test.
    service_io = specs["service_io"]["activate"]
    service_io(Context(), 60)
    host_io = specs["host_io"]["activate"]
    host_io(Context(), 60)
    assert controls[2] == (
        "redis-cart", "io.max",
        f"253:0 wbps={runner.SERVICE_IO_WRITE_BYTES_PER_SEC}",
    )
    assert controls[3] == ("redis-cart", "cpu.max", "max 100000")
    assert "--direct" in actors[-2][1]["arguments"]
    assert "--direct" in actors[-1][1]["arguments"]


def test_service_localnet_fault_targets_service_cgroup_and_network_namespace(
    monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    actors = []

    class Context:
        metadata = {}

        @staticmethod
        def start_actor(*args, **kwargs):
            actors.append((args, kwargs))

    runner.service_localnet(Context(), 60)

    assert actors == [(('localnet',), {
        "service": "frontend",
        "network_namespace": True,
        "duration": 60 + runner.FAULT_ACTOR_FAILSAFE_GRACE_SEC,
        "arguments": ["--threads", str(runner.SERVICE_LOCALNET_THREADS)],
        "name": "service-localnet-socket",
    })]
    assert Context.metadata["intervention_profile"] \
        == "service-cgroup-local-socket-v1"


def test_host_memory_fault_uses_bounded_reclaim_cgroup_and_churn():
    import scripts.run_final_fault_matrix as runner

    created = []
    actors = []

    class Context:
        metadata = {}

        @staticmethod
        def create_host_cgroup(name, controls):
            created.append((name, controls))
            return Path("/sys/fs/cgroup/proberca-final-host-memory")

        @staticmethod
        def start_actor(*args, **kwargs):
            actors.append((args, kwargs))

    runner.host_memory(Context(), 60)

    assert created == [(
        runner.HOST_MEMORY_CGROUP_NAME,
        {
            "memory.high": str(runner.HOST_MEMORY_HIGH_BYTES),
            "memory.max": str(runner.HOST_MEMORY_MAX_BYTES),
        },
    )]
    assert actors[0][0] == ("memory",)
    assert actors[0][1]["cgroup_path"] == Path(
        "/sys/fs/cgroup/proberca-final-host-memory"
    )
    assert "--churn" in actors[0][1]["arguments"]
    assert "--bulk-fill" in actors[0][1]["arguments"]
    assert actors[0][1]["arguments"][-2:] == [
        "--ready-after-bytes", str(runner.HOST_MEMORY_READY_BYTES),
    ]
    assert runner.HOST_MEMORY_HIGH_BYTES \
        < runner.HOST_MEMORY_READY_BYTES \
        < runner.HOST_MEMORY_PILOT_BYTES
    assert runner.HOST_MEMORY_PILOT_BYTES == 3 * 1024 * 1024 * 1024
    assert runner.HOST_MEMORY_HIGH_BYTES == 1024 * 1024 * 1024
    assert runner.HOST_MEMORY_MAX_BYTES == 4 * 1024 * 1024 * 1024
    assert actors[0][1]["ready_event"] == "memory_working_set_ready"
    assert Context.metadata["intervention_profile"] \
        == "host-memory-reclaim-v4"


def test_memory_actor_reports_readiness_after_real_chunk_threshold(
    monkeypatch,
):
    import scripts.final_fault_actor as actor

    actor.STOP.clear()
    actor.COUNTERS.clear()
    monkeypatch.setattr(actor, "MEMORY_BULK_FILL_CHUNK_BYTES", 4096)
    real_memset = actor.ctypes.memset
    fills = []
    progress = []
    ready_after_fill = []

    def counted_memset(address, value, length):
        fills.append(length)
        return real_memset(address, value, length)

    monkeypatch.setattr(actor.ctypes, "memset", counted_memset)
    actor.memory_actor(
        3 * 4096,
        time.monotonic() + 0.02,
        bulk_fill=True,
        ready_after_bytes=2 * 4096,
        ready_callback=lambda: ready_after_fill.append(len(fills)),
        progress_callback=lambda touched, total: progress.append(
            (touched, total)
        ),
    )

    assert fills == [4096, 4096, 4096]
    assert ready_after_fill == [2]
    assert progress == [
        (4096, 3 * 4096),
        (2 * 4096, 3 * 4096),
        (3 * 4096, 3 * 4096),
    ]
    assert actor.COUNTERS["bytes_touched"] == 3 * 4096
    with pytest.raises(ValueError, match="readiness threshold"):
        actor.memory_actor(
            4096,
            time.monotonic(),
            ready_after_bytes=8192,
        )


def test_continuous_futex_actor_blocks_waiters_without_spin():
    import scripts.final_fault_actor as actor

    actor.STOP.clear()
    actor.COUNTERS.clear()
    ready = []
    actor.futex_actor(
        3,
        1.0,
        time.monotonic() + 0.05,
        continuous_hold=True,
        ready_callback=lambda: ready.append(True),
    )

    assert ready == [True]
    assert actor.COUNTERS["lock_holds"] == 1
    assert actor.COUNTERS["lock_waiters"] == 3
    assert actor.COUNTERS["lock_acquires"] == 3


def test_fault_signal_qualification_accepts_root_and_rejects_competitor(
    monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    values = {
        ("normal", "io_psi"): [0.0] * 10,
        ("abnormal", "io_psi"): [0.25] * 10,
        ("normal", "cpu_throttle_ratio"): [0.0] * 10,
        ("abnormal", "cpu_throttle_ratio"): [0.0] * 10,
    }

    def metric_values(archive, selector):
        return values[(str(archive), selector["metric_name"])], 10

    monkeypatch.setattr(runner, "_metric_values", metric_values)
    requirement = {
        "metric_name": "io_psi",
        "scope": "service",
        "service_name": "redis-cart",
        "minimum_abnormal_median": 0.05,
        "minimum_median_lift": 0.04,
        "competitors": [{
            "metric_name": "cpu_throttle_ratio",
            "scope": "service",
            "service_name": "redis-cart",
            "maximum_abnormal_median": 0.10,
        }],
    }

    result = runner.qualify_fault_signal("normal", "abnormal", requirement)
    assert result["accepted"] is True
    assert result["median_lift"] == pytest.approx(0.25)

    values[("abnormal", "cpu_throttle_ratio")] = []
    result = runner.qualify_fault_signal("normal", "abnormal", requirement)
    assert result["competitors"][0]["abnormal"] is None

    values[("abnormal", "cpu_throttle_ratio")] = [0.8] * 10
    with pytest.raises(
        runner.ExperimentError, match="cpu_throttle_ratio.*dominates",
    ):
        runner.qualify_fault_signal("normal", "abnormal", requirement)


def test_problem_fault_specs_freeze_declared_root_signal_gates():
    import scripts.run_final_fault_matrix as runner

    specs = {item["fault_type"]: item for item in runner.experiment_specs()}
    assert {
        name: specs[name]["signal"]["metric_name"]
        for name in (
            "service_io", "service_lock", "service_localnet", "host_memory",
        )
    } == {
        "service_io": "io_psi",
        "service_lock": "futex_wait_time_rate",
        "service_localnet": "local_socket_failure_rate",
        "host_memory": "memory_psi",
    }
    assert all(
        specs[name]["signal"]["minimum_median_z"] == 5.0
        for name in (
            "service_io", "service_lock", "service_localnet", "host_memory",
        )
    )


def test_fault_signal_gate_uses_frozen_baseline_scale(monkeypatch):
    import scripts.run_final_fault_matrix as runner

    values = {
        "normal": [0.70] * 10,
        "abnormal": [0.86] * 10,
    }
    monkeypatch.setattr(
        runner, "_metric_values",
        lambda archive, _selector: (values[str(archive)], 10),
    )
    coordinate = "cluster::ns::service::futex_wait_time_rate"
    requirement = {
        "metric_name": "futex_wait_time_rate",
        "scope": "service",
        "service_name": "service",
        "root_coordinate": coordinate,
        "minimum_median_z": 5.0,
    }
    scale = {
        coordinate: {"center": 0.70, "final_scale": 0.02},
    }

    result = runner.qualify_fault_signal(
        "normal", "abnormal", requirement, scale_snapshot=scale,
    )
    assert result["minimum_abnormal_median"] == pytest.approx(0.80)
    assert result["minimum_median_lift"] == pytest.approx(0.10)

    values["abnormal"] = [0.79] * 10
    with pytest.raises(runner.ExperimentError, match="too small"):
        runner.qualify_fault_signal(
            "normal", "abnormal", requirement, scale_snapshot=scale,
        )


def test_fault_phase_callback_runs_before_archive_validation(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    experiment_root = tmp_path / "experiment"
    experiment_root.mkdir()
    events = []

    class Process:
        def __init__(self, command, **_kwargs):
            self.returncode = None
            self.poll_count = 0
            marker = Path(
                command[command.index("--capture-complete-marker") + 1]
            )
            marker.write_text(json.dumps({
                "phase": "capture_complete",
                "final_target_ns": 123_000_000_000,
                "timestamp_ns": 124_000_000_000,
            }), encoding="utf-8")

        def poll(self):
            self.poll_count += 1
            if self.poll_count == 1:
                return None
            self.returncode = 0
            return self.returncode

        def terminate(self):
            self.returncode = -15

        def wait(self, timeout=None):
            del timeout
            return self.returncode

        def kill(self):
            self.returncode = -9

    def validate(*_args):
        assert events == ["callback"]
        return {"dataset_id": "dataset"}

    monkeypatch.setattr(runner.subprocess, "Popen", Process)
    monkeypatch.setattr(runner, "validate_archives", validate)
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    result = runner.collect_phase(
        tmp_path, experiment_root, "abnormal", 5,
        on_capture_complete=lambda _payload: events.append("callback"),
    )

    assert events == ["callback"]
    assert result["capture_complete"]["final_target_ns"] == 123_000_000_000


def test_fault_phase_does_not_retry_after_capture_completion(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    experiment_root = tmp_path / "experiment"
    experiment_root.mkdir()
    processes = []
    callbacks = []

    class Process:
        def __init__(self, command, **_kwargs):
            processes.append(self)
            self.returncode = None
            self.poll_count = 0
            marker = Path(
                command[command.index("--capture-complete-marker") + 1]
            )
            marker.write_text(json.dumps({
                "phase": "capture_complete",
                "final_target_ns": 123_000_000_000,
                "timestamp_ns": 124_000_000_000,
            }), encoding="utf-8")

        def poll(self):
            self.poll_count += 1
            if self.poll_count == 1:
                return None
            self.returncode = 7
            return self.returncode

        def terminate(self):
            self.returncode = -15

        def wait(self, timeout=None):
            del timeout
            return self.returncode

        def kill(self):
            self.returncode = -9

    monkeypatch.setattr(runner.subprocess, "Popen", Process)
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        runner, "wait_data_plane",
        lambda *_args, **_kwargs: pytest.fail("post-capture retry"),
    )

    with pytest.raises(
        runner.ExperimentError, match="retry is forbidden",
    ):
        runner.collect_phase(
            tmp_path, experiment_root, "abnormal", 5,
            on_capture_complete=lambda payload: callbacks.append(payload),
        )

    assert len(processes) == 1
    assert len(callbacks) == 1
    assert callbacks[0]["final_target_ns"] == 123_000_000_000


def test_continuous_fault_capture_activates_on_exact_boundary_and_then_slices(
    tmp_path, monkeypatch,
):
    import scripts.run_final_fault_matrix as runner

    events = []
    start_ns = 100_000_000_000
    phase_target_ns = start_ns + 2_000_000_000
    final_target_ns = start_ns + 4_000_000_000

    class FakeProcess:
        returncode = 0

        def __init__(self, command, **_kwargs):
            values = {
                command[index]: Path(command[index + 1])
                for index in range(len(command) - 1)
                if command[index].startswith("--")
            }
            self.normal_root = values["--output"]
            self.burst_root = values["--burst-output"]
            self.phase_marker = values["--phase-boundary-marker"]
            self.final_marker = values["--capture-complete-marker"]
            self.normal_root.mkdir(parents=True)
            self.burst_root.mkdir(parents=True)
            self.calls = 0

        def poll(self):
            self.calls += 1
            if self.calls == 1:
                self.phase_marker.write_text(json.dumps({
                    "phase": "phase_boundary",
                    "boundary_sequence": 2,
                    "boundary_target_ns": phase_target_ns,
                    "timestamp_ns": phase_target_ns,
                }), encoding="utf-8")
                return None
            if self.calls == 2:
                self.final_marker.write_text(json.dumps({
                    "phase": "capture_complete",
                    "final_target_ns": final_target_ns,
                    "timestamp_ns": final_target_ns,
                }), encoding="utf-8")
                return None
            return 0

        def wait(self, timeout=None):
            return 0

    validation_results = iter((
        {
            "dataset_id": "a" * 64,
            "window_count": 4,
            "window_start_ns": start_ns,
            "window_end_ns": final_target_ns,
        },
        {
            "dataset_id": "b" * 64,
            "window_count": 2,
            "window_start_ns": start_ns,
            "window_end_ns": phase_target_ns,
        },
        {
            "dataset_id": "c" * 64,
            "window_count": 2,
            "window_start_ns": phase_target_ns,
            "window_end_ns": final_target_ns,
        },
    ))

    monkeypatch.setattr(runner.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        runner, "validate_archives",
        lambda *_args, **_kwargs: next(validation_results),
    )

    def fake_split(**kwargs):
        events.append(("slice", kwargs["start_index"], kwargs["window_count"]))
        return {}

    monkeypatch.setattr(runner, "split_aligned_archive_slice", fake_split)
    result = runner.collect_contiguous_phases(
        tmp_path,
        tmp_path / "trial",
        normal_windows=2,
        abnormal_windows=2,
        on_phase_boundary=lambda payload: events.append(
            ("activate", payload["boundary_target_ns"])
        ),
        on_capture_complete=lambda payload: events.append(
            ("deactivate", payload["final_target_ns"])
        ),
    )

    assert events == [
        ("activate", phase_target_ns),
        ("deactivate", final_target_ns),
        ("slice", 0, 2),
        ("slice", 2, 2),
    ]
    assert result["normal"]["window_end_ns"] == phase_target_ns
    assert result["abnormal"]["window_start_ns"] == phase_target_ns

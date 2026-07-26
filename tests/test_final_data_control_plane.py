from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from proberca.cli.analyze_collection import main as analyze_main
from proberca.cli.seal_collection import main as seal_main
from proberca.controlplane import FinalControlConfig, FinalControlPlane
from proberca.controlplane.model import MetricPropagationModel
from proberca.controlplane.observations import MetricResolver
from proberca.controlplane.service_model import ServiceRLS, allowed_service_graph
from proberca.data.schema import (
    EdgeMetricRecord,
    EvidenceObservationRecord,
    NodeMetricRecord,
    ServiceNodePlacement,
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


def _config() -> FinalControlConfig:
    return FinalControlConfig(
        baseline_min_windows=3,
        baseline_min_scale=0.1,
        service_lags=(1,),
        metric_lags=(1,),
        metric_min_training_rows=2,
        soft_threshold=2.0,
        soft_consecutive_windows=1,
        hard_threshold=4.0,
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
    variation = ((sequence - 1) % 3) - 1
    values = {
        "request_rate": 100.0 + 2.0 * variation,
        "request_failure_rate": 0.01 + 0.001 * variation,
        "request_latency_p95": 10.0 + variation,
        "cpu_usage_rate": 0.30 + 0.01 * variation,
        "cpu_throttle_ratio": 0.02 + 0.002 * variation,
        "memory_working_set_ratio": 0.40 + 0.01 * variation,
        "io_psi": 0.02 + 0.002 * variation,
        "futex_wait_time_rate": 0.01 + 0.001 * variation,
        "local_socket_failure_rate": 0.01 + 0.001 * variation,
    }
    if sequence == 9:
        values.update({
            "request_latency_p95": 20.0,
            "request_failure_rate": 0.03,
            "cpu_usage_rate": 0.75,
            "cpu_throttle_ratio": 0.20,
        })
    elif sequence >= 10:
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
            schema_version="1.0",
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
            unit=spec.unit,
            sample_count=10,
            coverage=1.0,
            event_loss_rate=0.0,
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
    variation = ((sequence - 1) % 3) - 1
    hosts = tuple(record(
        name=name, family=family, value=0.02 + 0.001 * variation,
        scope="node", service="host-metrics",
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
        target_id="cluster::ns::payment::cpu_usage_rate",
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
    assert fingerprint(contract) == FinalControlConfig().collection_contract_fingerprint
    control_payload = yaml.safe_load(
        Path("configs/final_control.yaml").read_text(encoding="utf-8")
    )
    assert set(control_payload) == set(FinalControlConfig.__dataclass_fields__)
    assert FinalControlConfig.from_dict(control_payload).to_dict() \
        == FinalControlConfig().to_dict()
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
    archive_dir = tmp_path / "sealed"
    assert seal_main([
        "--windows-jsonl", str(input_path),
        "--collection-contract", "configs/final_collection_contract.yaml",
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
        schema_version="1.0", timestamp_ns=0, window_sec=1,
        cluster_id="cluster", namespace="ns",
        src_service="checkout", dst_service="payment",
        src_pod_uid=None, dst_pod_uid=None, src_node="node-a", dst_node="node-b",
        protocol="tcp", metric_name="edge_latency_p95", value=10.0,
        unit="milliseconds", sample_count=10, coverage=1.0, event_loss_rate=0.0,
        source="final_window_aggregation", metric_kind="quantile", scope="service_pair",
        histogram_upper_bound=None, histogram_is_inf_bucket=False,
        histogram_is_cumulative=None, quantile=0.95,
    )
    metric, spec = MetricResolver(FinalControlConfig()).resolve(edge)
    assert metric.entity_id == "cluster::ns::checkout->payment::tcp"
    assert metric.root_category == "TCP"
    assert spec.role == "edge_latency"

    model = MetricPropagationModel(
        node_ids=("a", "b"), lags=(1,),
        coefficients={("a", "a", 1): 100.0, ("a", "b", 1): 2.0},
        semantic_mask=(("a", "a"), ("a", "b")),
        training_rows=4, healthy_cutoff_ns=10,
    )
    assert model.cross_prediction("a", {4: {"a": 7.0, "b": 3.0}}, 5) == 6.0


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
    assert (config.hard_threshold, config.hard_consecutive_windows) == (5.0, 2)
    control = FinalControlPlane(config)
    service_a = "cluster::ns::a"
    service_b = "cluster::ns::b"

    soft, hard = control._advance_alert_counters({service_a: 4.0}, {})
    assert not soft and not hard
    soft, hard = control._advance_alert_counters({service_b: 4.0}, {})
    assert not soft and not hard
    soft, hard = control._advance_alert_counters({service_a: 4.0}, {})
    assert not soft and not hard

    control = FinalControlPlane(config)
    soft, hard = control._advance_alert_counters({}, {"edge-a": 6.0})
    assert not hard
    soft, hard = control._advance_alert_counters({}, {"edge-a": 6.0})
    assert ("edge", "edge-a") in hard
    assert ("edge", "edge-a") not in soft


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


def test_topology_change_resets_every_healthy_model_segment():
    control = FinalControlPlane(_config())
    control.baseline.update("metric", 1.0)
    control._healthy_history[1] = {"metric": 0.0}
    control._signed_history[1] = {"metric": 0.0}
    control._soft_counts[("service", "service-a")] = 2
    control._hard_counts[("service", "service-a")] = 1
    control._healthy_topology_snapshot_id = "old"

    control._reset_healthy_segment("new")

    assert control.baseline.snapshot() == {}
    assert control._healthy_history == {}
    assert control._signed_history == {}
    assert control._soft_counts == {}
    assert control._hard_counts == {}
    assert control._healthy_topology_snapshot_id == "new"


def test_soft_context_keeps_using_its_frozen_topology_version(tmp_path):
    config = _config()
    old = _topology(snapshot_name="old-layout", valid_to_ns=9 * _NS)
    new = _topology(
        snapshot_name="new-layout",
        valid_from_ns=9 * _NS,
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
        elif sequence == 10:
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
        if item["timestamp_ns"] == 10 * _NS
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

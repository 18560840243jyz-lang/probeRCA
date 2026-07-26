from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from proberca.cli.analyze_collection import main as analyze_main
from proberca.cli.seal_collection import main as seal_main
from proberca.controlplane import FinalControlConfig, FinalControlPlane
from proberca.controlplane.model import MetricPropagationModel
from proberca.controlplane.observations import MetricResolver
from proberca.data.schema import (
    EdgeMetricRecord,
    EvidenceObservationRecord,
    NodeMetricRecord,
    ServiceNodePlacement,
    TopologySnapshot,
)
from proberca.dataplane.contracts import canonical_json, fingerprint
from proberca.dataplane import (
    CollectedWindow,
    CollectionArchive,
    CollectionArchiveError,
    CollectionArchiveNotSealedError,
    CollectionArchiveWriter,
    GroundTruthFieldError,
    burst_observation_quality,
    continuous_burst_strength,
    rare_event_strength,
)


_NS = 1_000_000_000
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
        hard_consecutive_windows=1,
        recovery_threshold=0.5,
        recovery_windows=2,
        burst_window_count=1,
        l1_penalty=0.05,
        fista_tolerance=1.0e-9,
    )


def _topology() -> TopologySnapshot:
    return TopologySnapshot(
        schema_version="1.0",
        snapshot_id="topology-1",
        valid_from_ns=0,
        valid_to_ns=100 * _NS,
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

    def record(*, name: str, family: str, value: float, scope: str, service: str):
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
            unit="ratio",
            sample_count=10,
            coverage=1.0,
            event_loss_rate=0.0,
            source="synthetic-final-test",
            metric_kind="gauge",
            scope=scope,
            histogram_upper_bound=None,
            histogram_is_inf_bucket=False,
            histogram_is_cumulative=None,
            quantile=None,
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
    return EvidenceObservationRecord(
        schema_version="1.0",
        evidence_id="burst-cpu-1",
        timestamp_ns=10 * _NS + _NS // 2,
        evidence_window_start_ns=10 * _NS,
        evidence_window_end_ns=11 * _NS,
        analysis_cutoff_ns=11 * _NS,
        cluster_id="cluster",
        namespace="ns",
        target_type="node",
        target_id="cluster::ns::payment::cpu_usage_rate",
        channel_id="sched.runqueue_wait_p95",
        source_type="burst_event",
        normalized_strength=0.9,
        observation_quality=1.0,
        reliability_weight=1.0,
        source_record_ids=["burst-record-1"],
        source_object_ids=[],
        independent_from_residual=True,
        provenance={"calibration_id": "healthy-burst-v1"},
        config_fingerprint="a" * 64,
    )


def _unknown_evidence() -> EvidenceObservationRecord:
    return replace(
        _evidence(),
        evidence_id="burst-unknown-1",
        channel_id="unmapped.mystery_signal",
        normalized_strength=0.8,
        source_record_ids=["burst-record-unknown"],
    )


def _window(sequence: int, *, complete: bool = True) -> CollectedWindow:
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
        topology_events=(_topology(),) if sequence == 1 else (),
        burst_evidence=(_evidence(), _unknown_evidence()) if sequence == 11 else (),
        collection_metadata={"collector": "synthetic-final-test"},
    )


def _file_hash(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_collection_must_be_complete_and_sealed_before_control(tmp_path):
    config = _config()
    archive_dir = tmp_path / "archive"
    writer = CollectionArchiveWriter(
        archive_dir,
        dataset_id="final-synthetic",
        collection_contract=config.collection_contract,
        source_description="synthetic final-scheme collection",
    )
    for sequence in range(1, 12):
        writer.append(_window(sequence))

    with pytest.raises(CollectionArchiveNotSealedError):
        CollectionArchive.load(archive_dir)

    archive = writer.seal()
    manifest_hash = _file_hash(archive_dir / "collection-manifest.json")
    windows_hash = _file_hash(archive_dir / "collected-windows.jsonl")
    run = FinalControlPlane(config).run(CollectionArchive.load(archive_dir))

    assert run.processed_window_count == 11
    assert len(run.results) == 1
    result = run.results[0]
    assert result.top_k[0].entity_id == "cluster::ns::payment"
    assert result.top_k[0].root_category == "CPU"
    assert result.top_k[0].score > 0.0
    assert result.top_k[0].burst_evidence_strength == pytest.approx(0.9)
    assert result.top_k[0].burst_evidence_ids == ("burst-cpu-1",)
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
        dataset_id="incomplete",
        collection_contract=config.collection_contract,
        source_description="incomplete collection",
    )
    with pytest.raises(CollectionArchiveError, match="incomplete service metric set"):
        writer.append(_window(1, complete=False))


def test_ground_truth_cannot_cross_collection_boundary(tmp_path):
    config = _config()
    with pytest.raises(GroundTruthFieldError):
        CollectionArchiveWriter(
            tmp_path / "archive",
            dataset_id="unsafe",
            collection_contract=config.collection_contract,
            source_description="unsafe collection",
            collection_metadata={"ground_truth": "payment::CPU"},
        )


def test_checked_in_contract_and_plane_imports_are_separate():
    contract = yaml.safe_load(
        Path("configs/final_collection_contract.yaml").read_text(encoding="utf-8")
    )
    assert fingerprint(contract) == FinalControlConfig().collection_contract_fingerprint
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
                for sequence in range(1, 12)),
        encoding="utf-8",
    )
    config_path = tmp_path / "control.yaml"
    config_path.write_text(
        yaml.safe_dump(_config().to_dict(), sort_keys=False), encoding="utf-8",
    )
    archive_dir = tmp_path / "sealed"
    assert seal_main([
        "--windows-jsonl", str(input_path),
        "--collection-contract", "configs/final_collection_contract.yaml",
        "--dataset-id", "cli-two-phase",
        "--source-description", "CLI separation test",
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
        source="synthetic-final-test", metric_kind="gauge", scope="service_pair",
        histogram_upper_bound=None, histogram_is_inf_bucket=False,
        histogram_is_cumulative=None, quantile=None,
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

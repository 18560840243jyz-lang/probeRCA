from __future__ import annotations

import ast
import math
from dataclasses import replace
from pathlib import Path

import pytest

from proberca.config import ProbeRCAConfig, dump_config_yaml, load_config_yaml
from proberca.data.index import (
    StableIndex,
    edge_id,
    node_id,
    shock_id,
)
from proberca.data.io import (
    read_record_json,
    read_records_jsonl,
    read_records_parquet,
    write_record_json,
    write_jsonl,
    write_records_jsonl,
    write_records_parquet,
)
from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    AlertEvent,
    BurstEventRecord,
    EdgeMetricRecord,
    IncidentLabel,
    MetricRecord,
    NodeMetricRecord,
    RCAReport,
    RootCause,
    TopologyEdge,
    TopologySnapshot,
    node_metric_from_legacy,
)


def make_node(**changes) -> NodeMetricRecord:
    values = {
        "schema_version": PROBERCA_SCHEMA_VERSION,
        "timestamp_ns": 1_000_000_000,
        "window_sec": 10,
        "cluster_id": "cluster-a",
        "node_name": "worker-a",
        "namespace": "observability",
        "service_name": "service-a",
        "pod_uid": "pod-a",
        "container_id": "container-a",
        "metric_family": "cpu",
        "metric_name": "cpu.throttled_usec",
        "value": 12.5,
        "unit": "us",
        "sample_count": 5,
        "coverage": 1.0,
        "event_loss_rate": 0.0,
        "source": "collector-a",
        "metric_kind": "gauge",
        "scope": "pod",
        "histogram_upper_bound": None,
        "histogram_is_inf_bucket": False,
        "histogram_is_cumulative": None,
        "quantile": None,
    }
    values.update(changes)
    return NodeMetricRecord(**values)


def make_edge(**changes) -> EdgeMetricRecord:
    values = {
        "schema_version": PROBERCA_SCHEMA_VERSION,
        "timestamp_ns": 1_000_000_000,
        "window_sec": 10,
        "cluster_id": "cluster-a",
        "namespace": "observability",
        "src_service": "service-a",
        "dst_service": "service-b",
        "src_pod_uid": "pod-a",
        "dst_pod_uid": "pod-b",
        "src_node": "worker-a",
        "dst_node": "worker-b",
        "protocol": "tcp",
        "metric_name": "tcp.rtt_p95_ms",
        "value": 42.0,
        "unit": "ms",
        "sample_count": 10,
        "coverage": 0.9,
        "event_loss_rate": 0.1,
        "source": "collector-a",
        "metric_kind": "gauge",
        "scope": "service_pair",
        "histogram_upper_bound": None,
        "histogram_is_inf_bucket": False,
        "histogram_is_cumulative": None,
        "quantile": None,
    }
    values.update(changes)
    return EdgeMetricRecord(**values)


def make_burst(**changes) -> BurstEventRecord:
    values = {
        "schema_version": PROBERCA_SCHEMA_VERSION,
        "event_id": "event-a",
        "timestamp_ns": 1_000_000_001,
        "event_type": "tcp.retransmit",
        "pid": 100,
        "tid": 101,
        "cgroup_id": 102,
        "container_id": "container-a",
        "pod_uid": "pod-a",
        "service_name": "service-a",
        "node_name": "worker-a",
        "src_service": "service-a",
        "dst_service": "service-b",
        "src_ip": "10.0.0.1",
        "dst_ip": "10.0.0.2",
        "src_port": 12345,
        "dst_port": 443,
        "protocol": "tcp",
        "value": 1.0,
        "unit": "count",
        "probe_mode": "burst",
        "burst_id": "burst-a",
        "lost_events": 0,
    }
    values.update(changes)
    return BurstEventRecord(**values)


def make_topology() -> TopologySnapshot:
    return TopologySnapshot(
        snapshot_id="snapshot-a",
        schema_version=PROBERCA_SCHEMA_VERSION,
        valid_from_ns=1_000_000_000,
        valid_to_ns=2_000_000_000,
        cluster_id="cluster-a",
        services=["observability::service-a", "observability::service-b"],
        call_edges=[TopologyEdge("service-a", "service-b", "call")],
        host_edges=[TopologyEdge("service-a", "service-b", "host")],
        resource_edges=[TopologyEdge("service-a", "service-b", "resource")],
    )


def make_alert() -> AlertEvent:
    return AlertEvent(
        schema_version=PROBERCA_SCHEMA_VERSION,
        alert_id="alert-a",
        timestamp_ns=1_000_000_000,
        state="hard",
        trigger_services=["observability::service-a"],
        trigger_edges=[edge_id(make_edge())],
        service_scores={"observability::service-a": 0.9},
        edge_scores={edge_id(make_edge()): 0.8},
        reason="hard threshold exceeded",
        frozen_baseline=True,
        frozen_service_model=True,
        frozen_metric_model=True,
    )


def make_label() -> IncidentLabel:
    return IncidentLabel(
        schema_version=PROBERCA_SCHEMA_VERSION,
        incident_id="incident-a",
        start_ns=1_000_000_000,
        end_ns=2_000_000_000,
        fault_mode="edge",
        edge_subtype="exogenous-edge-shock",
        root_service="service-a",
        root_metric=None,
        root_edge=edge_id(make_edge()),
        injection_method="controlled-test",
        seed=7,
    )


def make_report() -> RCAReport:
    root = RootCause(
        kind="edge",
        service_name=None,
        metric_name=None,
        edge_id=edge_id(make_edge()),
        fault_mode="exogenous-edge-shock",
        edge_subtype="exogenous-edge-shock",
    )
    return RCAReport(
        schema_version=PROBERCA_SCHEMA_VERSION,
        incident_id="incident-a",
        generated_at_ns=2_000_000_001,
        alert=make_alert(),
        primary_root=root,
        ranked_candidates=[{
            "object_type": "edge", "node_id": None, "edge_id": root.edge_id,
            "root_metric": None, "edge_subtype": "exogenous-edge-shock",
            "score": 0.9, "role": "root",
        }],
        symptoms=[{"kind": "node", "id": node_id(make_node()), "role": "propagated"}],
        propagation_paths=[{"nodes": ["service-a", "service-b"], "score": 0.8}],
        evidence=[{"type": "tcp.retransmit", "strength": 0.9}],
        quality={"coverage": 0.9, "event_loss_rate": 0.1},
        runtime={"total_ms": 12.0},
    )


def all_records():
    return [make_node(), make_edge(), make_burst(), make_topology(), make_alert(), make_label(), make_report()]


def valid_config_dict() -> dict:
    return {
        "window_sec": 10,
        "healthy_history_sec": 300,
        "alert": {
            "healthy_threshold": 0.2,
            "soft_threshold": 0.5,
            "soft_consecutive_windows": 2,
            "hard_threshold": 0.8,
            "hard_consecutive_windows": 2,
            "recovery_threshold": 0.1,
            "recovery_windows": 3,
            "recovery_cooldown_sec": 30,
        },
        "propagation": {
            "service_lags": [1, 2],
            "metric_lags": [1],
            "rls_forgetting_factor": 0.99,
            "metric_ridge": 0.1,
        },
        "candidate_graph": {
            "upstream_hops": 2,
            "downstream_hops": 1,
            "include_cohost": True,
            "include_shared_resource": True,
        },
        "burst": {"ttl_sec": 30, "max_ttl_sec": 120},
        "solver": {"method": "fista", "max_iterations": 500, "tolerance": 1e-6},
        "confidence": {"strong": 0.8, "weak": 0.4},
        "shock_templates": {
            "tcp.retrans_rate": {
                "source_metric_families": ["net_local"],
                "target_metric_families": ["request"],
            },
            "dns.timeout_rate": {
                "source_metric_families": ["net_local"],
                "target_metric_families": ["request"],
            },
        },
    }


def test_every_record_constructs_and_has_schema_version() -> None:
    for record in all_records():
        assert record.schema_version == PROBERCA_SCHEMA_VERSION
        assert type(record).from_dict(record.to_dict()) == record


@pytest.mark.parametrize(
    "metric_name",
    [
        "tcp.rtt_p95_ms",
        "tcp.retrans_rate",
        "tcp.rto_count",
        "tcp.rst_rate",
        "tcp.connect_fail_rate",
        "tcp.syn_retry_rate",
        "network.drop_rate",
        "dns.latency_p95_ms",
        "dns.timeout_rate",
        "sidecar.queue_p95_ms",
        "proxy.upstream_latency_p95_ms",
    ],
)
def test_edge_metric_supports_required_shock_metrics(metric_name: str) -> None:
    assert make_edge(metric_name=metric_name).metric_name == metric_name


@pytest.mark.parametrize(
    "event_type",
    [
        "sched.runqueue_wait",
        "sched.offcpu",
        "block.latency",
        "fs.read_latency",
        "fs.write_latency",
        "futex.wait",
        "tcp.retransmit",
        "tcp.rto",
        "tcp.rtt",
        "tcp.rst",
        "tcp.connect_fail",
        "dns.latency",
        "dns.timeout",
        "process.exec",
        "process.exit",
        "sidecar.queue",
        "proxy.upstream_latency",
    ],
)
def test_burst_event_supports_required_event_types(event_type: str) -> None:
    assert make_burst(event_type=event_type).event_type == event_type


def test_burst_nullable_identifiers_are_explicit_none() -> None:
    event = make_burst(
        event_type="process.exit",
        pid=None,
        tid=None,
        cgroup_id=None,
        container_id=None,
        pod_uid=None,
        service_name=None,
        node_name=None,
        src_service=None,
        dst_service=None,
        src_ip=None,
        dst_ip=None,
        src_port=None,
        dst_port=None,
        protocol=None,
        probe_mode="always_on",
        burst_id=None,
    )
    assert event.pid is None
    assert event.burst_id is None


@pytest.mark.parametrize("family", ["request", "cpu", "memory", "io", "net_local", "lock"])
def test_node_metric_families(family: str) -> None:
    assert make_node(metric_family=family).metric_family == family


@pytest.mark.parametrize("state", ["healthy", "soft", "hard", "recovery", "edge_anomaly"])
def test_alert_states(state: str) -> None:
    assert replace(make_alert(), state=state).state == state


@pytest.mark.parametrize("probe_mode", ["always_on", "burst"])
def test_probe_modes(probe_mode: str) -> None:
    burst_id = None if probe_mode == "always_on" else "burst-a"
    assert make_burst(probe_mode=probe_mode, burst_id=burst_id).probe_mode == probe_mode


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_values_are_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        make_node(value=value)
    with pytest.raises(ValueError):
        make_edge(value=value)
    with pytest.raises(ValueError):
        make_burst(value=value)


@pytest.mark.parametrize("field", ["coverage", "event_loss_rate"])
@pytest.mark.parametrize("value", [-0.01, 1.01])
def test_metric_quality_ranges_are_enforced(field: str, value: float) -> None:
    with pytest.raises(ValueError):
        make_node(**{field: value})
    with pytest.raises(ValueError):
        make_edge(**{field: value})


def test_empty_required_strings_and_invalid_enums_are_rejected() -> None:
    with pytest.raises(ValueError):
        make_node(service_name=" ")
    with pytest.raises(ValueError):
        make_edge(protocol="")
    with pytest.raises(ValueError):
        make_node(metric_family="network")
    with pytest.raises(ValueError):
        make_burst(event_type="unknown")
    with pytest.raises(ValueError):
        replace(make_alert(), state="unknown")


def test_invalid_integer_bounds_and_types_are_rejected() -> None:
    with pytest.raises(ValueError):
        make_node(window_sec=0)
    with pytest.raises(ValueError):
        make_node(sample_count=-1)
    with pytest.raises(TypeError):
        make_node(timestamp_ns=True)
    with pytest.raises(ValueError):
        make_burst(src_port=0)
    with pytest.raises(ValueError):
        make_burst(dst_port=65_536)
    with pytest.raises(ValueError):
        make_burst(lost_events=-1)


def test_unknown_missing_and_incompatible_schema_fail_fast() -> None:
    payload = make_node().to_dict()
    with pytest.raises(ValueError, match="unknown"):
        NodeMetricRecord.from_dict({**payload, "unexpected": 1})
    missing = dict(payload)
    del missing["value"]
    with pytest.raises(ValueError, match="missing"):
        NodeMetricRecord.from_dict(missing)
    with pytest.raises(ValueError, match="schema_version"):
        NodeMetricRecord.from_dict({**payload, "schema_version": "999.0"})
    with pytest.raises(TypeError):
        NodeMetricRecord.from_dict({**payload, "sample_count": "5"})
    with pytest.raises(TypeError):
        NodeMetricRecord.from_dict({**payload, "value": None})


def test_edge_record_cannot_be_parsed_as_node_record() -> None:
    with pytest.raises(ValueError):
        NodeMetricRecord.from_dict(make_edge().to_dict())


def test_topology_validity_and_relation_types_are_strict() -> None:
    with pytest.raises(ValueError):
        replace(make_topology(), valid_to_ns=1_000_000_000)
    with pytest.raises(ValueError):
        TopologyEdge("service-a", "service-b", "untyped")
    bad = make_topology().to_dict()
    bad["host_edges"][0]["relation_type"] = "call"
    with pytest.raises(ValueError):
        TopologySnapshot.from_dict(bad)


def test_incident_label_mode_and_time_consistency() -> None:
    with pytest.raises(ValueError):
        replace(make_label(), end_ns=1_000_000_000)
    with pytest.raises(ValueError):
        replace(make_label(), fault_mode="self", edge_subtype="exogenous-edge-shock")
    with pytest.raises(ValueError):
        replace(make_label(), fault_mode="edge", edge_subtype=None)


def test_rca_primary_root_rules() -> None:
    report = make_report()
    assert report.primary_root.kind == "edge"
    propagated = report.to_dict()
    propagated["primary_root"]["kind"] = "propagated"
    with pytest.raises(ValueError):
        RCAReport.from_dict(propagated)
    with pytest.raises(ValueError):
        RootCause(
            kind="node",
            service_name=None,
            metric_name=None,
            edge_id=None,
            fault_mode="self",
            edge_subtype=None,
        )
    symptom = report.symptoms[0]
    assert symptom["role"] == "propagated"


def test_node_edge_and_shock_ids_are_stable_and_distinct() -> None:
    node = make_node()
    edge = make_edge()
    assert node_id(node) == "cluster-a::observability::service-a::cpu.throttled_usec"
    assert edge_id(edge) == "cluster-a::observability::service-a->service-b::tcp::tcp.rtt_p95_ms"
    assert shock_id(edge) == "cluster-a::observability::service-a->service-b::tcp::shock::tcp.rtt_p95_ms"
    assert len({node_id(node), edge_id(edge), shock_id(edge)}) == 3


@pytest.mark.parametrize(
    "factory,changes",
    [
        (make_node, {"service_name": "bad::service"}),
        (make_edge, {"src_service": "bad->service"}),
        (make_edge, {"metric_name": "bad::metric"}),
    ],
)
def test_ambiguous_id_components_are_rejected(factory, changes) -> None:
    with pytest.raises(ValueError):
        factory(**changes)


def test_stable_index_is_order_independent_and_bidirectional(tmp_path) -> None:
    nodes = [node_id(make_node(metric_name="cpu.usage")), node_id(make_node())]
    edges = [edge_id(make_edge(metric_name="tcp.retrans_rate")), edge_id(make_edge())]
    shocks = [shock_id(make_edge(metric_name="tcp.retrans_rate")), shock_id(make_edge())]
    forward = StableIndex.build(node_ids=nodes, edge_ids=edges, shock_ids=shocks)
    reverse = StableIndex.build(
        node_ids=list(reversed(nodes)),
        edge_ids=list(reversed(edges)),
        shock_ids=list(reversed(shocks)),
    )
    assert forward.id_to_index == reverse.id_to_index
    assert forward.index_to_id == reverse.index_to_id
    for stable_id, integer_index in forward.id_to_index.items():
        assert forward.id_at(integer_index) == stable_id
        assert forward.index_of(stable_id) == integer_index

    json_path = tmp_path / "index.json"
    npz_path = tmp_path / "index.npz"
    forward.save_json(json_path)
    forward.save_npz(npz_path)
    assert StableIndex.load_json(json_path) == forward
    assert StableIndex.load_npz(npz_path) == forward


def test_stable_index_rejects_duplicate_ids() -> None:
    stable_id = node_id(make_node())
    with pytest.raises(ValueError, match="duplicate"):
        StableIndex.build(node_ids=[stable_id, stable_id], edge_ids=[], shock_ids=[])


def test_rca_quality_probabilities_are_bounded() -> None:
    report = make_report()
    with pytest.raises(ValueError):
        replace(report, quality={"coverage": 1.01, "event_loss_rate": 0.0})
    with pytest.raises(ValueError):
        replace(report, quality={"coverage": 1.0, "event_loss_rate": -0.01})


def test_stable_index_rejects_negative_integer_index() -> None:
    index = StableIndex.build(node_ids=[node_id(make_node())], edge_ids=[], shock_ids=[])
    with pytest.raises(IndexError):
        index.id_at(-1)


def test_json_and_jsonl_round_trip(tmp_path) -> None:
    record = make_node()
    json_path = tmp_path / "record.json"
    jsonl_path = tmp_path / "records.jsonl"
    write_record_json(json_path, record)
    assert read_record_json(json_path) == record
    records = all_records()
    write_records_jsonl(jsonl_path, records)
    restored = read_records_jsonl(jsonl_path)
    assert restored == records
    assert all(type(actual) is type(expected) for actual, expected in zip(restored, records))


def test_legacy_jsonl_writer_revalidates_strict_records(tmp_path) -> None:
    node = make_node()
    object.__setattr__(node, "value", math.nan)
    with pytest.raises(ValueError):
        write_jsonl(tmp_path / "invalid.jsonl", [node])


def test_node_edge_and_mixed_parquet_round_trip(tmp_path) -> None:
    node_path = tmp_path / "node.parquet"
    edge_path = tmp_path / "edge.parquet"
    mixed_path = tmp_path / "mixed.parquet"
    node = make_node()
    edge = make_edge()
    write_records_parquet(node_path, [node])
    write_records_parquet(edge_path, [edge])
    write_records_parquet(mixed_path, all_records())
    assert read_records_parquet(node_path) == [node]
    assert read_records_parquet(edge_path) == [edge]
    restored = read_records_parquet(mixed_path)
    assert restored == all_records()
    assert all(type(actual) is type(expected) for actual, expected in zip(restored, all_records()))


@pytest.mark.parametrize("bad_value", [math.nan, math.inf, -math.inf])
def test_parquet_writer_revalidates_non_finite_values(tmp_path, bad_value: float) -> None:
    node = make_node()
    object.__setattr__(node, "value", bad_value)
    with pytest.raises(ValueError):
        write_records_parquet(tmp_path / "bad.parquet", [node])


def test_config_yaml_round_trip_and_strict_validation(tmp_path) -> None:
    config = ProbeRCAConfig.from_dict(valid_config_dict())
    path = tmp_path / "proberca.yaml"
    dump_config_yaml(path, config)
    assert load_config_yaml(path) == config
    assert config.solver.method == "fista"


@pytest.mark.parametrize(
    "path,value",
    [
        (("window_sec",), 0),
        (("healthy_history_sec",), 0),
        (("alert", "recovery_threshold"), 0.3),
        (("propagation", "service_lags"), [0]),
        (("propagation", "metric_lags"), []),
        (("propagation", "rls_forgetting_factor"), 0.0),
        (("propagation", "rls_forgetting_factor"), 1.1),
        (("burst", "ttl_sec"), 0),
        (("burst", "max_ttl_sec"), 10),
        (("solver", "method"), "admm"),
        (("solver", "max_iterations"), 0),
        (("confidence", "strong"), 0.3),
        (("confidence", "weak"), -0.1),
        (("confidence", "strong"), 1.1),
    ],
)
def test_config_rejects_invalid_values(path, value) -> None:
    payload = valid_config_dict()
    target = payload
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value
    with pytest.raises((TypeError, ValueError)):
        ProbeRCAConfig.from_dict(payload)


def test_config_rejects_unknown_missing_and_service_specific_templates() -> None:
    payload = valid_config_dict()
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="unknown"):
        ProbeRCAConfig.from_dict(payload)
    payload = valid_config_dict()
    del payload["window_sec"]
    with pytest.raises(ValueError, match="missing"):
        ProbeRCAConfig.from_dict(payload)
    payload = valid_config_dict()
    payload["shock_templates"]["service-a::tcp.retrans_rate"] = payload["shock_templates"].pop(
        "tcp.retrans_rate"
    )
    with pytest.raises(ValueError):
        ProbeRCAConfig.from_dict(payload)


def test_legacy_metric_record_and_adapter_shape_remain_compatible() -> None:
    legacy = MetricRecord(
        timestamp=1.0,
        service="service-a",
        instance="pod-a",
        node="worker-a",
        metric="cpu.usage",
        value=0.5,
        source="legacy-collector",
        incident_id=None,
    )
    node = node_metric_from_legacy(
        legacy,
        schema_version=PROBERCA_SCHEMA_VERSION,
        window_sec=10,
        cluster_id="cluster-a",
        namespace="observability",
        metric_family="cpu",
        unit="cores",
        sample_count=1,
        coverage=1.0,
        event_loss_rate=0.0,
        metric_kind="gauge",
        scope="pod",
        histogram_upper_bound=None,
        histogram_is_inf_bucket=False,
        histogram_is_cumulative=None,
        quantile=None,
    )
    assert node.service_name == legacy.service
    assert node.pod_uid == legacy.instance
    assert node.node_name == legacy.node
    assert node.timestamp_ns == 1_000_000_000


def test_online_inference_does_not_import_incident_label() -> None:
    inference_dir = Path(__file__).parents[1] / "proberca" / "inference"
    offenders = []
    for path in inference_dir.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for syntax_node in ast.walk(tree):
            if isinstance(syntax_node, ast.ImportFrom) and any(
                alias.name == "IncidentLabel" for alias in syntax_node.names
            ):
                offenders.append(str(path))
            if isinstance(syntax_node, ast.Import) and any(
                alias.name.endswith("IncidentLabel") for alias in syntax_node.names
            ):
                offenders.append(str(path))
    assert offenders == []


def test_new_production_modules_do_not_hardcode_service_names() -> None:
    root = Path(__file__).parents[1]
    paths = [
        root / "proberca" / "data" / "schema.py",
        root / "proberca" / "data" / "index.py",
        root / "proberca" / "data" / "io.py",
        root / "proberca" / "config.py",
    ]
    banned = ("paymentservice", "checkoutservice", "online-boutique")
    for path in paths:
        text = path.read_text(encoding="utf-8").lower()
        assert all(name not in text for name in banned)

from __future__ import annotations

import inspect
import json
import math
from dataclasses import replace
from pathlib import Path

import pytest

import test_p1_data_contracts as p1
from proberca.config import MetricAggregationSpec
from proberca.data.index import StableIndex, edge_id, node_id, shock_id
from proberca.data.io import (
    read_record_json,
    read_records_jsonl,
    read_records_parquet,
    write_records_jsonl,
    write_records_parquet,
)
from proberca.data.schema import (
    MetricRecord,
    NodeMetricRecord,
    node_metric_from_legacy,
)


def aggregation_payload(method: str = "sum") -> dict:
    return {
        "method": method,
        "input_metric_kind": "delta_counter",
        "source_scope": "pod",
        "target_scope": "service",
        "input_metric_ids": ["observability::service-a::cpu.usage"],
        "input_series_ids": None,
        "numerator_metric_id": None,
        "denominator_metric_id": None,
        "numerator_metric_kind": None,
        "denominator_metric_kind": None,
        "numerator_scope": None,
        "denominator_scope": None,
        "output_metric_name": None,
        "output_metric_kind": "delta_counter",
        "output_unit": "count",
        "missing_component_policy": "missing",
        "zero_denominator_policy": None,
        "median_weight": None,
        "histogram_inputs": None,
        "output_quantiles": None,
    }


def test_all_top_level_records_have_fixed_unambiguous_record_type() -> None:
    assert [record.record_type for record in p1.all_records()] == [
        "node_metric",
        "edge_metric",
        "burst_event",
        "topology_snapshot",
        "alert_event",
        "incident_label",
        "rca_report",
    ]


@pytest.mark.parametrize(
    "metric_kind",
    ["gauge", "monotonic_counter", "delta_counter"],
)
def test_scalar_metric_kinds_are_legal_without_distribution_fields(metric_kind: str) -> None:
    node = p1.make_node(metric_kind=metric_kind)
    assert node.metric_kind == metric_kind
    assert node.histogram_upper_bound is None
    assert node.histogram_is_cumulative is None
    assert node.quantile is None


def test_histogram_bucket_constructs_and_round_trips(tmp_path) -> None:
    node = p1.make_node(
        metric_kind="histogram_bucket",
        histogram_upper_bound=50.0,
        histogram_is_cumulative=True,
    )
    edge = p1.make_edge(
        metric_kind="histogram_bucket",
        histogram_upper_bound=100.0,
        histogram_is_cumulative=False,
    )
    path = tmp_path / "histograms.parquet"
    write_records_parquet(path, [node, edge])
    restored = read_records_parquet(path)
    assert restored == [node, edge]
    assert restored[0].histogram_upper_bound == 50.0
    assert restored[0].histogram_is_cumulative is True
    assert restored[0].quantile is None


def test_histogram_requires_bound_and_cumulative_flag() -> None:
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="histogram_bucket", histogram_is_cumulative=True)
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="histogram_bucket", histogram_upper_bound=10.0)
    with pytest.raises(TypeError):
        p1.make_node(
            metric_kind="histogram_bucket",
            histogram_upper_bound=10.0,
            histogram_is_cumulative=1,
        )


@pytest.mark.parametrize("metric_kind", ["gauge", "monotonic_counter", "delta_counter"])
def test_non_histogram_rejects_histogram_fields(metric_kind: str) -> None:
    with pytest.raises(ValueError):
        p1.make_node(metric_kind=metric_kind, histogram_upper_bound=10.0)
    with pytest.raises(ValueError):
        p1.make_node(metric_kind=metric_kind, histogram_is_cumulative=False)


def test_quantile_constructs_only_with_valid_probability() -> None:
    node = p1.make_node(metric_kind="quantile", quantile=0.99)
    assert node.quantile == 0.99
    assert node.histogram_upper_bound is None
    assert node.histogram_is_cumulative is None


@pytest.mark.parametrize("value", [0.0, 1.0, -0.1, 1.1, math.nan, math.inf])
def test_quantile_rejects_out_of_range_or_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="quantile", quantile=value)


def test_quantile_and_histogram_fields_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError):
        p1.make_node(
            metric_kind="quantile",
            quantile=0.95,
            histogram_upper_bound=100.0,
        )
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="gauge", quantile=0.95)


def test_pod_scope_requires_pod_uid() -> None:
    with pytest.raises(ValueError):
        p1.make_node(scope="pod", pod_uid=None)


@pytest.mark.parametrize("scope", ["flow", "pod_pair", "service_pair"])
def test_node_rejects_edge_scopes(scope: str) -> None:
    with pytest.raises(ValueError):
        p1.make_node(scope=scope)


@pytest.mark.parametrize("scope", ["pod", "service", "node"])
def test_edge_rejects_node_scopes(scope: str) -> None:
    with pytest.raises(ValueError):
        p1.make_edge(scope=scope)


@pytest.mark.parametrize("scope", ["pod", "service", "node"])
def test_node_accepts_only_node_scopes(scope: str) -> None:
    pod_uid = "pod-a" if scope == "pod" else None
    assert p1.make_node(scope=scope, pod_uid=pod_uid).scope == scope


@pytest.mark.parametrize("scope", ["flow", "pod_pair", "service_pair"])
def test_edge_accepts_only_edge_scopes(scope: str) -> None:
    assert p1.make_edge(scope=scope).scope == scope


def test_unknown_record_type_is_rejected_without_shape_guessing(tmp_path) -> None:
    path = tmp_path / "unknown.json"
    path.write_text(
        json.dumps({"record_type": "unknown_metric", "record": p1.make_node().to_dict()}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown record_type"):
        read_record_json(path)


def test_envelope_and_record_type_conflict_is_rejected(tmp_path) -> None:
    path = tmp_path / "conflict.json"
    path.write_text(
        json.dumps({"record_type": "edge_metric", "record": p1.make_node().to_dict()}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="record_type"):
        read_record_json(path)


def test_record_payload_cannot_change_its_fixed_type() -> None:
    payload = p1.make_node().to_dict()
    payload["record_type"] = "edge_metric"
    with pytest.raises(ValueError, match="record_type"):
        NodeMetricRecord.from_dict(payload)


def test_mixed_jsonl_restores_concrete_types_from_record_type(tmp_path) -> None:
    records = p1.all_records()
    path = tmp_path / "mixed.jsonl"
    write_records_jsonl(path, records)
    restored = read_records_jsonl(path)
    assert restored == records
    assert [type(record) for record in restored] == [type(record) for record in records]
    assert [record.record_type for record in restored] == [record.record_type for record in records]


def test_mixed_parquet_preserves_semantics_none_and_numeric_types(tmp_path) -> None:
    histogram = p1.make_node(
        metric_kind="histogram_bucket",
        histogram_upper_bound=25.0,
        histogram_is_cumulative=True,
    )
    quantile = p1.make_edge(metric_kind="quantile", quantile=0.95)
    records = [histogram, quantile, *p1.all_records()[2:]]
    path = tmp_path / "mixed.parquet"
    write_records_parquet(path, records)
    restored = read_records_parquet(path)
    assert restored == records
    assert isinstance(restored[0].histogram_upper_bound, float)
    assert restored[0].quantile is None
    assert restored[1].histogram_upper_bound is None
    assert isinstance(restored[1].quantile, float)
    assert [record.record_type for record in restored] == [record.record_type for record in records]


def test_stable_index_remains_order_independent_with_metric_semantics() -> None:
    node_ids = [node_id(p1.make_node()), node_id(p1.make_node(metric_name="cpu.usage"))]
    edge_ids = [edge_id(p1.make_edge()), edge_id(p1.make_edge(metric_name="tcp.retrans_rate"))]
    shock_ids = [shock_id(p1.make_edge()), shock_id(p1.make_edge(metric_name="tcp.retrans_rate"))]
    first = StableIndex.build(node_ids=node_ids, edge_ids=edge_ids, shock_ids=shock_ids)
    second = StableIndex.build(
        node_ids=list(reversed(node_ids)),
        edge_ids=list(reversed(edge_ids)),
        shock_ids=list(reversed(shock_ids)),
    )
    assert first == second


@pytest.mark.parametrize("method", ["sum", "last_same_series", "median_max"])
def test_basic_aggregation_methods_are_explicit(method: str) -> None:
    payload = aggregation_payload(method)
    payload["source_scope"] = "service"
    payload["target_scope"] = "service"
    payload["output_metric_name"] = "cpu.usage"
    if method == "last_same_series":
        payload["input_series_ids"] = ["series-a"]
    if method == "median_max":
        payload.update(input_metric_kind="gauge", output_metric_kind="gauge", median_weight=0.5)
    assert MetricAggregationSpec.from_dict(payload).method == method


def test_ratio_from_components_requires_explicit_components() -> None:
    payload = aggregation_payload("ratio_from_components")
    payload.update(
        {
            "input_metric_kind": "delta_counter",
            "input_metric_ids": None,
            "numerator_metric_id": "observability::service-a::request.error_count",
            "denominator_metric_id": "observability::service-a::request.count",
            "numerator_metric_kind": "delta_counter",
            "denominator_metric_kind": "delta_counter",
            "numerator_scope": "service",
            "denominator_scope": "service",
            "source_scope": "service",
            "target_scope": "service",
            "output_metric_name": "request.error_rate",
            "output_metric_kind": "gauge",
            "output_unit": "ratio",
            "zero_denominator_policy": "missing",
        }
    )
    spec = MetricAggregationSpec.from_dict(payload)
    assert spec.numerator_metric_id is not None
    assert spec.denominator_metric_id is not None
    for missing in ("numerator_metric_id", "denominator_metric_id"):
        invalid = dict(payload)
        invalid[missing] = None
        with pytest.raises(ValueError):
            MetricAggregationSpec.from_dict(invalid)


def test_histogram_merge_quantile_requires_histogram_and_quantiles() -> None:
    payload = aggregation_payload("histogram_merge_quantile")
    payload.update(
        {
            "input_metric_kind": "histogram_bucket",
            "input_metric_ids": None,
            "output_metric_name": "request.latency",
            "output_metric_kind": "quantile",
            "output_unit": "ms",
            "histogram_inputs": [
                {"metric_id": "bucket-10", "metric_identity": "request.latency", "metric_kind": "histogram_bucket", "unit": "ms", "scope": "pod", "upper_bound": 10.0, "is_inf_bucket": False, "is_cumulative": True},
                {"metric_id": "bucket-inf", "metric_identity": "request.latency", "metric_kind": "histogram_bucket", "unit": "ms", "scope": "pod", "upper_bound": None, "is_inf_bucket": True, "is_cumulative": True},
            ],
            "output_quantiles": [0.5, 0.95, 0.99],
        }
    )
    spec = MetricAggregationSpec.from_dict(payload)
    assert spec.output_quantiles == [0.5, 0.95, 0.99]
    invalid = dict(payload)
    invalid["output_quantiles"] = []
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(invalid)


def test_cross_scope_precomputed_quantile_cannot_be_aggregated() -> None:
    invalid = aggregation_payload("sum")
    invalid["input_metric_kind"] = "quantile"
    with pytest.raises(ValueError, match="reject_cross_scope_quantile"):
        MetricAggregationSpec.from_dict(invalid)

    explicit_rejection = dict(invalid)
    explicit_rejection["method"] = "reject_cross_scope_quantile"
    explicit_rejection.update(output_metric_name=None, output_metric_kind=None, output_unit=None)
    assert MetricAggregationSpec.from_dict(explicit_rejection).method == "reject_cross_scope_quantile"

    robust_policy = dict(invalid)
    robust_policy["method"] = "median_max"
    robust_policy.update(output_metric_name="latency", output_metric_kind="quantile", output_unit="ms", median_weight=0.5)
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(robust_policy)


def test_legacy_conversion_requires_explicit_metric_kind() -> None:
    legacy = MetricRecord(1.0, "service-a", "pod-a", "worker-a", "cpu.usage", 0.5, "legacy")
    with pytest.raises(TypeError):
        node_metric_from_legacy(
            legacy,
            schema_version="1.0",
            window_sec=10,
            cluster_id="cluster-a",
            namespace="observability",
            metric_family="cpu",
            unit="cores",
            sample_count=1,
            coverage=1.0,
            event_loss_rate=0.0,
            scope="pod",
            histogram_upper_bound=None,
            histogram_is_inf_bucket=False,
            histogram_is_cumulative=None,
            quantile=None,
        )


def test_aggregation_contract_has_no_metric_name_heuristics_or_fixed_services() -> None:
    source = inspect.getsource(MetricAggregationSpec).lower()
    assert '"p99"' not in source
    assert '"count"' not in source
    assert ".endswith(" not in source
    assert ".startswith(" not in source
    for path in (
        Path(inspect.getsourcefile(MetricAggregationSpec)),
        Path(inspect.getsourcefile(NodeMetricRecord)),
    ):
        text = path.read_text(encoding="utf-8").lower()
        assert "paymentservice" not in text
        assert "checkoutservice" not in text
        assert "online-boutique" not in text

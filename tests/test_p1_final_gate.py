from __future__ import annotations

from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
from proberca.config import (
    HistogramBucketInputSpec,
    MetricAggregationSpec,
    MonotonicCounterPolicy,
)
from proberca.data.index import StableIndex, migrate_v1_stable_id, node_id
from proberca.data.io import read_records_jsonl, read_records_parquet, write_records_jsonl, write_records_parquet
from proberca.data.schema import (
    MetricRecord,
    MetricRegistry,
    MetricSemantics,
    RCAReport,
    RootCause,
    TopologyEdge,
    TopologySnapshot,
    node_metric_from_registry,
)


def bucket(bound: float | None, *, inf: bool = False, unit: str = "ms", cumulative: bool = True,
           identity: str = "request.latency") -> HistogramBucketInputSpec:
    return HistogramBucketInputSpec.from_dict({
        "metric_id": f"cluster-a::observability::service-a::request.latency_bucket::{bound}",
        "metric_identity": identity,
        "metric_kind": "histogram_bucket",
        "unit": unit,
        "scope": "pod",
        "upper_bound": bound,
        "is_inf_bucket": inf,
        "is_cumulative": cumulative,
    })


def spec(method: str, kind: str = "gauge", source: str = "pod", target: str = "service") -> dict:
    return {
        "method": method,
        "input_metric_kind": kind,
        "source_scope": source,
        "target_scope": target,
        "input_metric_ids": ["cluster-a::observability::service-a::cpu.usage"],
        "input_series_ids": None,
        "numerator_metric_id": None,
        "denominator_metric_id": None,
        "numerator_metric_kind": None,
        "denominator_metric_kind": None,
        "numerator_scope": None,
        "denominator_scope": None,
        "output_metric_name": "cpu.usage.service",
        "output_metric_kind": kind,
        "output_unit": "cores",
        "missing_component_policy": "missing",
        "zero_denominator_policy": None,
        "median_weight": None,
        "histogram_inputs": None,
        "output_quantiles": None,
    }


@pytest.mark.parametrize("method", ["sum", "median_max", "last_same_series", "ratio_from_components"])
def test_pod_quantile_cannot_cross_scope(method: str) -> None:
    payload = spec(method, "quantile")
    if method == "median_max":
        payload["median_weight"] = 0.5
    if method == "last_same_series":
        payload["input_series_ids"] = ["series-a"]
    if method == "ratio_from_components":
        payload.update(input_metric_ids=None, numerator_metric_id="n", denominator_metric_id="d",
                       numerator_metric_kind="gauge", denominator_metric_kind="gauge",
                       numerator_scope="pod", denominator_scope="pod",
                       zero_denominator_policy="missing")
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)


def test_cross_scope_quantile_is_explicit_rejection_only() -> None:
    payload = spec("reject_cross_scope_quantile", "quantile")
    payload.update(output_metric_name=None, output_metric_kind=None, output_unit=None)
    assert MetricAggregationSpec.from_dict(payload).method == "reject_cross_scope_quantile"


def test_same_series_quantile_can_only_use_last_same_series() -> None:
    payload = spec("last_same_series", "quantile", "pod", "pod")
    payload["input_series_ids"] = ["cluster-a::observability::service-a::pod-a::latency"]
    assert MetricAggregationSpec.from_dict(payload).method == "last_same_series"


@pytest.mark.parametrize("method", ["sum", "median_max"])
def test_same_scope_quantile_cannot_be_reaggregated(method: str) -> None:
    payload = spec(method, "quantile", "pod", "pod")
    payload["median_weight"] = 0.5 if method == "median_max" else None
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)


def test_finite_and_inf_histogram_buckets_round_trip(tmp_path) -> None:
    finite = p1.make_node(metric_kind="histogram_bucket", value=4, histogram_upper_bound=10.0,
                          histogram_is_inf_bucket=False, histogram_is_cumulative=True)
    inf = replace(finite, histogram_upper_bound=None, histogram_is_inf_bucket=True, value=5)
    path = tmp_path / "buckets.parquet"
    write_records_parquet(path, [finite, inf])
    restored = read_records_parquet(path)
    assert restored == [finite, inf]
    assert restored[1].histogram_upper_bound is None and restored[1].histogram_is_inf_bucket is True


def test_histogram_float_inf_and_negative_count_fail() -> None:
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="histogram_bucket", histogram_upper_bound=float("inf"),
                     histogram_is_inf_bucket=False, histogram_is_cumulative=True)
    with pytest.raises(ValueError):
        p1.make_node(metric_kind="histogram_bucket", value=-1, histogram_upper_bound=1.0,
                     histogram_is_inf_bucket=False, histogram_is_cumulative=True)


def histogram_spec(inputs: list[HistogramBucketInputSpec]) -> dict:
    payload = spec("histogram_merge_quantile", "histogram_bucket")
    payload.update(input_metric_ids=None, output_metric_name="request.latency", output_metric_kind="quantile",
                   output_unit="ms", histogram_inputs=[item.to_dict() for item in inputs],
                   output_quantiles=[0.5, 0.95, 0.99])
    return payload


def test_histogram_merge_contract_is_complete() -> None:
    payload = histogram_spec([bucket(10.0), bucket(50.0), bucket(None, inf=True)])
    restored = MetricAggregationSpec.from_dict(payload)
    assert restored.output_quantiles == [0.5, 0.95, 0.99]
    assert restored.to_dict() == payload


def test_histogram_merge_rejects_incompatible_scope_and_output_kind() -> None:
    payload = histogram_spec([bucket(10.0), bucket(None, inf=True)])
    payload["target_scope"] = "node"
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)
    payload = histogram_spec([bucket(10.0), bucket(None, inf=True)])
    payload["output_metric_kind"] = "gauge"
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)


@pytest.mark.parametrize("inputs", [
    [bucket(None, inf=True), bucket(None, inf=True)],
    [bucket(None, inf=True), bucket(10.0)],
    [bucket(10.0, unit="ms"), bucket(50.0, unit="s")],
    [bucket(10.0, cumulative=True), bucket(50.0, cumulative=False)],
    [bucket(10.0), bucket(50.0, identity="other")],
])
def test_invalid_histogram_sets_fail(inputs) -> None:
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(histogram_spec(inputs))


def test_quantile_cannot_feed_histogram_merge() -> None:
    payload = histogram_spec([bucket(10.0), bucket(None, inf=True)])
    payload["input_metric_kind"] = "quantile"
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)


def test_median_max_requires_valid_weight_and_gauge() -> None:
    payload = spec("median_max")
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(payload)
    payload["median_weight"] = 0.6
    assert MetricAggregationSpec.from_dict(payload).median_weight == 0.6
    for value in (-0.1, 1.1):
        payload["median_weight"] = value
        with pytest.raises(ValueError):
            MetricAggregationSpec.from_dict(payload)
    for kind in ("delta_counter", "monotonic_counter", "histogram_bucket", "quantile"):
        payload.update(median_weight=0.5, input_metric_kind=kind, output_metric_kind=kind)
        with pytest.raises(ValueError):
            MetricAggregationSpec.from_dict(payload)


def test_counter_sum_and_reset_contract() -> None:
    raw = spec("sum", "monotonic_counter")
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(raw)
    delta = spec("sum", "delta_counter")
    assert MetricAggregationSpec.from_dict(delta).input_metric_kind == "delta_counter"
    invalid_output = dict(delta); invalid_output["output_metric_kind"] = "gauge"
    with pytest.raises(ValueError):
        MetricAggregationSpec.from_dict(invalid_output)
    policy = MonotonicCounterPolicy.from_dict({
        "delta_before_cross_series_sum": True,
        "value_decrease_means_reset": True,
        "reset_policy": "use_current_value",
    })
    assert policy.value_decrease_means_reset is True


def ratio_spec() -> dict:
    payload = spec("ratio_from_components", "delta_counter")
    payload.update(input_metric_ids=None, numerator_metric_id="error-count", denominator_metric_id="request-count",
                   numerator_metric_kind="delta_counter", denominator_metric_kind="delta_counter",
                   numerator_scope="service", denominator_scope="service", source_scope="service",
                   target_scope="service", output_metric_name="error-rate", output_metric_kind="gauge",
                   output_unit="ratio", zero_denominator_policy="missing")
    return payload


def test_ratio_policies_and_scope_are_strict() -> None:
    assert MetricAggregationSpec.from_dict(ratio_spec()).zero_denominator_policy == "missing"
    for field in ("numerator_metric_id", "denominator_metric_id"):
        payload = ratio_spec(); payload[field] = None
        with pytest.raises(ValueError): MetricAggregationSpec.from_dict(payload)
    payload = ratio_spec(); payload["denominator_scope"] = "pod"
    with pytest.raises(ValueError): MetricAggregationSpec.from_dict(payload)
    payload = ratio_spec(); payload["zero_denominator_policy"] = "zero"
    with pytest.raises(ValueError): MetricAggregationSpec.from_dict(payload)


def test_cluster_aware_ids_and_explicit_v1_migration(tmp_path) -> None:
    a = p1.make_node(cluster_id="cluster-a")
    b = p1.make_node(cluster_id="cluster-b")
    assert node_id(a) != node_id(b)
    assert node_id(a).startswith("cluster-a::")
    legacy = "observability::service-a::cpu.throttled_usec"
    migrated = migrate_v1_stable_id(legacy, kind="node", cluster_id="cluster-a")
    assert migrated == node_id(a)
    with pytest.raises(ValueError):
        StableIndex.build(node_ids=[legacy, migrated], edge_ids=[], shock_ids=[])
    index = StableIndex.build(node_ids=[node_id(b), node_id(a)], edge_ids=[], shock_ids=[])
    path = tmp_path / "index.npz"; index.save_npz(path)
    assert StableIndex.load_npz(path) == index
    payload = index.to_dict()
    payload["entries"][0]["id"] = legacy
    with pytest.raises(ValueError):
        StableIndex.from_dict(payload)


def test_legacy_registry_is_exact_and_missing_entries_fail() -> None:
    legacy = MetricRecord(1.0, "service-a", "pod-a", "worker-a", "opaque.metric", 1.0, "legacy")
    registry = MetricRegistry({
        "registered-id": MetricSemantics("gauge", "pod", None, False, None, None)
    })
    common = dict(schema_version="1.0", window_sec=10, cluster_id="cluster-a",
                  namespace="observability", metric_family="cpu", unit="cores",
                  sample_count=1, coverage=1.0, event_loss_rate=0.0)
    with pytest.raises(KeyError):
        node_metric_from_registry(legacy, registry=registry, metric_id="opaque.metric", **common)
    converted = node_metric_from_registry(legacy, registry=registry, metric_id="registered-id", **common)
    assert converted.metric_kind == "gauge"


def test_topology_rejects_bad_endpoints_time_duplicates_and_loops() -> None:
    with pytest.raises(ValueError):
        replace(p1.make_topology(), call_edges=[TopologyEdge("service-a", "missing", "call")])
    with pytest.raises(ValueError):
        replace(p1.make_topology(), valid_to_ns=1_000_000_000)
    with pytest.raises(ValueError):
        replace(p1.make_topology(), services=["service-a", "service-a"])
    with pytest.raises(ValueError):
        replace(p1.make_topology(), call_edges=[TopologyEdge("service-a", "service-a", "call")])


def test_root_and_candidate_structural_validation() -> None:
    with pytest.raises(ValueError):
        RootCause("node", "service-a", "cpu", "edge-a", "self", None)
    with pytest.raises(ValueError):
        RootCause("edge", None, None, "edge-a", "propagated-edge", None)
    with pytest.raises(ValueError):
        RootCause("ambiguous", "service-a", None, None, "ambiguous", None)
    report = p1.make_report().to_dict()
    report["ranked_candidates"] = [{"object_type": "edge", "node_id": None, "edge_id": "edge-a",
                                     "root_metric": None, "edge_subtype": "propagated-edge",
                                     "score": 0.8, "role": "propagated"}]
    with pytest.raises(ValueError):
        RCAReport.from_dict(report)


def test_mixed_jsonl_and_parquet_still_use_record_type(tmp_path) -> None:
    records = p1.all_records()
    jsonl = tmp_path / "mixed.jsonl"; parquet = tmp_path / "mixed.parquet"
    write_records_jsonl(jsonl, records); write_records_parquet(parquet, records)
    assert [type(item) for item in read_records_jsonl(jsonl)] == [type(item) for item in records]
    assert [type(item) for item in read_records_parquet(parquet)] == [type(item) for item in records]

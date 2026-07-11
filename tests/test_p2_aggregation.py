from __future__ import annotations

import json
from dataclasses import replace

import pytest

import test_p1_data_contracts as p1
from proberca.aggregation import (
    AggregationPlan,
    CounterDeltaTracker,
    LateRecordError,
    RejectedWindowError,
    WindowAggregator,
)
from proberca.config import HistogramBucketInputSpec, MetricAggregationSpec, MonotonicCounterPolicy, WindowConfig


def spec(method="sum", kind="delta_counter", source="pod", target="service", **changes):
    payload = {
        "method": method,
        "input_metric_kind": kind,
        "source_scope": source,
        "target_scope": target,
        "input_metric_ids": ["cluster-a::observability::service-a::request.count"],
        "input_series_ids": None,
        "numerator_metric_id": None,
        "denominator_metric_id": None,
        "numerator_metric_kind": None,
        "denominator_metric_kind": None,
        "numerator_scope": None,
        "denominator_scope": None,
        "output_metric_name": "request.count",
        "output_metric_kind": kind,
        "output_unit": "count",
        "missing_component_policy": "missing",
        "zero_denominator_policy": None,
        "median_weight": None,
        "histogram_inputs": None,
        "output_quantiles": None,
    }
    payload.update(changes)
    return MetricAggregationSpec.from_dict(payload)


def node(ts=1, metric="request.count", value=1, kind="delta_counter", pod="pod-a", **changes):
    values = {"timestamp_ns": ts, "window_sec": 1, "metric_family": "request", "metric_name": metric,
              "value": value, "metric_kind": kind, "scope": "pod", "pod_uid": pod,
              "unit": "count", "sample_count": 1}
    values.update(changes)
    return p1.make_node(**values)


def test_window_boundaries_order_lateness_and_empty(tmp_path):
    output = "cluster-a::observability::service-a::request.count"
    agg = WindowAggregator(1, 0, AggregationPlan([(output, spec())]))
    records = [node(999_999_999, value=2), node(0, value=1)]
    for record in reversed(records):
        agg.add(record)
    batch = agg.finalize(0)
    assert (batch.window_start_ns, batch.window_end_ns) == (0, 1_000_000_000)
    assert batch.node_records[0].value == 3
    assert batch.node_records[0].timestamp_ns == 1_000_000_000
    assert agg.finalize(1_000_000_000).node_records == []
    with pytest.raises(LateRecordError):
        agg.add(node(1, value=9))
    with pytest.raises(ValueError):
        agg.add(p1.make_node(timestamp_ns=1, window_sec=2))
    path = tmp_path / "window.json"
    pending = WindowAggregator(1, 0, AggregationPlan([(output, spec())]))
    pending.add(node(2_000_000_000, value=4))
    pending.save_json(path)
    restored = WindowAggregator.load_json(path, AggregationPlan([(output, spec())]))
    assert restored.finalize(2_000_000_000) == pending.finalize(2_000_000_000)
    ordered = WindowAggregator(1, 0, AggregationPlan([(output, spec())]))
    ordered.finalize(1_000_000_000)
    with pytest.raises(ValueError, match="chronological"):
        ordered.finalize(0)


def test_input_order_is_deterministic():
    output = "cluster-a::observability::service-a::request.count"
    records = [node(3, value=3, pod="pod-b"), node(2, value=2)]
    results = []
    for ordered in (records, list(reversed(records))):
        agg = WindowAggregator(1, 0, AggregationPlan([(output, spec())]))
        for record in ordered:
            agg.add(record)
        results.append(agg.finalize(0))
    assert results[0] == results[1]


def test_window_config_and_out_of_order_counter_are_deterministic():
    assert WindowConfig.from_dict({"window_sec": 1, "allowed_lateness_sec": 0}).allowed_lateness_sec == 0
    with pytest.raises(ValueError):
        WindowConfig.from_dict({"window_sec": 1, "allowed_lateness_sec": -1})
    output = "cluster-a::observability::service-a::request.count"
    raw = [node(2, value=15, kind="monotonic_counter"), node(1, value=10, kind="monotonic_counter")]
    agg = WindowAggregator(1, 0, AggregationPlan([(output, spec())]),
                           CounterDeltaTracker(counter_policy("use_current_value")))
    for record in raw:
        agg.add(record)
    batch = agg.finalize(0)
    assert batch.node_records[0].value == 5
    assert batch.missing_outputs[0].reason_code == "insufficient_history"
    delayed = WindowAggregator(1, 2, AggregationPlan([(output, spec())]))
    delayed.add(node(1, value=1))
    with pytest.raises(ValueError, match="watermark"):
        delayed.finalize(0, watermark_ns=2_999_999_999)
    assert delayed.finalize(0, watermark_ns=3_000_000_000).node_records[0].value == 1


def test_sum_median_and_quality_are_hand_verifiable():
    count_id = "cluster-a::observability::service-a::request.count"
    gauge_id = "cluster-a::observability::service-a::cpu.usage"
    gauge = spec("median_max", "gauge", output_metric_name="cpu.usage", output_metric_kind="gauge",
                 output_unit="cores", input_metric_ids=[gauge_id], median_weight=0.25)
    plan = AggregationPlan([(count_id, spec()), (gauge_id, gauge)])
    records = [
        node(1, value=2, sample_count=1, coverage=1.0, event_loss_rate=0.0),
        node(2, value=4, pod="pod-b", sample_count=3, coverage=0.5, event_loss_rate=0.2),
        node(3, metric="cpu.usage", value=2, kind="gauge", unit="cores"),
        node(4, metric="cpu.usage", value=6, kind="gauge", unit="cores", pod="pod-b"),
    ]
    batch = plan.execute(records, 0, 1_000_000_000)
    by_name = {record.metric_name: record for record in batch.node_records}
    assert by_name["request.count"].value == 6
    assert by_name["request.count"].sample_count == 4
    assert by_name["request.count"].coverage == pytest.approx(0.625)
    assert by_name["request.count"].event_loss_rate == pytest.approx(0.15)
    assert by_name["cpu.usage"].value == pytest.approx(5.5)


def test_last_same_series_and_conflict():
    metric_id = "cluster-a::observability::service-a::cpu.usage"
    first = node(1, metric="cpu.usage", value=2, kind="gauge", unit="cores")
    latest = replace(first, timestamp_ns=2, value=5)
    last = spec("last_same_series", "gauge", "pod", "pod", input_metric_ids=[metric_id],
                input_series_ids=[first.series_id], output_metric_name="cpu.usage",
                output_metric_kind="gauge", output_unit="cores")
    plan = AggregationPlan([(metric_id, last)])
    assert plan.execute([latest, first], 0, 1_000_000_000).node_records[0].value == 5
    conflict = replace(latest, value=6)
    batch = plan.execute([first, latest, conflict], 0, 1_000_000_000)
    assert batch.node_records == []
    assert [issue.reason_code for issue in batch.invalid_outputs] == ["duplicate_series_value"]
    with pytest.raises(ValueError):
        plan.execute([first, replace(first, pod_uid="pod-b")], 0, 1_000_000_000)


def counter_policy(reset_policy):
    return MonotonicCounterPolicy.from_dict({"delta_before_cross_series_sum": True,
        "value_decrease_means_reset": True, "reset_policy": reset_policy})


def test_counter_delta_normal_reset_and_snapshot(tmp_path):
    tracker = CounterDeltaTracker(counter_policy("use_current_value"))
    first = node(1, value=10, kind="monotonic_counter")
    assert tracker.process(first)[0] is None
    delta, issues = tracker.process(replace(first, timestamp_ns=2, value=14))
    assert delta.value == 4 and issues == []
    reset, issues = tracker.process(replace(first, timestamp_ns=3, value=2))
    assert reset.value == 2 and issues[0].reason_code == "counter_reset"
    assert reset.value >= 0
    path = tmp_path / "counter.json"
    tracker.save_json(path)
    restored = CounterDeltaTracker.load_json(path, counter_policy("use_current_value"))
    assert restored.process(replace(first, timestamp_ns=4, value=5)) == tracker.process(replace(first, timestamp_ns=4, value=5))


def test_counter_reset_policies_and_per_series_delta():
    first_a = node(1, value=10, kind="monotonic_counter", pod="pod-a")
    first_b = node(1, value=20, kind="monotonic_counter", pod="pod-b")
    mark = CounterDeltaTracker(counter_policy("mark_missing"))
    mark.process(first_a)
    delta, issues = mark.process(replace(first_a, timestamp_ns=2, value=1))
    assert delta is None and issues[0].reason_code == "counter_reset"
    reject = CounterDeltaTracker(counter_policy("reject_window"))
    reject.process(first_a)
    with pytest.raises(RejectedWindowError):
        reject.process(replace(first_a, timestamp_ns=2, value=1))
    normal = CounterDeltaTracker(counter_policy("use_current_value"))
    for record in (first_b, first_a):
        normal.process(record)
    da = normal.process(replace(first_a, timestamp_ns=2, value=13))[0]
    db = normal.process(replace(first_b, timestamp_ns=2, value=25))[0]
    assert da.value + db.value == 8


def ratio_spec(numerator, denominator):
    return spec("ratio_from_components", "delta_counter", "service", "service",
                input_metric_ids=None, numerator_metric_id=numerator, denominator_metric_id=denominator,
                numerator_metric_kind="delta_counter", denominator_metric_kind="delta_counter",
                numerator_scope="service", denominator_scope="service", output_metric_name="request.error_rate",
                output_metric_kind="gauge", output_unit="ratio", zero_denominator_policy="missing")


def test_ratio_value_missing_zero_and_quality():
    numerator = "cluster-a::observability::service-a::request.errors"
    denominator = "cluster-a::observability::service-a::request.total"
    num_spec = spec(input_metric_ids=[numerator], output_metric_name="request.errors")
    den_spec = spec(input_metric_ids=[denominator], output_metric_name="request.total")
    ratio_id = "cluster-a::observability::service-a::request.error_rate"
    plan = AggregationPlan([(numerator, num_spec), (denominator, den_spec), (ratio_id, ratio_spec(numerator, denominator))])
    records = [node(1, metric="request.errors", value=2, coverage=0.8, event_loss_rate=0.1),
               node(1, metric="request.total", value=10, coverage=0.6, event_loss_rate=0.3)]
    result = plan.execute(records, 0, 1_000_000_000)
    ratio = next(record for record in result.node_records if record.metric_name == "request.error_rate")
    assert ratio.value == pytest.approx(0.2)
    assert ratio.coverage == 0.6 and ratio.event_loss_rate == 0.3
    for subset, reason in ((records[:1], "missing_component"), (records[1:], "missing_component"),
                           ([records[0], replace(records[1], value=0)], "zero_denominator")):
        batch = plan.execute(subset, 0, 1_000_000_000)
        assert not any(record.metric_name == "request.error_rate" for record in batch.node_records)
        assert reason in [issue.reason_code for issue in batch.missing_outputs]


def histogram_spec(cumulative=True):
    metric_id = "cluster-a::observability::service-a::request.latency_bucket"
    inputs = [HistogramBucketInputSpec(metric_id, "request.latency", "histogram_bucket", "ms", "pod", bound, inf, cumulative)
              for bound, inf in ((10.0, False), (50.0, False), (None, True))]
    return spec("histogram_merge_quantile", "histogram_bucket", input_metric_ids=None,
                output_metric_name="request.latency", output_metric_kind="quantile", output_unit="ms",
                histogram_inputs=inputs, output_quantiles=[0.5, 0.95, 0.99])


def bucket(ts, pod, bound, value, cumulative=True, inf=False):
    return node(ts, metric="request.latency_bucket", value=value, kind="histogram_bucket", pod=pod,
                unit="ms", histogram_upper_bound=bound, histogram_is_inf_bucket=inf,
                histogram_is_cumulative=cumulative)


@pytest.mark.parametrize("cumulative,values,expected", [
    (True, [(3, 5, 5), (2, 4, 4)], [10.0, 50.0, 50.0]),
    (False, [(3, 2, 1), (2, 1, 1)], [10.0, 50.0, 50.0]),
])
def test_histogram_merge_quantiles(cumulative, values, expected):
    records = []
    for pod, counts in zip(("pod-a", "pod-b"), values):
        records += [bucket(1, pod, 10.0, counts[0], cumulative), bucket(2, pod, 50.0, counts[1], cumulative),
                    bucket(3, pod, None, counts[2], cumulative, True)]
    output = "cluster-a::observability::service-a::request.latency"
    batch = AggregationPlan([(output, histogram_spec(cumulative))]).execute(records, 0, 1_000_000_000)
    assert [record.value for record in batch.node_records] == expected
    assert [record.quantile for record in batch.node_records] == [0.5, 0.95, 0.99]
    assert all(record.scope == "service" and record.metric_kind == "quantile" for record in batch.node_records)


def test_histogram_inf_issue_and_invalid_inputs():
    output = "cluster-a::observability::service-a::request.latency"
    plan = AggregationPlan([(output, histogram_spec(True))])
    records = [bucket(1, "pod-a", 10.0, 1), bucket(2, "pod-a", 50.0, 1), bucket(3, "pod-a", None, 10, inf=True)]
    batch = plan.execute(records, 0, 1_000_000_000)
    assert batch.node_records[-1].value == 50.0
    assert "quantile_in_inf_bucket" in [issue.reason_code for issue in batch.quality_issues]
    missing_total = plan.execute(records[:-1], 0, 1_000_000_000)
    assert missing_total.node_records == []
    assert missing_total.invalid_outputs[0].reason_code == "invalid_histogram"
    non_monotonic = [bucket(1, "pod-a", 10.0, 5), bucket(2, "pod-a", 50.0, 4), bucket(3, "pod-a", None, 4, inf=True)]
    assert plan.execute(non_monotonic, 0, 1_000_000_000).invalid_outputs[0].reason_code == "invalid_histogram"


def test_reject_quantile_and_plan_graph_validation():
    metric_id = "cluster-a::observability::service-a::request.latency"
    reject = spec("reject_cross_scope_quantile", "quantile", input_metric_ids=[metric_id],
                  output_metric_name=None, output_metric_kind=None, output_unit=None)
    batch = AggregationPlan([("rejected", reject)]).execute(
        [node(1, metric="request.latency", value=10, kind="quantile", unit="ms", quantile=0.99)], 0, 1_000_000_000)
    assert batch.node_records == [] and batch.invalid_outputs[0].reason_code == "incompatible_scope"
    with pytest.raises(ValueError):
        AggregationPlan([("same", spec()), ("same", spec())])
    one = ratio_spec("two", "base")
    two = ratio_spec("one", "base")
    with pytest.raises(ValueError):
        AggregationPlan([("one", one), ("two", two), ("base", spec())])
    with pytest.raises(ValueError, match="missing aggregation spec"):
        AggregationPlan([("configured", spec())]).execute([node(1, metric="other")], 0, 1_000_000_000)

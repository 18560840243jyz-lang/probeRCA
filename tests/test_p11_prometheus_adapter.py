from __future__ import annotations

import math

import pytest

from proberca.config import PrometheusQuerySpec
from proberca.data.schema import EdgeMetricRecord, NodeMetricRecord
from proberca.metrics.prometheus_client import (
    PrometheusClient, PrometheusResponseError, PrometheusSample,
)
from proberca.metrics.record_adapter import MetricMappingError, records_from_samples

from test_p11_config_security import query_spec
from test_p11_mapping_topology import inventory_with_backends


class Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {"status": "success", "data": {"result": []}}

    def json(self):
        return self._payload


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def test_prometheus_range_is_half_open_and_end_sample_is_excluded():
    payload = {"status": "success", "data": {"result": [{
        "metric": {"namespace": "observability", "pod": "pod-a"},
        "values": [[1.0, "2"], [1.999, "3"], [2.0, "4"]],
    }]}}
    client = PrometheusClient(
        "https://metrics.invalid", session=Session([Response(payload=payload)]),
        timeout_sec=1, max_retries=0)
    samples, metadata = client.query_window(query_spec(), 1_000_000_000, 2_000_000_000)
    assert [item.value for item in samples] == [2.0, 3.0]
    assert metadata.sample_count == 2


def test_prometheus_retries_5xx_but_not_authorization_failure():
    session = Session([Response(500), Response(payload={"status": "success", "data": {"result": []}})])
    client = PrometheusClient("https://metrics.invalid", session=session,
                              timeout_sec=1, max_retries=1, sleep=lambda _: None)
    client.query_window(query_spec(allow_empty=True), 0, 1_000_000_000)
    assert len(session.calls) == 2
    denied = PrometheusClient("https://metrics.invalid", session=Session([Response(401)]),
                              timeout_sec=1, max_retries=3, sleep=lambda _: None)
    with pytest.raises(PrometheusResponseError):
        denied.query_window(query_spec(), 0, 1_000_000_000)


def test_nonfinite_and_conflicting_samples_fail_without_fill():
    with pytest.raises(ValueError):
        PrometheusSample({}, 1, math.inf)
    samples = [PrometheusSample({"namespace": "observability", "pod": "pod-a"}, 1, 1.0),
               PrometheusSample({"namespace": "observability", "pod": "pod-a"}, 1, 2.0)]
    with pytest.raises(PrometheusResponseError):
        PrometheusClient.normalize_samples(samples, 0, 2)


def test_node_sample_maps_to_exact_p1_record_without_anomaly():
    revision = inventory_with_backends().freeze(2)
    sample = PrometheusSample({"namespace": "observability", "pod": "pod-a"},
                              1_500_000_000, 7.0)
    records = records_from_samples(query_spec(), (sample,), revision, window_sec=1)
    assert len(records) == 1 and isinstance(records[0], NodeMetricRecord)
    assert records[0].pod_uid == "pod-1"
    assert records[0].metric_kind == "monotonic_counter"
    assert not hasattr(records[0], "anomaly_score")


def test_edge_mapping_requires_exact_source_destination_and_protocol():
    revision = inventory_with_backends(second_service=True).freeze(2)
    spec = query_spec(
        spec_id="edge-rtt", record_type="edge_metric", metric_kind="gauge",
        counter_semantics=None, metric_name="network.rtt", metric_family="request",
        expected_scope="service_pair", service_resolution="explicit_service",
        source_resolution="service_label", destination_resolution="service_label",
        protocol_label="protocol",
        label_mapping={"source": "source", "destination": "destination",
                       "namespace": "namespace", "protocol": "protocol"},
        required_labels=["source", "destination", "namespace", "protocol"])
    sample = PrometheusSample({"source": "svc-a", "destination": "svc-b",
                               "namespace": "observability", "protocol": "http"},
                              1_500_000_000, 5.0)
    record = records_from_samples(spec, (sample,), revision, 1)[0]
    assert isinstance(record, EdgeMetricRecord)
    assert record.src_service == "svc-a" and record.dst_service == "svc-b"
    with pytest.raises(MetricMappingError):
        records_from_samples(spec, (PrometheusSample({}, 1_500_000_000, 1.0),), revision, 1)

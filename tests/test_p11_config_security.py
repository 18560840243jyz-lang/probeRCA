from __future__ import annotations

from dataclasses import replace

import pytest

from proberca.config import (
    KubernetesConfig, LeaderElectionConfig, LiveConfig, PrometheusConfig,
    PrometheusQuerySpec, RetentionConfig,
)


def query_spec(**changes):
    values = {
        "spec_id": "node-cpu", "enabled": True, "record_type": "node_metric",
        "promql": "container_cpu_total", "query_mode": "range",
        "metric_name": "runtime.cpu", "metric_family": "cpu",
        "metric_kind": "monotonic_counter", "counter_semantics": "raw_cumulative",
        "signal_spec_id": "cpu-signal", "aggregation_spec_id": "cpu-output",
        "unit": "seconds", "value_field": "value",
        "label_mapping": {"namespace": "namespace", "pod": "pod"},
        "required_labels": ["namespace", "pod"], "optional_labels": [],
        "service_resolution": "pod", "source_resolution": None,
        "destination_resolution": None, "protocol_label": None,
        "histogram_le_label": None, "quantile_label": None,
        "expected_scope": "pod", "allow_empty": False,
        "quality_policy": "strict", "query_timeout_sec": 5.0,
    }
    values.update(changes)
    return PrometheusQuerySpec.from_dict(values)


def test_kubernetes_config_requires_explicit_cluster_and_namespace_scope():
    with pytest.raises(ValueError):
        KubernetesConfig(cluster_id="", namespaces=("observability",)).validate()
    with pytest.raises(ValueError):
        KubernetesConfig(cluster_id="cluster-a", namespaces=()).validate()
    valid = KubernetesConfig(cluster_id="cluster-a", namespaces=("observability",))
    valid.validate()
    assert valid.pod_service_ambiguity_policy == "fail"


def test_watch_times_and_endpoint_policy_are_strict():
    with pytest.raises(ValueError):
        KubernetesConfig(
            cluster_id="cluster-a", namespaces=("observability",),
            reconnect_initial_sec=3, reconnect_max_sec=2).validate()
    with pytest.raises(ValueError):
        KubernetesConfig(
            cluster_id="cluster-a", namespaces=("observability",),
            endpoint_ready_policy="guess").validate()


def test_prometheus_url_rejects_query_credentials_and_secret_is_not_fingerprinted(tmp_path):
    token = tmp_path / "token"
    token.write_text("one", encoding="utf-8")
    first = PrometheusConfig(enabled=True, base_url="https://metrics.invalid",
                             token_file=str(token), query_specs=(query_spec(),))
    first.validate()
    fingerprint = first.fingerprint
    token.write_text("two", encoding="utf-8")
    assert first.fingerprint == fingerprint
    with pytest.raises(ValueError):
        replace(first, base_url="https://metrics.invalid?token=secret").validate()


def test_query_spec_counter_histogram_and_edge_semantics_are_explicit():
    assert query_spec().counter_semantics == "raw_cumulative"
    with pytest.raises(ValueError):
        query_spec(counter_semantics="delta", promql="rate(container_cpu_total[1m])",
                   metric_kind="monotonic_counter")
    with pytest.raises(ValueError):
        query_spec(record_type="edge_metric", source_resolution=None,
                   destination_resolution=None)
    histogram = query_spec(
        metric_kind="histogram_bucket", counter_semantics=None,
        histogram_le_label="le")
    assert histogram.histogram_le_label == "le"


def test_live_lease_and_retention_constraints():
    with pytest.raises(ValueError):
        LeaderElectionConfig(lease_duration_sec=10, renew_deadline_sec=10,
                             retry_period_sec=2).validate()
    with pytest.raises(ValueError):
        RetentionConfig(checkpoint_generations=1).validate()
    with pytest.raises(ValueError):
        LiveConfig(window_sec=2).validate(engine_window_sec=1)
    LiveConfig(window_sec=1).validate(engine_window_sec=1)

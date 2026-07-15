from __future__ import annotations

from types import SimpleNamespace

import pytest

from proberca.k8s.inventory import KubernetesInventory
from proberca.metrics.record_adapter import call_edges_from_samples, records_from_samples
from proberca.metrics.record_adapter import MetricMappingError
from test_p11_config_security import query_spec


def _revision():
    inventory = KubernetesInventory(
        "cluster", required_kinds=("Service",), stale_after_sec=3600,
        namespace_scope=("test-ns",))
    services = []
    for index, name in enumerate(("source", "target"), 1):
        services.append({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"namespace": "test-ns", "name": name,
                         "uid": f"svc-{index}", "resourceVersion": "1"},
        })
    inventory.replace_kind("Service", services, "1", 1_500_000_000)
    return inventory.freeze(1_500_000_000)


def _sample(timestamp_ns, value, **labels):
    return SimpleNamespace(timestamp_ns=timestamp_ns, value=value, labels=labels,
                           sample_id=f"sample-{timestamp_ns}-{value}")


def _spec(record_type, metric_name, kind="gauge"):
    mapping = {
        "namespace": "namespace", "source": "source_service",
        "destination": "destination_service", "protocol": "protocol",
    }
    return query_spec(
        spec_id=f"{record_type}-spec", record_type=record_type,
        promql="configured_query", metric_name=metric_name,
        metric_family="runtime", metric_kind=kind,
        counter_semantics="delta" if kind == "delta_counter" else None,
        unit="count" if kind == "delta_counter" else "seconds",
        expected_scope="service_pair", service_resolution="explicit_service",
        source_resolution="service_label", destination_resolution="service_label",
        protocol_label="protocol", label_mapping=mapping,
        required_labels=list(mapping.values()))


def test_real_query_samples_map_to_edge_and_call_contracts_with_half_open_window():
    revision = _revision()
    labels = {"namespace": "test-ns", "source_service": "source",
              "destination_service": "target", "protocol": "http"}
    samples = [_sample(1_100_000_000, 0.02, **labels),
               _sample(2_000_000_000, 0.03, **labels)]
    edge = records_from_samples(_spec("edge_metric", "configured_edge"), samples[:1], revision, 1)
    calls = call_edges_from_samples(
        _spec("call_edge", "configured_calls", "delta_counter"), samples,
        revision, 1_000_000_000, 2_000_000_000)
    assert len(edge) == 1
    assert len(calls) == 1
    assert calls[0].request_count == 0.02
    assert calls[0].source_service_id.endswith("::source")
    assert calls[0].destination_service_id.endswith("::target")


def test_production_python_does_not_embed_smoke_metric_names():
    from pathlib import Path

    root = Path(__file__).parents[1] / "proberca"
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.py"))
    assert "proberca_smoke_process_rss_bytes" not in source
    assert "proberca_smoke_edge_rtt_seconds" not in source
    assert "proberca_smoke_requests_total" not in source


def test_zero_request_sample_does_not_create_active_call_edge():
    labels = {"namespace": "test-ns", "source_service": "source",
              "destination_service": "target", "protocol": "http"}
    assert call_edges_from_samples(
        _spec("call_edge", "configured_calls", "delta_counter"),
        [_sample(1_100_000_000, 0.0, **labels)], _revision(),
        1_000_000_000, 2_000_000_000) == ()


def test_duplicate_call_sample_is_idempotent():
    labels = {"namespace": "test-ns", "source_service": "source",
              "destination_service": "target", "protocol": "http"}
    sample = _sample(1_100_000_000, 2.0, **labels)
    calls = call_edges_from_samples(
        _spec("call_edge", "configured_calls", "delta_counter"),
        [sample, sample], _revision(), 1_000_000_000, 2_000_000_000)
    assert calls[0].request_count == 2.0


@pytest.mark.parametrize("field,value", [
    ("source_service", "missing"), ("destination_service", "missing"),
    ("protocol", ""), ("namespace", ""),
])
def test_call_mapping_rejects_unknown_or_missing_exact_identity(field, value):
    labels = {"namespace": "test-ns", "source_service": "source",
              "destination_service": "target", "protocol": "http"}
    labels[field] = value
    with pytest.raises(MetricMappingError):
        call_edges_from_samples(
            _spec("call_edge", "configured_calls", "delta_counter"),
            [_sample(1_100_000_000, 1.0, **labels)], _revision(),
            1_000_000_000, 2_000_000_000)


def test_plain_http_prometheus_requires_explicit_test_switch():
    from dataclasses import replace
    from proberca.config import PrometheusConfig
    with pytest.raises(ValueError, match="allow_insecure_test_endpoint"):
        PrometheusConfig(enabled=True, base_url="http://127.0.0.1:9090").validate()
    replace(PrometheusConfig(enabled=True, base_url="http://127.0.0.1:9090"),
            allow_insecure_test_endpoint=True).validate()


def test_smoke_configmap_builds_canonical_engine_and_uses_subwindow_scrapes():
    from pathlib import Path

    import yaml

    from proberca.config import ProbeRCAConfig
    from proberca.orchestration.engine import ProbeRCAEngine

    manifest = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/proberca-live-configmap.yaml"
    ).read_text(encoding="utf-8"))
    config = ProbeRCAConfig.from_dict(
        yaml.safe_load(manifest["data"]["config.yaml"])
    )
    ProbeRCAEngine.from_config(config)
    assert config.window_sec >= 5
    assert config.live.window_sec == config.window_sec
    assert (
        config.live.maximum_catchup_windows * config.window_sec >= 7_200
    )
    call_spec = config.prometheus.call_edge_query_specs[0]
    assert f"[{config.window_sec}s]" in call_spec.promql
    assert len(config.aggregation_specs) == 3
    assert {item.record_type for item in config.prometheus.query_specs} == {
        "node_metric", "edge_metric",
    }
    assert {item.record_type for item in config.prometheus.call_edge_query_specs} == {
        "call_edge",
    }

    prometheus = yaml.safe_load(Path(
        "deploy/kubernetes/test/p11-smoke/prometheus-configmap.yaml"
    ).read_text(encoding="utf-8"))
    assert "scrape_interval: 250ms" in prometheus["data"]["prometheus.yml"]

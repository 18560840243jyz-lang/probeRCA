from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from proberca.dataplane.primitive_exporter import (
    FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION,
    FinalPrimitiveExporterConfig,
    _select_metric_lines,
)
from proberca.dataplane.prometheus_text import (
    PrometheusSample,
    parse_prometheus_text,
    render_prometheus_text,
)
from proberca.dataplane.raw import RawCollectionError


def test_prometheus_text_labels_are_parsed_once_and_escaped():
    samples = parse_prometheus_text(
        'metric_total{le="0.00025",server="dns://:53",'
        'route="a\\\\b\\\"c"} 7\n'
    )
    assert len(samples) == 1
    assert samples[0].label_dict == {
        "le": "0.00025",
        "route": 'a\\b"c',
        "server": "dns://:53",
    }
    rendered = render_prometheus_text(
        samples, timestamp_ms=1_234_000
    )
    assert 'le="0.00025"' in rendered
    assert 'server="dns://:53"' in rendered
    assert rendered.endswith(" 7 1234000\n")


def test_metric_selection_ignores_unrelated_invalid_beyla_family():
    exposition = (
        'messaging_duration_bucket{destination="\x00"} 1\n'
        'rpc_server_duration_seconds_count{service_name="payment"} 2\n'
    )
    with pytest.raises(RawCollectionError, match="labels"):
        parse_prometheus_text(exposition)
    selected = _select_metric_lines(
        exposition,
        frozenset({"rpc_server_duration_seconds_count"}),
    )
    samples = parse_prometheus_text(selected)
    assert [item.name for item in samples] == [
        "rpc_server_duration_seconds_count"
    ]


def test_renderer_rejects_negative_or_duplicate_output():
    with pytest.raises(RawCollectionError, match="non-negative"):
        PrometheusSample.create("counter_total", {}, -1)
    sample = PrometheusSample.create("counter_total", {"a": "b"}, 1)
    with pytest.raises(RawCollectionError, match="duplicate"):
        render_prometheus_text(
            (sample, sample), timestamp_ms=1_000
        )


def test_final_exporter_config_is_frozen_and_one_second():
    payload = yaml.safe_load(Path(
        "configs/final_primitive_exporter.example.yaml"
    ).read_text(encoding="utf-8"))
    config = FinalPrimitiveExporterConfig.from_dict(payload)
    assert config.schema_version == FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION
    assert config.snapshot_period_sec == 1
    assert "kube-system/kube-dns" in config.include_services
    assert len(config.include_services) == 12
    invalid = dict(payload)
    invalid["snapshot_period_sec"] = 2
    with pytest.raises(RawCollectionError, match="frozen range"):
        FinalPrimitiveExporterConfig.from_dict(invalid)


def test_final_bpf_normal_path_is_map_aggregated_and_window_safe():
    bpf = Path(
        "bpf/final_normal/final_normal.bpf.c"
    ).read_text(encoding="utf-8")
    loader = Path(
        "bpf/user/proberca_final_loader.c"
    ).read_text(encoding="utf-8")
    assert "BPF_MAP_TYPE_RINGBUF" not in bpf
    assert "BPF_MAP_TYPE_PERF_EVENT_ARRAY" not in bpf
    assert "futex_wait_ns_total" in bpf
    assert "dns_edge_counters" in bpf
    assert '"futex_starts"' in loader
    assert "collect_active_futex_waits" in loader
    assert "--snapshot" in loader


def test_deployment_uses_pinned_beyla_without_unused_service_graph():
    manifest = Path(
        "deploy/final-dataplane/beyla.yaml"
    ).read_text(encoding="utf-8")
    assert "grafana/beyla:3.15.0@sha256:" in manifest
    assert "application_service_graph" not in manifest
    assert "context_propagation" not in manifest
    scrape = yaml.safe_load(Path(
        "deploy/final-dataplane/prometheus-scrape-job.yaml"
    ).read_text(encoding="utf-8"))
    assert scrape["honor_timestamps"] is True
    assert scrape["scrape_interval"] == "250ms"

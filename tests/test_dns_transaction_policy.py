from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml

from proberca.dataplane.dns_policy import (
    DnsAggregationPolicy,
    aggregate_dns_role_buckets,
)
from proberca.dataplane.dns_semantic_audit import DnsSemanticRecord
from proberca.dataplane.raw import RawCollectionError


def _policy() -> DnsAggregationPolicy:
    payload = yaml.safe_load(Path(
        "configs/final_dns_aggregation_policy.yaml"
    ).read_text(encoding="utf-8"))
    return DnsAggregationPolicy.from_dict(payload)


def _record(
    *,
    container_name: str = "server",
    role: str = "application",
    cgroup_id: int = 1,
    qname_class: str = "cluster_service",
    outcome: str = "SUCCESS",
    retry_count: int = 0,
    tcp_fallback: bool = False,
    latency_ns: int | None = 1_000_000,
) -> DnsSemanticRecord:
    return DnsSemanticRecord(
        source_service="frontend",
        pod_uid="pod-1",
        container_name=container_name,
        container_role=role,
        cgroup_id=cgroup_id,
        resolver_ip="10.96.0.10",
        qname="paymentservice.online-boutique.svc.cluster.local.",
        qname_hash="hash",
        qname_class=qname_class,
        qtype="A",
        protocol="udp",
        rcode=None if outcome in {"SUCCESS", "TIMEOUT"} else outcome,
        final_outcome=outcome,
        retry_count=retry_count,
        tcp_fallback=tcp_fallback,
        first_query_ns=1,
        final_response_ns=2 if outcome != "TIMEOUT" else None,
        observed_response_latency_ns=latency_ns,
        successful_latency_ns=(
            latency_ns if outcome == "SUCCESS" else None
        ),
        matched_to_pcap=True,
    )


def test_udp_success_and_retry_after_success_are_one_logical_query() -> None:
    bucket = aggregate_dns_role_buckets((
        _record(retry_count=1),
    ), _policy())[0]
    assert bucket.logical_query_count == 1
    assert bucket.success_count == 1
    assert bucket.failure_count == 0
    assert bucket.latency_sample_count == 1


def test_udp_truncated_then_tcp_success_is_one_success() -> None:
    bucket = aggregate_dns_role_buckets((
        _record(tcp_fallback=True),
    ), _policy())[0]
    assert bucket.logical_query_count == 1
    assert bucket.success_count == 1
    assert bucket.failure_count == 0
    assert bucket.latency_p95_ms == 1.0


def test_servfail_and_timeout_never_enter_success_latency() -> None:
    buckets = aggregate_dns_role_buckets((
        _record(outcome="SERVFAIL", latency_ns=2_000_000),
        _record(outcome="TIMEOUT", latency_ns=5_000_000_000),
    ), _policy())
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket.logical_query_count == 2
    assert bucket.failure_count == 2
    assert bucket.latency_sample_count == 0
    assert bucket.latency_p95_ms is None


def test_nxdomain_uses_frozen_qname_class_policy() -> None:
    included, excluded = aggregate_dns_role_buckets((
        _record(outcome="NXDOMAIN"),
        _record(
            outcome="NXDOMAIN",
            qname_class="search_expansion",
        ),
    ), _policy())
    assert included.qname_class == "cluster_service"
    assert included.logical_query_count == 1
    assert included.failure_count == 1
    assert excluded.qname_class == "search_expansion"
    assert excluded.logical_query_count == 0
    assert excluded.excluded_count == 1


def test_same_service_containers_and_cgroups_never_auto_merge() -> None:
    app = _record(outcome="SERVFAIL", cgroup_id=11)
    sidecar = dataclasses.replace(
        _record(),
        container_name="proberca-healthy-dns-exposure",
        container_role="dns-sidecar",
        cgroup_id=22,
    )
    buckets = aggregate_dns_role_buckets((app, sidecar), _policy())
    assert {
        (
            item.container_role,
            item.formal_action,
            item.logical_query_count,
            item.failure_count,
            item.excluded_count,
        )
        for item in buckets
    } == {
        ("application", "include", 1, 1, 0),
        ("dns-sidecar", "separate", 0, 0, 1),
    }


def test_metadata_probe_is_retained_but_excluded_before_aggregation() -> None:
    bucket = aggregate_dns_role_buckets((
        _record(
            outcome="SERVFAIL",
            qname_class="metadata_probe",
        ),
    ), _policy())[0]
    assert bucket.formal_action == "record_only"
    assert bucket.logical_query_count == 0
    assert bucket.failure_count == 0
    assert bucket.excluded_count == 1


def test_transaction_role_cannot_disagree_with_frozen_policy() -> None:
    with pytest.raises(RawCollectionError, match="role disagrees"):
        aggregate_dns_role_buckets((
            _record(role="dns-sidecar"),
        ), _policy())


def test_unknown_outcome_and_failure_latency_fail_closed() -> None:
    with pytest.raises(RawCollectionError, match="unknown DNS final"):
        aggregate_dns_role_buckets((
            _record(outcome="MAYBE", latency_ns=None),
        ), _policy())
    with pytest.raises(RawCollectionError, match="successful latency"):
        aggregate_dns_role_buckets((
            dataclasses.replace(
                _record(outcome="SERVFAIL"),
                successful_latency_ns=1,
            ),
        ), _policy())


def test_policy_rejects_string_booleans() -> None:
    payload = yaml.safe_load(Path(
        "configs/final_dns_aggregation_policy.yaml"
    ).read_text(encoding="utf-8"))
    payload["diagnostic_full_qname"] = "false"
    with pytest.raises(RawCollectionError, match="must be boolean"):
        DnsAggregationPolicy.from_dict(payload)


def test_included_qname_class_must_partition_nxdomain() -> None:
    payload = yaml.safe_load(Path(
        "configs/final_dns_aggregation_policy.yaml"
    ).read_text(encoding="utf-8"))
    payload["qname_classes"][0]["nxdomain_is_failure"] = False
    with pytest.raises(RawCollectionError, match="classify NXDOMAIN"):
        DnsAggregationPolicy.from_dict(payload)


def test_frozen_policy_covers_monitored_container_names() -> None:
    policy = _policy()
    assert {
        name: (
            policy.role_rule(name).role,
            policy.role_rule(name).formal_action,
        )
        for name in (
            "server", "redis", "coredns",
            "proberca-healthy-dns-exposure", "resolver",
        )
    } == {
        "server": ("application", "include"),
        "redis": ("application", "include"),
        "coredns": ("dns-resolver", "separate"),
        "proberca-healthy-dns-exposure": (
            "dns-sidecar", "separate",
        ),
        "resolver": ("diagnostic", "separate"),
    }

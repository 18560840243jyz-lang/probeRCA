from __future__ import annotations

from proberca.dataplane.dns_diagnostic import DnsTransaction
from proberca.dataplane.dns_semantic_audit import (
    classify_qname,
    final_outcome,
    semantic_records,
)


def _transaction(
    *,
    qname: str,
    qtype: str,
    port: int,
    txid: int,
    started_ns: int,
    latency_ns: int,
    rcode: str,
) -> DnsTransaction:
    return DnsTransaction(
        transaction_id=txid,
        qname=qname,
        qtype=qtype,
        protocol="udp",
        client_ip="10.244.0.2",
        client_port=port,
        server_ip="10.96.0.10",
        first_query_ns=started_ns,
        last_query_ns=started_ns,
        response_ns=started_ns + latency_ns,
        retry_count=0,
        rcode=rcode,
        tc_flag=0,
        latency_ns=latency_ns,
        matched=True,
        timeout_reason=None,
    )


def test_qname_classification_is_preaggregation_and_deterministic() -> None:
    search = (
        "online-boutique.svc.cluster.local.",
        "svc.cluster.local.",
    )
    assert classify_qname(
        "metadata.google.internal.", search_domains=search
    ) == "metadata_probe"
    assert classify_qname(
        "paymentservice.online-boutique.svc.cluster.local.",
        search_domains=search,
    ) == "cluster_service"
    assert classify_qname(
        "kubernetes.default.svc.cluster.local.",
        search_domains=search,
    ) == "cluster_service"
    assert classify_qname(
        "api.example.com.online-boutique.svc.cluster.local.",
        search_domains=search,
    ) == "search_expansion"


def test_late_servfail_is_one_timeout_not_a_latency_sample() -> None:
    item = _transaction(
        qname="metadata.google.internal.",
        qtype="AAAA",
        port=40000,
        txid=99,
        started_ns=10_000_000_000,
        latency_ns=6_000_000_000,
        rcode="SERVFAIL",
    )
    assert final_outcome(item, timeout_ns=5_000_000_000) == "TIMEOUT"


def test_same_service_containers_remain_separate() -> None:
    start = 10_000_000_000
    metadata = {
        "pod_uid": "pod-1",
        "resolver_ip": "10.96.0.10",
        "audit_start_ns": start,
        "audit_end_ns": start + 1_000_000_000,
        "containers": [
            {
                "name": "server",
                "role": "application",
                "cgroup_id": 1,
            },
            {
                "name": "dns-sidecar",
                "role": "dns-sidecar",
                "cgroup_id": 2,
            },
        ],
    }
    app = _transaction(
        qname="metadata.google.internal.",
        qtype="A",
        port=40000,
        txid=1,
        started_ns=start + 100,
        latency_ns=10_000_000,
        rcode="SERVFAIL",
    )
    sidecar = _transaction(
        qname="paymentservice.online-boutique.svc.cluster.local.",
        qtype="A",
        port=40001,
        txid=2,
        started_ns=start + 200,
        latency_ns=1_000_000,
        rcode="NOERROR",
    )
    events = {
        "server": [{
            "event_type": 18,
            "src_port": 40000,
            "transaction_id": 1,
            "timestamp_ns": start + 100,
        }],
        "dns-sidecar": [{
            "event_type": 18,
            "src_port": 40001,
            "transaction_id": 2,
            "timestamp_ns": start + 200,
        }],
    }
    records = semantic_records(
        metadata=metadata,
        transactions=(app, sidecar),
        ebpf_by_container=events,
        search_domains=("online-boutique.svc.cluster.local.",),
        timeout_ns=5_000_000_000,
        source_service="frontend",
    )
    assert len(records) == 2
    assert {
        (item.container_role, item.qname_class, item.final_outcome)
        for item in records
    } == {
        ("application", "metadata_probe", "SERVFAIL"),
        ("dns-sidecar", "cluster_service", "SUCCESS"),
    }

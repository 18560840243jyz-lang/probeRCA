from __future__ import annotations

import json
import socket
import struct
from pathlib import Path

from proberca.dataplane.dns_diagnostic import (
    EVENT_DNS_QUERY,
    EVENT_DNS_RESPONSE,
    build_reconciliation,
    build_transactions,
    enrich_ebpf_events,
    parse_ebpf_jsonl,
    parse_tcpdump_text,
)


def _ipv4_number(address: str) -> int:
    return struct.unpack("=I", socket.inet_aton(address))[0]


def test_tcpdump_parser_deduplicates_nat_view_and_matches_response(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tcpdump.txt"
    path.write_text(
        """\
1785214976.562065 veth0 In IP (tos 0x0, proto UDP (17), length 94)
    10.244.0.77.43760 > 10.96.0.10.53: 19676+ A? kubernetes.default.svc.cluster.local. (66)
1785214976.562135 veth1 Out IP (tos 0x0, proto UDP (17), length 94)
    10.244.0.77.43760 > 10.244.0.23.53: 19676+ A? kubernetes.default.svc.cluster.local. (66)
1785214976.562599 veth1 In IP (tos 0x0, proto UDP (17), length 130)
    10.244.0.23.53 > 10.244.0.77.43760: 19676*- q: A? kubernetes.default.svc.cluster.local. 1/0/0 (102)
1785214976.562617 veth0 Out IP (tos 0x0, proto UDP (17), length 130)
    10.96.0.10.53 > 10.244.0.77.43760: 19676*- q: A? kubernetes.default.svc.cluster.local. 1/0/0 (102)
""",
        encoding="utf-8",
    )
    packets = parse_tcpdump_text(
        path,
        pod_ip="10.244.0.77",
        dns_cluster_ip="10.96.0.10",
    )
    assert len(packets) == 2
    transactions = build_transactions(packets)
    assert len(transactions) == 1
    assert transactions[0].matched is True
    assert transactions[0].rcode == "NOERROR"
    assert transactions[0].retry_count == 0
    assert transactions[0].latency_ns == 552_000


def test_tcpdump_parser_preserves_retry_and_servfail(tmp_path: Path) -> None:
    path = tmp_path / "tcpdump.txt"
    path.write_text(
        """\
10.000000 veth0 In IP (tos 0x0, proto UDP (17), length 70)
    10.244.0.77.40000 > 10.96.0.10.53: 99+ A? kubernetes.default.svc.cluster.local. (42)
11.000000 veth0 In IP (tos 0x0, proto UDP (17), length 70)
    10.244.0.77.40000 > 10.96.0.10.53: 99+ A? kubernetes.default.svc.cluster.local. (42)
11.010000 veth0 Out IP (tos 0x0, proto UDP (17), length 70)
    10.96.0.10.53 > 10.244.0.77.40000: 99 ServFail- q: A? kubernetes.default.svc.cluster.local. 0/0/0 (42)
""",
        encoding="utf-8",
    )
    transactions = build_transactions(parse_tcpdump_text(
        path,
        pod_ip="10.244.0.77",
        dns_cluster_ip="10.96.0.10",
    ))
    assert len(transactions) == 1
    assert transactions[0].retry_count == 1
    assert transactions[0].rcode == "SERVFAIL"
    assert transactions[0].latency_ns == 1_010_000_000


def test_ebpf_events_are_filtered_by_pod_and_enriched(tmp_path: Path) -> None:
    path = tmp_path / "ebpf.jsonl"
    records = (
        {
            "record_type": "event",
            "event_type": EVENT_DNS_QUERY,
            "timestamp_ns": 10_000_000_000,
            "monotonic_ns": 1,
            "cgroup_id": 123,
            "src_ipv4": _ipv4_number("10.244.0.77"),
            "dst_ipv4": _ipv4_number("10.96.0.10"),
            "src_port": 40000,
            "dst_port": 53,
            "protocol": 17,
            "transaction_id": 99,
            "rcode": 0,
            "duration_ns": 0,
        },
        {
            "record_type": "event",
            "event_type": EVENT_DNS_QUERY,
            "timestamp_ns": 10_000_000_000,
            "monotonic_ns": 2,
            "cgroup_id": 456,
            "src_ipv4": _ipv4_number("10.244.0.88"),
            "dst_ipv4": _ipv4_number("10.96.0.10"),
            "src_port": 40001,
            "dst_port": 53,
            "protocol": 17,
            "transaction_id": 100,
            "rcode": 0,
            "duration_ns": 0,
        },
    )
    path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    filtered = parse_ebpf_jsonl(path, pod_ip="10.244.0.77")
    assert len(filtered) == 1
    enriched = enrich_ebpf_events(
        filtered,
        (),
        pod_uid="uid-1",
        service="diagnostic",
        netns="42",
    )
    assert enriched[0]["cgroup_id"] == 123
    assert enriched[0]["pod_uid"] == "uid-1"
    assert enriched[0]["pid"] is None


def test_reconciliation_keeps_application_pcap_ebpf_separate() -> None:
    result = build_reconciliation(
        [
            {
                "success": True,
                "duration_ns": 1_000_000,
            },
        ],
        (),
        (),
        [
            {
                "event_type": EVENT_DNS_QUERY,
                "cgroup_id": 123,
            },
            {
                "event_type": EVENT_DNS_RESPONSE,
                "cgroup_id": 123,
            },
        ],
    )
    assert result["application"]["logical_resolution_count"] == 1
    assert result["application"]["success_count"] == 1
    assert result["pcap"]["logical_transaction_count"] == 0
    assert result["ebpf"]["query_event_count"] == 1
    assert result["ebpf"]["response_event_count"] == 1

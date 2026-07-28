from __future__ import annotations

import csv
import ipaddress
import json
import math
import re
import socket
import statistics
import struct
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator


EVENT_DNS_QUERY = 18
EVENT_DNS_RESPONSE = 19
EVENT_DNS_TIMEOUT = 20
DNS_EVENT_TYPES = frozenset({
    EVENT_DNS_QUERY,
    EVENT_DNS_RESPONSE,
    EVENT_DNS_TIMEOUT,
})

_PACKET_HEADER = re.compile(
    r"^(?P<timestamp>\d+\.\d+)\s+\S+\s+(?:In|Out)\s+"
    r"IP\s+.*proto\s+(?P<protocol>UDP|TCP).*length\s+"
    r"(?P<ip_length>\d+)\)"
)
_FLOW = re.compile(
    r"^\s*(?P<src_ip>\d+(?:\.\d+){3})\.(?P<src_port>\d+)\s+>\s+"
    r"(?P<dst_ip>\d+(?:\.\d+){3})\.(?P<dst_port>\d+):\s+"
    r"(?P<payload>.*)$"
)
_TXID = re.compile(r"(?:^|\]\s+)(?P<txid>\d+)(?P<flags>[^ ]*)\s+")
_QUESTION = re.compile(
    r"(?:q:\s+)?(?P<qtype>A|AAAA|CNAME|MX|NS|PTR|SOA|SRV|TXT)\?\s+"
    r"(?P<qname>[^ ]+)"
)
_MESSAGE_LENGTH = re.compile(r"\((?P<length>\d+)\)\s*$")


@dataclass(frozen=True)
class PcapDnsPacket:
    timestamp_ns: int
    protocol: str
    src_ip: str
    src_port: int
    dst_ip: str
    dst_port: int
    transaction_id: int
    qname: str
    qtype: str
    qr_flag: int
    tc_flag: int
    rcode: str | None
    message_length: int | None


@dataclass(frozen=True)
class DnsTransaction:
    transaction_id: int
    qname: str
    qtype: str
    protocol: str
    client_ip: str
    client_port: int
    server_ip: str
    first_query_ns: int
    last_query_ns: int
    response_ns: int | None
    retry_count: int
    rcode: str | None
    tc_flag: int
    latency_ns: int | None
    matched: bool
    timeout_reason: str | None


def _percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, math.ceil(
        fraction * len(ordered)
    ) - 1))
    return ordered[index]


def _epoch_text_to_ns(value: str) -> int:
    seconds, separator, fraction = value.partition(".")
    if not separator:
        return int(seconds) * 1_000_000_000
    nanoseconds = int((fraction + "000000000")[:9])
    return int(seconds) * 1_000_000_000 + nanoseconds


def parse_application_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("record_type") == "application_dns_lookup":
                records.append(record)
    return records


def _rcode(payload: str, qr_flag: int) -> str | None:
    if not qr_flag:
        return None
    lowered = payload.lower()
    names = (
        ("servfail", "SERVFAIL"),
        ("nxdomain", "NXDOMAIN"),
        ("refused", "REFUSED"),
        ("formerr", "FORMERR"),
        ("notimp", "NOTIMP"),
    )
    for needle, name in names:
        if needle in lowered:
            return name
    return "NOERROR"


def parse_tcpdump_text(
    path: Path,
    *,
    pod_ip: str,
    dns_cluster_ip: str,
) -> list[PcapDnsPacket]:
    packets = []
    pending_header: re.Match[str] | None = None
    with path.open(encoding="utf-8", errors="replace") as source:
        for raw_line in source:
            header = _PACKET_HEADER.match(raw_line)
            if header:
                pending_header = header
                continue
            if pending_header is None:
                continue
            flow = _FLOW.match(raw_line)
            if not flow:
                continue
            values = flow.groupdict()
            src_ip = values["src_ip"]
            dst_ip = values["dst_ip"]
            src_port = int(values["src_port"])
            dst_port = int(values["dst_port"])
            is_query = (
                src_ip == pod_ip
                and dst_ip == dns_cluster_ip
                and dst_port == 53
            )
            is_response = (
                src_ip == dns_cluster_ip
                and dst_ip == pod_ip
                and src_port == 53
            )
            if not (is_query or is_response):
                pending_header = None
                continue
            payload = values["payload"]
            txid = _TXID.search(payload)
            question = _QUESTION.search(payload)
            if txid is None or question is None:
                pending_header = None
                continue
            message_length = _MESSAGE_LENGTH.search(payload)
            qr_flag = int(is_response)
            flags = txid.group("flags")
            packets.append(PcapDnsPacket(
                timestamp_ns=_epoch_text_to_ns(
                    pending_header.group("timestamp")
                ),
                protocol=pending_header.group("protocol").lower(),
                src_ip=src_ip,
                src_port=src_port,
                dst_ip=dst_ip,
                dst_port=dst_port,
                transaction_id=int(txid.group("txid")),
                qname=question.group("qname").rstrip(".") + ".",
                qtype=question.group("qtype"),
                qr_flag=qr_flag,
                tc_flag=int("|" in flags),
                rcode=_rcode(payload, qr_flag),
                message_length=(
                    int(message_length.group("length"))
                    if message_length else None
                ),
            ))
            pending_header = None
    return packets


def build_transactions(packets: Iterable[PcapDnsPacket]) -> list[DnsTransaction]:
    groups: dict[
        tuple[str, int, str, int, int, str, str, str],
        list[PcapDnsPacket],
    ] = defaultdict(list)
    for packet in packets:
        client_ip = packet.src_ip if not packet.qr_flag else packet.dst_ip
        client_port = (
            packet.src_port if not packet.qr_flag else packet.dst_port
        )
        server_ip = packet.dst_ip if not packet.qr_flag else packet.src_ip
        key = (
            client_ip,
            client_port,
            server_ip,
            53,
            packet.transaction_id,
            packet.qname,
            packet.qtype,
            packet.protocol,
        )
        groups[key].append(packet)
    output = []
    for key, values in sorted(
        groups.items(), key=lambda item: min(
            packet.timestamp_ns for packet in item[1]
        )
    ):
        current_queries: list[PcapDnsPacket] = []
        for packet in sorted(values, key=lambda item: item.timestamp_ns):
            if not packet.qr_flag:
                current_queries.append(packet)
                continue
            if not current_queries:
                continue
            output.append(_transaction_from_packets(
                key, current_queries, packet
            ))
            current_queries = []
        if current_queries:
            output.append(_transaction_from_packets(
                key, current_queries, None
            ))
    return sorted(output, key=lambda item: item.first_query_ns)


def _transaction_from_packets(
    key: tuple[str, int, str, int, int, str, str, str],
    queries: list[PcapDnsPacket],
    response: PcapDnsPacket | None,
) -> DnsTransaction:
    latency_ns = (
        response.timestamp_ns - queries[0].timestamp_ns
        if response else None
    )
    return DnsTransaction(
        transaction_id=key[4],
        qname=key[5],
        qtype=key[6],
        protocol=key[7],
        client_ip=key[0],
        client_port=key[1],
        server_ip=key[2],
        first_query_ns=queries[0].timestamp_ns,
        last_query_ns=queries[-1].timestamp_ns,
        response_ns=response.timestamp_ns if response else None,
        retry_count=max(0, len(queries) - 1),
        rcode=response.rcode if response else None,
        tc_flag=max(
            (
                packet.tc_flag
                for packet in queries + ([response] if response else [])
            ),
            default=0,
        ),
        latency_ns=latency_ns,
        matched=response is not None,
        timeout_reason=None if response else "pcap_response_missing",
    )


def _event_ipv4(value: Any) -> str:
    numeric = int(value)
    return socket.inet_ntoa(struct.pack("=I", numeric))


def parse_ebpf_jsonl(
    path: Path,
    *,
    pod_ip: str,
) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("record_type") != "event":
                continue
            if int(record.get("event_type", 0)) not in DNS_EVENT_TYPES:
                continue
            src_ip = _event_ipv4(record["src_ipv4"])
            dst_ip = _event_ipv4(record["dst_ipv4"])
            if pod_ip not in {src_ip, dst_ip}:
                continue
            record["src_ip"] = src_ip
            record["dst_ip"] = dst_ip
            records.append(record)
    return records


def _transaction_index(
    transactions: Iterable[DnsTransaction],
) -> dict[tuple[int, int], list[DnsTransaction]]:
    index: dict[tuple[int, int], list[DnsTransaction]] = defaultdict(list)
    for item in transactions:
        index[(item.client_port, item.transaction_id)].append(item)
    return index


def enrich_ebpf_events(
    records: Iterable[dict[str, Any]],
    transactions: Iterable[DnsTransaction],
    *,
    pod_uid: str,
    service: str,
    netns: str,
) -> list[dict[str, Any]]:
    transactions = tuple(transactions)
    index = _transaction_index(transactions)
    txid_index: dict[int, list[DnsTransaction]] = defaultdict(list)
    for transaction in transactions:
        txid_index[transaction.transaction_id].append(transaction)
    output = []
    retry_indexes: Counter[tuple[int, int]] = Counter()
    for record in records:
        event_type = int(record["event_type"])
        key = (
            int(record["src_port"]),
            int(record["transaction_id"]),
        )
        candidates = index.get(key, [])
        if not candidates and event_type == EVENT_DNS_TIMEOUT:
            candidates = txid_index.get(
                int(record["transaction_id"]), []
            )
        timestamp_ns = int(record["timestamp_ns"])
        transaction = min(
            candidates,
            key=lambda item: abs(item.first_query_ns - timestamp_ns),
            default=None,
        )
        retry_index = retry_indexes[key]
        if event_type == EVENT_DNS_QUERY:
            retry_indexes[key] += 1
        output.append({
            "monotonic_timestamp": int(record["monotonic_ns"]),
            "epoch_timestamp": timestamp_ns,
            "cgroup_id": int(record["cgroup_id"]),
            "netns": netns,
            "pod_uid": pod_uid,
            "service": service,
            "pid": None,
            "tid": None,
            "process_identity_unavailable_reason": (
                "cgroup_skb hook does not provide reliable originating pid/tid"
            ),
            "src_ip": record["src_ip"],
            "src_port": int(record["src_port"]),
            "dst_ip": record["dst_ip"],
            "dst_port": int(record["dst_port"]),
            "protocol": (
                "udp" if int(record["protocol"]) == 17 else "tcp"
            ),
            "dns_transaction_id": int(record["transaction_id"]),
            "qname": transaction.qname if transaction else None,
            "qtype": transaction.qtype if transaction else None,
            "qr_flag": int(event_type == EVENT_DNS_RESPONSE),
            "tc_flag": transaction.tc_flag if transaction else None,
            "rcode": (
                int(record["rcode"])
                if event_type == EVENT_DNS_RESPONSE else None
            ),
            "message_length": None,
            "retry_index": retry_index,
            "matched": bool(transaction and transaction.matched),
            "latency_ns": (
                int(record["duration_ns"])
                if event_type == EVENT_DNS_RESPONSE else None
            ),
            "timeout_reason": (
                "ebpf_ttl_expired"
                if event_type == EVENT_DNS_TIMEOUT else None
            ),
            "event_type": event_type,
            "source_record": record,
        })
    return output


def _rcode_counts(transactions: Iterable[DnsTransaction]) -> dict[str, int]:
    return dict(sorted(Counter(
        item.rcode
        for item in transactions
        if item.rcode is not None
    ).items()))


def build_reconciliation(
    application: list[dict[str, Any]],
    packets: list[PcapDnsPacket],
    transactions: list[DnsTransaction],
    ebpf: list[dict[str, Any]],
) -> dict[str, Any]:
    successful_transactions = [
        item for item in transactions
        if item.matched and item.rcode == "NOERROR"
    ]
    application_durations_ms = [
        int(item["duration_ns"]) / 1_000_000.0
        for item in application if item.get("success")
    ]
    pcap_durations_ms = [
        item.latency_ns / 1_000_000.0
        for item in successful_transactions if item.latency_ns is not None
    ]
    event_counts = Counter(
        int(item["event_type"]) for item in ebpf
    )
    cgroups = sorted({int(item["cgroup_id"]) for item in ebpf})
    return {
        "application": {
            "logical_resolution_count": len(application),
            "success_count": sum(
                bool(item.get("success")) for item in application
            ),
            "failure_count": sum(
                not bool(item.get("success")) for item in application
            ),
            "latency_p95_ms": _percentile(
                application_durations_ms, 0.95
            ),
        },
        "pcap": {
            "logical_transaction_count": len(transactions),
            "udp_query_packet_count": sum(
                not item.qr_flag and item.protocol == "udp"
                for item in packets
            ),
            "tcp_query_packet_count": sum(
                not item.qr_flag and item.protocol == "tcp"
                for item in packets
            ),
            "response_count": sum(
                item.matched for item in transactions
            ),
            "rcode_counts": _rcode_counts(transactions),
            "timeout_count": sum(
                not item.matched for item in transactions
            ),
            "retry_count": sum(
                item.retry_count for item in transactions
            ),
            "latency_p95_ms": _percentile(pcap_durations_ms, 0.95),
        },
        "ebpf": {
            "query_event_count": event_counts[EVENT_DNS_QUERY],
            "response_event_count": event_counts[EVENT_DNS_RESPONSE],
            "timeout_event_count": event_counts[EVENT_DNS_TIMEOUT],
            "cgroup_ids": cgroups,
        },
    }


def write_outputs(
    output_dir: Path,
    *,
    application: list[dict[str, Any]],
    packets: list[PcapDnsPacket],
    transactions: list[DnsTransaction],
    ebpf: list[dict[str, Any]],
    reconciliation: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    with (output_dir / "pcap-dns-packets.jsonl").open(
        "w", encoding="utf-8"
    ) as target:
        for packet in packets:
            target.write(json.dumps(asdict(packet), sort_keys=True) + "\n")
    with (output_dir / "dns-transactions.jsonl").open(
        "w", encoding="utf-8"
    ) as target:
        for transaction in transactions:
            target.write(
                json.dumps(asdict(transaction), sort_keys=True) + "\n"
            )
    with (output_dir / "enriched-ebpf-dns-events.jsonl").open(
        "w", encoding="utf-8"
    ) as target:
        for record in ebpf:
            target.write(json.dumps(record, sort_keys=True) + "\n")
    report = {
        "schema_version": "1.0",
        "classification": "DNS_TRANSACTION_DIAGNOSTIC",
        "metadata": metadata,
        "reconciliation": reconciliation,
    }
    (output_dir / "reconciliation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "transactions.csv").open(
        "w", encoding="utf-8", newline=""
    ) as target:
        fields = tuple(DnsTransaction.__dataclass_fields__)
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for transaction in transactions:
            writer.writerow(asdict(transaction))
    app = reconciliation["application"]
    pcap = reconciliation["pcap"]
    bpf = reconciliation["ebpf"]
    markdown = f"""# DNS transaction reconciliation

| Statistic | Application | PCAP | eBPF |
| --- | ---: | ---: | ---: |
| Logical resolutions / transactions | {app['logical_resolution_count']} | {pcap['logical_transaction_count']} | — |
| UDP query packets | — | {pcap['udp_query_packet_count']} | {bpf['query_event_count']} |
| TCP query packets | — | {pcap['tcp_query_packet_count']} | — |
| Responses | {app['success_count']} | {pcap['response_count']} | {bpf['response_event_count']} |
| Timeouts | {app['failure_count']} | {pcap['timeout_count']} | {bpf['timeout_event_count']} |
| Retries | — | {pcap['retry_count']} | — |
| P95 latency (ms) | {app['latency_p95_ms']} | {pcap['latency_p95_ms']} | see enriched events |

RCODE counts: `{json.dumps(pcap['rcode_counts'], sort_keys=True)}`

The enriched eBPF stream preserves cgroup identity and joins qname/qtype
from the simultaneous PCAP. `pid` and `tid` remain null because a
`cgroup_skb` hook cannot reliably recover the originating process.
"""
    (output_dir / "report.md").write_text(markdown, encoding="utf-8")


def reconcile(
    *,
    application_path: Path,
    tcpdump_text_path: Path,
    ebpf_path: Path,
    output_dir: Path,
    pod_ip: str,
    dns_cluster_ip: str,
    pod_uid: str,
    service: str,
    netns: str,
) -> dict[str, Any]:
    application = parse_application_jsonl(application_path)
    packets = parse_tcpdump_text(
        tcpdump_text_path,
        pod_ip=pod_ip,
        dns_cluster_ip=dns_cluster_ip,
    )
    transactions = build_transactions(packets)
    raw_ebpf = parse_ebpf_jsonl(ebpf_path, pod_ip=pod_ip)
    enriched = enrich_ebpf_events(
        raw_ebpf,
        transactions,
        pod_uid=pod_uid,
        service=service,
        netns=netns,
    )
    reconciliation = build_reconciliation(
        application, packets, transactions, enriched
    )
    write_outputs(
        output_dir,
        application=application,
        packets=packets,
        transactions=transactions,
        ebpf=enriched,
        reconciliation=reconciliation,
        metadata={
            "pod_ip": str(ipaddress.ip_address(pod_ip)),
            "dns_cluster_ip": str(ipaddress.ip_address(dns_cluster_ip)),
            "pod_uid": pod_uid,
            "service": service,
            "netns": netns,
        },
    )
    return reconciliation

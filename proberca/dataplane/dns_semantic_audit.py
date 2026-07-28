from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from .dns_diagnostic import (
    EVENT_DNS_QUERY,
    DnsTransaction,
    build_transactions,
    parse_ebpf_jsonl,
    parse_tcpdump_text,
)


DNS_SEMANTIC_AUDIT_SCHEMA = "probeRCA-dns-semantic-audit-v1"
DEFAULT_MATCH_TOLERANCE_NS = 5_000_000


@dataclass(frozen=True)
class DnsSemanticRecord:
    source_service: str
    pod_uid: str
    container_name: str
    container_role: str
    cgroup_id: int
    resolver_ip: str
    qname: str
    qname_hash: str
    qname_class: str
    qtype: str
    protocol: str
    rcode: str | None
    final_outcome: str
    retry_count: int
    tcp_fallback: bool
    first_query_ns: int
    final_response_ns: int | None
    observed_response_latency_ns: int | None
    successful_latency_ns: int | None
    matched_to_pcap: bool


def parse_search_domains(text: str) -> tuple[str, ...]:
    for raw_line in text.splitlines():
        parts = raw_line.strip().split()
        if parts and parts[0] == "search":
            return tuple(
                item.casefold().rstrip(".") + "."
                for item in parts[1:]
            )
    return ()


def classify_qname(
    qname: str,
    *,
    search_domains: Iterable[str],
) -> str:
    normalized = qname.casefold().rstrip(".") + "."
    if normalized == "metadata.google.internal.":
        return "metadata_probe"
    if normalized.endswith(".in-addr.arpa.") \
            or normalized.endswith(".ip6.arpa."):
        return "reverse_lookup"
    # A canonical Kubernetes Service FQDN has exactly
    # <service>.<namespace> before the fixed cluster suffix.  Recognize this
    # before search-domain expansion; otherwise a legitimate name from a
    # different namespace (for example kubernetes.default) is mistaken for an
    # expanded external name.
    cluster_suffix = ".svc.cluster.local."
    if normalized.endswith(cluster_suffix):
        service_identity = normalized[:-len(cluster_suffix)]
        if service_identity.count(".") == 1 and all(
            label for label in service_identity.split(".")
        ):
            return "cluster_service"
    for suffix in sorted(search_domains, key=len, reverse=True):
        suffix = suffix.casefold().rstrip(".") + "."
        marker = "." + suffix
        if not normalized.endswith(marker):
            continue
        prefix = normalized[:-len(marker)]
        if "." in prefix:
            return "search_expansion"
        break
    return "external_or_special"


def qname_hash(qname: str) -> str:
    normalized = qname.casefold().rstrip(".") + "."
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def final_outcome(
    transaction: DnsTransaction,
    *,
    timeout_ns: int,
) -> str:
    if transaction.response_ns is None \
            or transaction.latency_ns is None \
            or transaction.latency_ns >= timeout_ns:
        return "TIMEOUT"
    return {
        "NOERROR": "SUCCESS",
        "SERVFAIL": "SERVFAIL",
        "REFUSED": "REFUSED",
        "NXDOMAIN": "NXDOMAIN",
    }.get(transaction.rcode or "", "TRANSPORT_ERROR")


def _transaction_index(
    transactions: Iterable[DnsTransaction],
) -> dict[tuple[int, int], tuple[DnsTransaction, ...]]:
    index: dict[tuple[int, int], list[DnsTransaction]] = {}
    for item in transactions:
        index.setdefault(
            (item.client_port, item.transaction_id), []
        ).append(item)
    return {
        key: tuple(sorted(values, key=lambda item: item.first_query_ns))
        for key, values in index.items()
    }


def _match_transaction(
    event: dict[str, Any],
    index: dict[tuple[int, int], tuple[DnsTransaction, ...]],
    *,
    tolerance_ns: int,
) -> DnsTransaction | None:
    candidates = index.get(
        (int(event["src_port"]), int(event["transaction_id"])),
        (),
    )
    if not candidates:
        return None
    timestamp_ns = int(event["timestamp_ns"])
    candidate = min(
        candidates,
        key=lambda item: abs(item.first_query_ns - timestamp_ns),
    )
    if abs(candidate.first_query_ns - timestamp_ns) > tolerance_ns:
        return None
    return candidate


def semantic_records(
    *,
    metadata: dict[str, Any],
    transactions: Iterable[DnsTransaction],
    ebpf_by_container: dict[str, list[dict[str, Any]]],
    search_domains: Iterable[str],
    timeout_ns: int,
    source_service: str,
    match_tolerance_ns: int = DEFAULT_MATCH_TOLERANCE_NS,
) -> tuple[DnsSemanticRecord, ...]:
    start_ns = int(metadata["audit_start_ns"])
    end_ns = int(metadata["audit_end_ns"])
    index = _transaction_index(transactions)
    containers = {
        item["name"]: item for item in metadata["containers"]
    }
    output = []
    for container_name, events in sorted(ebpf_by_container.items()):
        identity = containers[container_name]
        for event in events:
            if int(event["event_type"]) != EVENT_DNS_QUERY:
                continue
            timestamp_ns = int(event["timestamp_ns"])
            if not start_ns <= timestamp_ns < end_ns:
                continue
            transaction = _match_transaction(
                event, index, tolerance_ns=match_tolerance_ns
            )
            if transaction is None:
                raise ValueError(
                    "eBPF DNS query cannot be matched to PCAP: "
                    f"{container_name}:{event['src_port']}:"
                    f"{event['transaction_id']}:{timestamp_ns}"
                )
            outcome = final_outcome(
                transaction, timeout_ns=timeout_ns
            )
            output.append(DnsSemanticRecord(
                source_service=source_service,
                pod_uid=str(metadata["pod_uid"]),
                container_name=container_name,
                container_role=str(identity["role"]),
                cgroup_id=int(identity["cgroup_id"]),
                resolver_ip=str(metadata["resolver_ip"]),
                qname=transaction.qname,
                qname_hash=qname_hash(transaction.qname),
                qname_class=classify_qname(
                    transaction.qname,
                    search_domains=search_domains,
                ),
                qtype=transaction.qtype,
                protocol=transaction.protocol,
                rcode=transaction.rcode,
                final_outcome=outcome,
                retry_count=transaction.retry_count,
                tcp_fallback=bool(
                    transaction.protocol == "tcp"
                    or transaction.tc_flag
                ),
                first_query_ns=transaction.first_query_ns,
                final_response_ns=transaction.response_ns,
                observed_response_latency_ns=transaction.latency_ns,
                successful_latency_ns=(
                    transaction.latency_ns
                    if outcome == "SUCCESS" else None
                ),
                matched_to_pcap=True,
            ))
    return tuple(sorted(output, key=lambda item: (
        item.first_query_ns, item.container_name, item.qname, item.qtype
    )))


def _group_counts(
    records: Iterable[DnsSemanticRecord],
    fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    counter = Counter(
        tuple(getattr(item, field) for field in fields)
        for item in records
    )
    return [
        {
            **dict(zip(fields, key)),
            "count": count,
        }
        for key, count in sorted(
            counter.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


def build_semantic_report(
    records: Iterable[DnsSemanticRecord],
    *,
    metadata: dict[str, Any],
    search_domains: Iterable[str],
    timeout_ns: int,
) -> dict[str, Any]:
    records = tuple(records)
    failures = tuple(
        item for item in records if item.final_outcome != "SUCCESS"
    )
    servfail = tuple(
        item for item in records if item.final_outcome == "SERVFAIL"
    )
    timeouts = tuple(
        item for item in records if item.final_outcome == "TIMEOUT"
    )
    roles = _group_counts(
        records,
        ("source_service", "container_name", "container_role",
         "cgroup_id", "qname_class", "final_outcome"),
    )
    return {
        "schema_version": DNS_SEMANTIC_AUDIT_SCHEMA,
        "classification": "DNS_FAILURE_SEMANTIC_AUDIT",
        "metadata": {
            "namespace": metadata["namespace"],
            "pod": metadata["pod"],
            "pod_uid": metadata["pod_uid"],
            "resolver_ip": metadata["resolver_ip"],
            "audit_start_ns": int(metadata["audit_start_ns"]),
            "audit_end_ns": int(metadata["audit_end_ns"]),
            "timeout_ns": int(timeout_ns),
            "search_domains": list(search_domains),
        },
        "totals": {
            "logical_transactions": len(records),
            "success": sum(
                item.final_outcome == "SUCCESS" for item in records
            ),
            "failure": len(failures),
            "servfail": len(servfail),
            "refused": sum(
                item.final_outcome == "REFUSED" for item in records
            ),
            "nxdomain": sum(
                item.final_outcome == "NXDOMAIN" for item in records
            ),
            "timeout": len(timeouts),
            "transport_error": sum(
                item.final_outcome == "TRANSPORT_ERROR"
                for item in records
            ),
            "retry_count": sum(item.retry_count for item in records),
            "tcp_fallback_count": sum(
                item.tcp_fallback for item in records
            ),
        },
        "by_source_and_outcome": roles,
        "qtype_distribution": _group_counts(
            records,
            ("container_name", "container_role", "qtype",
             "final_outcome"),
        ),
        "servfail_top20": _group_counts(
            servfail,
            ("container_name", "container_role", "qname",
             "qname_hash", "qname_class", "qtype"),
        )[:20],
        "timeout_top20": _group_counts(
            timeouts,
            ("container_name", "container_role", "qname",
             "qname_hash", "qname_class", "qtype"),
        )[:20],
    }


def write_semantic_outputs(
    output_dir: Path,
    *,
    records: Iterable[DnsSemanticRecord],
    report: dict[str, Any],
) -> None:
    records = tuple(records)
    output_dir.mkdir(parents=True, exist_ok=False)
    with (output_dir / "dns-semantic-transactions.jsonl").open(
        "w", encoding="utf-8"
    ) as target:
        for item in records:
            target.write(json.dumps(
                asdict(item), sort_keys=True
            ) + "\n")
    fields = tuple(DnsSemanticRecord.__dataclass_fields__)
    with (output_dir / "dns-semantic-transactions.csv").open(
        "w", encoding="utf-8", newline=""
    ) as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for item in records:
            writer.writerow(asdict(item))
    (output_dir / "dns-semantic-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    totals = report["totals"]
    rows = "\n".join(
        "| {container_name} | {container_role} | {qname_class} | "
        "{final_outcome} | {count} |".format(**item)
        for item in report["by_source_and_outcome"]
    )
    markdown = f"""# DNS failure semantic audit

| Statistic | Count |
| --- | ---: |
| Logical transactions | {totals['logical_transactions']} |
| Success | {totals['success']} |
| SERVFAIL | {totals['servfail']} |
| Timeout | {totals['timeout']} |
| NXDOMAIN | {totals['nxdomain']} |
| REFUSED | {totals['refused']} |
| Transport error | {totals['transport_error']} |
| Retry packets | {totals['retry_count']} |
| TCP fallback | {totals['tcp_fallback_count']} |

| Container | Role | qname class | Final outcome | Count |
| --- | --- | --- | --- | ---: |
{rows}

`TIMEOUT` is assigned when the final response is absent or arrives at/after
the frozen {report['metadata']['timeout_ns'] / 1_000_000:.0f} ms deadline.
Failed transactions never contribute to successful latency samples.
"""
    (output_dir / "dns-semantic-report.md").write_text(
        markdown, encoding="utf-8"
    )


def run_semantic_audit(
    *,
    capture_dir: Path,
    output_dir: Path,
    timeout_ms: int,
    source_service: str,
) -> dict[str, Any]:
    metadata = json.loads(
        (capture_dir / "metadata.json").read_text(encoding="utf-8")
    )
    search_domains = parse_search_domains(
        (capture_dir / "pod-resolv.conf").read_text(encoding="utf-8")
    )
    packets = parse_tcpdump_text(
        capture_dir / "tcpdump.txt",
        pod_ip=str(metadata["pod_ip"]),
        dns_cluster_ip=str(metadata["resolver_ip"]),
    )
    transactions = build_transactions(packets)
    ebpf_by_container = {
        item["name"]: parse_ebpf_jsonl(
            capture_dir / f"ebpf-{item['name']}.jsonl",
            pod_ip=str(metadata["pod_ip"]),
        )
        for item in metadata["containers"]
    }
    records = semantic_records(
        metadata=metadata,
        transactions=transactions,
        ebpf_by_container=ebpf_by_container,
        search_domains=search_domains,
        timeout_ns=int(timeout_ms) * 1_000_000,
        source_service=source_service,
    )
    report = build_semantic_report(
        records,
        metadata=metadata,
        search_domains=search_domains,
        timeout_ns=int(timeout_ms) * 1_000_000,
    )
    write_semantic_outputs(
        output_dir, records=records, report=report
    )
    return report

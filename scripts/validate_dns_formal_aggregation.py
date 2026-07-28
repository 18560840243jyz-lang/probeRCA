from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import yaml

from proberca.dataplane.dns_policy import DnsAggregationPolicy
from proberca.dataplane.primitive_exporter import FinalPrimitiveExporter


FORMAL_PREFIX = "proberca_dns_edge_"
AUDIT_METRIC = "proberca_dns_policy_transaction_total"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the frozen DNS attribution policy against one real "
            "BPF map snapshot without running alerting or RCA."
        )
    )
    parser.add_argument("--snapshot-jsonl", type=Path, required=True)
    parser.add_argument("--identity-json", type=Path, required=True)
    parser.add_argument("--application-jsonl", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--dns-cluster-ip", required=True)
    parser.add_argument("--validation-service", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _load_jsonl(path: Path) -> tuple[dict, ...]:
    return tuple(
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    records = _load_jsonl(args.snapshot_jsonl)
    identities = json.loads(
        args.identity_json.read_text(encoding="utf-8")
    )
    policy = DnsAggregationPolicy.from_dict(yaml.safe_load(
        args.policy.read_text(encoding="utf-8")
    ))
    cgroup_identity = {
        int(item["cgroup_id"]): SimpleNamespace(
            namespace=item["namespace"],
            service=item["service"],
            container=item["container"],
        )
        for item in identities
    }
    inventory = SimpleNamespace(service_cluster_ips={
        args.dns_cluster_ip: ("kube-system", "kube-dns"),
    })
    exporter = FinalPrimitiveExporter.__new__(FinalPrimitiveExporter)
    exporter.dns_policy = policy
    samples = exporter._dns_samples(
        inventory, records, cgroup_identity
    )

    audit = []
    formal = []
    for sample in samples:
        labels = sample.label_dict
        item = {
            "metric": sample.name,
            "labels": labels,
            "value": sample.value,
        }
        if sample.name == AUDIT_METRIC:
            audit.append(item)
        elif sample.name.startswith(FORMAL_PREFIX):
            formal.append(item)

    application = _load_jsonl(args.application_jsonl)
    successful_lookups = sum(
        item.get("record_type") == "application_dns_lookup"
        and item.get("success") is True
        for item in application
    )
    validation_formal = [
        item for item in formal
        if item["labels"]["src_service"] == args.validation_service
    ]
    query = [
        item for item in validation_formal
        if item["metric"] == "proberca_dns_edge_query_total"
    ]
    success = [
        item for item in validation_formal
        if item["metric"] == "proberca_dns_edge_success_total"
    ]
    failures = [
        item for item in validation_formal
        if item["metric"] in {
            "proberca_dns_edge_timeout_total",
            "proberca_dns_edge_servfail_total",
            "proberca_dns_edge_refused_total",
            "proberca_dns_edge_nxdomain_failure_total",
            "proberca_dns_edge_transport_error_total",
        }
    ]
    infinite_bucket = [
        item for item in validation_formal
        if item["metric"]
        == "proberca_dns_edge_success_latency_milliseconds_bucket"
        and item["labels"].get("le") == "+Inf"
    ]
    checks = {
        "one_validation_query_series": len(query) == 1,
        "one_validation_success_series": len(success) == 1,
        "one_validation_infinite_bucket": len(infinite_bucket) == 1,
        "application_success_at_least_55": successful_lookups >= 55,
        "application_and_bpf_counts_equal": (
            len(query) == 1
            and query[0]["value"] == successful_lookups
        ),
        "all_validation_transactions_succeeded": (
            len(query) == 1
            and len(success) == 1
            and query[0]["value"] == success[0]["value"]
            and sum(item["value"] for item in failures) == 0
        ),
        "success_histogram_count_matches_success": (
            len(success) == 1
            and len(infinite_bucket) == 1
            and success[0]["value"] == infinite_bucket[0]["value"]
        ),
        "no_sidecar_in_formal_metrics": not any(
            item["labels"].get("src_container_role") == "dns-sidecar"
            for item in formal
        ),
        "only_cluster_service_names_are_formal": all(
            item["labels"].get("qname_class") == "cluster_service"
            for item in formal
        ),
        "policy_fingerprint_matches": all(
            item["labels"].get("dns_policy") == policy.fingerprint
            for item in (*audit, *formal)
        ),
    }
    report = {
        "schema_version": "probeRCA-dns-formal-validation-v1",
        "classification": (
            "PASS" if all(checks.values()) else "FAIL"
        ),
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "application_successful_lookups": successful_lookups,
        "checks": checks,
        "audit_samples": sorted(
            audit,
            key=lambda item: (
                item["labels"].get("src_service", ""),
                item["labels"].get("src_container_role", ""),
                item["labels"].get("qname_class", ""),
                item["labels"].get("final_outcome", ""),
            ),
        ),
        "formal_samples": sorted(
            formal,
            key=lambda item: (
                item["labels"].get("src_service", ""),
                item["metric"],
                item["labels"].get("le", ""),
            ),
        ),
    }
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "validation-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (args.output / "formal-metrics.jsonl").write_text(
        "".join(
            json.dumps(item, sort_keys=True) + "\n"
            for item in report["formal_samples"]
        ),
        encoding="utf-8",
    )
    (args.output / "policy-audit-metrics.jsonl").write_text(
        "".join(
            json.dumps(item, sort_keys=True) + "\n"
            for item in report["audit_samples"]
        ),
        encoding="utf-8",
    )
    print(json.dumps({
        "classification": report["classification"],
        "output": str(args.output),
        "checks": checks,
    }, sort_keys=True))
    return 0 if report["classification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

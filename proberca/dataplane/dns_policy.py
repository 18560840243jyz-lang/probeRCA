from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable

from .dns_semantic_audit import DnsSemanticRecord
from .raw import RawCollectionError


DNS_AGGREGATION_POLICY_SCHEMA = "probeRCA-dns-aggregation-policy-v1"
FORMAL_FAILURE_OUTCOMES = frozenset({
    "SERVFAIL", "REFUSED", "TIMEOUT", "TRANSPORT_ERROR",
})
FINAL_OUTCOMES = FORMAL_FAILURE_OUTCOMES | frozenset({
    "SUCCESS", "NXDOMAIN",
})


def _strict_mapping(
    payload: Any,
    fields: set[str],
    name: str,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RawCollectionError(f"{name} must be a mapping")
    unknown = sorted(set(payload) - fields)
    missing = sorted(fields - set(payload))
    if unknown or missing:
        raise RawCollectionError(
            f"{name} fields mismatch; unknown={unknown}, missing={missing}"
        )
    return dict(payload)


@dataclass(frozen=True)
class ContainerRoleRule:
    role: str
    exact_container_names: tuple[str, ...]
    formal_action: str


@dataclass(frozen=True)
class DnsQnameClassRule:
    qname_class: str
    formal_action: str
    nxdomain_is_failure: bool


@dataclass(frozen=True)
class DnsAggregationPolicy:
    schema_version: str
    policy_id: str
    timeout_ms: int
    formal_qname_storage: str
    diagnostic_full_qname: bool
    container_roles: tuple[ContainerRoleRule, ...]
    qname_classes: tuple[DnsQnameClassRule, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DnsAggregationPolicy":
        values = _strict_mapping(
            payload,
            {
                "schema_version", "policy_id", "timeout_ms",
                "formal_qname_storage", "diagnostic_full_qname",
                "container_roles", "qname_classes",
            },
            "DNS aggregation policy",
        )
        raw_roles = values["container_roles"]
        raw_qnames = values["qname_classes"]
        if not isinstance(raw_roles, list) or not isinstance(raw_qnames, list):
            raise RawCollectionError(
                "DNS role and qname rules must be lists"
            )
        roles = []
        for index, item in enumerate(raw_roles):
            role = _strict_mapping(
                item,
                {"role", "exact_container_names", "formal_action"},
                f"DNS container role {index}",
            )
            names = role["exact_container_names"]
            if not isinstance(names, list) or any(
                not isinstance(name, str) or not name
                for name in names
            ):
                raise RawCollectionError(
                    "DNS exact_container_names must be non-empty strings"
                )
            roles.append(ContainerRoleRule(
                role=str(role["role"]),
                exact_container_names=tuple(str(name) for name in names),
                formal_action=str(role["formal_action"]),
            ))
        qnames = []
        for index, item in enumerate(raw_qnames):
            rule = _strict_mapping(
                item,
                {
                    "qname_class", "formal_action",
                    "nxdomain_is_failure",
                },
                f"DNS qname class {index}",
            )
            if type(rule["nxdomain_is_failure"]) is not bool:
                raise RawCollectionError(
                    "DNS nxdomain_is_failure must be boolean"
                )
            qnames.append(DnsQnameClassRule(
                qname_class=str(rule["qname_class"]),
                formal_action=str(rule["formal_action"]),
                nxdomain_is_failure=bool(rule["nxdomain_is_failure"]),
            ))
        if isinstance(values["timeout_ms"], bool) \
                or not isinstance(values["timeout_ms"], int):
            raise RawCollectionError("DNS timeout_ms must be an integer")
        if type(values["diagnostic_full_qname"]) is not bool:
            raise RawCollectionError(
                "DNS diagnostic_full_qname must be boolean"
            )
        result = cls(
            schema_version=str(values["schema_version"]),
            policy_id=str(values["policy_id"]),
            timeout_ms=int(values["timeout_ms"]),
            formal_qname_storage=str(values["formal_qname_storage"]),
            diagnostic_full_qname=bool(values["diagnostic_full_qname"]),
            container_roles=tuple(roles),
            qname_classes=tuple(qnames),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != DNS_AGGREGATION_POLICY_SCHEMA:
            raise RawCollectionError(
                "unsupported DNS aggregation policy schema"
            )
        if not self.policy_id:
            raise RawCollectionError("DNS policy_id is empty")
        if not 100 <= self.timeout_ms <= 60_000:
            raise RawCollectionError("DNS timeout is outside its range")
        if self.formal_qname_storage != "sha256":
            raise RawCollectionError(
                "formal DNS qname storage must be sha256"
            )
        valid_actions = {"include", "separate", "record_only"}
        role_names = [item.role for item in self.container_roles]
        container_names = [
            name
            for item in self.container_roles
            for name in item.exact_container_names
        ]
        qname_classes = [
            item.qname_class for item in self.qname_classes
        ]
        if (
            len(role_names) != len(set(role_names))
            or len(container_names) != len(set(container_names))
            or len(qname_classes) != len(set(qname_classes))
        ):
            raise RawCollectionError("DNS policy rules are not unique")
        if not self.container_roles or not self.qname_classes:
            raise RawCollectionError("DNS policy rules are empty")
        if any(
            item.formal_action not in valid_actions
            for item in (*self.container_roles, *self.qname_classes)
        ):
            raise RawCollectionError("DNS policy action is invalid")
        if any(
            not item.role or not item.exact_container_names
            for item in self.container_roles
        ) or any(
            not item.qname_class for item in self.qname_classes
        ):
            raise RawCollectionError("DNS policy rule identity is empty")
        if not any(
            item.formal_action == "include"
            for item in self.container_roles
        ) or not any(
            item.formal_action == "include"
            for item in self.qname_classes
        ):
            raise RawCollectionError(
                "DNS policy must include a formal role and qname class"
            )
        if any(
            item.formal_action == "include"
            and not item.nxdomain_is_failure
            for item in self.qname_classes
        ):
            raise RawCollectionError(
                "included DNS qname classes must classify NXDOMAIN"
            )

    @property
    def fingerprint(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "timeout_ms": self.timeout_ms,
            "formal_qname_storage": self.formal_qname_storage,
            "diagnostic_full_qname": self.diagnostic_full_qname,
            "container_roles": [
                {
                    "role": item.role,
                    "exact_container_names": list(
                        item.exact_container_names
                    ),
                    "formal_action": item.formal_action,
                }
                for item in self.container_roles
            ],
            "qname_classes": [
                {
                    "qname_class": item.qname_class,
                    "formal_action": item.formal_action,
                    "nxdomain_is_failure": item.nxdomain_is_failure,
                }
                for item in self.qname_classes
            ],
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def role_rule(self, container_name: str) -> ContainerRoleRule:
        matches = [
            item for item in self.container_roles
            if container_name in item.exact_container_names
        ]
        if len(matches) != 1:
            raise RawCollectionError(
                "DNS container role is missing or ambiguous: "
                f"{container_name}"
            )
        return matches[0]

    def qname_rule(self, qname_class: str) -> DnsQnameClassRule:
        matches = [
            item for item in self.qname_classes
            if item.qname_class == qname_class
        ]
        if len(matches) != 1:
            raise RawCollectionError(
                "DNS qname class is missing or ambiguous: "
                f"{qname_class}"
            )
        return matches[0]

    def included(self, record: DnsSemanticRecord) -> bool:
        return (
            self.role_rule(record.container_name).formal_action == "include"
            and
            self.qname_rule(record.qname_class).formal_action == "include"
        )

    def outcome_is_failure(self, record: DnsSemanticRecord) -> bool:
        if record.final_outcome not in FINAL_OUTCOMES:
            raise RawCollectionError(
                f"unknown DNS final outcome {record.final_outcome!r}"
            )
        if record.final_outcome in FORMAL_FAILURE_OUTCOMES:
            return True
        if record.final_outcome == "NXDOMAIN":
            return self.qname_rule(
                record.qname_class
            ).nxdomain_is_failure
        return False


@dataclass(frozen=True)
class DnsRoleBucket:
    source_service: str
    container_role: str
    formal_action: str
    qname_class: str
    logical_query_count: int
    success_count: int
    failure_count: int
    excluded_count: int
    latency_sample_count: int
    latency_p95_ms: float | None


def _p95(values: Iterable[float]) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def aggregate_dns_role_buckets(
    records: Iterable[DnsSemanticRecord],
    policy: DnsAggregationPolicy,
) -> tuple[DnsRoleBucket, ...]:
    groups: dict[
        tuple[str, str, str, str],
        list[DnsSemanticRecord],
    ] = {}
    for item in records:
        role = policy.role_rule(item.container_name)
        qname = policy.qname_rule(item.qname_class)
        if item.container_role != role.role:
            raise RawCollectionError(
                "DNS transaction container role disagrees with policy"
            )
        if item.final_outcome not in FINAL_OUTCOMES:
            raise RawCollectionError(
                f"unknown DNS final outcome {item.final_outcome!r}"
            )
        if (
            item.final_outcome == "SUCCESS"
            and (
                item.successful_latency_ns is None
                or item.successful_latency_ns < 0
            )
        ) or (
            item.final_outcome != "SUCCESS"
            and item.successful_latency_ns is not None
        ):
            raise RawCollectionError(
                "DNS successful latency is inconsistent with final outcome"
            )
        action = (
            "include"
            if role.formal_action == "include"
            and qname.formal_action == "include"
            else (
                "separate"
                if role.formal_action == "separate"
                else "record_only"
            )
        )
        key = (
            item.source_service, role.role,
            action, item.qname_class,
        )
        groups.setdefault(key, []).append(item)
    output = []
    for key, values in sorted(groups.items()):
        included = key[2] == "include"
        success = sum(
            item.final_outcome == "SUCCESS" for item in values
        )
        failure = sum(
            policy.outcome_is_failure(item) for item in values
        )
        latencies = [
            item.successful_latency_ns / 1_000_000.0
            for item in values
            if item.final_outcome == "SUCCESS"
            and item.successful_latency_ns is not None
        ]
        output.append(DnsRoleBucket(
            source_service=key[0],
            container_role=key[1],
            formal_action=key[2],
            qname_class=key[3],
            logical_query_count=len(values) if included else 0,
            success_count=success if included else 0,
            failure_count=failure if included else 0,
            excluded_count=0 if included else len(values),
            latency_sample_count=len(latencies) if included else 0,
            latency_p95_ms=_p95(latencies) if included else None,
        ))
    return tuple(output)

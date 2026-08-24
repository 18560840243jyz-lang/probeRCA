"""Fail-closed remote injector sessions for the multi-node campaign.

The orchestration layer never sends arbitrary shell commands.  It names a
frozen injector profile and an immutable runtime binding; the worker agent is
responsible for translating that allow-listed operation into host actions.
Direct effectiveness evidence is returned separately from mutation state so
an RCA prediction can never make an injection appear successful.
"""

from __future__ import annotations

import copy
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .model import fingerprint


class InjectorExecutionError(RuntimeError):
    pass


class AgentClient(Protocol):
    def invoke(
        self, node_id: str, action: str, payload: dict[str, Any],
    ) -> dict[str, Any]: ...


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@>+-]{0,255}$")


@dataclass(frozen=True)
class TargetBinding:
    node_id: str
    entity_kind: str
    entity_id: str
    runtime_identity_fingerprint: str
    attributes: dict[str, Any]

    def __post_init__(self) -> None:
        if self.entity_kind not in {"service", "host", "tcp_edge"}:
            raise ValueError("unsupported campaign target kind")
        for value in (self.node_id, self.entity_id):
            if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
                raise ValueError("campaign target identifier is invalid")
        value = self.runtime_identity_fingerprint
        if not isinstance(value, str) or len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("runtime identity fingerprint must be SHA-256")

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "entity_kind": self.entity_kind,
            "entity_id": self.entity_id,
            "runtime_identity_fingerprint": self.runtime_identity_fingerprint,
            "attributes": copy.deepcopy(self.attributes),
        }


def evaluate_effectiveness_criterion(
    criterion: dict[str, Any], baseline: dict[str, Any], active: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Evaluate direct resource evidence without consulting alerts or RCA."""

    failures: list[str] = []
    if active.get("metric") != criterion.get("metric"):
        failures.append("direct_metric_mismatch")
    baseline_value = float(baseline.get("primary_value", 0.0))
    active_value = float(active.get("primary_value", 0.0))
    conditions = active.get("conditions", {})
    counters_before = baseline.get("counters", {})
    counters_after = active.get("counters", {})
    for key, expected in criterion.items():
        if key == "metric":
            continue
        if key.endswith("_delta_min"):
            counter = key[:-10]
            delta = float(counters_after.get(counter, 0.0)) - float(
                counters_before.get(counter, 0.0)
            )
            if delta < float(expected):
                failures.append(f"direct_counter_insufficient:{counter}")
        elif key.endswith("_required") and bool(expected):
            if key == "direct_counter_lift_required" \
                    or key == "direct_psi_lift_required":
                passed = active_value > baseline_value
            else:
                passed = conditions.get(key) is True
            if not passed:
                failures.append(f"direct_condition_failed:{key}")
        else:
            failures.append(f"unsupported_effectiveness_criterion:{key}")
    return not failures, failures


def run_injector_pilot(
    *,
    profile: dict[str, Any],
    target: TargetBinding,
    client: AgentClient,
    dataset_id: str,
    dataset_sha256: str,
    simulated: bool = False,
    clock_ns: Callable[[], int] = time.time_ns,
    evidence_reader: Callable[
        [str, dict[str, Any], TargetBinding], dict[str, Any]
    ] | None = None,
) -> dict[str, Any]:
    """Run one apply/evidence/cleanup transaction with unconditional cleanup."""

    if len(dataset_sha256) != 64:
        raise InjectorExecutionError("Pilot dataset SHA-256 is required")
    if not simulated and evidence_reader is None:
        raise InjectorExecutionError(
            "real Pilot requires an independent direct-evidence reader"
        )
    profile_id = str(profile.get("profile_id", ""))
    mechanism = str(profile.get("mechanism", ""))
    if not profile_id or not mechanism:
        raise InjectorExecutionError("injector profile identity is incomplete")
    session_id = fingerprint({
        "profile_id": profile_id,
        "target": target.as_dict(),
        "dataset_id": dataset_id,
        "started_at_ns": clock_ns(),
    })
    common = {
        "session_id": session_id,
        "profile_id": profile_id,
        "mechanism": mechanism,
        "target": target.as_dict(),
        "effectiveness_criterion": copy.deepcopy(
            profile["effectiveness_criterion"]
        ),
        "contamination_checks": list(profile.get("contamination_checks", ())),
    }
    before = client.invoke(target.node_id, "snapshot", common)
    if before.get("runtime_identity_fingerprint") != \
            target.runtime_identity_fingerprint:
        raise InjectorExecutionError("runtime identity changed before Pilot apply")
    if not before.get("state_fingerprint"):
        raise InjectorExecutionError("worker agent returned no baseline state")
    baseline_evidence = (
        evidence_reader("baseline", profile, target)
        if evidence_reader is not None
        else client.invoke(target.node_id, "measure", common)
    )
    applied_at_ns: int | None = None
    cleaned_at_ns: int | None = None
    active_evidence: dict[str, Any] = {}
    cleanup: dict[str, Any] = {}
    primary_error: Exception | None = None
    try:
        apply_result = client.invoke(target.node_id, "apply", {
            **common,
            "intensity": copy.deepcopy(profile["intensity"]),
        })
        if apply_result.get("applied") is not True:
            raise InjectorExecutionError("worker agent did not confirm apply")
        applied_at_ns = clock_ns()
        active_evidence = (
            evidence_reader("active", profile, target)
            if evidence_reader is not None
            else client.invoke(target.node_id, "measure", common)
        )
    except Exception as error:  # cleanup is mandatory even for partial apply
        primary_error = error
    finally:
        try:
            cleanup = client.invoke(target.node_id, "cleanup", common)
            cleaned_at_ns = clock_ns()
        except Exception as cleanup_error:
            if primary_error is None:
                primary_error = cleanup_error
            cleanup = {"cleaned": False, "error": str(cleanup_error)}
    after = client.invoke(target.node_id, "snapshot", common)
    cleanup_passed = (
        cleanup.get("cleaned") is True
        and after.get("state_fingerprint") == before.get("state_fingerprint")
        and after.get("runtime_identity_fingerprint")
        == before.get("runtime_identity_fingerprint")
    )
    effectiveness_passed = False
    effectiveness_failures: list[str] = []
    if primary_error is None:
        effectiveness_passed, effectiveness_failures = evaluate_effectiveness_criterion(
            profile["effectiveness_criterion"], baseline_evidence,
            active_evidence,
        )
    contamination = active_evidence.get("contamination", {})
    required_contamination = tuple(profile.get("contamination_checks", ()))
    missing_contamination = [
        name for name in required_contamination if name not in contamination
    ]
    contaminated = sorted(
        name for name in required_contamination if contamination.get(name) is True
    )
    contamination_passed = not missing_contamination and not contaminated
    report = {
        "schema_version": "probeRCA-injector-pilot-evidence-v1",
        "profile_id": profile_id,
        "mechanism": mechanism,
        "target": target.as_dict(),
        "session_id": session_id,
        "dataset_id": dataset_id,
        "sha256": dataset_sha256,
        "simulated": bool(simulated),
        "applied_at_ns": applied_at_ns,
        "cleaned_at_ns": cleaned_at_ns,
        "baseline_state_fingerprint": before.get("state_fingerprint"),
        "restored_state_fingerprint": after.get("state_fingerprint"),
        "effectiveness_passed": effectiveness_passed,
        "effectiveness_failures": effectiveness_failures,
        "contamination_passed": contamination_passed,
        "missing_contamination_checks": missing_contamination,
        "contaminated_dimensions": contaminated,
        "cleanup_passed": cleanup_passed,
        "baseline_evidence": baseline_evidence,
        "active_evidence": active_evidence,
        "execution_error": str(primary_error) if primary_error else None,
    }
    report["accepted"] = (
        primary_error is None and effectiveness_passed
        and contamination_passed and cleanup_passed
    )
    report["report_fingerprint"] = fingerprint(report)
    return report


def freeze_injector_registry(
    candidate_registry: dict[str, Any], reports: list[dict[str, Any]],
) -> dict[str, Any]:
    """Freeze candidates only from real, accepted, one-profile-one-Pilot reports."""

    if candidate_registry.get("status") != "candidate":
        raise InjectorExecutionError("only a candidate registry can be frozen")
    by_profile: dict[str, dict[str, Any]] = {}
    for report in reports:
        profile_id = report.get("profile_id")
        if not profile_id or profile_id in by_profile:
            raise InjectorExecutionError("Pilot evidence profile IDs must be unique")
        by_profile[profile_id] = report
    expected = {item["profile_id"] for item in candidate_registry["profiles"]}
    if set(by_profile) != expected:
        raise InjectorExecutionError("Pilot evidence does not cover every injector")
    frozen = copy.deepcopy(candidate_registry)
    frozen.pop("registry_fingerprint", None)
    frozen["status"] = "frozen"
    for profile in frozen["profiles"]:
        report = by_profile[profile["profile_id"]]
        if report.get("simulated") is not False:
            raise InjectorExecutionError("simulated Pilot evidence cannot freeze injectors")
        if report.get("accepted") is not True:
            raise InjectorExecutionError(
                f"Pilot evidence failed for {profile['profile_id']}"
            )
        if len(str(report.get("sha256", ""))) != 64:
            raise InjectorExecutionError("Pilot evidence has no dataset SHA-256")
        profile["pilot_evidence"] = {
            key: report[key] for key in (
                "dataset_id", "sha256", "report_fingerprint",
                "effectiveness_passed", "contamination_passed",
                "cleanup_passed",
            )
        }
    frozen["registry_fingerprint"] = fingerprint(frozen)
    return frozen

"""Actual-time, fail-closed injector lifecycle run beside data collection."""

from __future__ import annotations

import copy
import time
from typing import Any, Callable

from .execution import AgentClient, InjectorExecutionError, TargetBinding
from .model import fingerprint


def _wait_until(
    target_ns: int, *, clock_ns: Callable[[], int], sleeper: Callable[[float], None],
) -> None:
    while True:
        remaining = target_ns - clock_ns()
        if remaining <= 0:
            return
        sleeper(min(remaining / 1_000_000_000.0, 0.25))


def run_scheduled_injection(
    *,
    profile: dict[str, Any],
    target: TargetBinding,
    client: AgentClient,
    dataset_id: str,
    apply_target_ns: int,
    cleanup_target_ns: int,
    clock_ns: Callable[[], int] = time.time_ns,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if apply_target_ns >= cleanup_target_ns:
        raise ValueError("injector cleanup must follow apply")
    if clock_ns() >= apply_target_ns:
        raise InjectorExecutionError("scheduled injection started after apply target")
    session_id = fingerprint({
        "dataset_id": dataset_id,
        "profile_id": profile["profile_id"],
        "target": target.as_dict(),
        "apply_target_ns": apply_target_ns,
        "cleanup_target_ns": cleanup_target_ns,
    })
    common = {
        "session_id": session_id,
        "profile_id": profile["profile_id"],
        "mechanism": profile["mechanism"],
        "target": target.as_dict(),
        "effectiveness_criterion": copy.deepcopy(
            profile["effectiveness_criterion"]
        ),
        "contamination_checks": list(profile.get("contamination_checks", ())),
    }
    before = client.invoke(target.node_id, "snapshot", common)
    if before.get("runtime_identity_fingerprint") != \
            target.runtime_identity_fingerprint:
        raise InjectorExecutionError("runtime identity changed before scheduled apply")
    apply_started_ns = apply_confirmed_ns = None
    cleanup_started_ns = cleanup_confirmed_ns = None
    apply_result: dict[str, Any] = {}
    cleanup_result: dict[str, Any] = {}
    primary_error: Exception | None = None
    try:
        _wait_until(apply_target_ns, clock_ns=clock_ns, sleeper=sleeper)
        apply_started_ns = clock_ns()
        apply_result = client.invoke(target.node_id, "apply", {
            **common, "intensity": copy.deepcopy(profile["intensity"]),
        })
        apply_confirmed_ns = clock_ns()
        if apply_result.get("applied") is not True:
            raise InjectorExecutionError("worker did not confirm scheduled apply")
        _wait_until(cleanup_target_ns, clock_ns=clock_ns, sleeper=sleeper)
    except Exception as error:
        primary_error = error
    finally:
        cleanup_started_ns = clock_ns()
        try:
            cleanup_result = client.invoke(target.node_id, "cleanup", common)
            cleanup_confirmed_ns = clock_ns()
        except Exception as error:
            if primary_error is None:
                primary_error = error
            cleanup_result = {"cleaned": False, "error": str(error)}
    after = client.invoke(target.node_id, "snapshot", common)
    cleanup_passed = (
        cleanup_result.get("cleaned") is True
        and before.get("state_fingerprint") == after.get("state_fingerprint")
        and before.get("runtime_identity_fingerprint")
        == after.get("runtime_identity_fingerprint")
    )
    report = {
        "schema_version": "probeRCA-scheduled-injection-session-v1",
        "dataset_id": dataset_id,
        "profile_id": profile["profile_id"],
        "mechanism": profile["mechanism"],
        "target": target.as_dict(),
        "session_id": session_id,
        "planned_apply_ns": apply_target_ns,
        "planned_cleanup_ns": cleanup_target_ns,
        "apply_command_start_ns": apply_started_ns,
        "apply_confirmed_ns": apply_confirmed_ns,
        "cleanup_command_start_ns": cleanup_started_ns,
        "cleanup_confirmed_ns": cleanup_confirmed_ns,
        "apply_result": apply_result,
        "cleanup_result": cleanup_result,
        "baseline_state_fingerprint": before.get("state_fingerprint"),
        "restored_state_fingerprint": after.get("state_fingerprint"),
        "cleanup_passed": cleanup_passed,
        "execution_error": str(primary_error) if primary_error else None,
    }
    report["accepted"] = primary_error is None and cleanup_passed
    report["report_fingerprint"] = fingerprint(report)
    if primary_error is not None:
        raise InjectorExecutionError(
            f"scheduled injection failed after cleanup: {primary_error}"
        )
    if not cleanup_passed:
        raise InjectorExecutionError("scheduled injection cleanup did not restore state")
    return report

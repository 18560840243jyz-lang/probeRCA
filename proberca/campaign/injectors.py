"""Frozen injector registry produced only after direct-evidence Pilots."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .model import fingerprint


class InjectorRegistryError(ValueError):
    pass


_INTENSITY_FIELDS = {
    "service_cpu": frozenset({
        "actor_workers", "duty_cycle", "target_cgroup",
    }),
    "service_cpu_throttle": frozenset({
        "cpu_max_quota_us", "cpu_max_period_us",
    }),
    "service_memory": frozenset({
        "working_set_fraction_of_limit", "memory_high_fraction_of_limit",
    }),
    "service_io": frozenset({
        "write_bytes_per_sec", "file_bytes", "fsync_each_block",
    }),
    "service_futex": frozenset({"threads", "continuous_hold"}),
    "service_local_socket": frozenset({"threads", "network_namespace"}),
    "host_cpu": frozenset({"actor_workers", "isolated_cgroup"}),
    "host_memory": frozenset({
        "working_set_bytes", "memory_high_bytes", "isolated_cgroup",
    }),
    "host_io": frozenset({
        "file_bytes", "fsync_each_block", "isolated_cgroup",
    }),
    "host_nic": frozenset({"drop_percent", "direction", "interface"}),
    "tcp_latency": frozenset({"delay_ms", "direction"}),
    "tcp_failure": frozenset({"action", "direction"}),
}

_DIRECT_MUTATION_CONTROLS = {
    "service_cpu": frozenset(),
    "service_cpu_throttle": frozenset({"cpu.max"}),
    "service_memory": frozenset({"memory.high"}),
    "host_memory": frozenset({"memory.high"}),
    "host_nic": frozenset({"traffic_control"}),
    "tcp_latency": frozenset({"traffic_control"}),
    "tcp_failure": frozenset({"packet_filter"}),
}

_SECONDARY_SIGNAL_POLICY_FIELDS = frozenset({
    "metric", "reference_phase", "robust_scale_multiplier",
    "max_consecutive_excess_windows", "max_excess_fraction_lift",
    "forbidden_direct_controls", "incidental_status",
})


def load_injector_registry(path: Path, *, require_frozen: bool) -> dict[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != \
            "probeRCA-multinode-injector-registry-v1":
        raise InjectorRegistryError("unsupported injector registry schema")
    status = payload.get("status")
    if status not in {"candidate", "frozen"}:
        raise InjectorRegistryError("injector registry status is invalid")
    if require_frozen and status != "frozen":
        raise InjectorRegistryError("formal campaign requires a frozen injector registry")
    profiles = payload.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        raise InjectorRegistryError("injector registry profiles are empty")
    ids = [item.get("profile_id") for item in profiles]
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        raise InjectorRegistryError("injector profile IDs must be unique")
    for item in profiles:
        required = {
            "profile_id", "mechanism", "intensity", "cleanup",
            "effectiveness_criterion", "contamination_checks",
        }
        if not required.issubset(item):
            raise InjectorRegistryError(
                f"injector profile is incomplete: {item.get('profile_id')}"
            )
        mechanism = item.get("mechanism")
        if mechanism not in _INTENSITY_FIELDS:
            raise InjectorRegistryError("injector mechanism is not allow-listed")
        intensity = item.get("intensity")
        if not isinstance(intensity, dict) or set(intensity) != \
                _INTENSITY_FIELDS[mechanism]:
            raise InjectorRegistryError(
                f"injector intensity fields mismatch for {item['profile_id']}"
            )
        declared_controls = item.get("direct_mutation_controls")
        if declared_controls is not None:
            if not isinstance(declared_controls, list) or any(
                not isinstance(value, str) or not value for value in declared_controls
            ):
                raise InjectorRegistryError("direct mutation controls are invalid")
            expected = _DIRECT_MUTATION_CONTROLS.get(mechanism, frozenset())
            if frozenset(declared_controls) != expected:
                raise InjectorRegistryError(
                    f"direct mutation contract mismatch for {item['profile_id']}"
                )
        policy = item.get("secondary_signal_policy")
        if policy is not None:
            if not isinstance(policy, dict) or set(policy) != \
                    _SECONDARY_SIGNAL_POLICY_FIELDS:
                raise InjectorRegistryError(
                    f"secondary signal policy fields mismatch for {item['profile_id']}"
                )
            if declared_controls is None:
                raise InjectorRegistryError(
                    "secondary signal policy requires a direct mutation contract"
                )
            if policy["metric"] not in item["contamination_checks"]:
                raise InjectorRegistryError(
                    "secondary signal policy metric must be a contamination check"
                )
            if policy["reference_phase"] != "HEALTHY_PRE" \
                    or float(policy["robust_scale_multiplier"]) <= 0 \
                    or int(policy["max_consecutive_excess_windows"]) < 0 \
                    or not 0.0 <= float(policy["max_excess_fraction_lift"]) <= 1.0 \
                    or policy["incidental_status"] != \
                    "PASS_WITH_INCIDENTAL_SECONDARY_SIGNAL":
                raise InjectorRegistryError("secondary signal policy is invalid")
            forbidden = policy["forbidden_direct_controls"]
            if not isinstance(forbidden, list) or any(
                not isinstance(value, str) or not value for value in forbidden
            ):
                raise InjectorRegistryError(
                    "secondary signal forbidden controls are invalid"
                )
        if status == "frozen" and not item.get("pilot_evidence"):
            raise InjectorRegistryError(
                f"frozen injector lacks Pilot evidence: {item['profile_id']}"
            )
    result = dict(payload)
    result["registry_fingerprint"] = fingerprint({
        key: value for key, value in payload.items() if key != "registry_fingerprint"
    })
    existing = payload.get("registry_fingerprint")
    if existing is not None and existing != result["registry_fingerprint"]:
        raise InjectorRegistryError("injector registry fingerprint mismatch")
    return result


def validate_campaign_injectors(
    campaign_config: dict[str, Any], registry: dict[str, Any],
) -> None:
    profiles = {item["profile_id"]: item for item in registry["profiles"]}
    references = []
    for group in ("service", "host", "tcp"):
        references.extend(campaign_config["formal_fault_matrix"][group])
    references.extend(campaign_config["pilot_matrix"])
    for item in references:
        if item.get("mechanism") == "host_nic" \
                and not campaign_config["host_nic"]["supported"]:
            continue
        profile_id = item["injector_profile_id"]
        profile = profiles.get(profile_id)
        if profile is None:
            raise InjectorRegistryError(f"missing injector profile: {profile_id}")
        if profile["mechanism"] != item["mechanism"]:
            raise InjectorRegistryError(
                f"injector mechanism mismatch for {profile_id}"
            )

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
    "tcp_failure": frozenset({"loss_percent", "direction"}),
}


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

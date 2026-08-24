"""Two-level Go/No-Go evaluation for the one-time multi-node campaign."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .model import fingerprint


@dataclass(frozen=True)
class CampaignPreflightObservation:
    repository_tests_passed: bool
    deterministic_manifests_passed: bool
    cms_roundtrip_passed: bool
    interruption_resume_rehearsal_passed: bool
    fake_agent_rehearsal_passed: bool
    filesystem_restore_rehearsal_passed: bool
    object_store_adapter_tested: bool
    node_reports: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    object_store_readback_passed: bool = False
    frozen_image_digests: bool = False
    placement_verified: bool = False
    injector_registry_frozen: bool = False
    load_profile_frozen: bool = False
    real_pilot_count: int = 0
    expected_pilot_count: int = 12
    central_free_bytes: int = 0
    required_central_free_bytes: int = 300 * 1024 ** 3


def evaluate_campaign_preflight(
    observation: CampaignPreflightObservation,
) -> dict[str, Any]:
    code_checks = {
        "repository_tests": observation.repository_tests_passed,
        "deterministic_manifests": observation.deterministic_manifests_passed,
        "cms_roundtrip": observation.cms_roundtrip_passed,
        "interruption_resume_rehearsal":
            observation.interruption_resume_rehearsal_passed,
        "fake_agent_rehearsal": observation.fake_agent_rehearsal_passed,
        "filesystem_restore_rehearsal":
            observation.filesystem_restore_rehearsal_passed,
        "object_store_adapter": observation.object_store_adapter_tested,
    }
    node_checks: dict[str, bool] = {}
    for report in observation.node_reports:
        node_id = str(report.get("node_id", "unknown"))
        node_checks[f"{node_id}:cgroup_v2"] = report.get("cgroup_v2") is True
        node_checks[f"{node_id}:btf"] = report.get("btf") is True
        node_checks[f"{node_id}:clock"] = report.get("clock_synchronized") is True
        node_checks[f"{node_id}:clock_offset"] = \
            report.get("clock_offset_within_10ms") is True
        node_checks[f"{node_id}:swap"] = report.get("swap_disabled") is True
        node_checks[f"{node_id}:tools"] = report.get("required_tools") is True
        node_checks[f"{node_id}:disk"] = int(report.get("free_bytes", 0)) >= int(
            report.get("required_free_bytes", 100 * 1024 ** 3)
        )
        node_checks[f"{node_id}:cpu_capacity"] = int(
            report.get("logical_cpu_count", 0)
        ) >= int(report.get("required_logical_cpu_count", 8))
        node_checks[f"{node_id}:memory_capacity"] = int(
            report.get("memory_bytes", 0)
        ) >= int(report.get("required_memory_bytes", 16 * 1024 ** 3))
        if report.get("role") == "worker":
            node_checks[f"{node_id}:fault_interface"] = \
                report.get("fault_interface_ready") is True
            node_checks[f"{node_id}:host_fault_cgroup"] = \
                report.get("host_fault_cgroup_ready") is True
            node_checks[f"{node_id}:node_exporter"] = \
                report.get("node_exporter_ready") is True
            node_checks[f"{node_id}:beyla"] = report.get("beyla_ready") is True
    infrastructure_checks = {
        "four_nodes_reported": len(observation.node_reports) == 4,
        "object_store_independent_readback": observation.object_store_readback_passed,
        "frozen_image_digests": observation.frozen_image_digests,
        "fixed_placement": observation.placement_verified,
        "injector_registry_frozen": observation.injector_registry_frozen,
        "load_profile_frozen": observation.load_profile_frozen,
        "all_real_pilots": observation.real_pilot_count == observation.expected_pilot_count,
        "central_storage_capacity": observation.central_free_bytes
        >= observation.required_central_free_bytes,
        **node_checks,
    }
    code_ready = all(code_checks.values())
    formal_go = code_ready and all(infrastructure_checks.values())
    report = {
        "schema_version": "probeRCA-multinode-preflight-report-v1",
        "pre_rent_code_ready": code_ready,
        "formal_campaign_go": formal_go,
        "status": (
            "FORMAL_CAMPAIGN_GO" if formal_go else
            "INFRASTRUCTURE_AND_REAL_PILOTS_PENDING" if code_ready else
            "PRE_RENT_CODE_NOT_READY"
        ),
        "code_checks": code_checks,
        "infrastructure_checks": infrastructure_checks,
        "failed_code_checks": sorted(
            key for key, passed in code_checks.items() if not passed
        ),
        "pending_infrastructure_checks": sorted(
            key for key, passed in infrastructure_checks.items() if not passed
        ),
    }
    report["report_fingerprint"] = fingerprint(report)
    return report

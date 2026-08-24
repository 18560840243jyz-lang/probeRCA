"""Strict construction of physical load-qualification reports.

The observation is produced from collected data and infrastructure telemetry.
No Soft/Hard, READY, FISTA, or RCA field is accepted as a qualification gate.
"""

from __future__ import annotations

from typing import Any

from .gates import QualificationObservation, evaluate_qualification
from .model import fingerprint


class QualificationReportError(ValueError):
    pass


_OBSERVATION_FIELDS = frozenset(QualificationObservation.__dataclass_fields__)
_FORBIDDEN_CONTROL_FIELDS = frozenset({
    "soft", "hard", "ready", "rca", "fista", "top_k", "alert_count",
})


def build_qualification_report(
    campaign_config: dict[str, Any], observation_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(observation_payload, dict):
        raise QualificationReportError("qualification observation must be a mapping")
    forbidden = sorted(_FORBIDDEN_CONTROL_FIELDS & set(observation_payload))
    if forbidden:
        raise QualificationReportError(
            f"control-plane outputs cannot gate load qualification: {forbidden}"
        )
    missing = sorted(_OBSERVATION_FIELDS - set(observation_payload))
    extra = sorted(set(observation_payload) - _OBSERVATION_FIELDS)
    if missing or extra:
        raise QualificationReportError(
            f"qualification observation fields mismatch: missing={missing}, extra={extra}"
        )
    profiles = {
        item["profile_id"]: item
        for item in campaign_config["load_qualification"]["profiles"]
    }
    profile_id = str(observation_payload["profile_id"])
    if profile_id not in profiles:
        raise QualificationReportError("qualification profile is not frozen in campaign")
    profile = profiles[profile_id]
    if float(observation_payload["target_arrival_rate_rps"]) != float(
        profile["target_arrival_rate_rps"]
    ):
        raise QualificationReportError("qualification arrival rate differs from profile")
    formal_edges = tuple(sorted(
        f"{item['src']}->{item['dst']}"
        for item in campaign_config["formal_tcp_edges"]
    ))
    if tuple(sorted(observation_payload["required_tcp_edges"])) != formal_edges:
        raise QualificationReportError("qualification required edge scope is not formal")
    if tuple(sorted(observation_payload["observed_tcp_edges"])) != formal_edges:
        raise QualificationReportError("qualification did not observe the exact formal edge scope")
    coordinate_sets = {}
    for projected_name, required_name in (
        ("projected_baseline_rows", "required_baseline_rows"),
        ("projected_av_rows", "required_av_rows"),
    ):
        projected = observation_payload[projected_name]
        required = observation_payload[required_name]
        if (
            not isinstance(projected, dict) or not isinstance(required, dict)
            or set(projected) != set(required) or not projected
        ):
            raise QualificationReportError(
                f"qualification {projected_name} does not cover formal coordinates"
            )
        coordinate_sets[projected_name] = set(projected)
    av_count = int(
        campaign_config["formal_scope"]["theoretical_root_coordinates"]
    )
    if len(coordinate_sets["projected_av_rows"]) != av_count:
        raise QualificationReportError(
            "qualification A_v projection does not cover every formal root"
        )
    baseline_coordinates = coordinate_sets["projected_baseline_rows"]
    if not coordinate_sets["projected_av_rows"] <= baseline_coordinates:
        raise QualificationReportError(
            "qualification Baseline projection omits an A_v target"
        )
    if len(baseline_coordinates) > int(
        campaign_config["formal_scope"]["expected_records_per_window"]
    ):
        raise QualificationReportError(
            "qualification Baseline projection exceeds the formal contract"
        )
    observation = QualificationObservation(**observation_payload)
    gate = evaluate_qualification(
        observation, campaign_config["load_qualification"]["objective_gates"],
    )
    core = {
        "schema_version": "probeRCA-multinode-load-qualification-report-v1",
        "campaign_config_fingerprint": fingerprint(campaign_config),
        "profile_id": profile_id,
        "profile_fingerprint": fingerprint(profile),
        "observation": observation_payload,
        "qualified": gate["qualified"],
        "reasons": gate["reasons"],
        "measured_rps": gate["measured_rps"],
        "target_arrival_rate_rps": gate["target_arrival_rate_rps"],
    }
    return {**core, "report_fingerprint": fingerprint(core)}

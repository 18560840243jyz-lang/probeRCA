"""Freeze the highest objective-qualified open-loop profile."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from .model import fingerprint


class LoadProfileError(RuntimeError):
    pass


def freeze_load_profile(
    campaign_config: dict[str, Any], qualification_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    profiles = campaign_config["load_qualification"]["profiles"]
    expected = {item["profile_id"]: item for item in profiles}
    reports = {str(item.get("profile_id")): item for item in qualification_reports}
    if len(reports) != len(qualification_reports) or set(reports) != set(expected):
        raise LoadProfileError("qualification must cover each profile exactly once")
    if any(
        not isinstance(item.get("report_fingerprint"), str)
        or item["report_fingerprint"] != fingerprint({
            key: value for key, value in item.items()
            if key != "report_fingerprint"
        })
        for item in reports.values()
    ):
        raise LoadProfileError("qualification report fingerprint mismatch")
    qualified = [
        profile for profile in profiles
        if reports[profile["profile_id"]].get("qualified") is True
    ]
    if not qualified:
        raise LoadProfileError("no load profile passed objective qualification")
    selected = max(
        qualified, key=lambda item: float(item["target_arrival_rate_rps"]),
    )
    core = {
        "schema_version": "probeRCA-frozen-multinode-load-profile-v1",
        "selection": "highest-objective-qualified",
        "campaign_config_fingerprint": fingerprint(campaign_config),
        "selected_profile": copy.deepcopy(selected),
        "qualification_report_fingerprints": {
            profile_id: reports[profile_id]["report_fingerprint"]
            for profile_id in sorted(reports)
        },
    }
    return {**core, "load_profile_fingerprint": fingerprint(core)}


def load_frozen_load_profile(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != \
            "probeRCA-frozen-multinode-load-profile-v1":
        raise LoadProfileError("unsupported frozen load profile schema")
    supplied = payload.get("load_profile_fingerprint")
    if supplied != fingerprint({
        key: value for key, value in payload.items()
        if key != "load_profile_fingerprint"
    }):
        raise LoadProfileError("frozen load profile fingerprint mismatch")
    return payload

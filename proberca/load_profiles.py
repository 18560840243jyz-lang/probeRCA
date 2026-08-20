"""Validated, reproducible healthy-load profiles for single-VM experiments."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from proberca.dataplane.contracts import fingerprint


LOAD_PROFILE_SCHEMA_VERSION = "probeRCA-healthy-load-profiles-v1"


class HealthyLoadProfileError(ValueError):
    """A healthy-load profile is incomplete or internally inconsistent."""


@dataclass(frozen=True)
class WorkloadProfile:
    workload_id: str
    deployment: str
    container: str
    replicas: int
    environment: dict[str, str]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "WorkloadProfile":
        expected = {
            "workload_id", "deployment", "container", "replicas",
            "environment",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise HealthyLoadProfileError("workload profile fields mismatch")
        replicas = payload["replicas"]
        environment = payload["environment"]
        if isinstance(replicas, bool) or not isinstance(replicas, int) \
                or replicas <= 0:
            raise HealthyLoadProfileError(
                "workload replicas must be a positive integer"
            )
        if not isinstance(environment, dict) or not environment \
                or any(
                    not isinstance(key, str) or not key
                    or not isinstance(value, (str, int, float))
                    for key, value in environment.items()
                ):
            raise HealthyLoadProfileError(
                "workload environment must be a non-empty scalar mapping"
            )
        values = {
            "workload_id": payload["workload_id"],
            "deployment": payload["deployment"],
            "container": payload["container"],
            "replicas": replicas,
            "environment": {
                str(key): str(value)
                for key, value in sorted(environment.items())
            },
        }
        if any(
            not isinstance(values[name], str) or not values[name]
            for name in ("workload_id", "deployment", "container")
        ):
            raise HealthyLoadProfileError(
                "workload identity fields must be non-empty strings"
            )
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workload_id": self.workload_id,
            "deployment": self.deployment,
            "container": self.container,
            "replicas": self.replicas,
            "environment": dict(sorted(self.environment.items())),
        }


@dataclass(frozen=True)
class HealthyLoadProfile:
    profile_id: str
    scale_percent: int
    workloads: tuple[WorkloadProfile, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "HealthyLoadProfile":
        if not isinstance(payload, dict) or set(payload) != {
            "profile_id", "scale_percent", "workloads",
        }:
            raise HealthyLoadProfileError("healthy profile fields mismatch")
        scale = payload["scale_percent"]
        if isinstance(scale, bool) or not isinstance(scale, int) \
                or not 0 < scale <= 100:
            raise HealthyLoadProfileError(
                "profile scale_percent must be in [1,100]"
            )
        workloads = tuple(
            WorkloadProfile.from_dict(item)
            for item in payload["workloads"]
        )
        identities = [item.workload_id for item in workloads]
        if not workloads or len(identities) != len(set(identities)) \
                or identities != sorted(identities):
            raise HealthyLoadProfileError(
                "profile workloads must be non-empty, sorted, and unique"
            )
        profile_id = payload["profile_id"]
        if not isinstance(profile_id, str) or not profile_id:
            raise HealthyLoadProfileError("profile_id must be non-empty")
        return cls(profile_id, scale, workloads)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "scale_percent": self.scale_percent,
            "workloads": [item.to_dict() for item in self.workloads],
        }

    @property
    def profile_fingerprint(self) -> str:
        return fingerprint(self.to_dict())


@dataclass(frozen=True)
class HealthyLoadProfiles:
    schema_version: str
    status: str
    namespace: str
    selected_profile_id: str | None
    selected_profile_fingerprint: str | None
    qualification: dict[str, Any]
    profiles: tuple[HealthyLoadProfile, ...]

    @classmethod
    def load(cls, path: str | Path) -> "HealthyLoadProfiles":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or set(payload) != {
            "schema_version", "status", "namespace",
            "selected_profile_id", "selected_profile_fingerprint",
            "qualification", "profiles",
        }:
            raise HealthyLoadProfileError("load profile config fields mismatch")
        profiles = tuple(
            HealthyLoadProfile.from_dict(item)
            for item in payload["profiles"]
        )
        result = cls(
            schema_version=payload["schema_version"],
            status=payload["status"],
            namespace=payload["namespace"],
            selected_profile_id=payload["selected_profile_id"],
            selected_profile_fingerprint=(
                payload["selected_profile_fingerprint"]
            ),
            qualification=dict(payload["qualification"]),
            profiles=profiles,
        )
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != LOAD_PROFILE_SCHEMA_VERSION:
            raise HealthyLoadProfileError(
                "unsupported healthy-load profile schema"
            )
        if self.status not in {"qualifying", "frozen"}:
            raise HealthyLoadProfileError("invalid load profile status")
        if not isinstance(self.namespace, str) or not self.namespace:
            raise HealthyLoadProfileError("load namespace is required")
        profile_ids = [item.profile_id for item in self.profiles]
        scales = [item.scale_percent for item in self.profiles]
        if profile_ids != sorted(profile_ids) \
                or len(profile_ids) != len(set(profile_ids)):
            raise HealthyLoadProfileError(
                "profile IDs must be sorted and unique"
            )
        if scales != [25, 40, 55]:
            raise HealthyLoadProfileError(
                "qualification profiles must be exactly 25, 40, and 55 percent"
            )
        signatures = [
            tuple(
                (
                    item.workload_id,
                    item.deployment,
                    item.container,
                    tuple(sorted(item.environment)),
                )
                for item in profile.workloads
            )
            for profile in self.profiles
        ]
        if any(signature != signatures[0] for signature in signatures[1:]):
            raise HealthyLoadProfileError(
                "all load profiles must preserve the same workload structure"
            )
        expected_qualification = {
            "duration_windows", "provisional_baseline_windows",
        }
        if set(self.qualification) != expected_qualification:
            raise HealthyLoadProfileError(
                "load qualification fields mismatch"
            )
        duration = self.qualification["duration_windows"]
        baseline = self.qualification["provisional_baseline_windows"]
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (duration, baseline)
        ) or baseline >= duration:
            raise HealthyLoadProfileError(
                "load qualification window counts are invalid"
            )
        if self.status == "qualifying":
            if self.selected_profile_id is not None \
                    or self.selected_profile_fingerprint is not None:
                raise HealthyLoadProfileError(
                    "qualifying config cannot freeze a selected profile"
                )
        else:
            selected = self.selected()
            if self.selected_profile_fingerprint \
                    != selected.profile_fingerprint:
                raise HealthyLoadProfileError(
                    "selected profile fingerprint mismatch"
                )

    def profile(self, profile_id: str) -> HealthyLoadProfile:
        matches = [
            profile for profile in self.profiles
            if profile.profile_id == profile_id
        ]
        if len(matches) != 1:
            raise HealthyLoadProfileError(
                f"unknown healthy-load profile: {profile_id}"
            )
        return matches[0]

    def selected(self) -> HealthyLoadProfile:
        if self.selected_profile_id is None:
            raise HealthyLoadProfileError(
                "healthy-load profile has not been frozen"
            )
        return self.profile(self.selected_profile_id)


def kubectl_profile_commands(
    config: HealthyLoadProfiles,
    profile: HealthyLoadProfile,
    *,
    kubeconfig: str = "/home/jyz/.kube/config",
    context: str = "kind-proberca-ob",
) -> tuple[tuple[str, ...], ...]:
    """Return the complete generic command sequence for one load profile."""
    commands: list[tuple[str, ...]] = []
    base = (
        "kubectl", "--kubeconfig", kubeconfig,
        "--context", context, "-n", config.namespace,
    )
    for workload in profile.workloads:
        resource = f"deployment/{workload.deployment}"
        commands.append((*base, "scale", resource, f"--replicas={workload.replicas}"))
        commands.append((
            *base, "set", "env", resource,
            f"--containers={workload.container}",
            *(f"{key}={value}" for key, value in workload.environment.items()),
        ))
        commands.append((
            *base, "annotate", resource,
            f"proberca.io/load-profile={profile.profile_id}",
            (
                "proberca.io/load-profile-fingerprint="
                f"{profile.profile_fingerprint}"
            ),
            "--overwrite",
        ))
        commands.append((
            *base, "rollout", "status", resource, "--timeout=180s",
        ))
    return tuple(commands)

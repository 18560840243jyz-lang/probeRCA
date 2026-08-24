"""Generate the frozen four-node campaign without consulting RCA outcomes."""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .model import CampaignCase, RootCoordinate, fingerprint, private_commitment
from .injectors import validate_campaign_injectors


class CampaignConfigError(ValueError):
    pass


_FORMAL_ROOT_METRICS = {
    "service": frozenset({
        "cpu_usage_rate", "cpu_throttle_ratio", "memory_working_set_ratio",
        "io_psi", "futex_wait_time_rate", "local_socket_failure_rate",
    }),
    "host": frozenset({
        "cpu_psi", "memory_psi", "io_psi", "nic_drop_error_rate",
    }),
    "tcp": frozenset({"edge_latency_p95", "edge_failure_rate"}),
}


def load_campaign_config(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CampaignConfigError("campaign config must be a mapping")
    if payload.get("schema_version") != "probeRCA-multinode-campaign-v1":
        raise CampaignConfigError("unsupported campaign config schema")
    _validate_config(payload)
    return payload


def _validate_config(config: dict[str, Any]) -> None:
    scope = config["formal_scope"]
    placement = config["placement"]
    services = [service for values in placement.values() for service in values]
    if len(services) != int(scope["service_count"]) or len(services) != len(set(services)):
        raise CampaignConfigError("placement must contain each formal service exactly once")
    if set(placement) != set(config["workers"]):
        raise CampaignConfigError("placement workers do not match the formal worker set")
    contract = scope["metric_contract"]
    expected_records = (
        int(scope["service_count"]) * int(contract["service"])
        + int(scope["worker_count"]) * int(contract["host"])
        + int(scope["tcp_edge_count"]) * int(contract["tcp_edge"])
    )
    if expected_records != int(scope["expected_records_per_window"]):
        raise CampaignConfigError("formal 9/4/3 record count is inconsistent")
    edges = config.get("formal_tcp_edges")
    edge_ids = [f"{item['src']}->{item['dst']}" for item in edges or ()]
    if (
        not isinstance(edges, list)
        or len(edges) != int(scope["tcp_edge_count"])
        or len(edge_ids) != len(set(edge_ids))
        or any(
            item.get("src") not in services or item.get("dst") not in services
            for item in edges
        )
    ):
        raise CampaignConfigError("formal directed TCP edge set is inconsistent")
    selected = {
        f"{item['src']}->{item['dst']}"
        for item in config["selected_tcp_fault_edges"]
    }
    if not selected <= set(edge_ids):
        raise CampaignConfigError("selected TCP fault edge is outside formal scope")
    profiles = config["load_qualification"]["profiles"]
    profile_ids = [item["profile_id"] for item in profiles]
    if len(profiles) != 3 or len(profile_ids) != len(set(profile_ids)):
        raise CampaignConfigError("qualification requires three unique profiles")
    if [int(item["scale_percent"]) for item in profiles] != [25, 40, 55]:
        raise CampaignConfigError("qualification profile scales must be 25/40/55")
    for profile in profiles:
        if float(profile["target_arrival_rate_rps"]) <= 0 or int(profile["workers"]) <= 0:
            raise CampaignConfigError("load profile rate and workers must be positive")
        weights = profile["behavior_weights"]
        if set(weights) != {
            "browse_search_list", "detail_recommendation_ad_currency", "cart", "checkout",
        } or sum(int(value) for value in weights.values()) != 100:
            raise CampaignConfigError("load behavior weights must define the frozen 100% mix")
    if config["load_qualification"].get("control_plane_results_are_gates") is not False:
        raise CampaignConfigError("load qualification must not gate on control-plane output")
    matrix = config["formal_fault_matrix"]
    for group, allowed in _FORMAL_ROOT_METRICS.items():
        invalid = sorted({
            item.get("metric") for item in matrix[group]
            if item.get("metric") not in allowed
        })
        if invalid:
            raise CampaignConfigError(
                f"{group} fault labels are outside the formal 9/4/3 roots: {invalid}"
            )
    pilot_metrics = {
        (item["entity_kind"], item["metric"])
        for item in config["pilot_matrix"]
    }
    allowed_pilots = {
        *(("service", item) for item in _FORMAL_ROOT_METRICS["service"]),
        *(("host", item) for item in _FORMAL_ROOT_METRICS["host"]),
        *(("tcp_edge", item) for item in _FORMAL_ROOT_METRICS["tcp"]),
    }
    if not pilot_metrics <= allowed_pilots:
        raise CampaignConfigError("Pilot labels are outside the formal 9/4/3 roots")


def _seed(master_seed: int, name: str) -> int:
    return int(fingerprint({"master_seed": master_seed, "name": name})[:16], 16)


def _coordinate(
    entity_kind: str,
    entity_id: str,
    metric: str,
    mechanism: str,
    injector_profile_id: str,
) -> RootCoordinate:
    coordinate_id = f"{entity_kind}:{entity_id}:{metric}"
    return RootCoordinate(
        coordinate_id=coordinate_id,
        entity_kind=entity_kind,
        entity_id=entity_id,
        metric=metric,
        mechanism=mechanism,
        injector_profile_id=injector_profile_id,
    )


def _formal_coordinates(config: dict[str, Any]) -> list[RootCoordinate]:
    matrix = config["formal_fault_matrix"]
    result: list[RootCoordinate] = []
    for item in matrix["service"]:
        for service in item["targets"]:
            result.append(_coordinate(
                "service", service, item["metric"], item["mechanism"],
                item["injector_profile_id"],
            ))
    for item in matrix["host"]:
        if item["metric"] == "nic_drop_error_rate" \
                and not config["host_nic"]["supported"]:
            continue
        for host in item["targets"]:
            result.append(_coordinate(
                "host", host, item["metric"], item["mechanism"],
                item["injector_profile_id"],
            ))
    for item in matrix["tcp"]:
        for edge in item["targets"]:
            result.append(_coordinate(
                "tcp_edge", f"{edge['src']}->{edge['dst']}", item["metric"],
                item["mechanism"], item["injector_profile_id"],
            ))
    ids = [item.coordinate_id for item in result]
    if len(ids) != len(set(ids)):
        raise CampaignConfigError("formal root coordinates are not unique")
    return result


def _non_adjacent_shuffle(
    cases: list[CampaignCase], rng: random.Random,
) -> list[CampaignCase]:
    candidate = list(cases)
    for _ in range(10000):
        rng.shuffle(candidate)
        if all(
            left.coordinate is None or right.coordinate is None
            or left.coordinate.coordinate_id != right.coordinate.coordinate_id
            for left, right in zip(candidate, candidate[1:])
        ):
            return list(candidate)
    raise CampaignConfigError("could not construct non-adjacent campaign order")


def _pilot_coordinates(config: dict[str, Any]) -> list[RootCoordinate]:
    result = []
    for item in config["pilot_matrix"]:
        if item["mechanism"] == "host_nic" and not config["host_nic"]["supported"]:
            continue
        result.append(_coordinate(
            item["entity_kind"], item["entity_id"], item["metric"],
            item["mechanism"], item["injector_profile_id"],
        ))
    return result


@dataclass(frozen=True)
class CampaignPlan:
    public_manifest: dict[str, Any]
    private_manifest: dict[str, Any]
    summary: dict[str, Any]


def build_campaign_plan(
    config: dict[str, Any], *, commitment_secret: bytes,
    frozen_injectors: dict[str, Any] | None = None,
    frozen_load_profile: dict[str, Any] | None = None,
) -> CampaignPlan:
    _validate_config(config)
    if (frozen_injectors is None) != (frozen_load_profile is None):
        raise CampaignConfigError(
            "formal manifest requires both frozen injectors and load profile"
        )
    if frozen_injectors is not None:
        if frozen_injectors.get("status") != "frozen":
            raise CampaignConfigError("formal manifest requires frozen injectors")
        validate_campaign_injectors(config, frozen_injectors)
        if frozen_load_profile.get("campaign_config_fingerprint") != fingerprint(config):
            raise CampaignConfigError("frozen load profile belongs to another campaign")
        selected_load = frozen_load_profile.get("selected_profile")
        if selected_load not in config["load_qualification"]["profiles"]:
            raise CampaignConfigError("frozen load profile is not a campaign candidate")
    master_seed = int(config["master_seed"])
    episode_seconds = int(config["timing"]["episode_seconds"])
    healthy_seconds = int(config["timing"]["healthy_seconds"])
    repeats = int(config["formal_fault_matrix"]["repeats"])
    if repeats != 3:
        raise CampaignConfigError("formal matrix requires exactly three repeats")

    coordinates = _formal_coordinates(config)
    formal_cases: list[CampaignCase] = []
    validation_by_coordinate: dict[str, int] = {}
    for coordinate in coordinates:
        choices = [1, 2, 3]
        rng = random.Random(_seed(master_seed, f"split:{coordinate.coordinate_id}"))
        validation_repeat = rng.choice(choices)
        validation_by_coordinate[coordinate.coordinate_id] = validation_repeat
        for repeat in choices:
            split = "validation" if repeat == validation_repeat else "test"
            formal_cases.append(CampaignCase(
                case_id="",
                split=split,
                case_kind="fault",
                duration_seconds=episode_seconds,
                repeat=repeat,
                coordinate=coordinate,
                seed=_seed(master_seed, f"episode:{coordinate.coordinate_id}:{repeat}"),
                storage_class="sealed-test" if split == "test" else "ordinary",
                metadata={
                    "load_seed": _seed(master_seed, f"load:{coordinate.coordinate_id}:{repeat}"),
                    "injector_seed": _seed(master_seed, f"injector:{coordinate.coordinate_id}:{repeat}"),
                },
            ))

    validation = [case for case in formal_cases if case.split == "validation"]
    test = [case for case in formal_cases if case.split == "test"]
    validation = _non_adjacent_shuffle(
        validation, random.Random(_seed(master_seed, "validation-order")),
    )
    test = _non_adjacent_shuffle(
        test, random.Random(_seed(master_seed, "test-order")),
    )

    def assign_ids(cases: list[CampaignCase], prefix: str) -> list[CampaignCase]:
        result = []
        for index, case in enumerate(cases, 1):
            result.append(CampaignCase(
                case_id=f"{prefix}{index:06d}", split=case.split,
                case_kind=case.case_kind, duration_seconds=case.duration_seconds,
                repeat=case.repeat, coordinate=case.coordinate, seed=case.seed,
                storage_class=case.storage_class, metadata=case.metadata,
            ))
        return result

    validation = assign_ids(validation, "V")
    test_faults = assign_ids(test, "T")

    healthy = [
        CampaignCase(
            case_id=f"H-{split.upper()}", split=split, case_kind="healthy",
            duration_seconds=healthy_seconds,
            seed=_seed(master_seed, f"healthy:{split}"),
        )
        for split in ("dev", "validation", "test")
    ]
    pilots = [
        CampaignCase(
            case_id=f"P{index:03d}", split="dev", case_kind="pilot",
            duration_seconds=episode_seconds, repeat=1, coordinate=coordinate,
            seed=_seed(master_seed, f"pilot:{coordinate.coordinate_id}"),
        )
        for index, coordinate in enumerate(_pilot_coordinates(config), 1)
    ]
    controls = []
    for split, prefix in (("dev", "CD"), ("validation", "CV")):
        for repeat in range(1, 4):
            controls.append(CampaignCase(
                case_id=f"{prefix}{repeat:03d}", split=split,
                case_kind="control", duration_seconds=episode_seconds,
                repeat=repeat, seed=_seed(master_seed, f"control:{split}:{repeat}"),
            ))
    test_controls = [
        CampaignCase(
            case_id="", split="test", case_kind="control",
            duration_seconds=episode_seconds, repeat=repeat,
            seed=_seed(master_seed, f"control:test:{repeat}"),
            storage_class="sealed-test",
        )
        for repeat in range(1, 4)
    ]
    opaque_test = test_faults + test_controls
    opaque_rng = random.Random(_seed(master_seed, "opaque-test-order"))
    opaque_test = _non_adjacent_shuffle(opaque_test, opaque_rng)
    opaque_test = [
        CampaignCase(
            case_id=f"T{index:06d}", split=case.split,
            case_kind=case.case_kind, duration_seconds=case.duration_seconds,
            repeat=case.repeat, coordinate=case.coordinate, seed=case.seed,
            storage_class="sealed-test", metadata=case.metadata,
        )
        for index, case in enumerate(opaque_test, 1)
    ]

    schedule = (
        [healthy[0]] + pilots + [item for item in controls if item.split == "dev"]
        + [healthy[1]] + validation
        + [item for item in controls if item.split == "validation"]
        + [healthy[2]] + opaque_test
    )
    private_cases = [case.private_dict() for case in schedule]
    public_cases = [case.public_dict(commitment_secret) for case in schedule]
    config_fingerprint = fingerprint(config)
    public_core = {
        "schema_version": "probeRCA-multinode-public-manifest-v1",
        "campaign_config_fingerprint": config_fingerprint,
        "randomization_commitment_hmac_sha256": private_commitment(
            {"master_seed": master_seed}, commitment_secret,
        ),
        "host_nic_supported": bool(config["host_nic"]["supported"]),
        "executable": frozen_injectors is not None,
        "injector_registry_fingerprint": (
            frozen_injectors["registry_fingerprint"]
            if frozen_injectors is not None else None
        ),
        "load_profile_id": (
            frozen_load_profile["selected_profile"]["profile_id"]
            if frozen_load_profile is not None else None
        ),
        "load_profile_fingerprint": (
            frozen_load_profile["load_profile_fingerprint"]
            if frozen_load_profile is not None else None
        ),
        "expected_formal_record_count_per_window": int(
            config["formal_scope"]["expected_records_per_window"]
        ),
        "cases": public_cases,
    }
    public_manifest = dict(public_core)
    public_manifest["manifest_fingerprint"] = fingerprint(public_core)
    private_core = {
        "schema_version": "probeRCA-multinode-private-manifest-v1",
        "public_manifest_fingerprint": public_manifest["manifest_fingerprint"],
        "campaign_config_fingerprint": config_fingerprint,
        "validation_repeat_by_coordinate": validation_by_coordinate,
        "frozen_injector_registry": frozen_injectors,
        "frozen_load_profile": frozen_load_profile,
        "cases": private_cases,
    }
    private_manifest = dict(private_core)
    private_manifest["manifest_fingerprint"] = fingerprint(private_core)

    formal_count = len(coordinates) * 3
    summary = {
        "matrix_status": "VALID",
        "rent_server_go_no_go": (
            "NO_GO_PENDING_RESTORE_PREFLIGHT"
            if frozen_injectors is not None
            else "NO_GO_PENDING_INJECTOR_AND_RESTORE_PREFLIGHT"
        ),
        "host_nic_supported": bool(config["host_nic"]["supported"]),
        "formal_coordinate_count": len(coordinates),
        "formal_fault_count": formal_count,
        "validation_fault_count": len(validation),
        "test_fault_count": len(test_faults),
        "pilot_count": len(pilots),
        "healthy_count": len(healthy),
        "control_count": len(controls) + len(test_controls),
        "core_dataset_count": len(schedule),
        "overhead_experiment_count": len(config["overhead_experiments"]),
        "synthetic_group_count": int(config["synthetic"]["group_count"]),
        "public_manifest_fingerprint": public_manifest["manifest_fingerprint"],
    }
    expected = 135 if config["host_nic"]["supported"] else 131
    if len(schedule) != expected:
        raise CampaignConfigError(
            f"campaign count mismatch: expected {expected}, got {len(schedule)}"
        )
    return CampaignPlan(public_manifest, private_manifest, summary)

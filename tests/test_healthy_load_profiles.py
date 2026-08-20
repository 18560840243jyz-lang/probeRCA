from __future__ import annotations

from pathlib import Path

from proberca.controlplane.load_qualification import (
    project_av_coverage,
    project_baseline_coverage,
    summarize_score_episodes,
)
from proberca.load_profiles import (
    HealthyLoadProfiles,
    kubectl_profile_commands,
)


PROFILE_CONFIG = Path("configs/final_healthy_load_profiles.yaml")


def test_candidate_profiles_preserve_one_configured_workload_structure():
    config = HealthyLoadProfiles.load(PROFILE_CONFIG)

    assert config.status == "frozen"
    assert config.selected().profile_id == "single-vm-qualified-55"
    assert [item.scale_percent for item in config.profiles] == [25, 40, 55]
    signatures = [
        [
            (
                workload.workload_id,
                workload.deployment,
                workload.container,
                tuple(workload.environment),
            )
            for workload in profile.workloads
        ]
        for profile in config.profiles
    ]
    assert signatures[1:] == [signatures[0], signatures[0]]
    assert len({item.profile_fingerprint for item in config.profiles}) == 3


def test_profile_commands_are_derived_from_configuration():
    config = HealthyLoadProfiles.load(PROFILE_CONFIG)
    profile = config.profile("single-vm-qualified-40")
    commands = kubectl_profile_commands(config, profile)
    rendered = tuple(" ".join(command) for command in commands)

    for workload in profile.workloads:
        assert any(
            f"scale deployment/{workload.deployment} "
            f"--replicas={workload.replicas}" in command
            for command in rendered
        )
        assert any(
            f"set env deployment/{workload.deployment}" in command
            and all(
                f"{key}={value}" in command
                for key, value in workload.environment.items()
            )
            for command in rendered
        )
        assert any(
            f"proberca.io/load-profile={profile.profile_id}" in command
            and profile.profile_fingerprint in command
            for command in rendered
        )


def test_episode_summary_collapses_consecutive_alert_windows():
    entity = ("edge", "cluster::ns::a->b::tcp")
    episodes = summarize_score_episodes(
        (
            (1, {entity: 3.1}),
            (2, {entity: 3.2}),
            (3, {entity: 3.3}),
            (4, {entity: 4.0}),
            (5, {entity: 0.0}),
            (6, {entity: 3.5}),
            (7, {entity: 3.6}),
            (8, {entity: 3.7}),
        ),
        threshold=3.0,
        consecutive_windows=3,
    )

    assert len(episodes) == 2
    assert episodes[0]["duration_windows"] == 4
    assert episodes[0]["maximum_score"] == 4.0
    assert episodes[1]["duration_windows"] == 3


def test_episode_summary_preserves_role_evidence_for_gate_policy():
    entity = ("edge", "cluster::ns::a->b::tcp")
    episodes = summarize_score_episodes(
        (
            (1, {entity: 5.1}, {entity: {"edge_latency": 5.1}}),
            (2, {entity: 5.4}, {entity: {
                "edge_latency": 5.4, "edge_failure": 0.0,
            }}),
        ),
        threshold=5.0,
        consecutive_windows=2,
    )

    assert episodes[0]["latency_maximum_score"] == 5.4
    assert episodes[0]["failure_maximum_score"] == 0.0


def test_hard_candidate_at_two_windows_and_confirmation_at_three():
    entity = ("edge", "cluster::ns::a->b::tcp")
    windows = tuple(
        (timestamp, {entity: score})
        for timestamp, score in enumerate((5.1, 5.4, 5.2), start=1)
    )

    candidates = summarize_score_episodes(
        windows[:2], threshold=5.0, consecutive_windows=2,
    )
    confirmed_at_two = summarize_score_episodes(
        windows[:2], threshold=5.0, consecutive_windows=3,
    )
    confirmed_at_three = summarize_score_episodes(
        windows, threshold=5.0, consecutive_windows=3,
    )

    assert len(candidates) == 1
    assert confirmed_at_two == []
    assert len(confirmed_at_three) == 1


def test_coverage_projection_uses_coordinate_specific_model_minimums():
    baseline = project_baseline_coverage(
        {"metric-a": {
            "baseline_sample_count": 6,
            "minimum_healthy_samples": 6,
        }},
        qualification_windows=300,
        calibration_windows=600,
    )
    av = project_av_coverage(
        {
            "sparse-edge": {
                "allowed_feature_count": 4,
                "valid_training_rows": 16,
                "minimum_training_rows": 8,
            },
            "no-parent": {
                "allowed_feature_count": 0,
                "valid_training_rows": 0,
                "minimum_training_rows": 0,
            },
        },
        qualification_windows=300,
        calibration_windows=600,
    )

    assert baseline["metric-a"]["projected_valid_windows"] == 12
    assert baseline["metric-a"]["qualified"] is True
    assert av["sparse-edge"]["projected_training_rows"] == 32
    assert av["sparse-edge"]["qualification_required_rows"] == 16
    assert av["sparse-edge"]["qualified"] is True
    assert av["no-parent"]["qualified"] is True

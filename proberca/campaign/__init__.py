"""Deterministic, label-isolated orchestration for the multi-node campaign."""

from .generator import CampaignPlan, build_campaign_plan, load_campaign_config
from .gates import (
    EpisodeIntegrityObservation,
    QualificationObservation,
    evaluate_episode_integrity,
    evaluate_qualification,
)
from .phases import FaultLifecycle, classify_window_phase
from .state import CampaignState
from .restore import compare_restored_copy, verify_sha256s
from .execution import (
    TargetBinding,
    freeze_injector_registry,
    run_injector_pilot,
)
from .preflight import CampaignPreflightObservation, evaluate_campaign_preflight
from .storage import restore_dataset, upload_and_verify_dataset

__all__ = [
    "CampaignPlan",
    "CampaignState",
    "EpisodeIntegrityObservation",
    "FaultLifecycle",
    "QualificationObservation",
    "CampaignPreflightObservation",
    "TargetBinding",
    "build_campaign_plan",
    "classify_window_phase",
    "compare_restored_copy",
    "evaluate_episode_integrity",
    "evaluate_qualification",
    "evaluate_campaign_preflight",
    "freeze_injector_registry",
    "load_campaign_config",
    "restore_dataset",
    "run_injector_pilot",
    "upload_and_verify_dataset",
    "verify_sha256s",
]

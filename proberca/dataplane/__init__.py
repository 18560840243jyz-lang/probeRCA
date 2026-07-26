"""Collection-only data plane for the final ProbeRCA-BPF workflow."""

from .archive import (
    CollectionArchive,
    CollectionArchiveError,
    CollectionArchiveIntegrityError,
    CollectionArchiveNotSealedError,
    CollectionArchiveWriter,
)
from .adapters import from_engine_window, seal_engine_windows
from .burst import (
    BurstNormalizationError,
    burst_observation_quality,
    continuous_burst_strength,
    rare_event_strength,
)
from .contracts import CollectedWindow, GroundTruthFieldError

__all__ = [
    "BurstNormalizationError",
    "CollectedWindow",
    "CollectionArchive",
    "CollectionArchiveWriter",
    "CollectionArchiveError",
    "CollectionArchiveIntegrityError",
    "CollectionArchiveNotSealedError",
    "GroundTruthFieldError",
    "from_engine_window",
    "seal_engine_windows",
    "burst_observation_quality",
    "continuous_burst_strength",
    "rare_event_strength",
]

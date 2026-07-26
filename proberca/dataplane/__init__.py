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
from .burst_collection import (
    BurstChannelCalibration,
    BurstEvidenceCollector,
    RawBurstSample,
)
from .burst_archive import (
    BurstArchive,
    BurstArchiveWriter,
    RawBurstWindow,
)
from .burst_live import (
    FinalLiveBurstConfig,
    FinalLiveBurstSource,
    load_final_live_burst_config,
)
from .collector import (
    FinalDataPlaneCollector,
    FinalLiveCollectionRunner,
    FinalLiveCollectorConfig,
    collector_build_fingerprint,
)
from .contracts import CollectedWindow, GroundTruthFieldError
from .final_aggregation import (
    COMPONENTS,
    FinalAggregationResult,
    FinalWindowAggregator,
)
from .raw import RawCollectionError, RawCollectionWindow, RawMetricSample
from .sources import (
    CompositePrimitiveSource,
    PrometheusPrimitiveQuery,
    PrometheusPrimitiveSource,
    PrometheusSourceConfig,
)

__all__ = [
    "BurstNormalizationError",
    "BurstChannelCalibration",
    "BurstEvidenceCollector",
    "BurstArchive",
    "BurstArchiveWriter",
    "RawBurstWindow",
    "RawBurstSample",
    "FinalLiveBurstConfig",
    "FinalLiveBurstSource",
    "load_final_live_burst_config",
    "CollectedWindow",
    "CollectionArchive",
    "CollectionArchiveWriter",
    "CollectionArchiveError",
    "CollectionArchiveIntegrityError",
    "CollectionArchiveNotSealedError",
    "GroundTruthFieldError",
    "RawCollectionError",
    "RawCollectionWindow",
    "RawMetricSample",
    "COMPONENTS",
    "FinalAggregationResult",
    "FinalWindowAggregator",
    "FinalDataPlaneCollector",
    "FinalLiveCollectionRunner",
    "FinalLiveCollectorConfig",
    "collector_build_fingerprint",
    "CompositePrimitiveSource",
    "PrometheusPrimitiveQuery",
    "PrometheusPrimitiveSource",
    "PrometheusSourceConfig",
    "from_engine_window",
    "seal_engine_windows",
    "burst_observation_quality",
    "continuous_burst_strength",
    "rare_event_strength",
]

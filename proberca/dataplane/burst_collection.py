"""Independent Burst-source normalization for the final data plane."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable

from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION,
    EvidenceObservationRecord,
)

from .burst import (
    burst_observation_quality,
    continuous_burst_strength,
    rare_event_strength,
)
from .contracts import assert_label_safe, fingerprint
from .raw import RawCollectionError


RAW_BURST_SCHEMA_VERSION = "probeRCA-final-raw-burst-v1"
_HEX = frozenset("0123456789abcdef")
BURST_CHANNEL_MODES = {
    "sched.runqueue_wait_p95": "continuous",
    "sched.wakeup_latency_p95": "continuous",
    "memory.major_page_fault_rate": "rare",
    "memory.direct_reclaim_stall": "continuous",
    "memory.oom_victim": "rare",
    "block.latency_p95": "continuous",
    "block.queue_wait_p95": "continuous",
    "futex.wait_count": "rare",
    "futex.wait_p95": "continuous",
    "socket.queue_wait_p95": "continuous",
    "socket.backlog_overflow": "rare",
    "socket.accept_connect_failure": "rare",
    "host.sched.runqueue_wait_p95": "continuous",
    "host.sched.wakeup_latency_p95": "continuous",
    "host.memory.direct_reclaim_stall": "continuous",
    "host.memory.oom_victim": "rare",
    "host.block.latency_p95": "continuous",
    "host.block.queue_wait_p95": "continuous",
    "nic.queue_drop_rate": "rare",
    "nic.error_rate": "rare",
    "nic.softirq_latency_p95": "continuous",
    "tcp.retrans_rate": "rare",
    "tcp.rto_rate": "rare",
    "tcp.rtt_p95": "continuous",
    "tcp.connect_failure_rate": "rare",
    "tcp.rst_rate": "rare",
    "dns.query_latency_p95": "continuous",
    "dns.timeout_rate": "rare",
    "dns.rcode_failure_rate": "rare",
}


def _opaque(prefix: str, value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(prefix + ":")
        and len(value) == len(prefix) + 65
        and all(character in _HEX for character in value[len(prefix) + 1:])
    )


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RawCollectionError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise RawCollectionError(f"{name} must be finite")
    return result


def _probability(name: str, value: Any) -> float:
    result = _finite(name, value)
    if not 0.0 <= result <= 1.0:
        raise RawCollectionError(f"{name} must be in [0,1]")
    return result


@dataclass(frozen=True)
class BurstChannelCalibration:
    channel_id: str
    mode: str
    rare_event_threshold: float | None
    healthy_values: tuple[float, ...]
    transform: str
    polarity: int
    z_cap: float
    minimum_healthy_samples: int
    minimum_scale: float
    calibration_id: str

    @classmethod
    def create(cls, **values) -> "BurstChannelCalibration":
        payload = dict(values)
        payload.pop("calibration_id", None)
        for name in ("healthy_values",):
            payload[name] = tuple(payload[name])
        calibration_id = fingerprint({
            **payload, "healthy_values": list(payload["healthy_values"]),
        })
        result = cls(**payload, calibration_id=calibration_id)
        result.validate()
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BurstChannelCalibration":
        if not isinstance(payload, dict) \
                or set(payload) != set(cls.__dataclass_fields__):
            raise RawCollectionError("Burst calibration fields mismatch")
        values = dict(payload)
        if not isinstance(values["healthy_values"], list):
            raise RawCollectionError("healthy_values must be a list")
        values["healthy_values"] = tuple(values["healthy_values"])
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if not isinstance(self.channel_id, str) or not self.channel_id:
            raise RawCollectionError("Burst calibration channel_id is required")
        if self.mode not in {"rare", "continuous"}:
            raise RawCollectionError("Burst calibration mode is invalid")
        if self.mode == "rare":
            threshold = _finite(
                "rare_event_threshold", self.rare_event_threshold
            )
            if threshold <= 0 or self.healthy_values:
                raise RawCollectionError(
                    "rare Burst calibration must use only a positive threshold"
                )
        elif self.rare_event_threshold is not None:
            raise RawCollectionError(
                "continuous Burst calibration cannot declare a rare threshold"
            )
        if self.transform not in {"identity", "log1p"} \
                or self.polarity not in {-1, 1}:
            raise RawCollectionError("Burst transform/polarity is invalid")
        _finite("z_cap", self.z_cap)
        _finite("minimum_scale", self.minimum_scale)
        if isinstance(self.minimum_healthy_samples, bool) \
                or not isinstance(self.minimum_healthy_samples, int) \
                or self.minimum_healthy_samples <= 0:
            raise RawCollectionError(
                "minimum_healthy_samples must be positive"
            )
        if any(not math.isfinite(float(value)) for value in self.healthy_values):
            raise RawCollectionError("healthy Burst values must be finite")
        # ``create`` fingerprints the payload before calibration_id exists.
        expected = fingerprint({
            key: value for key, value in (
                asdict(self) | {"healthy_values": list(self.healthy_values)}
            ).items() if key != "calibration_id"
        })
        if self.calibration_id != expected:
            raise RawCollectionError("Burst calibration_id mismatch")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["healthy_values"] = list(self.healthy_values)
        return payload


@dataclass(frozen=True)
class RawBurstSample:
    schema_version: str
    source_record_id: str
    source_object_id: str
    timestamp_ns: int
    cluster_id: str
    namespace: str
    entity_type: str
    entity_id: str
    channel_id: str
    value: float
    exposure: float | None
    coverage: float
    event_loss_rate: float
    mapping_quality: float

    @classmethod
    def create(cls, **values) -> "RawBurstSample":
        payload = dict(values)
        payload["schema_version"] = RAW_BURST_SCHEMA_VERSION
        payload.pop("source_record_id", None)
        candidate = cls(
            source_record_id="source:" + ("0" * 64), **payload
        )
        candidate._validate(check_source=False)
        identity = candidate.to_dict()
        identity.pop("source_record_id")
        return replace(
            candidate,
            source_record_id="source:" + fingerprint(identity),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawBurstSample":
        if not isinstance(payload, dict) \
                or set(payload) != set(cls.__dataclass_fields__):
            raise RawCollectionError("raw Burst fields mismatch")
        result = cls(**payload)
        result.validate()
        return result

    def _validate(self, *, check_source: bool) -> None:
        if self.schema_version != RAW_BURST_SCHEMA_VERSION:
            raise RawCollectionError("unsupported raw Burst schema")
        if isinstance(self.timestamp_ns, bool) \
                or not isinstance(self.timestamp_ns, int) \
                or self.timestamp_ns < 0:
            raise RawCollectionError("raw Burst timestamp is invalid")
        for name in (
            "cluster_id", "namespace", "entity_id", "channel_id",
        ):
            if not isinstance(getattr(self, name), str) \
                    or not getattr(self, name):
                raise RawCollectionError(f"raw Burst {name} is required")
        if self.entity_type not in {"service", "host", "edge"}:
            raise RawCollectionError("raw Burst entity_type is invalid")
        value = _finite("raw Burst value", self.value)
        if value < 0:
            raise RawCollectionError("raw Burst value must be non-negative")
        object.__setattr__(self, "value", value)
        if self.exposure is not None:
            exposure = _finite("raw Burst exposure", self.exposure)
            if exposure < 0:
                raise RawCollectionError(
                    "raw Burst exposure must be non-negative"
                )
            object.__setattr__(self, "exposure", exposure)
        mode = BURST_CHANNEL_MODES.get(self.channel_id)
        if mode is None:
            raise RawCollectionError("unknown raw Burst channel")
        if mode == "rare" and self.exposure is None:
            raise RawCollectionError(
                "event-count Burst channel requires exposure"
            )
        if mode == "continuous" and self.exposure is not None:
            raise RawCollectionError(
                "continuous Burst channel cannot declare exposure"
            )
        for name in ("coverage", "event_loss_rate", "mapping_quality"):
            object.__setattr__(
                self, name, _probability(name, getattr(self, name))
            )
        if not _opaque("object", self.source_object_id):
            raise RawCollectionError(
                "raw Burst source_object_id must be object:SHA-256"
            )
        payload = self.to_dict()
        if check_source:
            if not _opaque("source", self.source_record_id):
                raise RawCollectionError(
                    "raw Burst source_record_id must be source:SHA-256"
                )
            supplied = payload.pop("source_record_id")
            if supplied != "source:" + fingerprint(payload):
                raise RawCollectionError(
                    "raw Burst source identity mismatch"
                )
            payload["source_record_id"] = supplied
        assert_label_safe(payload)

    def validate(self) -> None:
        self._validate(check_source=True)

    def to_dict(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


class BurstEvidenceCollector:
    """Normalize independent Burst samples into archive evidence records."""

    def __init__(
        self,
        *,
        collection_contract: dict[str, Any],
        collector_build_id: str,
        calibrations: Iterable[BurstChannelCalibration],
    ):
        self.contract = dict(collection_contract)
        self.collector_build_id = collector_build_id
        self.calibrations = {
            item.channel_id: item for item in calibrations
        }
        roles = self.contract.get("burst_channel_roles") or []
        self.roles = {item["channel_id"]: item for item in roles}
        if set(self.calibrations) != set(self.roles):
            raise RawCollectionError(
                "Burst calibrations must cover every frozen channel exactly"
            )
        for item in self.calibrations.values():
            item.validate()
            if BURST_CHANNEL_MODES.get(item.channel_id) != item.mode:
                raise RawCollectionError(
                    f"Burst calibration mode mismatch for {item.channel_id}"
                )
        if not _opaque("source", "source:" + collector_build_id):
            raise RawCollectionError("collector_build_id must be SHA-256")

    def collect(
        self,
        *,
        samples: Iterable[RawBurstSample],
        window_start_ns: int,
        window_end_ns: int,
        residual_source_record_ids: Iterable[str],
    ) -> tuple[EvidenceObservationRecord, ...]:
        grouped: dict[tuple[str, str, str, str], list[RawBurstSample]] = {}
        residual = set(residual_source_record_ids)
        for item in samples:
            item.validate()
            if not window_start_ns <= item.timestamp_ns < window_end_ns:
                raise RawCollectionError(
                    "raw Burst sample is outside its evidence window"
                )
            role = self.roles.get(item.channel_id)
            if role is None or item.entity_type not in role["entity_types"]:
                raise RawCollectionError(
                    "raw Burst channel/target is not allowed"
                )
            if item.source_record_id in residual:
                raise RawCollectionError(
                    "Burst source overlaps normal residual source"
                )
            key = (
                item.namespace, item.entity_type,
                item.entity_id, item.channel_id,
            )
            grouped.setdefault(key, []).append(item)
        output = []
        for key, values in sorted(grouped.items()):
            namespace, entity_type, entity_id, channel_id = key
            calibration = self.calibrations[channel_id]
            if calibration.mode == "rare":
                if any(item.exposure is None for item in values):
                    raise RawCollectionError(
                        "rare Burst sample lacks exposure"
                    )
                count = sum(item.value for item in values)
                if not float(count).is_integer():
                    raise RawCollectionError(
                        "rare Burst event count must be integral"
                    )
                strength = rare_event_strength(
                    int(count),
                    sum(float(item.exposure) for item in values),
                    float(calibration.rare_event_threshold),
                )
            else:
                if len(values) != 1 or values[0].exposure is not None:
                    raise RawCollectionError(
                        "continuous Burst channel requires one value"
                    )
                strength = continuous_burst_strength(
                    values[0].value,
                    calibration.healthy_values,
                    polarity=calibration.polarity,
                    transform=calibration.transform,
                    z_cap=calibration.z_cap,
                    minimum_healthy_samples=(
                        calibration.minimum_healthy_samples
                    ),
                    minimum_scale=calibration.minimum_scale,
                )
            quality = burst_observation_quality(
                coverage=min(item.coverage for item in values),
                event_loss_rate=max(
                    item.event_loss_rate for item in values
                ),
                mapping_quality=min(
                    item.mapping_quality for item in values
                ),
            )
            source_ids = sorted({
                item.source_record_id for item in values
            })
            object_ids = sorted({
                item.source_object_id for item in values
            })
            evidence_id = fingerprint({
                "window_start_ns": window_start_ns,
                "window_end_ns": window_end_ns,
                "entity_id": entity_id,
                "channel_id": channel_id,
                "source_record_ids": source_ids,
                "calibration_id": calibration.calibration_id,
            })
            output.append(EvidenceObservationRecord(
                schema_version=PROBERCA_SCHEMA_VERSION,
                evidence_id=evidence_id,
                timestamp_ns=window_end_ns - 1,
                evidence_window_start_ns=window_start_ns,
                evidence_window_end_ns=window_end_ns,
                analysis_cutoff_ns=window_end_ns,
                cluster_id=values[0].cluster_id,
                namespace=namespace,
                target_type=(
                    "shock" if entity_type == "edge" else "node"
                ),
                target_id=entity_id,
                channel_id=channel_id,
                source_type="burst_event",
                normalized_strength=strength,
                observation_quality=quality,
                # The frozen scheme defines psi=a_B*w.  The control plane
                # multiplies these three fields, so reliability remains one
                # unless a separate, independently calibrated reliability
                # factor is introduced in a future frozen contract.
                reliability_weight=1.0,
                source_record_ids=source_ids,
                source_object_ids=object_ids,
                independent_from_residual=True,
                provenance={
                    "calibration_id": calibration.calibration_id,
                    "collector_build_fingerprint": self.collector_build_id,
                    "source_set_fingerprint": fingerprint(source_ids),
                },
                config_fingerprint=self.contract[
                    "burst_config_fingerprint"
                ],
            ))
        return tuple(output)

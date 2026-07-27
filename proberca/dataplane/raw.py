"""Strict raw observations accepted by the final collection-only data plane.

Raw observations contain only measurements and runtime identities.  They do
not contain alert state, incident identifiers, expected roots, or any other
control-plane result.  Source identifiers are content-derived opaque hashes so
normal metrics and Burst evidence can be proven disjoint at the archive
boundary.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Iterable

from .contracts import assert_label_safe, fingerprint


RAW_SAMPLE_SCHEMA_VERSION = "probeRCA-final-raw-sample-v1"
RAW_WINDOW_SCHEMA_VERSION = "probeRCA-final-raw-window-v1"
RAW_METRIC_KINDS = frozenset({
    "gauge", "monotonic_counter", "histogram_bucket",
})
RAW_ENTITY_TYPES = frozenset({"service", "host", "edge"})
RAW_SCOPES = frozenset({"pod", "node", "flow"})
_HEX = frozenset("0123456789abcdef")


class RawCollectionError(ValueError):
    """A raw measurement cannot be accepted by the final data plane."""


def _nonempty(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RawCollectionError(f"{name} must be a non-empty string")
    if "::" in value or "->" in value:
        raise RawCollectionError(f"{name} contains a stable-ID separator")
    return value


def _optional_identity(name: str, value: Any) -> str | None:
    if value is None:
        return None
    return _nonempty(name, value)


def _probability(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RawCollectionError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise RawCollectionError(f"{name} must be finite and in [0,1]")
    return result


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RawCollectionError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise RawCollectionError(f"{name} must be finite")
    return result


def _opaque(prefix: str, value: str) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(prefix + ":")
        and len(value) == len(prefix) + 65
        and all(character in _HEX for character in value[len(prefix) + 1:])
    )


@dataclass(frozen=True)
class RawMetricSample:
    """One source measurement before cross-series final aggregation."""

    schema_version: str
    source_record_id: str
    source_object_id: str
    timestamp_ns: int
    cluster_id: str
    entity_type: str
    component: str
    metric_family: str
    metric_kind: str
    unit: str
    scope: str
    series_id: str
    value: float
    namespace: str | None = None
    service_name: str | None = None
    node_name: str | None = None
    pod_uid: str | None = None
    container_id: str | None = None
    src_service: str | None = None
    dst_service: str | None = None
    dst_namespace: str | None = None
    src_pod_uid: str | None = None
    dst_pod_uid: str | None = None
    src_node: str | None = None
    dst_node: str | None = None
    protocol: str | None = None
    histogram_upper_bound: float | None = None
    histogram_is_inf_bucket: bool = False
    coverage: float = 1.0
    event_loss_rate: float = 0.0
    mapping_quality: float = 1.0

    @classmethod
    def create(cls, **values: Any) -> "RawMetricSample":
        payload = dict(values)
        payload["schema_version"] = RAW_SAMPLE_SCHEMA_VERSION
        payload.pop("source_record_id", None)
        payload.setdefault(
            "source_object_id",
            "object:" + fingerprint({
                "cluster_id": payload.get("cluster_id"),
                "entity_type": payload.get("entity_type"),
                "series_id": payload.get("series_id"),
            }),
        )
        candidate = cls(source_record_id="source:" + ("0" * 64), **payload)
        candidate._validate(check_source_id=False)
        identity = candidate.to_dict()
        identity.pop("source_record_id")
        return replace(
            candidate,
            source_record_id="source:" + fingerprint(identity),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawMetricSample":
        if not isinstance(payload, dict) or set(payload) != set(cls.__dataclass_fields__):
            raise RawCollectionError("raw sample fields mismatch")
        result = cls(**payload)
        result.validate()
        return result

    def _validate(self, *, check_source_id: bool) -> None:
        if self.schema_version != RAW_SAMPLE_SCHEMA_VERSION:
            raise RawCollectionError("unsupported raw sample schema")
        if isinstance(self.timestamp_ns, bool) or not isinstance(self.timestamp_ns, int) \
                or self.timestamp_ns < 0:
            raise RawCollectionError("raw timestamp_ns must be a non-negative integer")
        _nonempty("cluster_id", self.cluster_id)
        _nonempty("component", self.component)
        _nonempty("metric_family", self.metric_family)
        _nonempty("unit", self.unit)
        _nonempty("series_id", self.series_id)
        if self.entity_type not in RAW_ENTITY_TYPES:
            raise RawCollectionError("raw entity_type is invalid")
        if self.metric_kind not in RAW_METRIC_KINDS:
            raise RawCollectionError("raw metric_kind is invalid")
        if self.scope not in RAW_SCOPES:
            raise RawCollectionError("raw scope is invalid")
        object.__setattr__(self, "value", _finite("raw value", self.value))
        if self.value < 0:
            raise RawCollectionError("raw final-scheme components must be non-negative")
        for name in (
            "namespace", "service_name", "node_name", "pod_uid", "container_id",
            "src_service", "dst_service", "dst_namespace", "src_pod_uid",
            "dst_pod_uid", "src_node", "dst_node", "protocol",
        ):
            _optional_identity(name, getattr(self, name))
        if not _opaque("object", self.source_object_id):
            raise RawCollectionError("source_object_id must be object:SHA-256")
        if self.entity_type == "service":
            if self.scope != "pod" or not all((
                self.namespace, self.service_name, self.pod_uid,
            )):
                raise RawCollectionError(
                    "service components require namespace/service/pod identity"
                )
            if any((self.src_service, self.dst_service, self.protocol)):
                raise RawCollectionError("service component contains edge identity")
        elif self.entity_type == "host":
            if self.scope != "node" or not self.node_name:
                raise RawCollectionError("host components require node identity")
            if any((
                self.service_name, self.pod_uid, self.src_service,
                self.dst_service, self.protocol,
            )):
                raise RawCollectionError("host component contains non-host identity")
        else:
            if self.scope != "flow" or not all((
                self.namespace, self.src_service, self.dst_service, self.protocol,
            )):
                raise RawCollectionError(
                    "edge components require directed service-pair identity"
                )
            if any((self.service_name, self.pod_uid, self.node_name)):
                raise RawCollectionError("edge component contains node identity")
        if self.metric_kind == "histogram_bucket":
            if not isinstance(self.histogram_is_inf_bucket, bool):
                raise RawCollectionError("histogram infinity flag must be boolean")
            if self.histogram_is_inf_bucket:
                if self.histogram_upper_bound is not None:
                    raise RawCollectionError("+Inf bucket must not have an upper bound")
            else:
                bound = _finite(
                    "histogram_upper_bound", self.histogram_upper_bound
                )
                object.__setattr__(self, "histogram_upper_bound", bound)
        elif self.histogram_upper_bound is not None \
                or self.histogram_is_inf_bucket is not False:
            raise RawCollectionError(
                "non-histogram component contains histogram metadata"
            )
        object.__setattr__(self, "coverage", _probability("coverage", self.coverage))
        object.__setattr__(
            self,
            "event_loss_rate",
            _probability("event_loss_rate", self.event_loss_rate),
        )
        object.__setattr__(
            self,
            "mapping_quality",
            _probability("mapping_quality", self.mapping_quality),
        )
        payload = self.to_dict()
        if check_source_id:
            if not _opaque("source", self.source_record_id):
                raise RawCollectionError(
                    "source_record_id must be source:SHA-256"
                )
            supplied = payload.pop("source_record_id")
            if supplied != "source:" + fingerprint(payload):
                raise RawCollectionError(
                    "source_record_id does not match raw sample content"
                )
            payload["source_record_id"] = supplied
        assert_label_safe(payload)

    def validate(self) -> None:
        self._validate(check_source_id=True)

    @property
    def entity_key(self) -> tuple[str, ...]:
        if self.entity_type == "service":
            return (
                "service", self.cluster_id, self.namespace or "",
                self.service_name or "",
            )
        if self.entity_type == "host":
            return ("host", self.cluster_id, self.node_name or "")
        return (
            "edge", self.cluster_id, self.namespace or "",
            self.src_service or "", self.dst_service or "",
            self.dst_namespace or self.namespace or "", self.protocol or "",
        )

    @property
    def bucket_key(self) -> tuple[float | None, bool]:
        return self.histogram_upper_bound, self.histogram_is_inf_bucket

    @property
    def sortable_bucket_key(self) -> float:
        if self.histogram_is_inf_bucket:
            return math.inf
        if self.histogram_upper_bound is None:
            return -math.inf
        return self.histogram_upper_bound

    def to_dict(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True)
class RawCollectionWindow:
    """All raw normal-metric samples for one exact half-open window."""

    schema_version: str
    sequence: int
    window_start_ns: int
    window_end_ns: int
    cluster_id: str
    samples: tuple[RawMetricSample, ...]
    raw_window_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        cluster_id: str,
        samples: Iterable[RawMetricSample],
    ) -> "RawCollectionWindow":
        ordered = tuple(sorted(
            samples,
            key=lambda item: (
                item.timestamp_ns, item.entity_key, item.component,
                item.series_id, item.sortable_bucket_key,
                item.source_record_id,
            ),
        ))
        if any(not isinstance(item, RawMetricSample) for item in ordered):
            raise RawCollectionError(
                "raw window samples must be RawMetricSample records"
            )
        for item in ordered:
            item.validate()
        return cls._create_from_validated_samples(
            sequence=sequence,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            cluster_id=cluster_id,
            samples=ordered,
        )

    @classmethod
    def _create_from_validated_samples(
        cls,
        *,
        sequence: int,
        window_start_ns: int,
        window_end_ns: int,
        cluster_id: str,
        samples: Iterable[RawMetricSample],
    ) -> "RawCollectionWindow":
        """Build a window from source-created samples already validated once."""
        ordered = tuple(sorted(
            samples,
            key=lambda item: (
                item.timestamp_ns, item.entity_key, item.component,
                item.series_id, item.sortable_bucket_key,
                item.source_record_id,
            ),
        ))
        if any(not isinstance(item, RawMetricSample) for item in ordered):
            raise RawCollectionError(
                "raw window samples must be RawMetricSample records"
            )
        payload = {
            "schema_version": RAW_WINDOW_SCHEMA_VERSION,
            "sequence": sequence,
            "window_start_ns": window_start_ns,
            "window_end_ns": window_end_ns,
            "cluster_id": cluster_id,
            "samples": [item.to_dict() for item in ordered],
        }
        result = cls(
            schema_version=RAW_WINDOW_SCHEMA_VERSION,
            sequence=sequence,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
            cluster_id=cluster_id,
            samples=ordered,
            raw_window_fingerprint=fingerprint(payload),
        )
        result._validate_structure()
        assert_label_safe({
            "schema_version": result.schema_version,
            "sequence": result.sequence,
            "window_start_ns": result.window_start_ns,
            "window_end_ns": result.window_end_ns,
            "cluster_id": result.cluster_id,
        })
        return result

    @classmethod
    def _from_payload(cls, payload: dict[str, Any]) -> "RawCollectionWindow":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, dict) or set(payload) != expected:
            raise RawCollectionError("raw window fields mismatch")
        result = cls(
            schema_version=payload["schema_version"],
            sequence=payload["sequence"],
            window_start_ns=payload["window_start_ns"],
            window_end_ns=payload["window_end_ns"],
            cluster_id=payload["cluster_id"],
            samples=tuple(
                RawMetricSample.from_dict(item) for item in payload["samples"]
            ),
            raw_window_fingerprint=payload["raw_window_fingerprint"],
        )
        result.validate()
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawCollectionWindow":
        return cls._from_payload(dict(payload))

    def _validate_structure(self) -> None:
        if self.schema_version != RAW_WINDOW_SCHEMA_VERSION:
            raise RawCollectionError("unsupported raw window schema")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) \
                or self.sequence <= 0:
            raise RawCollectionError("raw window sequence must be positive")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (self.window_start_ns, self.window_end_ns)
        ) or self.window_start_ns >= self.window_end_ns:
            raise RawCollectionError("raw window interval is invalid")
        _nonempty("cluster_id", self.cluster_id)
        if not self.samples:
            raise RawCollectionError("raw window must contain samples")
        if any(item.cluster_id != self.cluster_id for item in self.samples):
            raise RawCollectionError("raw window crosses cluster identity")
        if any(
            item.timestamp_ns not in {
                self.window_start_ns, self.window_end_ns,
            }
            and not self.window_start_ns <= item.timestamp_ns < self.window_end_ns
            for item in self.samples
        ):
            raise RawCollectionError("raw sample is outside its window")
        source_ids = tuple(item.source_record_id for item in self.samples)
        if len(source_ids) != len(set(source_ids)):
            raise RawCollectionError("raw window contains duplicate source records")
    def validate(self) -> None:
        self._validate_structure()
        payload = self.to_dict()
        supplied = payload.pop("raw_window_fingerprint")
        if supplied != fingerprint(payload):
            raise RawCollectionError("raw window fingerprint mismatch")
        assert_label_safe({
            key: payload[key]
            for key in (
                "schema_version", "sequence", "window_start_ns",
                "window_end_ns", "cluster_id",
            )
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "window_start_ns": self.window_start_ns,
            "window_end_ns": self.window_end_ns,
            "cluster_id": self.cluster_id,
            "samples": [item.to_dict() for item in self.samples],
            "raw_window_fingerprint": self.raw_window_fingerprint,
        }

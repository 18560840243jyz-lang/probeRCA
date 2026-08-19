"""Real source adapters for final data-plane raw primitives.

Prometheus queries are restricted to raw cumulative counters, raw cumulative
histogram buckets, and gauges.  Rate functions, cross-series reductions, and
server-side quantiles are rejected because they would make the frozen
aggregation semantics unverifiable.
"""

from __future__ import annotations

import json
import re
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib.parse import parse_qs, urlparse

import requests
import yaml

from .contracts import fingerprint
from .final_aggregation import COMPONENTS
from .raw import RawCollectionError, RawMetricSample


SOURCE_CONFIG_SCHEMA_VERSION = "probeRCA-final-source-config-v1"
_FORBIDDEN_PROMQL = re.compile(
    r"(?i)\b(?:rate|irate|increase|delta|idelta|histogram_quantile|"
    r"sum|avg|average|quantile|topk|bottomk)\s*\("
)


def _strict_mapping(payload: Any, fields: set[str], name: str) -> dict:
    if not isinstance(payload, dict):
        raise RawCollectionError(f"{name} must be a mapping")
    unknown = sorted(set(payload) - fields)
    missing = sorted(fields - set(payload))
    if unknown or missing:
        raise RawCollectionError(
            f"{name} fields mismatch; unknown={unknown}, missing={missing}"
        )
    return dict(payload)


def _nonempty(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RawCollectionError(f"{name} must be a non-empty string")
    return value


@dataclass(frozen=True)
class PrometheusPrimitiveQuery:
    query_id: str
    component: str
    promql: str
    label_mapping: dict[str, str]
    required_labels: tuple[str, ...]
    optional_labels: tuple[str, ...]
    series_labels: tuple[str, ...]
    histogram_le_label: str | None
    value_scale: float
    histogram_bound_scale: float

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PrometheusPrimitiveQuery":
        values = _strict_mapping(
            payload, set(cls.__dataclass_fields__), "Prometheus primitive query"
        )
        for name in ("required_labels", "optional_labels", "series_labels"):
            if not isinstance(values[name], list):
                raise RawCollectionError(f"{name} must be a list")
            values[name] = tuple(values[name])
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        _nonempty("query_id", self.query_id)
        _nonempty("component", self.component)
        _nonempty("promql", self.promql)
        spec = COMPONENTS.get(self.component)
        if spec is None:
            raise RawCollectionError(
                f"query references unknown component {self.component!r}"
            )
        if _FORBIDDEN_PROMQL.search(self.promql):
            raise RawCollectionError(
                f"query {self.query_id} performs forbidden pre-aggregation"
            )
        if not isinstance(self.label_mapping, dict) or any(
            not isinstance(key, str) or not key
            or not isinstance(value, str) or not value
            for key, value in self.label_mapping.items()
        ):
            raise RawCollectionError("label_mapping must contain exact strings")
        for name in ("required_labels", "optional_labels", "series_labels"):
            values = getattr(self, name)
            if (
                (name != "optional_labels" and not values)
                or len(values) != len(set(values))
                or any(
                not isinstance(value, str) or not value for value in values
                )
            ):
                raise RawCollectionError(f"{name} must be non-empty and unique")
        if set(self.required_labels) & set(self.optional_labels):
            raise RawCollectionError("required and optional labels overlap")
        mapped = set(self.label_mapping.values())
        if not set(self.required_labels) <= mapped:
            raise RawCollectionError("required label lacks semantic mapping")
        if not set(self.series_labels) <= set(self.label_mapping):
            raise RawCollectionError("series label lacks semantic mapping")
        if spec.metric_kind == "histogram_bucket":
            if not self.histogram_le_label \
                    or self.label_mapping.get("histogram_upper_bound") \
                    != self.histogram_le_label:
                raise RawCollectionError(
                    "histogram query requires a mapped boundary label"
                )
        elif self.histogram_le_label is not None:
            raise RawCollectionError(
                "non-histogram query declares histogram_le_label"
            )
        for name in ("value_scale", "histogram_bound_scale"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) \
                    or float(value) <= 0:
                raise RawCollectionError(f"{name} must be positive")

    @property
    def query_fingerprint(self) -> str:
        self.validate()
        return fingerprint(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        for name in ("required_labels", "optional_labels", "series_labels"):
            result[name] = list(result[name])
        return result


@dataclass(frozen=True)
class PrometheusSourceConfig:
    base_url: str
    timeout_sec: float
    maximum_sample_age_sec: float
    reject_warnings: bool
    queries: tuple[PrometheusPrimitiveQuery, ...]
    range_query_chunk_windows: int = 120
    range_query_max_workers: int = 1
    final_target_wait_timeout_sec: float = 15.0
    sentinel_poll_interval_sec: float = 0.1

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PrometheusSourceConfig":
        normalized = dict(payload)
        normalized.setdefault("range_query_max_workers", 1)
        normalized.setdefault("final_target_wait_timeout_sec", 15.0)
        normalized.setdefault("sentinel_poll_interval_sec", 0.1)
        values = _strict_mapping(
            normalized, set(cls.__dataclass_fields__),
            "Prometheus source config",
        )
        if not isinstance(values["queries"], list):
            raise RawCollectionError("Prometheus queries must be a list")
        values["queries"] = tuple(
            PrometheusPrimitiveQuery.from_dict(item)
            for item in values["queries"]
        )
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        url = _nonempty("Prometheus base_url", self.base_url)
        if not url.startswith(("http://", "https://")):
            raise RawCollectionError("Prometheus base_url must be HTTP(S)")
        parsed = urlparse(url)
        forbidden_query_keys = {
            "token", "access_token", "authorization", "auth",
        }
        if parsed.username or parsed.password \
                or forbidden_query_keys & set(parse_qs(parsed.query)):
            raise RawCollectionError(
                "Prometheus base_url must not contain credentials"
            )
        if isinstance(self.timeout_sec, bool) \
                or not isinstance(self.timeout_sec, (int, float)) \
                or float(self.timeout_sec) <= 0:
            raise RawCollectionError("Prometheus timeout_sec must be positive")
        if isinstance(self.maximum_sample_age_sec, bool) \
                or not isinstance(
                    self.maximum_sample_age_sec, (int, float)
                ) \
                or not 0 < float(self.maximum_sample_age_sec) <= 5:
            raise RawCollectionError(
                "Prometheus maximum_sample_age_sec must be in (0, 5]"
            )
        if type(self.reject_warnings) is not bool:
            raise RawCollectionError("reject_warnings must be boolean")
        if isinstance(self.range_query_chunk_windows, bool) \
                or not isinstance(self.range_query_chunk_windows, int) \
                or self.range_query_chunk_windows <= 0:
            raise RawCollectionError(
                "range_query_chunk_windows must be a positive integer"
            )
        if isinstance(self.range_query_max_workers, bool) \
                or not isinstance(self.range_query_max_workers, int) \
                or not 1 <= self.range_query_max_workers <= 30:
            raise RawCollectionError(
                "range_query_max_workers must be in [1, 30]"
            )
        if isinstance(self.final_target_wait_timeout_sec, bool) \
                or not isinstance(
                    self.final_target_wait_timeout_sec, (int, float)
                ) \
                or not 1 <= float(self.final_target_wait_timeout_sec) <= 120:
            raise RawCollectionError(
                "final_target_wait_timeout_sec must be in [1, 120]"
            )
        if isinstance(self.sentinel_poll_interval_sec, bool) \
                or not isinstance(
                    self.sentinel_poll_interval_sec, (int, float)
                ) \
                or not 0.05 <= float(self.sentinel_poll_interval_sec) <= 1:
            raise RawCollectionError(
                "sentinel_poll_interval_sec must be in [0.05, 1]"
            )
        if not self.queries:
            raise RawCollectionError("Prometheus source requires queries")
        query_ids = [item.query_id for item in self.queries]
        components = [item.component for item in self.queries]
        if len(query_ids) != len(set(query_ids)):
            raise RawCollectionError("Prometheus query IDs are not unique")
        if len(components) != len(set(components)):
            raise RawCollectionError(
                "each raw component must have exactly one query"
            )

    @property
    def config_fingerprint(self) -> str:
        self.validate()
        return fingerprint({
            "base_url": self.base_url,
            "timeout_sec": self.timeout_sec,
            "maximum_sample_age_sec": self.maximum_sample_age_sec,
            "reject_warnings": self.reject_warnings,
            "range_query_chunk_windows": self.range_query_chunk_windows,
            "range_query_max_workers": self.range_query_max_workers,
            "final_target_wait_timeout_sec": (
                self.final_target_wait_timeout_sec
            ),
            "sentinel_poll_interval_sec": self.sentinel_poll_interval_sec,
            "queries": [item.to_dict() for item in self.queries],
        })


class RuntimeIdentityResolver(Protocol):
    """Minimum frozen-inventory interface needed by source adapters."""

    cluster_id: str
    service_uid_by_name: dict[tuple[str, str], str]
    pod_uid_by_name: dict[tuple[str, str], str]
    pod_to_services: dict[str, tuple[str, ...]]
    objects_by_kind: dict[str, dict[str, dict]]

    def resolve_service_for_pod(
        self, pod_uid: str, explicit_service: str | None = None,
    ) -> str:
        ...


class PrimitiveSource(Protocol):
    def collect(
        self,
        *,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision: RuntimeIdentityResolver,
    ) -> tuple[RawMetricSample, ...]:
        ...


class PrometheusPrimitiveSource:
    """Query raw Prometheus primitives at exact window boundaries."""

    def __init__(
        self,
        config: PrometheusSourceConfig,
        *,
        session: requests.Session | None = None,
    ):
        config.validate()
        self.config = config
        self.session = session or requests.Session()
        self._query_fingerprints = {
            query.query_id: query.query_fingerprint
            for query in config.queries
        }
        self._series_ids: dict[
            tuple[str, tuple[tuple[str, str | None], ...]], str
        ] = {}
        self._source_object_ids: dict[
            tuple[str, tuple[tuple[str, str], ...]], str
        ] = {}
        self.last_range_query_stats: dict[str, Any] = {}
        self.last_sentinel_wait_stats: dict[str, Any] = {}

    def wait_for_target_timestamp(
        self, *, target_timestamp_ns: int, cluster_id: str,
    ) -> None:
        """Wait until Prometheus has stored the exact final target sentinel."""

        if isinstance(target_timestamp_ns, bool) \
                or not isinstance(target_timestamp_ns, int) \
                or target_timestamp_ns <= 0 \
                or target_timestamp_ns % 1_000_000_000:
            raise RawCollectionError(
                "final primitive sentinel target must be an epoch second"
            )
        if not isinstance(cluster_id, str) or not cluster_id:
            raise RawCollectionError("sentinel cluster identity is required")
        target_sec = f"{target_timestamp_ns / 1_000_000_000:.9f}"
        promql = (
            "proberca_final_primitive_exporter_ready{cluster_id="
            + json.dumps(cluster_id)
            + "}"
        )
        started = time.monotonic()
        deadline = started + float(
            self.config.final_target_wait_timeout_sec
        )
        attempts = 0
        while True:
            attempts += 1
            response = self.session.get(
                self.config.base_url.rstrip("/") + "/api/v1/query_range",
                params={
                    "query": promql,
                    "start": target_sec,
                    "end": target_sec,
                    "step": "1",
                },
                timeout=float(self.config.timeout_sec),
            )
            if response.status_code >= 400:
                raise RawCollectionError(
                    "Prometheus final sentinel query failed with HTTP "
                    f"{response.status_code}"
                )
            try:
                payload = response.json()
            except Exception as error:
                raise RawCollectionError(
                    "Prometheus final sentinel response is not JSON"
                ) from error
            if payload.get("status") != "success":
                raise RawCollectionError(
                    "Prometheus final sentinel query did not succeed"
                )
            if self.config.reject_warnings and payload.get("warnings"):
                raise RawCollectionError(
                    "Prometheus final sentinel query returned warnings"
                )
            data = payload.get("data") or {}
            if data.get("resultType") != "matrix":
                raise RawCollectionError(
                    "Prometheus final sentinel result is not a matrix"
                )
            matches = []
            for series in data.get("result") or []:
                labels = series.get("metric") or {}
                if labels.get("cluster_id") != cluster_id:
                    continue
                for pair in series.get("values") or []:
                    if not isinstance(pair, list) or len(pair) != 2:
                        raise RawCollectionError(
                            "Prometheus final sentinel sample is invalid"
                        )
                    observed_ns = int(round(
                        float(pair[0]) * 1_000_000_000
                    ))
                    value = float(pair[1])
                    if observed_ns == target_timestamp_ns and value == 1.0:
                        matches.append((tuple(sorted(labels.items())), value))
            if len(matches) > 1:
                raise RawCollectionError(
                    "Prometheus final sentinel is duplicated"
                )
            if matches:
                self.last_sentinel_wait_stats = {
                    "target_timestamp_ns": target_timestamp_ns,
                    "attempts": attempts,
                    "wall_seconds": time.monotonic() - started,
                }
                return
            if time.monotonic() >= deadline:
                raise RawCollectionError(
                    "Prometheus did not store the final primitive target "
                    f"{target_timestamp_ns} before timeout"
                )
            time.sleep(float(self.config.sentinel_poll_interval_sec))

    def _range(
        self,
        query: PrometheusPrimitiveQuery,
        *,
        expected_timestamps_ns: tuple[int, ...],
    ):
        if not expected_timestamps_ns:
            raise RawCollectionError("Prometheus range request is empty")
        maximum_age = float(self.config.maximum_sample_age_sec)
        fresh_promql = (
            f"({query.promql}) and "
            f"(timestamp({query.promql}) == time()) and "
            f"((time() - timestamp({query.promql})) <= {maximum_age:.9f})"
        )
        started = time.perf_counter()
        response = self.session.get(
            self.config.base_url.rstrip("/") + "/api/v1/query_range",
            params={
                "query": fresh_promql,
                "start": (
                    f"{expected_timestamps_ns[0] / 1_000_000_000:.9f}"
                ),
                "end": (
                    f"{expected_timestamps_ns[-1] / 1_000_000_000:.9f}"
                ),
                "step": "1",
            },
            timeout=float(self.config.timeout_sec),
        )
        elapsed = time.perf_counter() - started
        response_bytes = len(getattr(response, "content", b"") or b"")
        if response.status_code >= 400:
            raise RawCollectionError(
                f"Prometheus query {query.query_id} failed "
                f"with HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except Exception as error:
            raise RawCollectionError("Prometheus response is not JSON") from error
        if payload.get("status") != "success":
            raise RawCollectionError(
                f"Prometheus query {query.query_id} did not succeed"
            )
        if self.config.reject_warnings and payload.get("warnings"):
            raise RawCollectionError(
                f"Prometheus query {query.query_id} returned warnings"
            )
        data = payload.get("data") or {}
        if data.get("resultType") != "matrix":
            raise RawCollectionError(
                f"Prometheus query {query.query_id} returned unsupported result type"
            )
        output = []
        expected = set(expected_timestamps_ns)
        seen: set[tuple[tuple[tuple[str, str], ...], int]] = set()
        returned_timestamps: set[int] = set()
        for series in data.get("result") or []:
            labels = series.get("metric") or {}
            if not isinstance(labels, dict):
                raise RawCollectionError("Prometheus labels are invalid")
            values = series.get("values")
            if not isinstance(values, list):
                raise RawCollectionError(
                    "Prometheus range series lacks values"
                )
            for pair in values:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise RawCollectionError("Prometheus sample is invalid")
                try:
                    observed_ns = int(round(float(pair[0]) * 1_000_000_000))
                    value = float(pair[1])
                except (TypeError, ValueError) as error:
                    raise RawCollectionError(
                        "Prometheus sample is not numeric"
                    ) from error
                if observed_ns not in expected:
                    raise RawCollectionError(
                        f"query {query.query_id} returned a chunk-external "
                        f"timestamp {observed_ns}"
                    )
                if not math.isfinite(value):
                    raise RawCollectionError(
                        f"query {query.query_id} returned a stale/non-finite sample"
                    )
                identity = tuple(sorted(labels.items())), observed_ns
                if identity in seen:
                    raise RawCollectionError(
                        f"query {query.query_id} returned a duplicate sample"
                    )
                seen.add(identity)
                returned_timestamps.add(observed_ns)
                output.append((labels, observed_ns, value))
        missing = sorted(expected - returned_timestamps)
        if missing:
            raise RawCollectionError(
                f"query {query.query_id} omitted evaluation timestamps "
                f"{missing}"
            )
        return tuple(output), elapsed, response_bytes

    @staticmethod
    def _semantic_labels(
        query: PrometheusPrimitiveQuery, labels: dict[str, str],
    ) -> dict[str, str | None]:
        missing = sorted(set(query.required_labels) - set(labels))
        if missing:
            raise RawCollectionError(
                f"query {query.query_id} lacks labels {missing}"
            )
        allowed = (
            set(query.required_labels)
            | set(query.optional_labels)
            | {"__name__"}
        )
        unknown = sorted(set(labels) - allowed)
        if unknown:
            raise RawCollectionError(
                f"query {query.query_id} returned unknown labels {unknown}"
            )
        return {
            semantic: labels.get(label)
            for semantic, label in query.label_mapping.items()
        }

    @staticmethod
    def _pod_identity(
        semantic: dict[str, str | None],
        revision: RuntimeIdentityResolver,
    ) -> tuple[str, str, str, str | None, str | None]:
        namespace = semantic.get("namespace")
        if not namespace:
            raise RawCollectionError("service sample lacks namespace")
        pod_uid = semantic.get("pod_uid")
        if not pod_uid:
            pod_name = semantic.get("pod")
            pod_uid = revision.pod_uid_by_name.get((namespace, pod_name or ""))
        if not pod_uid:
            raise RawCollectionError("service sample Pod cannot be resolved")
        explicit_service = semantic.get("service")
        try:
            service_id = revision.resolve_service_for_pod(
                pod_uid, explicit_service=explicit_service
            )
        except ValueError as error:
            if explicit_service is not None:
                raise RawCollectionError(str(error)) from error
            pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
            labels = (pod.get("metadata") or {}).get("labels") or {}
            workload_names = {
                value for key in (
                    "app.kubernetes.io/name", "app", "k8s-app",
                ) if (value := labels.get(key))
            }
            candidates = revision.pod_to_services.get(pod_uid, ())
            matches = [
                candidate for candidate in candidates
                if candidate.split("::")[2] in workload_names
            ]
            if len(matches) != 1:
                raise RawCollectionError(str(error)) from error
            service_id = matches[0]
        service = service_id.split("::")[2]
        pod = revision.objects_by_kind.get("Pod", {}).get(pod_uid) or {}
        node = (pod.get("spec") or {}).get("nodeName")
        return namespace, service, pod_uid, semantic.get("container_id"), node

    def _series_id(
        self,
        query: PrometheusPrimitiveQuery,
        semantic: dict[str, str | None],
    ) -> str:
        identity = tuple(
            (label, semantic.get(label))
            for label in query.series_labels
        )
        if any(value is None or value == "" for _label, value in identity):
            raise RawCollectionError(
                f"query {query.query_id} has incomplete series identity"
            )
        key = query.query_id, identity
        result = self._series_ids.get(key)
        if result is None:
            result = "series-" + fingerprint({
                "identity": dict(identity),
            })
            self._series_ids[key] = result
        return result

    def _sample(
        self,
        query: PrometheusPrimitiveQuery,
        labels: dict[str, str],
        timestamp_ns: int,
        value: float,
        revision: RuntimeIdentityResolver,
    ) -> RawMetricSample:
        semantic = self._semantic_labels(query, labels)
        component = COMPONENTS[query.component]
        common: dict[str, Any] = {
            "timestamp_ns": timestamp_ns,
            "cluster_id": revision.cluster_id,
            "entity_type": component.entity_type,
            "component": query.component,
            "metric_family": component.metric_family,
            "metric_kind": component.metric_kind,
            "unit": component.unit,
            "scope": component.scope,
            "series_id": self._series_id(query, semantic),
            "value": value * float(query.value_scale),
        }
        raw_coverage = semantic.get("source_coverage")
        if raw_coverage is not None:
            try:
                coverage = float(raw_coverage)
            except (TypeError, ValueError) as error:
                raise RawCollectionError(
                    "raw source coverage is not numeric"
                ) from error
            if coverage not in {0.0, 1.0}:
                raise RawCollectionError(
                    "raw source coverage must be zero or one"
                )
            common["coverage"] = coverage
        coverage_label = query.label_mapping.get("source_coverage")
        source_identity_labels = {
            key: item
            for key, item in labels.items()
            if key != coverage_label
        }
        source_key = (
            query.query_id,
            tuple(sorted(source_identity_labels.items())),
        )
        source_object_id = self._source_object_ids.get(source_key)
        if source_object_id is None:
            source_object_id = "object:" + fingerprint({
                "query": self._query_fingerprints[query.query_id],
                "labels": source_identity_labels,
            })
            self._source_object_ids[source_key] = source_object_id
        common["source_object_id"] = source_object_id
        if component.entity_type == "service":
            namespace, service, pod_uid, container_id, node = (
                self._pod_identity(semantic, revision)
            )
            common.update(
                namespace=namespace, service_name=service, pod_uid=pod_uid,
                container_id=container_id, node_name=node,
            )
        elif component.entity_type == "host":
            node = semantic.get("node")
            if not node:
                raise RawCollectionError("host sample lacks node label")
            known_nodes = {
                (item.get("metadata") or {}).get("name")
                for item in revision.objects_by_kind.get("Node", {}).values()
            }
            if node not in known_nodes:
                raise RawCollectionError("host sample references unknown node")
            common.update(node_name=node)
        else:
            namespace = semantic.get("namespace")
            destination_namespace = semantic.get("dst_namespace") or namespace
            source = semantic.get("src_service")
            destination = semantic.get("dst_service")
            protocol = semantic.get("protocol")
            if not all((
                namespace, destination_namespace, source, destination, protocol,
            )):
                raise RawCollectionError(
                    "edge sample lacks namespace/endpoints/protocol"
                )
            if protocol != ("dns" if query.component.startswith("dns_") else "tcp"):
                raise RawCollectionError(
                    "edge component and protocol are inconsistent"
                )
            for ns, service in (
                (namespace, source), (destination_namespace, destination),
            ):
                if (ns, service) not in revision.service_uid_by_name:
                    raise RawCollectionError(
                        "edge sample references an unknown Kubernetes Service"
                    )
            common.update(
                namespace=namespace, src_service=source,
                dst_service=destination, dst_namespace=destination_namespace,
                src_pod_uid=semantic.get("src_pod_uid"),
                dst_pod_uid=semantic.get("dst_pod_uid"),
                src_node=semantic.get("src_node"),
                dst_node=semantic.get("dst_node"),
                protocol=protocol,
            )
        if component.metric_kind == "histogram_bucket":
            raw_bound = semantic.get("histogram_upper_bound")
            if raw_bound is None:
                raise RawCollectionError("histogram sample lacks a boundary")
            histogram_consistent = (
                semantic.get("histogram_consistent") or "1"
            )
            if histogram_consistent not in {"0", "1"}:
                raise RawCollectionError(
                    "histogram consistency label must be zero or one"
                )
            is_inf = raw_bound.casefold() in {"+inf", "inf"}
            common.update(
                histogram_upper_bound=(
                    None if is_inf
                    else float(raw_bound) * float(query.histogram_bound_scale)
                ),
                histogram_is_inf_bucket=is_inf,
                histogram_consistent=histogram_consistent == "1",
            )
        return RawMetricSample.create(**common)

    def _jobs(
        self, window_start_ns: int, window_end_ns: int,
    ) -> tuple[tuple[
        PrometheusPrimitiveQuery, Any, int,
    ], ...]:
        jobs = []
        for query in self.config.queries:
            spec = COMPONENTS[query.component]
            timestamps = (
                (window_start_ns, window_end_ns)
                if spec.metric_kind in {
                    "monotonic_counter", "histogram_bucket",
                }
                else (window_end_ns,)
            )
            jobs.extend(
                (query, spec, requested_ns)
                for requested_ns in timestamps
            )
        return tuple(jobs)

    def _samples_from_responses(
        self,
        *,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision: RuntimeIdentityResolver,
        response_cache: dict[
            tuple[str, int],
            tuple[tuple[dict[str, str], int, float], ...],
        ],
        sample_cache: dict[
            tuple[
                str, int, int, tuple[tuple[str, str], ...], float,
            ],
            RawMetricSample,
        ],
    ) -> tuple[RawMetricSample, ...]:
        output = []
        query_counts = {
            query.query_id: 0 for query in self.config.queries
        }
        for query, spec, requested_ns in self._jobs(
            window_start_ns, window_end_ns,
        ):
            response = response_cache[(query.query_id, requested_ns)]
            for labels, observed_ns, value in response:
                if spec.metric_kind in {
                    "monotonic_counter", "histogram_bucket",
                } and observed_ns != requested_ns:
                    raise RawCollectionError(
                        f"query {query.query_id} did not return an exact "
                        f"boundary sample: requested={requested_ns}, "
                        f"observed={observed_ns}"
                    )
                if spec.metric_kind == "gauge" and not (
                    window_start_ns <= observed_ns <= window_end_ns
                ):
                    raise RawCollectionError(
                        f"query {query.query_id} returned a stale gauge"
                    )
                cache_key = (
                    query.query_id,
                    requested_ns,
                    observed_ns,
                    tuple(sorted(labels.items())),
                    value,
                )
                sample = sample_cache.get(cache_key)
                if sample is None:
                    sample = self._sample(
                        query, labels, observed_ns, value,
                        inventory_revision,
                    )
                    sample_cache[cache_key] = sample
                output.append(sample)
                query_counts[query.query_id] += 1
        for query in self.config.queries:
            if query_counts[query.query_id] == 0:
                raise RawCollectionError(
                    f"query {query.query_id} returned no raw samples"
                )
        source_ids = [item.source_record_id for item in output]
        if len(source_ids) != len(set(source_ids)):
            raise RawCollectionError(
                "Prometheus source returned duplicate raw samples"
            )
        return tuple(sorted(output, key=lambda item: (
            item.timestamp_ns, item.entity_key, item.component,
            item.series_id, item.sortable_bucket_key,
        )))

    def iter_collect_window_chunks(
        self,
        *,
        bounds: tuple[tuple[int, int], ...],
        inventory_revision: RuntimeIdentityResolver,
    ):
        if not bounds or any(
            isinstance(start_ns, bool)
            or isinstance(end_ns, bool)
            or not isinstance(start_ns, int)
            or not isinstance(end_ns, int)
            or start_ns >= end_ns
            for start_ns, end_ns in bounds
        ):
            raise RawCollectionError(
                "Prometheus batch bounds must be non-empty valid windows"
            )
        if any(
            end_ns - start_ns != 1_000_000_000
            or (
                index
                and start_ns != bounds[index - 1][1]
            )
            for index, (start_ns, end_ns) in enumerate(bounds)
        ):
            raise RawCollectionError(
                "Prometheus range bounds must be contiguous 1-second windows"
            )
        stats = {
            "request_count": 0,
            "response_bytes": 0,
            "chunk_wall_seconds": [],
            "request_seconds": [],
            "max_loaded_windows": 0,
        }
        self.last_range_query_stats = stats
        size = self.config.range_query_chunk_windows
        for offset in range(0, len(bounds), size):
            chunk = bounds[offset:offset + size]
            stats["max_loaded_windows"] = max(
                stats["max_loaded_windows"], len(chunk)
            )
            requests_to_run = []
            for query in self.config.queries:
                spec = COMPONENTS[query.component]
                expected = (
                    (chunk[0][0],) + tuple(end for _start, end in chunk)
                    if spec.metric_kind in {
                        "monotonic_counter", "histogram_bucket",
                    }
                    else tuple(end for _start, end in chunk)
                )
                requests_to_run.append((query, expected))
            chunk_started = time.perf_counter()
            with ThreadPoolExecutor(
                max_workers=min(
                    self.config.range_query_max_workers,
                    len(requests_to_run),
                )
            ) as executor:
                responses = tuple(executor.map(
                    lambda item: self._range(
                        item[0], expected_timestamps_ns=item[1],
                    ),
                    requests_to_run,
                ))
            stats["chunk_wall_seconds"].append(
                time.perf_counter() - chunk_started
            )
            stats["request_count"] += len(responses)
            stats["request_seconds"].extend(
                response[1] for response in responses
            )
            stats["response_bytes"] += sum(
                response[2] for response in responses
            )
            response_cache = {}
            for (query, expected), (response, _elapsed, _bytes) in zip(
                requests_to_run, responses,
            ):
                by_timestamp = {
                    timestamp_ns: [] for timestamp_ns in expected
                }
                for labels, timestamp_ns, value in response:
                    by_timestamp[timestamp_ns].append(
                        (labels, timestamp_ns, value)
                    )
                for timestamp_ns, items in by_timestamp.items():
                    response_cache[(query.query_id, timestamp_ns)] = tuple(
                        items
                    )
            sample_cache: dict[
                tuple[
                    str, int, int, tuple[tuple[str, str], ...], float,
                ],
                RawMetricSample,
            ] = {}
            yield tuple(
                self._samples_from_responses(
                    window_start_ns=start_ns,
                    window_end_ns=end_ns,
                    inventory_revision=inventory_revision,
                    response_cache=response_cache,
                    sample_cache=sample_cache,
                )
                for start_ns, end_ns in chunk
            )

    def collect_windows(
        self,
        *,
        bounds: tuple[tuple[int, int], ...],
        inventory_revision: RuntimeIdentityResolver,
    ) -> tuple[tuple[RawMetricSample, ...], ...]:
        return tuple(
            window
            for chunk in self.iter_collect_window_chunks(
                bounds=bounds,
                inventory_revision=inventory_revision,
            )
            for window in chunk
        )

    def collect(
        self,
        *,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision: RuntimeIdentityResolver,
    ) -> tuple[RawMetricSample, ...]:
        return self.collect_windows(
            bounds=((window_start_ns, window_end_ns),),
            inventory_revision=inventory_revision,
        )[0]


class CompositePrimitiveSource:
    """Combine independent normal-metric exporters without merging semantics."""

    def __init__(self, sources: Iterable[PrimitiveSource]):
        self.sources = tuple(sources)
        if not self.sources:
            raise RawCollectionError("at least one primitive source is required")

    def collect(self, **kwargs) -> tuple[RawMetricSample, ...]:
        output = tuple(
            sample
            for source in self.sources
            for sample in source.collect(**kwargs)
        )
        source_ids = [item.source_record_id for item in output]
        if len(source_ids) != len(set(source_ids)):
            raise RawCollectionError(
                "primitive sources returned overlapping source records"
            )
        components_and_series = [
            (
                item.timestamp_ns, item.entity_key, item.component,
                item.series_id, item.bucket_key,
            )
            for item in output
        ]
        if len(components_and_series) != len(set(components_and_series)):
            raise RawCollectionError(
                "multiple primitive sources own the same raw series"
            )
        return tuple(sorted(output, key=lambda item: (
            item.timestamp_ns, item.entity_key, item.component,
            item.series_id, item.sortable_bucket_key,
        )))


def load_prometheus_source_config(path: str | Path) -> PrometheusSourceConfig:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return PrometheusSourceConfig.from_dict(payload)

"""Real source adapters for final data-plane raw primitives.

Prometheus queries are restricted to raw cumulative counters, raw cumulative
histogram buckets, and gauges.  Rate functions, cross-series reductions, and
server-side quantiles are rejected because they would make the frozen
aggregation semantics unverifiable.
"""

from __future__ import annotations

import re
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
    reject_warnings: bool
    queries: tuple[PrometheusPrimitiveQuery, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PrometheusSourceConfig":
        values = _strict_mapping(
            payload, set(cls.__dataclass_fields__), "Prometheus source config"
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
        if type(self.reject_warnings) is not bool:
            raise RawCollectionError("reject_warnings must be boolean")
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
            "reject_warnings": self.reject_warnings,
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

    def _instant(self, query: PrometheusPrimitiveQuery, timestamp_ns: int):
        response = self.session.get(
            self.config.base_url.rstrip("/") + "/api/v1/query",
            params={
                "query": query.promql,
                "time": f"{timestamp_ns / 1_000_000_000:.9f}",
            },
            timeout=float(self.config.timeout_sec),
        )
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
        if data.get("resultType") not in {"vector", "matrix"}:
            raise RawCollectionError(
                f"Prometheus query {query.query_id} returned unsupported result type"
            )
        output = []
        for series in data.get("result") or []:
            labels = series.get("metric") or {}
            if not isinstance(labels, dict):
                raise RawCollectionError("Prometheus labels are invalid")
            values = series.get("values")
            if values is None:
                values = [series.get("value")]
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
                output.append((labels, observed_ns, value))
        return tuple(output)

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

    @staticmethod
    def _series_id(
        query: PrometheusPrimitiveQuery,
        semantic: dict[str, str | None],
    ) -> str:
        identity = {
            label: semantic.get(label) for label in query.series_labels
        }
        if any(value is None or value == "" for value in identity.values()):
            raise RawCollectionError(
                f"query {query.query_id} has incomplete series identity"
            )
        return "series-" + fingerprint({
            "identity": identity,
        })

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
            "source_object_id": "object:" + fingerprint({
                "query": query.query_fingerprint,
                "labels": labels,
            }),
        }
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
            is_inf = raw_bound.casefold() in {"+inf", "inf"}
            common.update(
                histogram_upper_bound=(
                    None if is_inf
                    else float(raw_bound) * float(query.histogram_bound_scale)
                ),
                histogram_is_inf_bucket=is_inf,
            )
        return RawMetricSample.create(**common)

    def collect(
        self,
        *,
        window_start_ns: int,
        window_end_ns: int,
        inventory_revision: RuntimeIdentityResolver,
    ) -> tuple[RawMetricSample, ...]:
        output = []
        for query in self.config.queries:
            spec = COMPONENTS[query.component]
            timestamps = (
                (window_start_ns, window_end_ns)
                if spec.metric_kind in {
                    "monotonic_counter", "histogram_bucket",
                }
                else (window_end_ns,)
            )
            query_count = 0
            for requested_ns in timestamps:
                for labels, observed_ns, value in self._instant(
                    query, requested_ns
                ):
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
                    output.append(self._sample(
                        query, labels, observed_ns, value, inventory_revision
                    ))
                    query_count += 1
            if query_count == 0:
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

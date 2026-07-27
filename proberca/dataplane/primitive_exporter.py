"""Live raw-primitive exporter for the frozen final ProbeRCA data plane.

This module performs source adaptation only.  It never computes the final
9/4/3/3 metrics, alert state, propagation matrices, residuals, or RCA scores.
Every exposed series is a cumulative counter, cumulative histogram bucket, or
instant gauge consumed later by :mod:`proberca.dataplane.final_aggregation`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

import requests
import yaml
from kubernetes import client, config as kubernetes_config
from kubernetes.utils.quantity import parse_quantity

from .prometheus_text import (
    PrometheusSample,
    parse_prometheus_text,
    render_prometheus_text,
)
from .raw import RawCollectionError


FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION = (
    "probeRCA-final-primitive-exporter-v3"
)
DNS_BUCKETS_MS = (
    0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0,
    50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 5000.0,
    None,
)
_HEALTH_ROUTES = frozenset({
    "/_healthz", "/health", "/healthz", "/ready", "/readiness",
    "/grpc.health.v1.Health/Check",
})
_TIMEOUT_TEXT = re.compile(r"(?i)(timeout|timed.?out|deadline)")
_BEYLA_REQUEST_METRICS = frozenset({
    "http_client_request_duration_seconds_count",
    "http_client_request_duration_seconds_bucket",
    "rpc_client_duration_seconds_count",
    "rpc_client_duration_seconds_bucket",
    "db_client_operation_duration_seconds_count",
    "db_client_operation_duration_seconds_bucket",
    "http_server_request_duration_seconds_count",
    "http_server_request_duration_seconds_bucket",
    "rpc_server_duration_seconds_count",
    "rpc_server_duration_seconds_bucket",
})
_COREDNS_METRICS = frozenset({
    "coredns_dns_request_duration_seconds_count",
    "coredns_dns_request_duration_seconds_bucket",
    "coredns_dns_responses_total",
})


def _select_metric_lines(
    text: str, metric_names: frozenset[str],
) -> str:
    return "\n".join(
        line for line in text.splitlines()
        if line
        and not line.startswith("#")
        and line.split("{", 1)[0].split(None, 1)[0] in metric_names
    )


def _strict_mapping(
    payload: Any, fields: set[str], name: str,
) -> dict[str, Any]:
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
class FinalPrimitiveExporterConfig:
    schema_version: str
    cluster_id: str
    kubeconfig_path: str
    kubernetes_context: str
    namespaces: tuple[str, ...]
    include_services: tuple[str, ...]
    kind_node_container: str
    beyla_port: int
    node_exporter_url: str
    bpf_loader_path: str
    bpf_map_directory: str
    dns_timeout_ms: int
    listen_host: str
    listen_port: int
    snapshot_period_sec: int
    source_timeout_sec: float

    @classmethod
    def from_dict(
        cls, payload: dict[str, Any],
    ) -> "FinalPrimitiveExporterConfig":
        values = _strict_mapping(
            payload, set(cls.__dataclass_fields__),
            "final primitive exporter config",
        )
        for name in ("namespaces", "include_services"):
            if not isinstance(values[name], list):
                raise RawCollectionError(f"{name} must be a list")
            values[name] = tuple(values[name])
        result = cls(**values)
        result.validate()
        return result

    def validate(self) -> None:
        if self.schema_version != FINAL_PRIMITIVE_EXPORTER_SCHEMA_VERSION:
            raise RawCollectionError(
                "unsupported final primitive exporter schema"
            )
        for name in (
            "cluster_id", "kubeconfig_path", "kubernetes_context",
            "kind_node_container", "node_exporter_url",
            "bpf_loader_path", "bpf_map_directory", "listen_host",
        ):
            _nonempty(name, getattr(self, name))
        if not self.node_exporter_url.startswith(("http://", "https://")):
            raise RawCollectionError("node_exporter_url must be HTTP(S)")
        if (
            not self.namespaces
            or len(self.namespaces) != len(set(self.namespaces))
            or any(not isinstance(item, str) or not item for item in self.namespaces)
        ):
            raise RawCollectionError("namespaces must be non-empty and unique")
        if (
            not self.include_services
            or len(self.include_services) != len(set(self.include_services))
            or any(
                len(item.split("/")) != 2
                or any(not part for part in item.split("/"))
                for item in self.include_services
            )
        ):
            raise RawCollectionError(
                "include_services must use unique namespace/service identities"
            )
        if not {
            item.split("/", 1)[0] for item in self.include_services
        } <= set(self.namespaces):
            raise RawCollectionError(
                "included service references an unlisted namespace"
            )
        for name, minimum, maximum in (
            ("beyla_port", 1, 65535),
            ("listen_port", 1, 65535),
            ("dns_timeout_ms", 100, 60000),
            ("snapshot_period_sec", 1, 1),
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) \
                    or not minimum <= value <= maximum:
                raise RawCollectionError(f"{name} is outside its frozen range")
        if isinstance(self.source_timeout_sec, bool) \
                or not isinstance(self.source_timeout_sec, (int, float)) \
                or not 0 < float(self.source_timeout_sec) <= 30:
            raise RawCollectionError("source_timeout_sec is invalid")


@dataclass(frozen=True)
class ContainerIdentity:
    namespace: str
    service: str
    pod: str
    pod_uid: str
    container: str
    container_id: str
    node: str
    cpu_request_cores: float

    @property
    def series(self) -> str:
        return hashlib.sha256(
            self.container_id.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class Inventory:
    containers: tuple[ContainerIdentity, ...]
    service_cluster_ips: dict[str, tuple[str, str]]
    services: frozenset[tuple[str, str]]
    node_names: tuple[str, ...]
    node_internal_ips: dict[str, str]
    coredns_pods: tuple[ContainerIdentity, ...]

    @property
    def container_by_coordinates(
        self,
    ) -> dict[tuple[str, str, str], ContainerIdentity]:
        return {
            (item.namespace, item.pod, item.container): item
            for item in self.containers
        }

    @property
    def containers_by_pod(
        self,
    ) -> dict[tuple[str, str], tuple[ContainerIdentity, ...]]:
        output: dict[
            tuple[str, str], list[ContainerIdentity]
        ] = {}
        for item in self.containers:
            output.setdefault(
                (item.namespace, item.pod), []
            ).append(item)
        return {
            key: tuple(sorted(value, key=lambda item: item.container))
            for key, value in output.items()
        }


@dataclass(frozen=True)
class _RequestRow:
    protocol: str
    count: PrometheusSample
    buckets: tuple[PrometheusSample, ...]
    namespace: str
    service: str
    pod: str
    container: str
    destination_namespace: str | None = None
    destination_service: str | None = None


def _labels_without(
    sample: PrometheusSample, *names: str,
) -> tuple[tuple[str, str], ...]:
    excluded = set(names)
    return tuple(
        (key, value) for key, value in sample.labels if key not in excluded
    )


def _series_hash(
    source: str, labels: Iterable[tuple[str, str]],
) -> str:
    payload = json.dumps(
        {"source": source, "labels": list(labels)},
        ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sample_index(
    samples: Iterable[PrometheusSample],
) -> dict[str, tuple[PrometheusSample, ...]]:
    output: dict[str, list[PrometheusSample]] = {}
    for sample in samples:
        output.setdefault(sample.name, []).append(sample)
    return {
        name: tuple(values) for name, values in output.items()
    }


def _histogram_index(
    sample_index: dict[str, tuple[PrometheusSample, ...]],
    bucket_names: Iterable[str],
) -> dict[
    tuple[str, tuple[tuple[str, str], ...]],
    tuple[PrometheusSample, ...],
]:
    output: dict[
        tuple[str, tuple[tuple[str, str], ...]],
        list[PrometheusSample],
    ] = {}
    for bucket_name in bucket_names:
        for sample in sample_index.get(bucket_name, ()):
            output.setdefault(
                (bucket_name, _labels_without(sample, "le")), []
            ).append(sample)
    return {
        key: tuple(values) for key, values in output.items()
    }


def _request_classification(
    protocol: str, labels: dict[str, str],
) -> tuple[bool, bool]:
    if protocol == "http":
        status_text = labels.get("http_response_status_code", "")
        error_type = labels.get("error_type", "")
        try:
            status = int(status_text) if status_text else 0
        except ValueError:
            status = 0
        timeout = status in {408, 504} or bool(_TIMEOUT_TEXT.search(error_type))
        error = (status >= 500 or bool(error_type)) and not timeout
        return error, timeout
    if protocol == "rpc":
        status_text = labels.get("rpc_grpc_status_code", "")
        error_type = labels.get("error_type", "")
        try:
            status = int(status_text) if status_text else 0
        except ValueError:
            status = 0
        timeout = status == 4 or bool(_TIMEOUT_TEXT.search(error_type))
        error = (status != 0 or bool(error_type)) and not timeout
        return error, timeout
    if protocol == "redis":
        error_type = labels.get("error_type", "")
        timeout = bool(_TIMEOUT_TEXT.search(error_type))
        return bool(error_type) and not timeout, timeout
    raise RawCollectionError(f"unsupported request protocol {protocol}")


def _business_route(protocol: str, labels: dict[str, str]) -> bool:
    if protocol == "http":
        return labels.get("http_route", "") not in _HEALTH_ROUTES
    if protocol == "rpc":
        return labels.get("rpc_method", "") not in _HEALTH_ROUTES
    if protocol == "redis":
        return labels.get("db_operation_name", "").upper() not in {
            "INFO", "PING",
        }
    return False


def _bucket_rows(
    histogram_index: dict[
        tuple[str, tuple[tuple[str, str], ...]],
        tuple[PrometheusSample, ...],
    ],
    count: PrometheusSample,
    bucket_name: str,
) -> tuple[PrometheusSample, ...]:
    output = histogram_index.get((bucket_name, count.labels), ())
    bounds = [item.label_dict.get("le") for item in output]
    if not output or len(bounds) != len(set(bounds)) or "+Inf" not in bounds:
        raise RawCollectionError(
            f"{count.name} lacks one complete cumulative histogram"
        )
    return tuple(sorted(
        output,
        key=lambda item: (
            float("inf") if item.label_dict["le"] == "+Inf"
            else float(item.label_dict["le"])
        ),
    ))


def _resolve_destination(
    address: str,
    *,
    caller_namespace: str,
    inventory: Inventory,
) -> tuple[str, str] | None:
    host = address.strip().rstrip(".")
    if not host:
        return None
    if host in inventory.service_cluster_ips:
        return inventory.service_cluster_ips[host]
    if ":" in host and host.count(":") == 1:
        host = host.rsplit(":", 1)[0]
    parts = host.split(".")
    service = parts[0]
    namespace = parts[1] if len(parts) >= 2 and parts[1] != "svc" \
        else caller_namespace
    candidate = (namespace, service)
    return candidate if candidate in inventory.services else None


class FinalPrimitiveExporter:
    """Collect source primitives and expose an aligned immutable snapshot."""

    def __init__(
        self,
        config: FinalPrimitiveExporterConfig,
        *,
        session: requests.Session | None = None,
        wall_clock_ns=time.time_ns,
        sleep=time.sleep,
    ):
        config.validate()
        self.config = config
        self.session = session or requests.Session()
        self.wall_clock_ns = wall_clock_ns
        self.sleep = sleep
        self._lock = threading.Lock()
        self._snapshot = ""
        self._snapshot_ns = 0
        self._last_error: str | None = None
        self._stop = threading.Event()
        self._active_task_ns: dict[str, float] = {}
        self._active_thread_ns: dict[str, float] = {}
        self._futex_raw_high_water_ns: dict[str, float] = {}
        self._futex_wait_ns: dict[str, float] = {}
        self._edge_sample_high_water: dict[
            tuple[str, tuple[tuple[str, str], ...]], PrometheusSample
        ] = {}
        self._edge_sample_raw: dict[
            tuple[str, tuple[tuple[str, str], ...]], PrometheusSample
        ] = {}
        self._service_sample_high_water: dict[
            tuple[str, tuple[tuple[str, str], ...]], PrometheusSample
        ] = {}
        self._service_sample_raw: dict[
            tuple[str, tuple[tuple[str, str], ...]], PrometheusSample
        ] = {}
        self._qdisc_drop_state: dict[str, tuple[float, float]] = {}
        self._last_capacity_ns: int | None = None
        self._inventory_cache: Inventory | None = None
        self._cgroup_path_cache: tuple[
            tuple[str, ...], dict[str, Path]
        ] | None = None
        kubernetes_config.load_kube_config(
            config_file=self.config.kubeconfig_path,
            context=self.config.kubernetes_context,
        )
        self.core = client.CoreV1Api()
        self._node_cgroup_root = self._resolve_kind_node_cgroup()

    def _resolve_kind_node_cgroup(self) -> Path:
        result = subprocess.run(
            [
                "docker", "inspect", "--format", "{{.Id}}",
                self.config.kind_node_container,
            ],
            check=True, capture_output=True, text=True,
            timeout=float(self.config.source_timeout_sec),
        )
        container_id = result.stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise RawCollectionError("kind node container identity is invalid")
        path = Path(
            "/sys/fs/cgroup/system.slice"
        ) / f"docker-{container_id}.scope"
        if not path.is_dir():
            raise RawCollectionError("kind node cgroup root is unavailable")
        return path

    @staticmethod
    def _pod_services(
        pod: Any,
        services: Iterable[Any],
        included: set[tuple[str, str]],
    ) -> tuple[str, ...]:
        namespace = pod.metadata.namespace
        labels = pod.metadata.labels or {}
        candidates = []
        for service in services:
            identity = (namespace, service.metadata.name)
            selector = service.spec.selector or {}
            if identity not in included or not selector:
                continue
            if all(labels.get(key) == value for key, value in selector.items()):
                candidates.append(service.metadata.name)
        if len(candidates) <= 1:
            return tuple(candidates)
        workload_names = {
            labels.get("app.kubernetes.io/name"),
            labels.get("app"),
            labels.get("k8s-app"),
        } - {None, ""}
        matches = sorted(set(candidates) & workload_names)
        if len(matches) != 1:
            raise RawCollectionError(
                f"Pod {namespace}/{pod.metadata.name} maps ambiguously "
                f"to services {sorted(candidates)}"
            )
        return tuple(matches)

    def _inventory(self) -> Inventory:
        included = {
            tuple(item.split("/", 1))
            for item in self.config.include_services
        }
        pods = []
        services = []
        for namespace in self.config.namespaces:
            pods.extend(self.core.list_namespaced_pod(namespace).items)
            services.extend(
                self.core.list_namespaced_service(namespace).items
            )
        known_services = {
            (item.metadata.namespace, item.metadata.name)
            for item in services
            if (item.metadata.namespace, item.metadata.name) in included
        }
        if known_services != included:
            raise RawCollectionError(
                "one or more configured Kubernetes Services are absent"
            )
        containers = []
        for pod in pods:
            matched_services = self._pod_services(
                pod, services, included
            )
            if not matched_services:
                continue
            if pod.status.phase != "Running" or pod.metadata.deletion_timestamp:
                raise RawCollectionError("monitored Pod is not stably running")
            statuses = {
                item.name: item
                for item in (pod.status.container_statuses or [])
            }
            for container in pod.spec.containers:
                status = statuses.get(container.name)
                if (
                    status is None
                    or not status.ready
                    or not status.started
                    or not status.container_id
                ):
                    raise RawCollectionError(
                        "monitored container runtime identity is incomplete"
                    )
                runtime_id = status.container_id.split("://", 1)[-1]
                if not re.fullmatch(r"[0-9a-f]{64}", runtime_id):
                    raise RawCollectionError(
                        "unsupported container runtime identity"
                    )
                requests = container.resources.requests or {}
                cpu_request = requests.get("cpu")
                if cpu_request is None:
                    raise RawCollectionError(
                        "monitored container lacks a CPU request"
                    )
                try:
                    cpu_request_cores = float(parse_quantity(cpu_request))
                except (TypeError, ValueError) as error:
                    raise RawCollectionError(
                        "monitored container has an invalid CPU request"
                    ) from error
                if cpu_request_cores <= 0:
                    raise RawCollectionError(
                        "monitored container CPU request is not positive"
                    )
                for service in matched_services:
                    containers.append(ContainerIdentity(
                        pod.metadata.namespace,
                        service,
                        pod.metadata.name,
                        pod.metadata.uid,
                        container.name,
                        runtime_id,
                        pod.spec.node_name,
                        cpu_request_cores,
                    ))
        covered = {
            (item.namespace, item.service) for item in containers
        }
        if covered != included:
            raise RawCollectionError(
                "configured services lack ready container identities"
            )
        node_items = self.core.list_node().items
        node_names = tuple(sorted(
            item.metadata.name for item in node_items
        ))
        internal_ips = {}
        for item in node_items:
            values = [
                address.address for address in item.status.addresses or []
                if address.type == "InternalIP"
            ]
            if len(values) != 1:
                raise RawCollectionError(
                    "Kubernetes node lacks one InternalIP"
                )
            internal_ips[item.metadata.name] = values[0]
        cluster_ips = {
            item.spec.cluster_ip: (
                item.metadata.namespace, item.metadata.name,
            )
            for item in services
            if (item.metadata.namespace, item.metadata.name) in included
            and item.spec.cluster_ip
            and item.spec.cluster_ip != "None"
        }
        coredns = tuple(
            item for item in containers
            if (item.namespace, item.service) == ("kube-system", "kube-dns")
        )
        return Inventory(
            tuple(sorted(
                containers,
                key=lambda item: (
                    item.namespace, item.service, item.pod, item.container,
                ),
            )),
            cluster_ips,
            frozenset(known_services),
            node_names,
            internal_ips,
            coredns,
        )

    def _fetch_url(
        self,
        url: str,
        *,
        metric_names: frozenset[str] | None = None,
    ) -> tuple[PrometheusSample, ...]:
        response = self.session.get(
            url,
            headers={"Accept-Encoding": "gzip"},
            timeout=float(self.config.source_timeout_sec),
        )
        if response.status_code != HTTPStatus.OK:
            raise RawCollectionError(
                f"source {url} returned HTTP {response.status_code}"
            )
        text = response.text
        if metric_names is not None:
            text = _select_metric_lines(text, metric_names)
        return parse_prometheus_text(text)

    def _coredns(
        self, pod: ContainerIdentity,
    ) -> tuple[PrometheusSample, ...]:
        name = f"http:{pod.pod}:9153"
        text = self.core.connect_get_namespaced_pod_proxy_with_path(
            name, pod.namespace, "metrics"
        )
        return parse_prometheus_text(
            _select_metric_lines(text, _COREDNS_METRICS)
        )

    def _bpf_snapshot(
        self, cgroup_ids: Iterable[int],
    ) -> tuple[dict[str, Any], ...]:
        selected_ids = tuple(sorted(set(cgroup_ids)))
        if not selected_ids or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in selected_ids
        ):
            raise RawCollectionError(
                "BPF snapshot requires active positive cgroup IDs"
            )
        command = [
            self.config.bpf_loader_path,
            "--snapshot", self.config.bpf_map_directory,
            "--timeout-ms", str(self.config.dns_timeout_ms),
        ]
        for cgroup_id in selected_ids:
            command.extend(("--cgroup-id", str(cgroup_id)))
        try:
            result = subprocess.run(
                command,
                check=True, capture_output=True, text=True,
                timeout=float(self.config.source_timeout_sec),
            )
        except subprocess.CalledProcessError as error:
            detail = (error.stderr or error.stdout or "").strip()
            raise RawCollectionError(
                "BPF snapshot command failed"
                + (f": {detail}" if detail else "")
            ) from error
        records = []
        for line in result.stdout.splitlines():
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise RawCollectionError(
                    "BPF snapshot output is not JSONL"
                ) from error
            if record.get("record_type") not in {"cgroup", "dns"}:
                raise RawCollectionError(
                    "BPF snapshot contains an unknown record"
                )
            records.append(record)
        return tuple(records)

    def _beyla(
        self, inventory: Inventory,
    ) -> tuple[PrometheusSample, ...]:
        if len(inventory.node_names) != 1:
            raise RawCollectionError(
                "single-VM exporter requires exactly one Kubernetes node"
            )
        node = inventory.node_names[0]
        url = (
            f"http://{inventory.node_internal_ips[node]}:"
            f"{self.config.beyla_port}/metrics"
        )
        return self._fetch_url(
            url, metric_names=_BEYLA_REQUEST_METRICS
        )

    @staticmethod
    def _resource_labels(
        item: ContainerIdentity,
    ) -> dict[str, str]:
        return {
            "namespace": item.namespace,
            "pod": item.pod,
            "container": item.container,
            "source_series": item.series,
        }

    def _bounded_futex_counter(
        self,
        key: str,
        raw_wait_ns: float,
        thread_capacity_increment_ns: float,
    ) -> float:
        if (
            not math.isfinite(raw_wait_ns)
            or raw_wait_ns < 0
            or not math.isfinite(thread_capacity_increment_ns)
            or thread_capacity_increment_ns < 0
        ):
            raise RawCollectionError("futex counter input is invalid")
        previous_raw = self._futex_raw_high_water_ns.get(key)
        if previous_raw is None:
            self._futex_raw_high_water_ns[key] = raw_wait_ns
            self._futex_wait_ns[key] = 0.0
            return 0.0
        raw_increment = max(0.0, raw_wait_ns - previous_raw)
        self._futex_raw_high_water_ns[key] = max(
            previous_raw, raw_wait_ns
        )
        accepted_increment = min(
            raw_increment, thread_capacity_increment_ns
        )
        value = (
            self._futex_wait_ns.get(key, 0.0) + accepted_increment
        )
        self._futex_wait_ns[key] = value
        return value

    def _active_cgroup_paths(
        self, inventory: Inventory,
    ) -> dict[str, Path]:
        active_ids = tuple(sorted({
            item.container_id for item in inventory.containers
        }))
        cached = self._cgroup_path_cache
        if (
            cached is not None
            and cached[0] == active_ids
            and all(path.is_dir() for path in cached[1].values())
        ):
            return dict(cached[1])
        active = set(active_ids)
        resolved: dict[str, Path] = {}
        try:
            for path in self._node_cgroup_root.rglob(
                "cri-containerd-*.scope"
            ):
                match = re.fullmatch(
                    r"cri-containerd-([0-9a-f]{64})\.scope",
                    path.name,
                )
                if match is None or match.group(1) not in active:
                    continue
                container_id = match.group(1)
                if container_id in resolved:
                    raise RawCollectionError(
                        "container runtime identity maps to multiple cgroups"
                    )
                if not path.is_dir():
                    raise RawCollectionError(
                        "active container cgroup path is unavailable"
                    )
                resolved[container_id] = path
        except OSError as error:
            raise RawCollectionError(
                "cannot enumerate active container cgroups"
            ) from error
        if set(resolved) != active:
            missing = sorted(active - set(resolved))
            raise RawCollectionError(
                f"active container cgroups are incomplete: {missing}"
            )
        self._cgroup_path_cache = (active_ids, dict(resolved))
        return resolved

    def _resource_samples(
        self,
        inventory: Inventory,
        cgroup_paths: dict[str, Path],
        bpf: tuple[dict[str, Any], ...],
        timestamp_ns: int,
    ) -> tuple[PrometheusSample, ...]:
        cgroup_ids: dict[int, ContainerIdentity] = {}
        output = []
        for identity in inventory.containers:
            labels = self._resource_labels(identity)
            path = cgroup_paths.get(identity.container_id)
            if path is None:
                raise RawCollectionError(
                    "monitored container lacks a resolved cgroup"
                )
            try:
                cpu_stat_lines = (path / "cpu.stat").read_text(
                    encoding="utf-8"
                ).splitlines()
                cpu_max_fields = (path / "cpu.max").read_text(
                    encoding="utf-8"
                ).split()
                memory_current_text = (path / "memory.current").read_text(
                    encoding="utf-8"
                ).strip()
                memory_max_text = (path / "memory.max").read_text(
                    encoding="utf-8"
                ).strip()
                memory_stat_lines = (path / "memory.stat").read_text(
                    encoding="utf-8"
                ).splitlines()
            except OSError as error:
                raise RawCollectionError(
                    "cannot read monitored cgroup resource primitives"
                ) from error
            cpu_stat: dict[str, int] = {}
            for line in cpu_stat_lines:
                fields = line.split()
                if (
                    len(fields) != 2
                    or fields[0] in cpu_stat
                    or not fields[1].isdigit()
                ):
                    raise RawCollectionError("cgroup cpu.stat is invalid")
                cpu_stat[fields[0]] = int(fields[1])
            memory_stat: dict[str, int] = {}
            for line in memory_stat_lines:
                fields = line.split()
                if (
                    len(fields) != 2
                    or fields[0] in memory_stat
                    or not fields[1].isdigit()
                ):
                    raise RawCollectionError("cgroup memory.stat is invalid")
                memory_stat[fields[0]] = int(fields[1])
            if (
                "usage_usec" not in cpu_stat
                or "nr_throttled" not in cpu_stat
                or "nr_periods" not in cpu_stat
                or len(cpu_max_fields) != 2
                or not cpu_max_fields[1].isdigit()
                or int(cpu_max_fields[1]) <= 0
                or not memory_current_text.isdigit()
                or not memory_max_text.isdigit()
                or "inactive_file" not in memory_stat
            ):
                raise RawCollectionError(
                    "cgroup resource primitives are incomplete"
                )
            cpu_time_ns = float(cpu_stat["usage_usec"] * 1000)
            memory_current = int(memory_current_text)
            working_set = float(max(
                0, memory_current - memory_stat["inactive_file"]
            ))
            memory_limit = float(int(memory_max_text))
            if cpu_max_fields[0] == "max":
                allocated_cpu = identity.cpu_request_cores
            elif cpu_max_fields[0].isdigit():
                allocated_cpu = (
                    int(cpu_max_fields[0]) / int(cpu_max_fields[1])
                )
            else:
                raise RawCollectionError("cgroup cpu.max is invalid")
            throttled = float(cpu_stat["nr_throttled"])
            periods = float(cpu_stat["nr_periods"])
            if allocated_cpu <= 0 or memory_limit <= 0:
                raise RawCollectionError(
                    "container resource denominator is not positive"
                )
            for metric_name, metric_value in (
                ("proberca_container_cpu_time_nanoseconds_total",
                 cpu_time_ns),
                ("proberca_container_allocated_cpu_cores", allocated_cpu),
                ("proberca_container_cpu_throttled_periods_total", throttled),
                ("proberca_container_cpu_periods_total", periods),
                ("proberca_container_memory_working_set_bytes", working_set),
                ("proberca_container_memory_limit_bytes", memory_limit),
            ):
                output.append(PrometheusSample.create(
                    metric_name, labels, metric_value
                ))
            cgroup_id = path.stat().st_ino
            if cgroup_id in cgroup_ids:
                raise RawCollectionError(
                    "multiple containers share one cgroup identity"
                )
            cgroup_ids[cgroup_id] = identity
        cgroup_records = {
            int(record["cgroup_id"]): record
            for record in bpf if record["record_type"] == "cgroup"
        }
        previous_ns = self._last_capacity_ns
        elapsed_ns = (
            0 if previous_ns is None else timestamp_ns - previous_ns
        )
        if elapsed_ns < 0 or elapsed_ns > 5_000_000_000:
            raise RawCollectionError(
                "active task/thread integration interval is invalid"
            )
        for cgroup_id, identity in cgroup_ids.items():
            path = cgroup_paths[identity.container_id]
            try:
                io_pressure = (path / "io.pressure").read_text(
                    encoding="utf-8"
                )
                process_count = len(
                    (path / "cgroup.procs").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
                thread_count = len(
                    (path / "cgroup.threads").read_text(
                        encoding="utf-8"
                    ).splitlines()
                )
            except OSError as error:
                raise RawCollectionError(
                    "cannot read monitored cgroup primitives"
                ) from error
            match = re.search(r"(?m)^some .*?\btotal=([0-9]+)\b", io_pressure)
            if match is None:
                raise RawCollectionError("cgroup io.pressure is invalid")
            io_psi_ns = float(int(match.group(1)) * 1000)
            key = identity.container_id
            self._active_task_ns[key] = (
                self._active_task_ns.get(key, 0.0)
                + float(process_count * elapsed_ns)
            )
            self._active_thread_ns[key] = (
                self._active_thread_ns.get(key, 0.0)
                + float(thread_count * elapsed_ns)
            )
            record = cgroup_records.get(cgroup_id, {})
            futex_wait_ns = self._bounded_futex_counter(
                key,
                float(record.get("futex_wait_ns_total", 0)),
                float(thread_count * elapsed_ns),
            )
            labels = self._resource_labels(identity)
            for metric_name, metric_value in (
                ("proberca_cgroup_io_psi_some_nanoseconds_total", io_psi_ns),
                ("proberca_cgroup_active_task_nanoseconds_total",
                 self._active_task_ns[key]),
                ("proberca_cgroup_futex_wait_nanoseconds_total",
                 futex_wait_ns),
                ("proberca_cgroup_active_thread_nanoseconds_total",
                 self._active_thread_ns[key]),
                ("proberca_cgroup_socket_backlog_overflow_total",
                 float(record.get("socket_backlog_overflow_total", 0))),
                ("proberca_cgroup_socket_accept_fail_total",
                 float(record.get("socket_accept_fail_total", 0))),
                ("proberca_cgroup_socket_local_rst_total",
                 float(record.get("socket_local_rst_total", 0))),
                ("proberca_cgroup_socket_local_drop_total",
                 float(record.get("socket_local_drop_total", 0))),
                ("proberca_cgroup_socket_operations_total",
                 float(record.get("socket_ops_total", 0))),
            ):
                output.append(PrometheusSample.create(
                    metric_name, labels, metric_value
                ))
        self._last_capacity_ns = timestamp_ns
        return tuple(output)

    @staticmethod
    def _request_rows(
        samples: tuple[PrometheusSample, ...],
        inventory: Inventory,
        *,
        edge: bool,
    ) -> tuple[_RequestRow, ...]:
        index = _sample_index(samples)
        definitions = (
            (
                "http", "http_client_request_duration_seconds_count",
                "http_client_request_duration_seconds_bucket",
            ),
            (
                "rpc", "rpc_client_duration_seconds_count",
                "rpc_client_duration_seconds_bucket",
            ),
            (
                "redis", "db_client_operation_duration_seconds_count",
                "db_client_operation_duration_seconds_bucket",
            ),
        ) if edge else (
            (
                "http", "http_server_request_duration_seconds_count",
                "http_server_request_duration_seconds_bucket",
            ),
            (
                "rpc", "rpc_server_duration_seconds_count",
                "rpc_server_duration_seconds_bucket",
            ),
            (
                "redis", "db_client_operation_duration_seconds_count",
                "db_client_operation_duration_seconds_bucket",
            ),
        )
        histogram_index = _histogram_index(
            index, (item[2] for item in definitions)
        )
        candidates: list[_RequestRow] = []
        for protocol, count_name, bucket_name in definitions:
            for count in index.get(count_name, ()):
                labels = count.label_dict
                if not _business_route(protocol, labels):
                    continue
                namespace = (
                    labels.get("k8s_namespace_name")
                    or labels.get("service_namespace")
                )
                service = labels.get("service_name")
                pod = labels.get("k8s_pod_name")
                container = labels.get("k8s_container_name")
                if not all((namespace, service, pod, container)):
                    raise RawCollectionError(
                        "Beyla request primitive lacks Kubernetes identity"
                    )
                if (namespace, service) not in inventory.services:
                    continue
                if edge:
                    destination = _resolve_destination(
                        labels.get("server_address", ""),
                        caller_namespace=namespace,
                        inventory=inventory,
                    )
                    if destination is None or destination == (namespace, service):
                        continue
                    destination_namespace, destination_service = destination
                else:
                    destination_namespace = None
                    destination_service = None
                    if protocol == "redis" and (
                        labels.get("server_address") != service
                    ):
                        continue
                buckets = _bucket_rows(
                    histogram_index, count, bucket_name
                )
                candidates.append(_RequestRow(
                    protocol, count, buckets, namespace, service,
                    pod, container, destination_namespace,
                    destination_service,
                ))
        if edge:
            return tuple(candidates)
        rpc_by_instance: dict[tuple[str, str, str], list[_RequestRow]] = {}
        non_rpc = []
        for row in candidates:
            if row.protocol != "rpc":
                non_rpc.append(row)
                continue
            rpc_by_instance.setdefault(
                (row.namespace, row.service, row.pod), []
            ).append(row)
        selected = list(non_rpc)
        for rows in rpc_by_instance.values():
            wildcard = [
                row for row in rows
                if row.count.label_dict.get("rpc_method") == "*"
            ]
            selected.extend(wildcard if wildcard else rows)
        return tuple(selected)

    @staticmethod
    def _render_request_rows(
        rows: Iterable[_RequestRow],
        *,
        edge: bool,
    ) -> tuple[PrometheusSample, ...]:
        output = []
        for row in rows:
            labels = row.count.label_dict
            source_series = _series_hash(
                f"{row.protocol}:{'edge' if edge else 'service'}",
                row.count.labels,
            )
            error, timeout = _request_classification(
                row.protocol, labels
            )
            if edge:
                common = {
                    "namespace": row.namespace,
                    "dst_namespace": row.destination_namespace or "",
                    "src_service": row.service,
                    "dst_service": row.destination_service or "",
                    "protocol": "tcp",
                    "source_series": source_series,
                }
                names = (
                    "proberca_tcp_edge_request_total",
                    "proberca_tcp_edge_error_total",
                    "proberca_tcp_edge_timeout_total",
                    "proberca_tcp_edge_latency_milliseconds_bucket",
                )
            else:
                common = {
                    "namespace": row.namespace,
                    "pod": row.pod,
                    "container": row.container,
                    "source_series": source_series,
                }
                names = (
                    "proberca_service_request_total",
                    "proberca_service_request_error_total",
                    "proberca_service_request_timeout_total",
                    "proberca_service_request_latency_milliseconds_bucket",
                )
            output.extend((
                PrometheusSample.create(names[0], common, row.count.value),
                PrometheusSample.create(
                    names[1], common, row.count.value if error else 0.0
                ),
                PrometheusSample.create(
                    names[2], common, row.count.value if timeout else 0.0
                ),
            ))
            for bucket in row.buckets:
                bound = bucket.label_dict["le"]
                histogram_labels = dict(common)
                histogram_labels["le"] = (
                    "+Inf" if bound == "+Inf"
                    else f"{float(bound) * 1000.0:.17g}"
                )
                output.append(PrometheusSample.create(
                    names[3], histogram_labels, bucket.value
                ))
        return tuple(output)

    def _persistent_edge_samples(
        self, samples: tuple[PrometheusSample, ...],
    ) -> tuple[PrometheusSample, ...]:
        raw_samples = getattr(self, "_edge_sample_raw", None)
        if raw_samples is None:
            raw_samples = {}
            self._edge_sample_raw = raw_samples
        present = {sample.identity for sample in samples}
        for sample in samples:
            if not sample.name.startswith("proberca_tcp_edge_"):
                raise RawCollectionError(
                    "edge persistence received a non-edge metric"
                )
            previous = self._edge_sample_high_water.get(sample.identity)
            previous_raw = raw_samples.get(sample.identity)
            if previous is None or previous_raw is None:
                value = sample.value
            elif sample.value >= previous_raw.value:
                value = (
                    previous.value
                    + sample.value
                    - previous_raw.value
                )
            else:
                # Beyla retires inactive series after its frozen TTL.  When
                # the same semantic series reappears, its Prometheus counter
                # starts again from zero.  Preserve every post-reset event by
                # rebasing the new raw counter onto the logical cumulative
                # total; max(old, new) would silently discard those events and
                # can make cumulative histogram bucket deltas non-monotonic.
                value = previous.value + sample.value
            self._edge_sample_high_water[sample.identity] = (
                PrometheusSample(
                    sample.name, sample.labels, value
                )
            )
            raw_samples[sample.identity] = sample
        output = []
        for key in sorted(self._edge_sample_high_water):
            sample = self._edge_sample_high_water[key]
            labels = sample.label_dict
            labels["source_coverage"] = (
                "1" if key in present else "0"
            )
            output.append(PrometheusSample.create(
                sample.name, labels, sample.value,
            ))
        return tuple(output)

    def _persistent_service_samples(
        self,
        samples: tuple[PrometheusSample, ...],
        inventory: Inventory,
    ) -> tuple[PrometheusSample, ...]:
        raw_samples = getattr(self, "_service_sample_raw", None)
        if raw_samples is None:
            raw_samples = {}
            self._service_sample_raw = raw_samples
        active = {
            (item.namespace, item.pod, item.container)
            for item in inventory.containers
        }
        for key, sample in tuple(
            self._service_sample_high_water.items()
        ):
            labels = sample.label_dict
            coordinates = (
                labels.get("namespace"),
                labels.get("pod"),
                labels.get("container"),
            )
            if coordinates not in active:
                del self._service_sample_high_water[key]
                raw_samples.pop(key, None)
        present: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
        for sample in samples:
            if not sample.name.startswith("proberca_service_"):
                raise RawCollectionError(
                    "service persistence received a non-service metric"
                )
            labels = sample.label_dict
            coordinates = (
                labels.get("namespace"),
                labels.get("pod"),
                labels.get("container"),
            )
            if coordinates not in active:
                # Beyla may retain a retired Pod/container time series for a
                # short interval after a rollout.  The immutable inventory is
                # the authority for this snapshot, so the retired series must
                # neither fail the exporter nor enter the high-water cache.
                continue
            present.add(sample.identity)
            previous = self._service_sample_high_water.get(sample.identity)
            previous_raw = raw_samples.get(sample.identity)
            if previous is None or previous_raw is None:
                value = sample.value
            elif sample.value >= previous_raw.value:
                value = (
                    previous.value
                    + sample.value
                    - previous_raw.value
                )
            else:
                value = previous.value + sample.value
            self._service_sample_high_water[sample.identity] = (
                PrometheusSample(sample.name, sample.labels, value)
            )
            raw_samples[sample.identity] = sample
        output = []
        for key in sorted(self._service_sample_high_water):
            sample = self._service_sample_high_water[key]
            labels = sample.label_dict
            labels["source_coverage"] = (
                "1" if key in present else "0"
            )
            output.append(PrometheusSample.create(
                sample.name, labels, sample.value,
            ))
        return tuple(output)

    @staticmethod
    def _stable_request_samples(
        samples: tuple[PrometheusSample, ...],
        *,
        edge: bool,
    ) -> tuple[PrometheusSample, ...]:
        coordinate_names = (
            (
                "namespace", "dst_namespace", "src_service",
                "dst_service", "protocol",
            )
            if edge else ("namespace", "pod", "container")
        )
        totals: dict[
            tuple[str, tuple[tuple[str, str], ...]], float
        ] = {}
        coverages: dict[
            tuple[str, tuple[tuple[str, str], ...]], float
        ] = {}
        for sample in samples:
            labels = sample.label_dict
            try:
                source_coverage = float(
                    labels.pop("source_coverage")
                )
            except (KeyError, TypeError, ValueError) as error:
                raise RawCollectionError(
                    "persistent request sample lacks source coverage"
                ) from error
            if source_coverage not in {0.0, 1.0}:
                raise RawCollectionError(
                    "request source coverage must be zero or one"
                )
            coordinates = {
                name: labels.get(name, "")
                for name in coordinate_names
            }
            if any(not value for value in coordinates.values()):
                raise RawCollectionError(
                    "persistent request sample lacks stable coordinates"
                )
            labels["source_series"] = _series_hash(
                "edge-aggregate" if edge else "service-aggregate",
                tuple(sorted(coordinates.items())),
            )
            identity = (
                sample.name,
                tuple(sorted(labels.items())),
            )
            totals[identity] = totals.get(identity, 0.0) + sample.value
            coverages[identity] = max(
                coverages.get(identity, 0.0), source_coverage,
            )
        return tuple(
            PrometheusSample.create(
                name,
                {
                    **dict(labels),
                    "source_coverage": (
                        "1" if coverages[(name, labels)] else "0"
                    ),
                },
                value,
            )
            for (name, labels), value in sorted(totals.items())
        )

    def _coredns_request_samples(
        self,
        inventory: Inventory,
        collected: dict[str, tuple[PrometheusSample, ...]] | None = None,
    ) -> tuple[PrometheusSample, ...]:
        output = []
        for pod in inventory.coredns_pods:
            samples = (
                self._coredns(pod)
                if collected is None
                else collected[pod.container_id]
            )
            index = _sample_index(samples)
            counts = index.get(
                "coredns_dns_request_duration_seconds_count", ()
            )
            if not counts:
                raise RawCollectionError(
                    "CoreDNS lacks request duration counters"
                )
            error_total = sum(
                item.value
                for item in index.get("coredns_dns_responses_total", ())
                if item.label_dict.get("rcode") not in {
                    None, "", "NOERROR",
                }
            )
            if len(counts) != 1:
                raise RawCollectionError(
                    "CoreDNS request count cardinality is ambiguous"
                )
            count = counts[0]
            histogram_index = _histogram_index(
                index,
                ("coredns_dns_request_duration_seconds_bucket",),
            )
            buckets = _bucket_rows(
                histogram_index, count,
                "coredns_dns_request_duration_seconds_bucket",
            )
            common = self._resource_labels(pod)
            output.extend((
                PrometheusSample.create(
                    "proberca_service_request_total",
                    common, count.value,
                ),
                PrometheusSample.create(
                    "proberca_service_request_error_total",
                    common, error_total,
                ),
                PrometheusSample.create(
                    "proberca_service_request_timeout_total", common, 0.0
                ),
            ))
            for bucket in buckets:
                bound = bucket.label_dict["le"]
                labels = dict(common)
                labels["le"] = (
                    "+Inf" if bound == "+Inf"
                    else f"{float(bound) * 1000.0:.17g}"
                )
                output.append(PrometheusSample.create(
                    "proberca_service_request_latency_milliseconds_bucket",
                    labels, bucket.value,
                ))
        return tuple(output)

    @staticmethod
    def _dns_samples(
        inventory: Inventory,
        bpf: tuple[dict[str, Any], ...],
        cgroup_identity: dict[int, ContainerIdentity],
    ) -> tuple[PrometheusSample, ...]:
        output = []
        for record in bpf:
            if record["record_type"] != "dns":
                continue
            identity = cgroup_identity.get(int(record["cgroup_id"]))
            destination = inventory.service_cluster_ips.get(
                record.get("server_ipv4", "")
            )
            if identity is None or destination is None:
                continue
            destination_namespace, destination_service = destination
            if (destination_namespace, destination_service) != (
                "kube-system", "kube-dns",
            ):
                continue
            common = {
                "namespace": identity.namespace,
                "dst_namespace": destination_namespace,
                "src_service": identity.service,
                "dst_service": destination_service,
                "protocol": "dns",
                "source_series": _series_hash(
                    "dns",
                    (
                        ("cgroup_id", str(record["cgroup_id"])),
                        ("server_ipv4", record["server_ipv4"]),
                    ),
                ),
            }
            buckets = record.get("latency_buckets")
            if not isinstance(buckets, list) \
                    or len(buckets) != len(DNS_BUCKETS_MS):
                raise RawCollectionError("BPF DNS histogram is invalid")
            raw_query_total = float(record["query_total"])
            timeout_total = float(record["timeout_total"])
            response_total = float(buckets[-1])
            raw_error_total = float(record["error_rcode_total"])
            error_total = min(raw_error_total, response_total)
            completed_total = response_total + timeout_total
            if (
                min(
                    raw_query_total, timeout_total,
                    response_total, raw_error_total,
                ) < 0
                or
                raw_query_total < completed_total
            ):
                raise RawCollectionError(
                    "BPF DNS counters violate completion invariants"
                )
            output.extend((
                PrometheusSample.create(
                    "proberca_dns_edge_query_total",
                    common, completed_total,
                ),
                PrometheusSample.create(
                    "proberca_dns_edge_timeout_total",
                    common, timeout_total,
                ),
                PrometheusSample.create(
                    "proberca_dns_edge_error_rcode_total",
                    common, error_total,
                ),
            ))
            for bound, value in zip(DNS_BUCKETS_MS, buckets):
                labels = dict(common)
                labels["le"] = (
                    "+Inf" if bound is None else f"{bound:.17g}"
                )
                output.append(PrometheusSample.create(
                    "proberca_dns_edge_latency_milliseconds_bucket",
                    labels, float(value),
                ))
        return tuple(output)

    def _qdisc_transmit_drop_totals(self) -> dict[str, float]:
        result = subprocess.run(
            ["tc", "-j", "-s", "qdisc", "show"],
            check=False,
            capture_output=True,
            text=True,
            timeout=float(self.config.source_timeout_sec),
        )
        if result.returncode != 0:
            raise RawCollectionError(
                "tc qdisc counters are unavailable: "
                + result.stderr.strip()
            )
        try:
            records = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise RawCollectionError(
                "tc qdisc counters are not valid JSON"
            ) from error
        if not isinstance(records, list):
            raise RawCollectionError("tc qdisc counter payload is not a list")
        raw: dict[str, float] = {}
        for record in records:
            if not isinstance(record, dict):
                raise RawCollectionError("tc qdisc record is not an object")
            interface = record.get("dev")
            drops = record.get("drops")
            if not isinstance(interface, str) or not interface:
                raise RawCollectionError("tc qdisc record lacks an interface")
            if not isinstance(drops, (int, float)) or drops < 0:
                raise RawCollectionError("tc qdisc drop counter is invalid")
            # Hierarchical qdiscs may expose the same dropped packet at more
            # than one level.  The maximum is a conservative non-duplicating
            # interface total.
            raw[interface] = max(raw.get(interface, 0.0), float(drops))
        totals: dict[str, float] = {}
        for interface in sorted(set(raw) | set(self._qdisc_drop_state)):
            current = raw.get(interface, 0.0)
            previous = self._qdisc_drop_state.get(interface)
            if previous is None:
                total = current
            else:
                last_raw, total = previous
                total += (
                    current - last_raw
                    if current >= last_raw
                    else current
                )
            self._qdisc_drop_state[interface] = (current, total)
            totals[interface] = total
        return totals

    def _host_samples(
        self,
        inventory: Inventory,
        collected: tuple[PrometheusSample, ...] | None = None,
    ) -> tuple[PrometheusSample, ...]:
        if len(inventory.node_names) != 1:
            raise RawCollectionError(
                "one node_exporter cannot represent multiple nodes"
            )
        node = inventory.node_names[0]
        samples = collected or self._fetch_url(
            self.config.node_exporter_url
        )
        index = _sample_index(samples)
        output = []
        pressure = {
            "proberca_node_cpu_psi_some_nanoseconds_total":
                "node_pressure_cpu_waiting_seconds_total",
            "proberca_node_memory_psi_some_nanoseconds_total":
                "node_pressure_memory_waiting_seconds_total",
            "proberca_node_io_psi_some_nanoseconds_total":
                "node_pressure_io_waiting_seconds_total",
        }
        for output_name, source_name in pressure.items():
            values = index.get(source_name, ())
            if len(values) != 1:
                raise RawCollectionError(
                    f"node_exporter lacks one {source_name} series"
                )
            output.append(PrometheusSample.create(
                output_name, {"node": node},
                values[0].value * 1_000_000_000.0,
            ))
        network = {
            "proberca_node_network_receive_drop_total":
                "node_network_receive_drop_total",
            "proberca_node_network_transmit_drop_total":
                "node_network_transmit_drop_total",
            "proberca_node_network_receive_error_total":
                "node_network_receive_errs_total",
            "proberca_node_network_transmit_error_total":
                "node_network_transmit_errs_total",
        }
        qdisc_transmit_drops = self._qdisc_transmit_drop_totals()
        for output_name, source_name in network.items():
            values = index.get(source_name, ())
            if not values:
                raise RawCollectionError(
                    f"node_exporter lacks {source_name}"
                )
            for value in values:
                interface = value.label_dict.get("device")
                if not interface:
                    raise RawCollectionError(
                        "node network primitive lacks interface"
                    )
                output.append(PrometheusSample.create(
                    output_name,
                    {"node": node, "interface": interface},
                    value.value + (
                        qdisc_transmit_drops.get(interface, 0.0)
                        if output_name
                        == "proberca_node_network_transmit_drop_total"
                        else 0.0
                    ),
                ))
        return tuple(output)

    def _cgroup_identity(
        self,
        inventory: Inventory,
        cgroup_paths: dict[str, Path],
    ) -> dict[int, ContainerIdentity]:
        output = {}
        for identity in inventory.containers:
            path = cgroup_paths.get(identity.container_id)
            if path is None or not path.is_dir():
                raise RawCollectionError("cgroup identity path is unavailable")
            cgroup_id = path.stat().st_ino
            previous = output.setdefault(cgroup_id, identity)
            if previous != identity:
                raise RawCollectionError("DNS cgroup identity is ambiguous")
        return output

    def collect_snapshot(
        self, timestamp_ns: int | None = None,
    ) -> str:
        timestamp_ns = timestamp_ns or self.wall_clock_ns()
        if timestamp_ns % 1_000_000_000 != 0:
            raise RawCollectionError(
                "final primitive snapshot must align to an epoch second"
            )
        inventory = self._inventory_cache
        if inventory is None:
            inventory = self._inventory()
            self._inventory_cache = inventory
        cgroup_paths = self._active_cgroup_paths(inventory)
        cgroup_identity = self._cgroup_identity(
            inventory, cgroup_paths
        )
        worker_count = (
            4 + len(inventory.coredns_pods)
        )
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            next_inventory_future = executor.submit(self._inventory)
            coredns_futures = {
                pod.container_id: executor.submit(self._coredns, pod)
                for pod in inventory.coredns_pods
            }
            beyla_future = executor.submit(self._beyla, inventory)
            host_future = executor.submit(
                self._fetch_url, self.config.node_exporter_url
            )
            bpf_future = executor.submit(
                self._bpf_snapshot, cgroup_identity
            )
            coredns = {
                container_id: future.result()
                for container_id, future in coredns_futures.items()
            }
            bpf = bpf_future.result()
            beyla = beyla_future.result()
            host = host_future.result()
            next_inventory = next_inventory_future.result()
        # The immutable inventory used to label this snapshot is never
        # replaced mid-collection.  A refresh is prepared concurrently for
        # the next snapshot; a changed Pod/container then either maps cleanly
        # on that next window or fails closed as missing coverage.
        self._inventory_cache = next_inventory
        service_rows = self._request_rows(
            beyla, inventory, edge=False
        )
        edge_rows = self._request_rows(
            beyla, inventory, edge=True
        )
        service_samples = list(self._render_request_rows(
            service_rows, edge=False
        ))
        service_samples.extend(
            self._coredns_request_samples(inventory, coredns)
        )
        service_samples = list(self._stable_request_samples(
            self._persistent_service_samples(
                tuple(service_samples), inventory
            ),
            edge=False,
        ))
        edge_samples = self._stable_request_samples(
            self._persistent_edge_samples(
                self._render_request_rows(edge_rows, edge=True)
            ),
            edge=True,
        )
        covered_services = {
            (item.label_dict["namespace"],
             next(
                 identity.service
                 for identity in inventory.containers
                 if identity.namespace == item.label_dict["namespace"]
                 and identity.pod == item.label_dict["pod"]
                 and identity.container == item.label_dict["container"]
             ))
            for item in service_samples
            if item.name == "proberca_service_request_total"
        }
        if covered_services != set(inventory.services):
            missing = sorted(set(inventory.services) - covered_services)
            raise RawCollectionError(
                f"Beyla/CoreDNS request coverage is incomplete: {missing}"
            )
        samples = [
            *service_samples,
            *edge_samples,
            *self._dns_samples(inventory, bpf, cgroup_identity),
            *self._resource_samples(
                inventory, cgroup_paths, bpf, timestamp_ns
            ),
            *self._host_samples(inventory, host),
            PrometheusSample.create(
                "proberca_final_primitive_exporter_ready",
                {"cluster_id": self.config.cluster_id}, 1.0,
            ),
        ]
        if not any(
            item.name == "proberca_tcp_edge_request_total"
            for item in samples
        ):
            raise RawCollectionError("Beyla returned no directed TCP edges")
        if not any(
            item.name == "proberca_dns_edge_query_total"
            for item in samples
        ):
            raise RawCollectionError("BPF returned no directed DNS edges")
        return render_prometheus_text(
            samples, timestamp_ms=timestamp_ns // 1_000_000
        )

    def snapshot_once(self, timestamp_ns: int | None = None) -> None:
        timestamp_ns = timestamp_ns or (
            self.wall_clock_ns() // 1_000_000_000
        ) * 1_000_000_000
        try:
            rendered = self.collect_snapshot(timestamp_ns)
        except Exception as error:
            with self._lock:
                self._last_error = f"{type(error).__name__}: {error}"
            raise
        with self._lock:
            self._snapshot = rendered
            self._snapshot_ns = timestamp_ns
            self._last_error = None

    def _snapshot_loop(self) -> None:
        while not self._stop.is_set():
            now = self.wall_clock_ns()
            second_ns = self.config.snapshot_period_sec * 1_000_000_000
            target = ((now + second_ns - 1) // second_ns) * second_ns
            remaining = target - self.wall_clock_ns()
            if remaining > 0 and self._stop.wait(
                    remaining / 1_000_000_000):
                return
            try:
                self.snapshot_once(target)
            except Exception as error:
                print(
                    "final primitive snapshot failed: "
                    f"{type(error).__name__}: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                continue

    def _response(self) -> tuple[str, int, str]:
        with self._lock:
            snapshot = self._snapshot
            timestamp_ns = self._snapshot_ns
            error = self._last_error
        age_ns = self.wall_clock_ns() - timestamp_ns
        fresh = (
            bool(snapshot)
            and 0 <= age_ns <= 3_000_000_000
            and error is None
        )
        return snapshot, timestamp_ns, "" if fresh else (error or "stale")

    def serve_forever(self) -> None:
        exporter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                snapshot, _timestamp_ns, error = exporter._response()
                if self.path == "/healthz":
                    body = ("ok\n" if not error else f"{error}\n").encode()
                    self.send_response(
                        HTTPStatus.OK if not error
                        else HTTPStatus.SERVICE_UNAVAILABLE
                    )
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path != "/metrics":
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                if error:
                    self.send_error(
                        HTTPStatus.SERVICE_UNAVAILABLE, error
                    )
                    return
                body = snapshot.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header(
                    "Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                return

        worker = threading.Thread(
            target=self._snapshot_loop,
            name="final-primitive-snapshot",
            daemon=True,
        )
        worker.start()
        server = ThreadingHTTPServer(
            (self.config.listen_host, self.config.listen_port), Handler
        )
        try:
            server.serve_forever(poll_interval=0.25)
        finally:
            self._stop.set()
            server.server_close()
            worker.join(timeout=5.0)


def load_final_primitive_exporter_config(
    path: str | Path,
) -> FinalPrimitiveExporterConfig:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return FinalPrimitiveExporterConfig.from_dict(payload)

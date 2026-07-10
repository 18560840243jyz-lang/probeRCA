"""Synthetic pseudo-distributed data generator for probeRCA P0 Step 2."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from proberca.data.io import write_jsonl
from proberca.data.schema import EvidenceRecord, IncidentRecord, MetricRecord
from proberca.graph.schema import GraphEdge

DEFAULT_SERVICES = [
    "frontend",
    "checkoutservice",
    "paymentservice",
    "cartservice",
    "productcatalogservice",
    "recommendationservice",
    "shippingservice",
    "currencyservice",
    "emailservice",
    "redis",
    "postgres",
]

DEFAULT_NODES = ["node-a", "node-b", "node-c"]

METRICS = [
    "request.rps",
    "request.error_rate",
    "request.p50_latency_ms",
    "request.p95_latency_ms",
    "request.p99_latency_ms",
    "request.in_flight",
    "cpu.usage",
    "cpu.throttled_usec",
    "cpu.pressure",
    "memory.usage",
    "memory.pressure",
    "net.rtt_ms",
    "net.retrans",
    "io.bio_latency_ms",
    "io.queue_depth",
    "lock.futex_wait_ms",
]

CALL_EDGES = [
    ("frontend", "checkoutservice"),
    ("checkoutservice", "paymentservice"),
    ("checkoutservice", "cartservice"),
    ("checkoutservice", "shippingservice"),
    ("checkoutservice", "emailservice"),
    ("checkoutservice", "currencyservice"),
    ("frontend", "productcatalogservice"),
    ("frontend", "recommendationservice"),
    ("recommendationservice", "productcatalogservice"),
    ("cartservice", "redis"),
    ("productcatalogservice", "postgres"),
]

SYNTHETIC_EDGES = [
    ("paymentservice.cpu.throttled_usec", "checkoutservice.request.p99_latency_ms"),
    ("checkoutservice.request.p99_latency_ms", "frontend.request.p99_latency_ms"),
    ("shippingservice.net.retrans", "checkoutservice.request.p99_latency_ms"),
    ("postgres.io.bio_latency_ms", "productcatalogservice.request.p99_latency_ms"),
    ("productcatalogservice.request.p99_latency_ms", "frontend.request.p99_latency_ms"),
    ("cartservice.lock.futex_wait_ms", "checkoutservice.request.p99_latency_ms"),
]

INCIDENT_SPECS = {
    "cpu_throttle": {
        "root_service": "paymentservice",
        "root_metric": "cpu.throttled_usec",
        "root_type": "CPU throttling",
        "symptom_service": "frontend",
        "injected_path": [
            "paymentservice.cpu.throttled_usec",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
        "evidence_type": "CPU",
        "evidence_metrics": ["cpu.throttled_usec", "cpu.pressure"],
        "probe_id": "synthetic_cpu_probe",
    },
    "network_delay": {
        "root_service": "shippingservice",
        "root_metric": "net.retrans",
        "root_type": "network instability",
        "symptom_service": "frontend",
        "injected_path": [
            "shippingservice.net.retrans",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
        "evidence_type": "Net",
        "evidence_metrics": ["net.retrans", "net.rtt_ms"],
        "probe_id": "synthetic_net_probe",
    },
    "io_slow": {
        "root_service": "postgres",
        "root_metric": "io.bio_latency_ms",
        "root_type": "storage I/O",
        "symptom_service": "frontend",
        "injected_path": [
            "postgres.io.bio_latency_ms",
            "productcatalogservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
        "evidence_type": "IO",
        "evidence_metrics": ["io.bio_latency_ms", "io.queue_depth"],
        "probe_id": "synthetic_io_probe",
    },
    "lock_contention": {
        "root_service": "cartservice",
        "root_metric": "lock.futex_wait_ms",
        "root_type": "lock contention",
        "symptom_service": "frontend",
        "injected_path": [
            "cartservice.lock.futex_wait_ms",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
        "evidence_type": "Lock",
        "evidence_metrics": ["lock.futex_wait_ms"],
        "probe_id": "synthetic_lock_probe",
    },
}


@dataclass
class SyntheticConfig:
    """Configuration for a single-VM pseudo-distributed synthetic dataset."""

    seed: int = 7
    services: list[str] | None = None
    instances_per_service: int = 2
    nodes: list[str] | None = None
    window_size_sec: int = 10
    baseline_windows: int = 30
    faulty_windows: int = 30
    noise_std: float = 0.05
    output_dir: str = "data/p0_single_vm/demo"

    def resolved_services(self) -> list[str]:
        """Return configured services or the default microservice set."""

        return list(self.services or DEFAULT_SERVICES)

    def resolved_nodes(self) -> list[str]:
        """Return configured nodes or the default pseudo-distributed nodes."""

        return list(self.nodes or DEFAULT_NODES)


@dataclass(frozen=True)
class _Instance:
    service: str
    instance: str
    node: str


@dataclass(frozen=True)
class _IncidentSpec:
    incident_type: str
    incident_id: str
    root_service: str
    root_metric: str
    root_type: str
    symptom_service: str
    injected_path: list[str]
    evidence_type: str
    evidence_metrics: list[str]
    probe_id: str
    start_ts: float
    end_ts: float


def _service_base(service: str) -> dict[str, float]:
    service_factor = 1.0 + (sum(ord(ch) for ch in service) % 17) / 100.0
    return {
        "request.rps": 80.0 * service_factor,
        "request.error_rate": 0.01,
        "request.p50_latency_ms": 25.0 * service_factor,
        "request.p95_latency_ms": 70.0 * service_factor,
        "request.p99_latency_ms": 110.0 * service_factor,
        "request.in_flight": 12.0 * service_factor,
        "cpu.usage": 0.35 * service_factor,
        "cpu.throttled_usec": 100.0 * service_factor,
        "cpu.pressure": 0.05,
        "memory.usage": 512.0 * service_factor,
        "memory.pressure": 0.04,
        "net.rtt_ms": 4.0 * service_factor,
        "net.retrans": 0.2,
        "io.bio_latency_ms": 3.5 * service_factor,
        "io.queue_depth": 1.5 * service_factor,
        "lock.futex_wait_ms": 0.8 * service_factor,
    }


def _positive_noisy_value(base: float, rng: np.random.Generator, noise_std: float) -> float:
    noise = rng.normal(0.0, noise_std)
    return float(max(0.0, base * (1.0 + noise)))


def _instances(config: SyntheticConfig) -> list[_Instance]:
    nodes = config.resolved_nodes()
    result: list[_Instance] = []
    index = 0
    for service in config.resolved_services():
        for instance_number in range(config.instances_per_service):
            node = nodes[index % len(nodes)]
            result.append(_Instance(service, f"{service}-{instance_number + 1}", node))
            index += 1
    return result


def _incident_specs(config: SyntheticConfig) -> list[_IncidentSpec]:
    total_windows = config.baseline_windows + config.faulty_windows
    specs: list[_IncidentSpec] = []
    for index, (incident_type, raw) in enumerate(INCIDENT_SPECS.items()):
        offset = index * total_windows * config.window_size_sec
        start_ts = offset + config.baseline_windows * config.window_size_sec
        end_ts = offset + total_windows * config.window_size_sec
        specs.append(
            _IncidentSpec(
                incident_type=incident_type,
                incident_id=f"inc-{index + 1:03d}-{incident_type}",
                root_service=raw["root_service"],
                root_metric=raw["root_metric"],
                root_type=raw["root_type"],
                symptom_service=raw["symptom_service"],
                injected_path=list(raw["injected_path"]),
                evidence_type=raw["evidence_type"],
                evidence_metrics=list(raw["evidence_metrics"]),
                probe_id=raw["probe_id"],
                start_ts=float(start_ts),
                end_ts=float(end_ts),
            )
        )
    return specs


def _path_services(path: list[str]) -> set[str]:
    services: set[str] = set()
    for node_metric in path:
        service, _, _metric = node_metric.partition(".")
        if service:
            services.add(service)
    return services


def _fault_adjustment(service: str, metric: str, spec: _IncidentSpec, position: int) -> float:
    path_services = _path_services(spec.injected_path)
    if service == spec.root_service and metric == spec.root_metric:
        return {
            "cpu.throttled_usec": 9000.0,
            "net.retrans": 25.0,
            "io.bio_latency_ms": 120.0,
            "lock.futex_wait_ms": 80.0,
        }.get(metric, 50.0)

    if service in path_services and metric in {"request.p95_latency_ms", "request.p99_latency_ms", "request.in_flight"}:
        path_index = max(0, next((i for i, item in enumerate(spec.injected_path) if item.startswith(service + ".")), 1))
        strength = max(0.35, 1.0 - path_index * 0.25)
        if service == spec.symptom_service and metric == "request.p99_latency_ms":
            strength = max(strength, 1.0)
        if metric == "request.p95_latency_ms":
            return 90.0 * strength
        if metric == "request.p99_latency_ms":
            return 160.0 * strength
        if metric == "request.in_flight":
            return 18.0 * strength

    if position > 0 and service == spec.root_service and metric in spec.evidence_metrics:
        return 8.0

    return 0.0


def _generate_metrics(config: SyntheticConfig, rng: np.random.Generator, instances: list[_Instance], specs: list[_IncidentSpec]) -> list[MetricRecord]:
    records: list[MetricRecord] = []
    total_windows = config.baseline_windows + config.faulty_windows

    for spec_index, spec in enumerate(specs):
        offset = spec_index * total_windows * config.window_size_sec
        for window_index in range(total_windows):
            timestamp = float(offset + window_index * config.window_size_sec)
            is_faulty = window_index >= config.baseline_windows
            position = window_index - config.baseline_windows + 1 if is_faulty else 0
            for instance in instances:
                base_values = _service_base(instance.service)
                for metric in METRICS:
                    value = _positive_noisy_value(base_values[metric], rng, config.noise_std)
                    if is_faulty:
                        value += _fault_adjustment(instance.service, metric, spec, position)
                    records.append(
                        MetricRecord(
                            timestamp=timestamp,
                            service=instance.service,
                            instance=instance.instance,
                            node=instance.node,
                            metric=metric,
                            value=float(value),
                            source="synthetic",
                            incident_id=spec.incident_id if is_faulty else None,
                        )
                    )
    return records


def _generate_evidence(config: SyntheticConfig, rng: np.random.Generator, instances: list[_Instance], specs: list[_IncidentSpec]) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    root_instances_by_service: dict[str, list[_Instance]] = {}
    for instance in instances:
        root_instances_by_service.setdefault(instance.service, []).append(instance)

    for spec in specs:
        root_instances = root_instances_by_service.get(spec.root_service, [])
        for window_index in range(config.faulty_windows):
            timestamp = float(spec.start_ts + window_index * config.window_size_sec)
            for instance in root_instances:
                for metric in spec.evidence_metrics:
                    base = _service_base(instance.service).get(metric, 1.0)
                    value = _positive_noisy_value(base, rng, config.noise_std) + _fault_adjustment(instance.service, metric, spec, window_index + 1)
                    records.append(
                        EvidenceRecord(
                            timestamp=timestamp,
                            service=instance.service,
                            instance=instance.instance,
                            node=instance.node,
                            evidence_type=spec.evidence_type,
                            metric=metric,
                            value=float(value),
                            source="synthetic",
                            probe_id=spec.probe_id,
                            sampling_rate=1.0,
                            incident_id=spec.incident_id,
                        )
                    )
    return records


def _generate_incidents(specs: list[_IncidentSpec]) -> list[IncidentRecord]:
    return [
        IncidentRecord(
            incident_id=spec.incident_id,
            root_service=spec.root_service,
            root_metric=spec.root_metric,
            root_type=spec.root_type,
            symptom_service=spec.symptom_service,
            start_ts=spec.start_ts,
            end_ts=spec.end_ts,
            injected_path=list(spec.injected_path),
        )
        for spec in specs
    ]


def _generate_graph(instances: list[_Instance]) -> list[GraphEdge]:
    edges: list[GraphEdge] = [GraphEdge(src=src, dst=dst, edge_type="call") for src, dst in CALL_EDGES]

    by_node: dict[str, list[_Instance]] = {}
    for instance in instances:
        by_node.setdefault(instance.node, []).append(instance)

    for node_instances in by_node.values():
        for left in node_instances:
            for right in node_instances:
                if left.instance != right.instance:
                    edges.append(GraphEdge(src=left.instance, dst=right.instance, edge_type="cohost"))

    edges.extend(GraphEdge(src=src, dst=dst, edge_type="synthetic") for src, dst in SYNTHETIC_EDGES)
    return edges


def _metadata(config: SyntheticConfig, metrics_count: int, evidence_count: int, incidents_count: int, graph_edges_count: int) -> dict[str, Any]:
    return {
        "seed": config.seed,
        "services": config.resolved_services(),
        "nodes": config.resolved_nodes(),
        "instances_per_service": config.instances_per_service,
        "window_size_sec": config.window_size_sec,
        "baseline_windows": config.baseline_windows,
        "faulty_windows": config.faulty_windows,
        "metrics_count": metrics_count,
        "evidence_count": evidence_count,
        "incidents_count": incidents_count,
        "graph_edges_count": graph_edges_count,
    }


def generate_dataset(config: SyntheticConfig) -> dict[str, Any]:
    """Generate a P0 synthetic pseudo-distributed dataset and write it to disk."""

    rng = np.random.default_rng(config.seed)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    instances = _instances(config)
    specs = _incident_specs(config)
    metrics = _generate_metrics(config, rng, instances, specs)
    evidence = _generate_evidence(config, rng, instances, specs)
    incidents = _generate_incidents(specs)
    graph_edges = _generate_graph(instances)

    paths = {
        "metrics": output_dir / "metrics.jsonl",
        "evidence": output_dir / "evidence.jsonl",
        "incidents": output_dir / "incidents.jsonl",
        "service_graph": output_dir / "service_graph.jsonl",
        "metadata": output_dir / "metadata.json",
    }

    write_jsonl(paths["metrics"], metrics)
    write_jsonl(paths["evidence"], evidence)
    write_jsonl(paths["incidents"], incidents)
    write_jsonl(paths["service_graph"], [asdict(edge) for edge in graph_edges])

    metadata = _metadata(config, len(metrics), len(evidence), len(incidents), len(graph_edges))
    paths["metadata"].write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "output_dir": str(output_dir),
        "metrics_path": str(paths["metrics"]),
        "evidence_path": str(paths["evidence"]),
        "incidents_path": str(paths["incidents"]),
        "service_graph_path": str(paths["service_graph"]),
        "metadata_path": str(paths["metadata"]),
        "metadata": metadata,
        "incidents": [asdict(incident) for incident in incidents],
    }

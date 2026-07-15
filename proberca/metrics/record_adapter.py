"""Canonical Prometheus sample to P1 metric-record adapter."""
from __future__ import annotations

from proberca.data.schema import (
    PROBERCA_SCHEMA_VERSION, EdgeMetricRecord, NodeMetricRecord,
)
from proberca.k8s.contracts import CallEdgeObservation, canonical_hash


class MetricMappingError(ValueError):
    """A sample cannot be mapped to exactly one P1 identity."""


def _semantic_labels(spec, sample):
    missing = [label for label in spec.required_labels if label not in sample.labels]
    if missing:
        raise MetricMappingError(f"required Prometheus labels missing: {missing}")
    allowed = set(spec.required_labels) | set(spec.optional_labels) | {"__name__"}
    if spec.quality_policy == "strict":
        unknown = sorted(set(sample.labels) - allowed)
        if unknown:
            raise MetricMappingError(f"unknown Prometheus labels: {unknown}")
    return {semantic: sample.labels.get(label)
            for semantic, label in spec.label_mapping.items()}


def _distribution(spec, labels):
    bound = None
    is_inf = False
    cumulative = None
    quantile = None
    if spec.metric_kind == "histogram_bucket":
        raw = labels.get("histogram_upper_bound") or labels.get("le")
        if raw is None:
            raise MetricMappingError("histogram bucket boundary is missing")
        is_inf = raw in {"+Inf", "Inf", "+inf", "inf"}
        bound = None if is_inf else float(raw)
        cumulative = True
    elif spec.metric_kind == "quantile":
        raw = labels.get("quantile")
        if raw is None:
            raise MetricMappingError("quantile label is missing")
        quantile = float(raw)
    return bound, is_inf, cumulative, quantile


def records_from_samples(spec, samples, revision, window_sec):
    output = []
    for sample in samples:
        labels = _semantic_labels(spec, sample)
        namespace = labels.get("namespace")
        if not namespace:
            raise MetricMappingError("namespace mapping is required")
        bound, is_inf, cumulative, quantile = _distribution(spec, labels)
        if spec.record_type == "node_metric":
            pod_uid = None
            container_id = labels.get("container_id")
            node_name = labels.get("node")
            service_name = labels.get("service")
            if spec.service_resolution in {"pod", "container", "pod_ip"}:
                if spec.service_resolution == "pod_ip":
                    matches = revision.pod_uids_by_ip.get(labels.get("pod_ip"), ())
                    if len(matches) != 1:
                        raise MetricMappingError("Pod IP is missing or ambiguous")
                    pod_uid = matches[0]
                else:
                    pod_uid = revision.pod_uid_by_name.get((namespace, labels.get("pod")))
                    if pod_uid is None:
                        raise MetricMappingError("Pod label does not resolve by exact name")
                try:
                    service_id = revision.resolve_service_for_pod(
                        pod_uid, explicit_service=service_name)
                except ValueError as error:
                    raise MetricMappingError(str(error)) from error
                service_name = service_id.split("::")[2]
                pod = revision.objects_by_kind["Pod"][pod_uid]
                node_name = (pod.get("spec") or {}).get("nodeName")
            elif spec.service_resolution == "explicit_service":
                if not service_name or (namespace, service_name) not in revision.service_uid_by_name:
                    raise MetricMappingError("explicit service does not exist")
            else:
                raise MetricMappingError("unsupported service_resolution")
            output.append(NodeMetricRecord(
                PROBERCA_SCHEMA_VERSION, sample.timestamp_ns, window_sec,
                revision.cluster_id, node_name, namespace, service_name, pod_uid,
                container_id, spec.metric_family, spec.metric_name, sample.value,
                spec.unit, 1, 1.0, 0.0, f"prometheus:{spec.spec_id}",
                spec.metric_kind, spec.expected_scope, bound, is_inf, cumulative, quantile))
        elif spec.record_type == "edge_metric":
            source, destination = labels.get("source"), labels.get("destination")
            protocol = labels.get("protocol")
            for service in (source, destination):
                if not service or (namespace, service) not in revision.service_uid_by_name:
                    raise MetricMappingError("edge service endpoint is missing or unknown")
            if not protocol:
                raise MetricMappingError("edge protocol is required")
            output.append(EdgeMetricRecord(
                PROBERCA_SCHEMA_VERSION, sample.timestamp_ns, window_sec,
                revision.cluster_id, namespace, source, destination, None, None,
                None, None, protocol, spec.metric_name, sample.value, spec.unit,
                1, 1.0, 0.0, f"prometheus:{spec.spec_id}", spec.metric_kind,
                spec.expected_scope, bound, is_inf, cumulative, quantile))
        else:
            raise MetricMappingError("call_edge samples require CallEdgeProvider")
    return sorted(output, key=lambda item: (item.timestamp_ns, item.record_type, item.stable_id))


def call_edges_from_samples(spec, samples, revision, window_start_ns, window_end_ns):
    if spec.record_type != "call_edge":
        raise MetricMappingError("call edge adapter requires record_type=call_edge")
    grouped, source_ids, seen = {}, {}, {}
    for sample in samples:
        if not window_start_ns <= sample.timestamp_ns < window_end_ns:
            continue
        labels = _semantic_labels(spec, sample)
        namespace = labels.get("namespace")
        source, destination, protocol = (
            labels.get("source"), labels.get("destination"), labels.get("protocol"))
        if not namespace or not source or not destination or not protocol:
            raise MetricMappingError("call edge namespace/endpoints/protocol are required")
        for service in (source, destination):
            if (namespace, service) not in revision.service_uid_by_name:
                raise MetricMappingError("call edge service endpoint is unknown")
        key = (namespace, source, destination, protocol)
        sample_id = getattr(sample, "sample_id", canonical_hash({
            "timestamp_ns": sample.timestamp_ns, "labels": sample.labels,
            "value": sample.value}))
        identity = (key, sample.timestamp_ns, sample_id)
        previous = seen.get(identity)
        if previous is not None:
            if previous != float(sample.value):
                raise MetricMappingError("conflicting duplicate call-edge sample")
            continue
        seen[identity] = float(sample.value)
        grouped[key] = grouped.get(key, 0.0) + float(sample.value)
        source_ids.setdefault(key, []).append(sample_id)
    output = []
    for key, request_count in sorted(grouped.items()):
        namespace, source, destination, protocol = key
        if request_count <= 0:
            continue
        source_id = f"{revision.cluster_id}::{namespace}::{source}"
        destination_id = f"{revision.cluster_id}::{namespace}::{destination}"
        seed = {"spec_id": spec.spec_id, "source": source_id, "destination": destination_id,
                "protocol": protocol, "start": window_start_ns, "end": window_end_ns}
        output.append(CallEdgeObservation(
            canonical_hash(seed), revision.cluster_id, (namespace,), source_id,
            destination_id, protocol, request_count, None, None, 1.0,
            window_start_ns, window_end_ns, f"prometheus:{spec.spec_id}",
            tuple(sorted(source_ids[key])),
            canonical_hash({"spec_id": spec.spec_id, "promql": spec.promql})))
    return tuple(output)

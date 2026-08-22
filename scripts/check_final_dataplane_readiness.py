#!/usr/bin/env python3
"""Fail closed until the formal primitive service and TCP scope are visible."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import yaml

from proberca.dataplane.prometheus_text import parse_prometheus_text


def _formal_tcp_edges(path: Path) -> tuple[str, frozenset[str]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("collector config must be a mapping")
    cluster_id = payload.get("cluster_id")
    edges = payload.get("formal_tcp_edge_entity_ids")
    if (
        not isinstance(cluster_id, str)
        or not cluster_id
        or not isinstance(edges, list)
        or not edges
        or len(edges) != len(set(edges))
        or any(
            not isinstance(edge, str)
            or not edge.startswith(f"{cluster_id}::")
            or not edge.endswith("::tcp")
            or edge.count("->") != 1
            for edge in edges
        )
    ):
        raise ValueError("formal TCP edge scope is invalid")
    return cluster_id, frozenset(edges)


def evaluate_formal_coverage(
    text: str,
    *,
    cluster_id: str,
    required_edges: frozenset[str],
) -> dict[str, object]:
    samples = parse_prometheus_text(text)
    exporter_ready = any(
        sample.name == "proberca_final_primitive_exporter_ready"
        and sample.value == 1.0
        for sample in samples
    )
    observed_edges = set()
    for sample in samples:
        if sample.name != "proberca_tcp_edge_request_total":
            continue
        labels = sample.label_dict
        required_labels = (
            "namespace", "src_service", "dst_service", "protocol",
        )
        if any(not labels.get(name) for name in required_labels):
            continue
        destination_namespace = labels.get(
            "dst_namespace", labels["namespace"]
        )
        observed_edges.add(
            f"{cluster_id}::{labels['namespace']}::"
            f"{labels['src_service']}->{labels['dst_service']}::"
            f"{labels['protocol']}"
        )
        if destination_namespace != labels["namespace"]:
            observed_edges.discard(
                f"{cluster_id}::{labels['namespace']}::"
                f"{labels['src_service']}->{labels['dst_service']}::"
                f"{labels['protocol']}"
            )
    missing = sorted(required_edges - observed_edges)
    return {
        "ready": exporter_ready and not missing,
        "exporter_ready": exporter_ready,
        "required_tcp_edges": len(required_edges),
        "observed_required_tcp_edges": len(required_edges - set(missing)),
        "missing_tcp_edges": missing,
    }


def wait_for_formal_coverage(
    *,
    collector_config: Path,
    metrics_url: str,
    timeout_sec: float,
    poll_interval_sec: float,
) -> dict[str, object]:
    cluster_id, required_edges = _formal_tcp_edges(collector_config)
    deadline = time.monotonic() + timeout_sec
    last_result: dict[str, object] = {
        "ready": False,
        "exporter_ready": False,
        "required_tcp_edges": len(required_edges),
        "observed_required_tcp_edges": 0,
        "missing_tcp_edges": sorted(required_edges),
    }
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urlopen(metrics_url, timeout=3.0) as response:
                text = response.read().decode("utf-8")
            last_result = evaluate_formal_coverage(
                text,
                cluster_id=cluster_id,
                required_edges=required_edges,
            )
            last_error = None
            if last_result["ready"]:
                return last_result
        except (HTTPError, URLError, OSError, UnicodeError, ValueError) as error:
            last_error = f"{type(error).__name__}: {error}"
        time.sleep(poll_interval_sec)
    diagnostic = dict(last_result)
    diagnostic["last_error"] = last_error
    raise TimeoutError(
        "formal primitive coverage did not become ready: "
        + json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector-config", type=Path, required=True)
    parser.add_argument(
        "--metrics-url",
        default="http://127.0.0.1:9477/metrics",
    )
    parser.add_argument("--timeout-sec", type=float, default=180.0)
    parser.add_argument("--poll-interval-sec", type=float, default=1.0)
    arguments = parser.parse_args()
    if arguments.timeout_sec <= 0 or arguments.poll_interval_sec <= 0:
        raise SystemExit("readiness timing must be positive")
    try:
        result = wait_for_formal_coverage(
            collector_config=arguments.collector_config,
            metrics_url=arguments.metrics_url,
            timeout_sec=arguments.timeout_sec,
            poll_interval_sec=arguments.poll_interval_sec,
        )
    except (OSError, ValueError, TimeoutError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

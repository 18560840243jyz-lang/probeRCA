"""Minimal metrics collection helpers for Online Boutique P2A-1/P2A-1R."""

from __future__ import annotations

import json
import math
import re
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

SUPPORTED_CADVISOR_METRICS = {
    "container_cpu_usage_seconds_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_memory_working_set_bytes",
    "container_memory_usage_bytes",
}


def run_cmd(cmd: list[str], timeout: int = 30) -> tuple[int, str, str]:
    """Run a command and return returncode, stdout, and stderr."""

    try:
        completed = subprocess.run(cmd, check=False, text=True, capture_output=True, timeout=timeout)
        return completed.returncode, completed.stdout, completed.stderr
    except subprocess.TimeoutExpired as exc:
        return 124, exc.stdout or "", exc.stderr or f"command timed out after {timeout}s"


def _load_json_command(cmd: list[str], timeout: int = 30) -> dict[str, Any]:
    code, stdout, stderr = run_cmd(cmd, timeout=timeout)
    if code != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\n{stderr}")
    return json.loads(stdout)


def _infer_service_from_pod_name(name: str) -> str:
    for suffix_pattern in (r"-[0-9a-f]{9,10}-[a-z0-9]{5}$", r"-[a-z0-9]{5}$"):
        stripped = re.sub(suffix_pattern, "", name)
        if stripped != name:
            return stripped
    return name


def get_pods(namespace: str) -> list[dict]:
    """Return selected Pod metadata from Kubernetes JSON output."""

    data = _load_json_command(["kubectl", "get", "pods", "-n", namespace, "-o", "json"], timeout=30)
    pods: list[dict] = []
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        labels = metadata.get("labels", {}) or {}
        name = metadata.get("name", "")
        containers = []
        for container_status in status.get("containerStatuses", []) or []:
            containers.append(
                {
                    "name": container_status.get("name"),
                    "container_id": container_status.get("containerID", ""),
                    "image": container_status.get("image"),
                    "ready": bool(container_status.get("ready")),
                    "state": container_status.get("state", {}),
                }
            )
        pods.append(
            {
                "name": name,
                "namespace": namespace,
                "labels": labels,
                "service": labels.get("app") or _infer_service_from_pod_name(name),
                "pod_ip": status.get("podIP"),
                "node": spec.get("nodeName"),
                "phase": status.get("phase"),
                "ready": all(c.get("ready") for c in containers) if containers else False,
                "containers": containers,
            }
        )
    return pods


def get_deployments(namespace: str) -> list[dict]:
    """Return selected Deployment readiness state."""

    data = _load_json_command(["kubectl", "get", "deploy", "-n", namespace, "-o", "json"], timeout=30)
    deployments: list[dict] = []
    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        spec = item.get("spec", {})
        status = item.get("status", {})
        replicas = int(spec.get("replicas") or 0)
        available = int(status.get("availableReplicas") or 0)
        ready = int(status.get("readyReplicas") or 0)
        deployments.append(
            {
                "name": metadata.get("name"),
                "replicas": replicas,
                "ready_replicas": ready,
                "available_replicas": available,
                "ready": replicas > 0 and ready == replicas and available == replicas,
            }
        )
    return deployments


def get_kubelet_summary(node_name: str) -> dict:
    """Collect kubelet summary JSON through the Kubernetes API server proxy."""

    return _load_json_command(["kubectl", "get", "--raw", f"/api/v1/nodes/{node_name}/proxy/stats/summary"], timeout=30)


def get_cadvisor_metrics(node_name: str) -> str:
    """Collect cAdvisor Prometheus text metrics through the Kubernetes API server proxy."""

    code, stdout, stderr = run_cmd(["kubectl", "get", "--raw", f"/api/v1/nodes/{node_name}/proxy/metrics/cadvisor"], timeout=30)
    if code != 0:
        raise RuntimeError(f"failed to collect cAdvisor metrics from {node_name}: {stderr}")
    return stdout


def _parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        labels[match.group(1)] = match.group(2).replace(r'\"', '"').replace(r'\\', '\\')
    return labels


def parse_prometheus_text_metrics(text: str) -> list[dict]:
    """Parse the cAdvisor Prometheus text format used by P2A-1R."""

    records: list[dict] = []
    line_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)(?:\s+\d+)?$")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = line_re.match(line)
        if not match:
            continue
        name = match.group(1)
        if name not in SUPPORTED_CADVISOR_METRICS:
            continue
        labels = _parse_labels(match.group(2) or "")
        if labels.get("namespace") != "online-boutique":
            continue
        if not labels.get("namespace") or not labels.get("pod") or not labels.get("container"):
            continue
        records.append({"name": name, "labels": labels, "value": float(match.group(3))})
    return records


def _pod_service_map(pods: list[dict]) -> dict[str, str]:
    return {pod.get("name", ""): pod.get("service") or _infer_service_from_pod_name(pod.get("name", "")) for pod in pods}


def build_cadvisor_service_snapshot(cadvisor_records: list[dict], pods: list[dict]) -> dict:
    """Map cAdvisor metric records into a service/pod/container counter snapshot."""

    pod_to_service = _pod_service_map(pods)
    snapshot: dict[str, dict] = {}
    metric_map = {
        "container_cpu_usage_seconds_total": "cpu_usage_seconds_total",
        "container_cpu_cfs_throttled_seconds_total": "cpu_cfs_throttled_seconds_total",
        "container_cpu_cfs_periods_total": "cpu_cfs_periods_total",
        "container_cpu_cfs_throttled_periods_total": "cpu_cfs_throttled_periods_total",
        "container_memory_working_set_bytes": "memory_working_set_bytes",
        "container_memory_usage_bytes": "memory_usage_bytes",
    }
    for record in cadvisor_records:
        labels = record.get("labels", {})
        pod = labels.get("pod")
        container = labels.get("container")
        if not pod or not container:
            continue
        service = pod_to_service.get(pod) or _infer_service_from_pod_name(pod)
        container_entry = snapshot.setdefault(service, {}).setdefault(pod, {}).setdefault(container, {})
        field = metric_map.get(record.get("name"))
        if field:
            container_entry[field] = float(record.get("value", 0.0))
    return snapshot


def _metric_record(timestamp: float, service: str, instance: str, node: str, metric: str, value: float, incident_id: str, source: str) -> dict:
    return {
        "timestamp": float(timestamp),
        "service": service,
        "instance": instance,
        "node": node,
        "metric": metric,
        "value": float(value),
        "source": source,
        "incident_id": incident_id,
    }


def compute_cadvisor_window_metrics(
    prev_snapshot: dict,
    curr_snapshot: dict,
    window_size_sec: int | float,
    timestamp: float | None = None,
    incident_id: str = "",
) -> list[dict]:
    """Convert two cAdvisor counter snapshots into MetricRecord-compatible records."""

    ts = float(timestamp if timestamp is not None else time.time())
    window = max(float(window_size_sec), 1e-6)
    records: list[dict] = []
    for service, pods in curr_snapshot.items():
        for pod, containers in pods.items():
            for container, curr_values in containers.items():
                prev_values = (((prev_snapshot.get(service) or {}).get(pod) or {}).get(container) or {})
                if "cpu_usage_seconds_total" in curr_values and "cpu_usage_seconds_total" in prev_values:
                    delta = curr_values["cpu_usage_seconds_total"] - prev_values["cpu_usage_seconds_total"]
                    if delta >= 0:
                        records.append(_metric_record(ts, service, pod, service, "cpu.usage", delta / window, incident_id, "kubelet_cadvisor"))
                if "cpu_cfs_throttled_seconds_total" in curr_values and "cpu_cfs_throttled_seconds_total" in prev_values:
                    delta = curr_values["cpu_cfs_throttled_seconds_total"] - prev_values["cpu_cfs_throttled_seconds_total"]
                    if delta >= 0:
                        records.append(_metric_record(ts, service, pod, service, "cpu.throttled_usec", delta * 1_000_000.0, incident_id, "kubelet_cadvisor"))
                periods_delta = None
                throttled_periods_delta = None
                if "cpu_cfs_periods_total" in curr_values and "cpu_cfs_periods_total" in prev_values:
                    delta = curr_values["cpu_cfs_periods_total"] - prev_values["cpu_cfs_periods_total"]
                    if delta >= 0:
                        periods_delta = delta
                if "cpu_cfs_throttled_periods_total" in curr_values and "cpu_cfs_throttled_periods_total" in prev_values:
                    delta = curr_values["cpu_cfs_throttled_periods_total"] - prev_values["cpu_cfs_throttled_periods_total"]
                    if delta >= 0:
                        throttled_periods_delta = delta
                        records.append(_metric_record(ts, service, pod, service, "cpu.throttled_periods", delta, incident_id, "kubelet_cadvisor"))
                if throttled_periods_delta is not None and periods_delta is not None:
                    records.append(
                        _metric_record(
                            ts,
                            service,
                            pod,
                            service,
                            "cpu.throttle_ratio",
                            throttled_periods_delta / max(periods_delta, 1.0),
                            incident_id,
                            "kubelet_cadvisor",
                        )
                    )
                memory_value = curr_values.get("memory_working_set_bytes", curr_values.get("memory_usage_bytes"))
                if memory_value is not None:
                    records.append(_metric_record(ts, service, pod, service, "memory.usage", memory_value, incident_id, "kubelet_cadvisor"))
    return records


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_crictl_stats_json(text: str) -> list[dict]:
    """Parse `crictl stats -o json` output into normalized container stats."""

    data = json.loads(text)
    stats = data.get("stats", []) if isinstance(data, dict) else []
    records: list[dict] = []
    for item in stats:
        attrs = item.get("attributes", {}) or {}
        cpu = item.get("cpu", {}) or {}
        memory = item.get("memory", {}) or {}
        records.append(
            {
                "id": attrs.get("id") or item.get("id") or "",
                "name": (attrs.get("metadata") or {}).get("name") or attrs.get("name") or "",
                "labels": attrs.get("labels") or {},
                "cpu_nanocores": _to_float(cpu.get("usageNanoCores")),
                "cpu_core_nanoseconds": _to_float(cpu.get("usageCoreNanoSeconds")),
                "memory_working_set_bytes": _to_float(memory.get("workingSetBytes") or memory.get("usageBytes")),
            }
        )
    return records


def parse_crictl_stats_text(text: str) -> list[dict]:
    """Best-effort parser for tabular `crictl stats` output."""

    records: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("container"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) >= 2:
            records.append({"id": parts[0], "name": parts[1], "labels": {}})
    return records


def get_crictl_stats() -> dict:
    """Fallback collection of container stats from the kind control-plane node."""

    json_cmd = ["docker", "exec", "proberca-ob-control-plane", "crictl", "stats", "-o", "json"]
    code, stdout, stderr = run_cmd(json_cmd, timeout=30)
    if code == 0:
        try:
            return {"available": True, "format": "json", "containers": parse_crictl_stats_json(stdout), "error": ""}
        except json.JSONDecodeError as exc:
            stderr = f"{stderr}\njson parse failed: {exc}"
    text_cmd = ["docker", "exec", "proberca-ob-control-plane", "crictl", "stats"]
    code_text, stdout_text, stderr_text = run_cmd(text_cmd, timeout=30)
    if code_text == 0:
        return {"available": True, "format": "text", "containers": parse_crictl_stats_text(stdout_text), "error": ""}
    return {"available": False, "format": "none", "containers": [], "error": (stderr_text or stderr).strip()}


def _container_id_suffix(container_id: str) -> str:
    if "://" in container_id:
        container_id = container_id.split("://", 1)[1]
    return container_id


def map_container_stats_to_services(pods: list[dict], crictl_stats: dict) -> dict:
    """Map fallback crictl stats to Online Boutique services and pod instances."""

    stats_records = crictl_stats.get("containers", []) if isinstance(crictl_stats, dict) else []
    by_id = {str(record.get("id", "")): record for record in stats_records}
    services: dict[str, dict] = {}
    for pod in pods:
        service = pod.get("service") or _infer_service_from_pod_name(pod.get("name", ""))
        service_entry = services.setdefault(service, {"instances": {}, "cpu.usage": None, "memory.usage": None})
        for container in pod.get("containers", []):
            pod_container_id = _container_id_suffix(container.get("container_id", ""))
            matched = None
            for stat_id, stat in by_id.items():
                if pod_container_id and (pod_container_id.startswith(stat_id) or stat_id.startswith(pod_container_id[:12])):
                    matched = stat
                    break
            if not matched:
                continue
            cpu_value = matched.get("cpu_nanocores")
            memory_value = matched.get("memory_working_set_bytes")
            service_entry["instances"][pod.get("name")] = {
                "cpu.usage": cpu_value,
                "memory.usage": memory_value,
                "container": container.get("name"),
            }
    for service_entry in services.values():
        cpu_values = [v["cpu.usage"] for v in service_entry["instances"].values() if v.get("cpu.usage") is not None]
        mem_values = [v["memory.usage"] for v in service_entry["instances"].values() if v.get("memory.usage") is not None]
        service_entry["cpu.usage"] = float(statistics.mean(cpu_values)) if cpu_values else None
        service_entry["memory.usage"] = float(statistics.mean(mem_values)) if mem_values else None
    return services


def try_collect_cgroup_cpu_stat(pods: list[dict]) -> dict:
    """Best-effort legacy cgroup cpu.stat collection. Not the P2A-1R primary path."""

    results: dict[str, dict] = {}
    for pod in pods:
        for container in pod.get("containers", []):
            cid = _container_id_suffix(container.get("container_id", ""))
            if not cid:
                continue
            short = cid[:12]
            script = (
                "p=$(find /sys/fs/cgroup -path '*" + short + "*/cpu.stat' 2>/dev/null | head -1); "
                "if [ -n \"$p\" ]; then cat \"$p\"; fi"
            )
            code, stdout, _stderr = run_cmd(["docker", "exec", "proberca-ob-control-plane", "sh", "-lc", script], timeout=10)
            if code != 0 or not stdout.strip():
                continue
            parsed: dict[str, float] = {}
            for line in stdout.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[0] in {"throttled_usec", "nr_throttled"}:
                    parsed[parts[0]] = float(parts[1])
            if parsed:
                results.setdefault(pod.get("service"), {})[pod.get("name")] = parsed
    return results


def summarize_http_samples(samples: list[dict]) -> dict:
    """Summarize curl HTTP code and latency samples."""

    if not samples:
        return {"rps": 0.0, "error_rate": 1.0, "p50_latency_ms": 0.0, "p95_latency_ms": 0.0, "p99_latency_ms": 0.0}
    latencies = sorted(float(sample.get("latency_sec", 0.0)) * 1000.0 for sample in samples)
    errors = sum(1 for sample in samples if int(sample.get("http_code", 0)) >= 400 or int(sample.get("http_code", 0)) == 0)
    elapsed = max(sum(float(sample.get("latency_sec", 0.0)) for sample in samples), 1e-6)

    def percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        index = min(len(values) - 1, max(0, math.ceil((pct / 100.0) * len(values)) - 1))
        return float(values[index])

    return {
        "rps": float(len(samples) / elapsed),
        "error_rate": float(errors / len(samples)),
        "p50_latency_ms": percentile(latencies, 50),
        "p95_latency_ms": percentile(latencies, 95),
        "p99_latency_ms": percentile(latencies, 99),
    }


def curl_frontend_requests(frontend_url: str, requests_per_window: int, timeout_sec: int) -> dict:
    """Send frontend requests with curl and summarize latency."""

    samples: list[dict] = []
    for _ in range(requests_per_window):
        cmd = ["curl", "-sS", "--max-time", str(timeout_sec), "-o", "/dev/null", "-w", "%{http_code} %{time_total}", frontend_url]
        code, stdout, _stderr = run_cmd(cmd, timeout=timeout_sec + 2)
        if code != 0:
            samples.append({"http_code": 0, "latency_sec": float(timeout_sec)})
            continue
        parts = stdout.strip().split()
        if len(parts) != 2:
            samples.append({"http_code": 0, "latency_sec": float(timeout_sec)})
            continue
        samples.append({"http_code": int(parts[0]), "latency_sec": float(parts[1])})
    summary = summarize_http_samples(samples)
    summary["samples"] = samples
    return summary


def _summary_fallback_records(summary: dict, pods: list[dict], timestamp: float, incident_id: str) -> list[dict]:
    pod_to_service = _pod_service_map(pods)
    records: list[dict] = []
    for pod_stats in summary.get("pods", []) or []:
        pod_ref = pod_stats.get("podRef", {})
        pod = pod_ref.get("name", "")
        namespace = pod_ref.get("namespace", "")
        if namespace != "online-boutique" or not pod:
            continue
        service = pod_to_service.get(pod) or _infer_service_from_pod_name(pod)
        for container in pod_stats.get("containers", []) or []:
            memory = ((container.get("memory") or {}).get("workingSetBytes"))
            if memory is not None:
                records.append(_metric_record(timestamp, service, pod, service, "memory.usage", float(memory), incident_id, "kubelet_summary"))
    return records


def collect_window_metrics(
    namespace: str,
    frontend_url: str,
    requests_per_window: int,
    request_timeout_sec: int,
    incident_id: str,
    timestamp: float | None = None,
    window_size_sec: int | float = 5,
    node_name: str | None = None,
) -> tuple[list[dict], dict]:
    """Collect one P2A-1R metrics window as MetricRecord-compatible dictionaries."""

    start = time.time()
    pods = get_pods(namespace)
    node = node_name or next((pod.get("node") for pod in pods if pod.get("node")), "proberca-ob-control-plane")
    prev_text = get_cadvisor_metrics(node)
    prev_snapshot = build_cadvisor_service_snapshot(parse_prometheus_text_metrics(prev_text), pods)

    frontend = curl_frontend_requests(frontend_url, requests_per_window, request_timeout_sec)
    elapsed = time.time() - start
    remaining = max(0.0, float(window_size_sec) - elapsed)
    if remaining > 0:
        time.sleep(remaining)

    ts = float(timestamp if timestamp is not None else time.time())
    curr_text = get_cadvisor_metrics(node)
    curr_snapshot = build_cadvisor_service_snapshot(parse_prometheus_text_metrics(curr_text), pods)
    records = compute_cadvisor_window_metrics(prev_snapshot, curr_snapshot, window_size_sec, timestamp=ts, incident_id=incident_id)

    summary_available = False
    summary_error = ""
    if not any(record.get("metric") == "memory.usage" for record in records):
        try:
            summary = get_kubelet_summary(node)
            summary_available = True
            records.extend(_summary_fallback_records(summary, pods, ts, incident_id))
        except Exception as exc:  # noqa: BLE001 - best-effort fallback only.
            summary_error = str(exc)

    for metric_name, value in [
        ("request.rps", frontend["rps"]),
        ("request.error_rate", frontend["error_rate"]),
        ("request.p50_latency_ms", frontend["p50_latency_ms"]),
        ("request.p95_latency_ms", frontend["p95_latency_ms"]),
        ("request.p99_latency_ms", frontend["p99_latency_ms"]),
    ]:
        records.append(_metric_record(ts, "frontend", "frontend", "frontend", metric_name, value, incident_id, "real_kubernetes_minimal"))

    meta = {
        "timestamp": ts,
        "pods_count": len(pods),
        "node_name": node,
        "cadvisor_metrics_available": True,
        "cadvisor_records_prev": len(parse_prometheus_text_metrics(prev_text)),
        "cadvisor_records_curr": len(parse_prometheus_text_metrics(curr_text)),
        "kubelet_summary_available": summary_available,
        "kubelet_summary_error": summary_error,
        "cgroup_cpu_stat_available": False,
        "frontend_summary": {k: v for k, v in frontend.items() if k != "samples"},
    }
    return records, meta


def write_json(path: str | Path, payload: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_jsonl(path: str | Path, records: list[dict]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

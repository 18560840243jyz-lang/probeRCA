"""I/O fault helpers for Online Boutique P2C-0 smoke tests."""

from __future__ import annotations

import json
import re
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.metrics import curl_frontend_requests, get_cadvisor_metrics, get_pods

SUPPORTED_FS_METRICS = {
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total",
    "container_fs_reads_total",
    "container_fs_writes_total",
    "container_fs_io_time_seconds_total",
    "container_memory_working_set_bytes",
}


def run_cmd(cmd: list[str], timeout: int = 30) -> tuple[int, str, str]:
    completed = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, check=False)
    return completed.returncode, completed.stdout, completed.stderr


def get_target_pod(namespace: str, service: str) -> dict[str, Any]:
    code, stdout, stderr = run_cmd(["kubectl", "get", "pod", "-n", namespace, "-l", f"app={service}", "-o", "json"], timeout=30)
    if code != 0:
        raise RuntimeError(f"kubectl get pod failed: {stderr}")
    data = json.loads(stdout)
    items = data.get("items", [])
    if not items:
        raise RuntimeError(f"no pod found for app={service} in namespace={namespace}")
    item = items[0]
    status = item.get("status", {})
    spec = item.get("spec", {})
    conditions = status.get("conditions", [])
    ready = status.get("phase") == "Running" and any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
    return {
        "name": item.get("metadata", {}).get("name", ""),
        "pod_ip": status.get("podIP", ""),
        "node_name": spec.get("nodeName", ""),
        "phase": status.get("phase", ""),
        "ready": bool(ready),
    }


def check_pod_io_tools(namespace: str, deployment: str) -> dict[str, Any]:
    script = """
which sh; which dd || true; which sync || true; id; df -h /tmp /data 2>/dev/null || df -h /tmp;
(touch /tmp/proberca_io_write_check && rm -f /tmp/proberca_io_write_check && echo TMP_WRITABLE=1) 2>/dev/null || echo TMP_WRITABLE=0;
(touch /data/proberca_io_write_check && rm -f /data/proberca_io_write_check && echo DATA_WRITABLE=1) 2>/dev/null || echo DATA_WRITABLE=0
""".strip()
    cmd = ["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "--", "sh", "-c", script]
    code, stdout, stderr = run_cmd(cmd, timeout=30)
    return {
        "returncode": code,
        "stderr": stderr,
        "raw_output": stdout,
        "sh_available": "/sh" in stdout or "sh" in stdout.splitlines()[:1],
        "dd_available": bool(re.search(r"/(bin|usr/bin)/dd", stdout)),
        "sync_available": bool(re.search(r"/(bin|usr/bin)/sync", stdout)),
        "tmp_available": "/tmp" in stdout or "overlay" in stdout,
        "data_available": "/data" in stdout,
        "tmp_writable": "TMP_WRITABLE=1" in stdout,
        "data_writable": "DATA_WRITABLE=1" in stdout,
    }




def _select_effective_temp_file(namespace: str, deployment: str, container: str, requested_temp_file: str) -> tuple[str, str]:
    check_script = f"touch {requested_temp_file}.check 2>/dev/null && rm -f {requested_temp_file}.check"
    code, _stdout, _stderr = run_cmd(["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", check_script], timeout=15)
    if code == 0:
        return requested_temp_file, "requested_path_writable"
    fallback = "/data/" + Path(requested_temp_file).name
    check_fallback = f"touch {fallback}.check 2>/dev/null && rm -f {fallback}.check"
    code2, _stdout2, stderr2 = run_cmd(["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", check_fallback], timeout=15)
    if code2 == 0:
        return fallback, "requested_path_read_only_used_data_fallback"
    raise RuntimeError(f"neither requested temp file nor /data fallback is writable: requested={requested_temp_file}, fallback={fallback}, stderr={stderr2}")


def start_io_stress(namespace: str, deployment: str, container: str, temp_file: str, block_size: str, block_count: int, duration_sec: int, log_path: str | Path | None = None) -> dict[str, Any]:
    target_log = Path(log_path) if log_path is not None else Path("/tmp/proberca_io_stress.log")
    target_log.parent.mkdir(parents=True, exist_ok=True)
    effective_temp_file, temp_file_reason = _select_effective_temp_file(namespace, deployment, container, temp_file)
    script = f"""
end=$(( $(date +%s) + {int(duration_sec)} ));
while [ $(date +%s) -lt $end ]; do
  if dd if=/dev/zero of={effective_temp_file} bs={block_size} count={int(block_count)} conv=fsync 2>&1; then
    sync || true;
  else
    dd if=/dev/zero of={effective_temp_file} bs={block_size} count={int(block_count)} 2>&1;
    sync || true;
  fi;
  rm -f {effective_temp_file};
done
""".strip()
    handle = target_log.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", script],
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    handle.close()
    time.sleep(1)
    return {"started": proc.poll() is None, "pid": proc.pid, "log_path": str(target_log), "command": script, "requested_temp_file": temp_file, "effective_temp_file": effective_temp_file, "temp_file_reason": temp_file_reason, "process": proc}


def cleanup_io_stress(namespace: str, deployment: str, container: str, temp_file: str) -> dict[str, Any]:
    fallback = "/data/" + Path(temp_file).name
    code, stdout, stderr = run_cmd(["kubectl", "exec", "-n", namespace, f"deploy/{deployment}", "-c", container, "--", "sh", "-c", f"rm -f {temp_file} {fallback}"], timeout=30)
    return {"returncode": code, "stdout": stdout, "stderr": stderr, "cleaned": code == 0, "requested_temp_file": temp_file, "fallback_temp_file": fallback}


def _parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        labels[match.group(1)] = match.group(2).replace(r'\"', '"').replace(r'\\', '\\')
    return labels


def parse_prometheus_text_metrics(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    line_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)(?:\s+\d+)?$")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = line_re.match(line)
        if not match:
            continue
        name = match.group(1)
        if name not in SUPPORTED_FS_METRICS:
            continue
        labels = _parse_labels(match.group(2) or "")
        if labels.get("namespace") != "online-boutique":
            continue
        if not labels.get("namespace") or not labels.get("pod") or not labels.get("container"):
            continue
        records.append({"name": name, "labels": labels, "value": float(match.group(3))})
    return records


def _infer_service_from_pod_name(pod_name: str) -> str:
    for suffix in ["-", "."]:
        if suffix in pod_name:
            parts = pod_name.split(suffix)
            if len(parts) >= 3:
                return "-".join(parts[:-2])
    return pod_name


def _pod_service_map(pods: list[dict[str, Any]]) -> dict[str, str]:
    return {str(pod.get("name", "")): str(pod.get("service") or (pod.get("labels") or {}).get("app") or _infer_service_from_pod_name(str(pod.get("name", "")))) for pod in pods}


def build_cadvisor_fs_snapshot(cadvisor_records: list[dict[str, Any]], pods: list[dict[str, Any]]) -> dict[str, Any]:
    pod_to_service = _pod_service_map(pods)
    metric_map = {
        "container_fs_reads_bytes_total": "fs_reads_bytes_total",
        "container_fs_writes_bytes_total": "fs_writes_bytes_total",
        "container_fs_reads_total": "fs_reads_total",
        "container_fs_writes_total": "fs_writes_total",
        "container_fs_io_time_seconds_total": "fs_io_time_seconds_total",
        "container_memory_working_set_bytes": "memory_working_set_bytes",
    }
    snapshot: dict[str, Any] = {}
    for record in cadvisor_records:
        labels = record.get("labels", {})
        pod = labels.get("pod")
        container = labels.get("container")
        if not pod or not container:
            continue
        service = pod_to_service.get(pod) or _infer_service_from_pod_name(pod)
        key = f"{service}/{pod}/{container}/{labels.get('device', '')}"
        entry = snapshot.setdefault(key, {"service": service, "pod": pod, "container": container, "device": labels.get("device", "")})
        field = metric_map.get(record.get("name"))
        if field:
            entry[field] = float(record.get("value", 0.0))
    return snapshot


def _delta(prev: dict[str, Any], curr: dict[str, Any], field: str) -> float | None:
    if field not in prev or field not in curr:
        return None
    value = float(curr[field]) - float(prev[field])
    if value < 0:
        return None
    return value


def compute_fs_delta(prev_snapshot: dict[str, Any], curr_snapshot: dict[str, Any], window_size_sec: int | float) -> dict[str, Any]:
    deltas: dict[str, Any] = {}
    for key, curr in curr_snapshot.items():
        prev = prev_snapshot.get(key)
        if not prev:
            continue
        metric_values: dict[str, float] = {}
        mapping = {
            "io.read_bytes": "fs_reads_bytes_total",
            "io.write_bytes": "fs_writes_bytes_total",
            "io.read_ops": "fs_reads_total",
            "io.write_ops": "fs_writes_total",
            "io.io_time_ms": "fs_io_time_seconds_total",
        }
        for metric, field in mapping.items():
            value = _delta(prev, curr, field)
            if value is None:
                continue
            if metric == "io.io_time_ms":
                value *= 1000.0
            metric_values[metric] = float(value)
        if metric_values:
            deltas[key] = {"service": curr.get("service"), "pod": curr.get("pod"), "container": curr.get("container"), "device": curr.get("device"), "metrics": metric_values}
    return deltas


def summarize_fs_metric(delta: dict[str, Any], service: str, metric: str) -> float:
    total = 0.0
    for entry in delta.values():
        if entry.get("service") != service:
            continue
        total += float((entry.get("metrics") or {}).get(metric, 0.0))
    return total


def collect_fs_snapshot(namespace: str, node_name: str = "proberca-ob-control-plane") -> dict[str, Any]:
    pods = get_pods(namespace)
    text = get_cadvisor_metrics(node_name)
    return build_cadvisor_fs_snapshot(parse_prometheus_text_metrics(text), pods)


def curl_frontend(frontend_url: str, requests: int, timeout_sec: int) -> dict[str, Any]:
    return curl_frontend_requests(frontend_url, requests, timeout_sec)

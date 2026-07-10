"""Lock contention sidecar helpers for Online Boutique P2D-0 smoke."""

from __future__ import annotations

import json
import shlex
import statistics
import subprocess
from typing import Any

from proberca.adapters.online_boutique.metrics import curl_frontend_requests


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
    containers = [c.get("name", "") for c in (item.get("spec", {}) or {}).get("containers", [])]
    return {
        "name": item.get("metadata", {}).get("name", ""),
        "pod_ip": status.get("podIP", ""),
        "node_name": spec.get("nodeName", ""),
        "phase": status.get("phase", ""),
        "ready": bool(ready),
        "containers": containers,
    }


def ensure_sidecar_image_loaded(image: str, cluster_name: str) -> dict[str, Any]:
    code, stdout, stderr = run_cmd(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], timeout=30)
    if code != 0:
        raise RuntimeError(f"docker images failed: {stderr}")
    local_present = image in set(stdout.splitlines())
    pull_result: dict[str, Any] | None = None
    if not local_present:
        pull_code, pull_stdout, pull_stderr = run_cmd(["docker", "pull", image], timeout=300)
        pull_result = {"returncode": pull_code, "stdout": pull_stdout, "stderr": pull_stderr}
        if pull_code != 0:
            raise RuntimeError(f"docker pull failed for {image}: {pull_stderr}")
    load_code, load_stdout, load_stderr = run_cmd(["kind", "load", "docker-image", image, "--name", cluster_name], timeout=300)
    platform_pull_result: dict[str, Any] | None = None
    platform_load_result: dict[str, Any] | None = None
    if load_code != 0:
        # Docker may keep a multi-platform manifest whose child content is not fully available
        # to kind import. Re-pull the same approved image for the VM/node architecture only.
        platform_pull_code, platform_pull_stdout, platform_pull_stderr = run_cmd(["docker", "pull", "--platform", "linux/amd64", image], timeout=300)
        platform_pull_result = {"returncode": platform_pull_code, "stdout": platform_pull_stdout, "stderr": platform_pull_stderr}
        if platform_pull_code != 0:
            raise RuntimeError(f"docker pull --platform linux/amd64 failed for {image}: {platform_pull_stderr}")
        load_code, load_stdout, load_stderr = run_cmd(["kind", "load", "docker-image", image, "--name", cluster_name], timeout=300)
        platform_load_result = {"returncode": load_code, "stdout": load_stdout, "stderr": load_stderr}
    manual_import_result: dict[str, Any] | None = None
    if load_code != 0:
        manual_cmd = f"docker save {shlex.quote(image)} | docker exec --privileged -i {shlex.quote(cluster_name + '-control-plane')} ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -"
        manual_code, manual_stdout, manual_stderr = run_cmd(["bash", "-lc", manual_cmd], timeout=300)
        manual_import_result = {"returncode": manual_code, "stdout": manual_stdout, "stderr": manual_stderr}
        if manual_code == 0:
            load_code = 0
    return {
        "image": image,
        "cluster_name": cluster_name,
        "local_present_before": local_present,
        "pulled": not local_present,
        "pull_result": pull_result,
        "platform_pull_result": platform_pull_result,
        "platform_load_result": platform_load_result,
        "manual_import_result": manual_import_result,
        "kind_load_returncode": load_code,
        "kind_load_stdout": load_stdout,
        "kind_load_stderr": load_stderr,
        "image_loaded": load_code == 0,
        "local_image_available": True,
        "node_pull_fallback_allowed": load_code != 0,
    }



def build_phaseaware_lockstress_python_command(
    window_size_sec: int,
    baseline_windows: int,
    faulty_windows: int,
    recovery_windows: int,
    workers: int,
    lock_hold_ms: int,
) -> str:
    """Build a sidecar command that emits real phase-aware lock measurements."""

    script = f"""
import json
import statistics
import threading
import time

WINDOW_SIZE_SEC = {int(window_size_sec)}
BASELINE_WINDOWS = {int(baseline_windows)}
FAULTY_WINDOWS = {int(faulty_windows)}
RECOVERY_WINDOWS = {int(recovery_windows)}
WORKERS = {int(workers)}
LOCK_HOLD_SEC = {float(lock_hold_ms) / 1000.0!r}
lock = threading.Lock()
active = threading.Event()
stop = threading.Event()
waits = []
waits_lock = threading.Lock()

def worker(worker_id):
    while not stop.is_set():
        if not active.is_set():
            time.sleep(0.01)
            continue
        started = time.perf_counter()
        with lock:
            waited_ms = (time.perf_counter() - started) * 1000.0
            with waits_lock:
                waits.append(waited_ms)
            time.sleep(LOCK_HOLD_SEC)

threads = [threading.Thread(target=worker, args=(idx,), daemon=True) for idx in range(WORKERS)]
for thread in threads:
    thread.start()

def percentile(values, percent):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percent / 100.0))
    index = max(0, min(index, len(ordered) - 1))
    return float(ordered[index])

def emit_window(phase, window_index, lock_active):
    with waits_lock:
        window_waits = list(waits)
        waits.clear()
    payload = {{
        "timestamp": time.time(),
        "phase": phase,
        "window_index": int(window_index),
        "lock_wait_ms_sum": float(sum(window_waits)) if window_waits else 0.0,
        "lock_wait_ms_mean": float(statistics.mean(window_waits)) if window_waits else 0.0,
        "lock_wait_ms_p95": percentile(window_waits, 95.0),
        "lock_contention_count": int(sum(1 for value in window_waits if value > 0.01)),
        "workers": WORKERS,
        "lock_active": bool(lock_active),
        "source": "real_phaseaware_sidecar_lockstress",
    }}
    print(json.dumps(payload, sort_keys=True), flush=True)

schedule = (["baseline"] * BASELINE_WINDOWS) + (["faulty"] * FAULTY_WINDOWS) + (["recovery"] * RECOVERY_WINDOWS)
for index, phase in enumerate(schedule, start=1):
    lock_active = phase == "faulty"
    if lock_active:
        active.set()
    else:
        active.clear()
        with waits_lock:
            waits.clear()
    time.sleep(WINDOW_SIZE_SEC)
    emit_window(phase, index, lock_active)
active.clear()
stop.set()
for thread in threads:
    thread.join(timeout=1.0)
print(json.dumps({{"timestamp": time.time(), "final": True, "source": "real_phaseaware_sidecar_lockstress"}}, sort_keys=True), flush=True)
""".strip()
    return "python3 -u -c " + shlex.quote(script) + "; sleep 3600"


def build_lockstress_python_command(duration_sec: int, workers: int, lock_hold_ms: int) -> str:
    script = f"""
import json
import statistics
import threading
import time

DURATION_SEC = {int(duration_sec)}
WORKERS = {int(workers)}
LOCK_HOLD_SEC = {float(lock_hold_ms) / 1000.0!r}
lock = threading.Lock()
stop = threading.Event()
waits = []
waits_lock = threading.Lock()

def worker(worker_id):
    while not stop.is_set():
        started = time.perf_counter()
        with lock:
            waited_ms = (time.perf_counter() - started) * 1000.0
            with waits_lock:
                waits.append(waited_ms)
            time.sleep(LOCK_HOLD_SEC)

threads = [threading.Thread(target=worker, args=(idx,), daemon=True) for idx in range(WORKERS)]
for thread in threads:
    thread.start()
start = time.time()
last = start
while time.time() - start < DURATION_SEC:
    time.sleep(1.0)
    now = time.time()
    if now - last >= 5.0:
        with waits_lock:
            current = list(waits)
        if current:
            payload = {{
                "timestamp": now,
                "lock_wait_ms_sum": float(sum(current)),
                "lock_wait_ms_mean": float(statistics.mean(current)),
                "lock_wait_ms_p95": float(statistics.quantiles(current, n=100)[94]) if len(current) >= 100 else float(max(current)),
                "lock_contention_count": int(sum(1 for value in current if value > 0.01)),
                "workers": WORKERS,
                "source": "real_sidecar_lockstress",
            }}
            print(json.dumps(payload, sort_keys=True), flush=True)
        last = now
stop.set()
for thread in threads:
    thread.join(timeout=1.0)
with waits_lock:
    final = list(waits)
payload = {{
    "timestamp": time.time(),
    "lock_wait_ms_sum": float(sum(final)) if final else 0.0,
    "lock_wait_ms_mean": float(statistics.mean(final)) if final else 0.0,
    "lock_wait_ms_p95": float(statistics.quantiles(final, n=100)[94]) if len(final) >= 100 else (float(max(final)) if final else 0.0),
    "lock_contention_count": int(sum(1 for value in final if value > 0.01)),
    "workers": WORKERS,
    "source": "real_sidecar_lockstress",
    "final": True,
}}
print(json.dumps(payload, sort_keys=True), flush=True)
""".strip()
    return "python3 -u -c " + shlex.quote(script) + "; sleep 3600"


def _get_deployment(namespace: str, deployment: str) -> dict[str, Any]:
    code, stdout, stderr = run_cmd(["kubectl", "get", "deploy", deployment, "-n", namespace, "-o", "json"], timeout=30)
    if code != 0:
        raise RuntimeError(f"kubectl get deployment failed: {stderr}")
    return json.loads(stdout)


def _container_names(deployment_json: dict[str, Any]) -> list[str]:
    return [str(c.get("name", "")) for c in deployment_json.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])]


def patch_cartservice_add_lockstress_sidecar(namespace: str, deployment: str, sidecar_name: str, image: str, command: str) -> dict[str, Any]:
    before = _get_deployment(namespace, deployment)
    names = _container_names(before)
    if sidecar_name in names:
        raise RuntimeError(f"sidecar already exists in deployment/{deployment}: {sidecar_name}")
    sidecar = {
        "name": sidecar_name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-c", command],
        "resources": {
            "requests": {"cpu": "100m", "memory": "64Mi"},
            "limits": {"cpu": "500m", "memory": "128Mi"},
        },
    }
    patch = json.dumps([{"op": "add", "path": "/spec/template/spec/containers/-", "value": sidecar}])
    code, stdout, stderr = run_cmd(["kubectl", "patch", f"deployment/{deployment}", "-n", namespace, "--type=json", "-p", patch], timeout=60)
    if code != 0:
        raise RuntimeError(f"kubectl patch add sidecar failed: {stderr}")
    rollout_code, rollout_stdout, rollout_stderr = run_cmd(["kubectl", "rollout", "status", f"deployment/{deployment}", "-n", namespace, "--timeout=180s"], timeout=210)
    if rollout_code != 0:
        raise RuntimeError(f"deployment rollout after sidecar add failed: {rollout_stderr}")
    after = _get_deployment(namespace, deployment)
    return {
        "sidecar_injected": sidecar_name in _container_names(after),
        "patch_stdout": stdout,
        "patch_stderr": stderr,
        "rollout_stdout": rollout_stdout,
        "rollout_stderr": rollout_stderr,
        "containers_before": names,
        "containers_after": _container_names(after),
    }


def remove_lockstress_sidecar(namespace: str, deployment: str, sidecar_name: str) -> dict[str, Any]:
    current = _get_deployment(namespace, deployment)
    names = _container_names(current)
    if sidecar_name not in names:
        return {"sidecar_removed": True, "sidecar_was_present": False, "containers_before": names, "containers_after": names}
    index = names.index(sidecar_name)
    patch = json.dumps([{"op": "remove", "path": f"/spec/template/spec/containers/{index}"}])
    code, stdout, stderr = run_cmd(["kubectl", "patch", f"deployment/{deployment}", "-n", namespace, "--type=json", "-p", patch], timeout=60)
    if code != 0:
        raise RuntimeError(f"kubectl patch remove sidecar failed: {stderr}")
    rollout_code, rollout_stdout, rollout_stderr = run_cmd(["kubectl", "rollout", "status", f"deployment/{deployment}", "-n", namespace, "--timeout=180s"], timeout=210)
    if rollout_code != 0:
        raise RuntimeError(f"deployment rollout after sidecar remove failed: {rollout_stderr}")
    after = _get_deployment(namespace, deployment)
    after_names = _container_names(after)
    return {
        "sidecar_removed": sidecar_name not in after_names,
        "sidecar_was_present": True,
        "patch_stdout": stdout,
        "patch_stderr": stderr,
        "rollout_stdout": rollout_stdout,
        "rollout_stderr": rollout_stderr,
        "containers_before": names,
        "containers_after": after_names,
    }


def get_sidecar_logs(namespace: str, deployment: str, sidecar_name: str, pod_name: str | None = None) -> str:
    target_pod = pod_name
    if not target_pod:
        pod = get_target_pod(namespace, deployment)
        target_pod = str(pod["name"])
    code, stdout, stderr = run_cmd(["kubectl", "logs", "-n", namespace, target_pod, "-c", sidecar_name, "--tail=200"], timeout=30)
    if code != 0:
        return f"STDERR: {stderr}\n"
    return stdout


def _first_float(payload: dict[str, Any], names: list[str], default: float = 0.0) -> float:
    for name in names:
        if name in payload and payload.get(name) is not None:
            try:
                return float(payload.get(name))
            except (TypeError, ValueError):
                continue
    return float(default)


def _is_lockstress_source(payload: dict[str, Any]) -> bool:
    return payload.get("source") in {"real_sidecar_lockstress", "real_phaseaware_sidecar_lockstress"}


def parse_lockstress_logs(log_text: str) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for raw in log_text.splitlines():
        line = raw.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not _is_lockstress_source(payload):
            continue
        if payload.get("final") is True and payload.get("source") == "real_phaseaware_sidecar_lockstress":
            continue
        records.append(payload)
    empty = {
        "lock_wait_ms_sum_total": 0.0,
        "lock_wait_ms_mean_avg": 0.0,
        "lock_wait_ms_p95_max": 0.0,
        "lock_contention_count_total": 0,
        "records_count": 0,
        "lock_metrics_available": False,
        "p95_parse_warning": False,
        "baseline_records_count": 0,
        "faulty_records_count": 0,
        "recovery_records_count": 0,
        "baseline_lock_wait_ms_sum_total": 0.0,
        "faulty_lock_wait_ms_sum_total": 0.0,
        "recovery_lock_wait_ms_sum_total": 0.0,
        "faulty_lock_contention_count_total": 0,
        "phaseaware_metrics_available": False,
        "records": [],
    }
    if not records:
        return empty

    phase_records = {"baseline": [], "faulty": [], "recovery": []}
    for row in records:
        phase = str(row.get("phase", ""))
        if phase in phase_records:
            phase_records[phase].append(row)

    means = [_first_float(row, ["lock_wait_ms_mean", "lock_wait_mean_ms", "wait_ms_mean"]) for row in records]
    p95_values = [_first_float(row, ["lock_wait_ms_p95", "lock_wait_p95_ms", "p95_wait_ms", "wait_ms_p95"]) for row in records]
    mean_avg = float(statistics.mean(means)) if means else 0.0
    p95_max = float(max(p95_values)) if p95_values else 0.0
    phaseaware = any(row.get("source") == "real_phaseaware_sidecar_lockstress" for row in records)
    faulty_records = phase_records["faulty"]
    faulty_warning_count = 0
    for row in faulty_records:
        p95 = _first_float(row, ["lock_wait_ms_p95", "lock_wait_p95_ms", "p95_wait_ms", "wait_ms_p95"])
        mean = _first_float(row, ["lock_wait_ms_mean", "lock_wait_mean_ms", "wait_ms_mean"])
        if p95 > 0.0 and p95 < mean:
            faulty_warning_count += 1
    p95_warning = bool(faulty_records and faulty_warning_count > (len(faulty_records) / 2.0))
    if not phaseaware and p95_max < mean_avg and p95_max > 0.0:
        p95_warning = True

    def sum_wait(rows: list[dict[str, Any]]) -> float:
        return float(sum(_first_float(row, ["lock_wait_ms_sum", "lock_wait_sum_ms", "wait_ms_sum"]) for row in rows))

    def sum_count(rows: list[dict[str, Any]]) -> int:
        return int(sum(_first_float(row, ["lock_contention_count", "contention_count"]) for row in rows))

    total_wait = sum_wait(records)
    total_count = sum_count(records)
    # Legacy sidecar records are cumulative; prefer the final cumulative record for totals.
    if not phaseaware:
        aggregate_record = records[-1]
        total_wait = _first_float(aggregate_record, ["lock_wait_ms_sum", "lock_wait_sum_ms", "wait_ms_sum"])
        total_count = int(_first_float(aggregate_record, ["lock_contention_count", "contention_count"]))

    return {
        "lock_wait_ms_sum_total": float(total_wait),
        "lock_wait_ms_mean_avg": mean_avg,
        "lock_wait_ms_p95_max": p95_max,
        "lock_contention_count_total": int(total_count),
        "records_count": len(records),
        "lock_metrics_available": True,
        "p95_parse_warning": p95_warning,
        "baseline_records_count": len(phase_records["baseline"]),
        "faulty_records_count": len(phase_records["faulty"]),
        "recovery_records_count": len(phase_records["recovery"]),
        "baseline_lock_wait_ms_sum_total": sum_wait(phase_records["baseline"]),
        "faulty_lock_wait_ms_sum_total": sum_wait(phase_records["faulty"]),
        "recovery_lock_wait_ms_sum_total": sum_wait(phase_records["recovery"]),
        "faulty_lock_contention_count_total": sum_count(phase_records["faulty"]),
        "phaseaware_metrics_available": phaseaware,
        "records": records,
    }

def curl_frontend(frontend_url: str, requests: int, timeout_sec: int) -> dict[str, Any]:
    result = curl_frontend_requests(frontend_url, requests, timeout_sec)
    samples = result.get("samples") or []
    result["http_ok"] = float(result.get("error_rate", 1.0)) == 0.0 and (not samples or any(int(item.get("http_code", 0)) // 100 == 2 for item in samples))
    return result


def evaluate_lock_fault_feasible(summary: dict[str, Any]) -> dict[str, Any]:
    failed: list[str] = []
    if summary.get("sidecar_injected") is not True:
        failed.append("sidecar_injected != true")
    if summary.get("sidecar_removed") is not True:
        failed.append("sidecar_removed != true")
    if summary.get("frontend_after_http_ok") is not True:
        failed.append("frontend_after_http_ok != true")
    if summary.get("lock_metrics_available") is not True:
        failed.append("lock_metrics_available != true")
    if float(summary.get("lock_contention_count_total", 0.0)) <= 0.0:
        failed.append("lock_contention_count_total <= 0")
    if float(summary.get("lock_wait_ms_sum_total", 0.0)) <= 0.0:
        failed.append("lock_wait_ms_sum_total <= 0")
    return {"lock_fault_feasible": not failed, "failed_checks": failed}

"""Network namespace helpers for Online Boutique real network smoke tests."""

from __future__ import annotations

import json
import re
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any


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
    conditions = status.get("conditions", [])
    ready = status.get("phase") == "Running" and any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
    return {
        "name": item.get("metadata", {}).get("name", ""),
        "pod_ip": status.get("podIP", ""),
        "node_name": status.get("hostIP", ""),
        "phase": status.get("phase", ""),
        "ready": bool(ready),
    }


def get_pod_sandbox_id(kind_node_container: str, namespace: str, pod_name: str) -> str:
    code, stdout, stderr = run_cmd([
        "docker", "exec", kind_node_container, "crictl", "pods", "--namespace", namespace, "--name", pod_name, "-o", "json"
    ], timeout=30)
    if code != 0:
        raise RuntimeError(f"crictl pods failed: {stderr}")
    data = json.loads(stdout)
    pods = data.get("items") or data.get("pods") or []
    for pod in pods:
        meta = pod.get("metadata", {})
        if meta.get("name") == pod_name:
            return str(pod.get("id") or pod.get("sandboxId") or "")
    if pods:
        pod = pods[0]
        return str(pod.get("id") or pod.get("sandboxId") or "")
    raise RuntimeError(f"no sandbox found for pod {pod_name}")


def get_pod_netns_pid(kind_node_container: str, sandbox_id: str) -> int:
    code, stdout, stderr = run_cmd(["docker", "exec", kind_node_container, "crictl", "inspectp", sandbox_id], timeout=30)
    if code != 0:
        raise RuntimeError(f"crictl inspectp failed: {stderr}")
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        Path("data/p2_online_boutique/inspectp_debug.txt").parent.mkdir(parents=True, exist_ok=True)
        Path("data/p2_online_boutique/inspectp_debug.txt").write_text(stdout, encoding="utf-8")
        raise RuntimeError("inspectp output is not JSON; saved debug output") from exc
    candidates = [
        data.get("info", {}).get("pid"),
        data.get("status", {}).get("pid"),
        data.get("pid"),
    ]
    for candidate in candidates:
        if candidate not in (None, ""):
            return int(candidate)
    Path("data/p2_online_boutique/inspectp_debug.json").parent.mkdir(parents=True, exist_ok=True)
    Path("data/p2_online_boutique/inspectp_debug.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    raise RuntimeError("inspectp JSON did not contain info.pid; saved debug output")


def run_in_pod_netns(kind_node_container: str, pid: int, args: list[str], timeout: int = 30) -> tuple[int, str, str]:
    return run_cmd(["docker", "exec", kind_node_container, "nsenter", "-t", str(pid), "-n", *args], timeout=timeout)


def check_tc_available(kind_node_container: str) -> bool:
    return run_cmd(["docker", "exec", kind_node_container, "which", "tc"], timeout=10)[0] == 0


def get_tc_qdisc(kind_node_container: str, pid: int, device: str = "eth0") -> str:
    return run_in_pod_netns(kind_node_container, pid, ["tc", "qdisc", "show", "dev", device], timeout=30)[1]


def apply_netem_fault(kind_node_container: str, pid: int, device: str, delay_ms: int, jitter_ms: int, loss_percent: float) -> dict[str, Any]:
    args = ["tc", "qdisc", "replace", "dev", device, "root", "netem", "delay", f"{delay_ms}ms", f"{jitter_ms}ms", "loss", f"{loss_percent}%"]
    code, stdout, stderr = run_in_pod_netns(kind_node_container, pid, args, timeout=30)
    return {"returncode": code, "stdout": stdout, "stderr": stderr, "applied": code == 0, "command": args}


def clear_netem_fault(kind_node_container: str, pid: int, device: str) -> dict[str, Any]:
    args = ["tc", "qdisc", "del", "dev", device, "root"]
    code, stdout, stderr = run_in_pod_netns(kind_node_container, pid, args, timeout=30)
    benign = code != 0 and ("No such file" in stderr or "Invalid argument" in stderr or "Cannot delete" in stderr)
    return {"returncode": code, "stdout": stdout, "stderr": stderr, "restored": code == 0 or benign, "command": args}


def parse_proc_net_snmp(text: str) -> dict[str, Any]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    result: dict[str, Any] = {}
    i = 0
    while i + 1 < len(lines):
        header = lines[i]
        values = lines[i + 1]
        if header.startswith("Tcp:") and values.startswith("Tcp:"):
            keys = header.split()[1:]
            vals = values.split()[1:]
            tcp = {key: int(float(value)) for key, value in zip(keys, vals)}
            for key in ["RetransSegs", "OutSegs", "InSegs", "ActiveOpens", "PassiveOpens"]:
                result[key] = tcp.get(key, 0)
        i += 2
    return result


def collect_proc_net_snmp(kind_node_container: str, pid: int) -> dict[str, Any]:
    code, stdout, stderr = run_in_pod_netns(kind_node_container, pid, ["cat", "/proc/net/snmp"], timeout=30)
    return {"available": code == 0, "returncode": code, "stderr": stderr, "raw": stdout, "tcp": parse_proc_net_snmp(stdout) if code == 0 else {}}


def parse_ss_rtt(text: str) -> dict[str, Any]:
    values: list[float] = []
    for match in re.finditer(r"rtt:([0-9.]+)(?:/([0-9.]+))?", text):
        try:
            values.append(float(match.group(1)))
        except ValueError:
            pass
    if not values:
        return {"available": False, "rtt_ms": None, "samples": 0}
    return {"available": True, "rtt_ms": float(statistics.mean(values)), "samples": len(values), "min_rtt_ms": min(values), "max_rtt_ms": max(values)}


def collect_ss_rtt(kind_node_container: str, pid: int) -> dict[str, Any]:
    code, stdout, stderr = run_in_pod_netns(kind_node_container, pid, ["ss", "-tin"], timeout=30)
    parsed = parse_ss_rtt(stdout if code == 0 else "")
    parsed.update({"returncode": code, "stderr": stderr, "raw": stdout})
    return parsed


def curl_frontend(frontend_url: str, requests: int, timeout_sec: int) -> dict[str, Any]:
    latencies: list[float] = []
    errors = 0
    start = time.time()
    for _ in range(int(requests)):
        code, stdout, stderr = run_cmd(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{time_total}", "--max-time", str(timeout_sec), frontend_url], timeout=timeout_sec + 2)
        if code != 0:
            errors += 1
            continue
        parts = stdout.strip().split()
        if len(parts) != 2 or not parts[0].startswith("2"):
            errors += 1
            continue
        try:
            latencies.append(float(parts[1]) * 1000.0)
        except ValueError:
            errors += 1
    elapsed = max(time.time() - start, 1e-9)
    sorted_lat = sorted(latencies)

    def pct(q: float) -> float | None:
        if not sorted_lat:
            return None
        idx = min(len(sorted_lat) - 1, max(0, int(round((len(sorted_lat) - 1) * q))))
        return float(sorted_lat[idx])

    total = int(requests)
    return {
        "request_count": total,
        "success_count": len(latencies),
        "error_count": errors,
        "error_rate": errors / total if total else 0.0,
        "http_ok": bool(latencies) and errors == 0,
        "p50_latency_ms": pct(0.50),
        "p95_latency_ms": pct(0.95),
        "p99_latency_ms": pct(0.99),
        "rps": total / elapsed,
    }

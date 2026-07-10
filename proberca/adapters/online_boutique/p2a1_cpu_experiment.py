"""P2A-1/P2A-1R real CPU fault injection experiment orchestration."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.cpu_fault import (
    get_deployment_resources,
    inject_cpu_limit_fault,
    restore_deployment_resources,
    wait_deployment_ready,
    write_fault_logs,
)
from proberca.adapters.online_boutique.metrics import (
    collect_window_metrics,
    curl_frontend_requests,
    get_deployments,
    run_cmd,
    write_json,
    write_jsonl,
)
from proberca.adapters.online_boutique.topology import write_online_boutique_service_graph


CPU_INCIDENT_ID = "ob-cpu-paymentservice-001"


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value == "":
        return ""
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _significant_lines(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append((indent, raw.strip()))
    return lines


def _parse_block(lines: list[tuple[int, str]], index: int, indent: int) -> tuple[Any, int]:
    if index < len(lines) and lines[index][1].startswith("- "):
        values = []
        while index < len(lines) and lines[index][0] == indent and lines[index][1].startswith("- "):
            values.append(_parse_scalar(lines[index][1][2:].strip()))
            index += 1
        return values, index
    result: dict[str, Any] = {}
    while index < len(lines):
        current_indent, content = lines[index]
        if current_indent < indent:
            break
        if current_indent > indent:
            break
        if content.startswith("- "):
            break
        key, sep, value = content.partition(":")
        if not sep:
            raise ValueError(f"invalid config line: {content}")
        key = key.strip()
        value = value.strip()
        index += 1
        if value:
            result[key] = _parse_scalar(value)
        else:
            child, index = _parse_block(lines, index, indent + 2)
            result[key] = child
    return result, index


def load_simple_yaml(path: str | Path) -> dict:
    """Load the limited YAML subset used by P2A configs without external deps."""

    lines = _significant_lines(Path(path).read_text(encoding="utf-8"))
    data, index = _parse_block(lines, 0, 0)
    if index != len(lines) or not isinstance(data, dict):
        raise ValueError(f"failed to parse config: {path}")
    return data


def _save_command_output(cmd: list[str], output_path: Path, timeout: int = 30) -> dict:
    code, stdout, stderr = run_cmd(cmd, timeout=timeout)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(stdout + ("\nSTDERR:\n" + stderr if stderr else ""), encoding="utf-8")
    return {"cmd": cmd, "returncode": code, "path": str(output_path), "stderr": stderr}


def _start_port_forward_if_needed(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    if curl_frontend_requests(frontend_url, 1, 3)["error_rate"] == 0.0:
        return None
    log_path = output_dir / "frontend_port_forward.log"
    handle = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"],
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(5)
    if curl_frontend_requests(frontend_url, 1, 3)["error_rate"] != 0.0:
        proc.terminate()
        raise RuntimeError(f"frontend smoke test failed for {frontend_url}")
    return proc


def _check_ready(namespace: str) -> None:
    deployments = get_deployments(namespace)
    not_ready = [item for item in deployments if not item.get("ready")]
    if not deployments:
        raise RuntimeError(f"no deployments found in namespace {namespace}")
    if not_ready:
        raise RuntimeError(f"deployments not ready: {not_ready}")


def _incident_id_from_experiment_id(experiment_id: str) -> str:
    if experiment_id.startswith("ob_cpu_paymentservice_repeat_"):
        suffix = experiment_id.rsplit("_", 1)[-1]
        return f"ob-cpu-paymentservice-repeat-{suffix}"
    if experiment_id.startswith("ob_cpu_paymentservice_001"):
        return experiment_id.replace("ob_cpu_paymentservice_001", CPU_INCIDENT_ID)
    return experiment_id.replace("_", "-")


def build_incident_record(start_ts: float, end_ts: float, config: dict) -> dict:
    exp = config["experiment"]
    return {
        "incident_id": _incident_id_from_experiment_id(str(exp["experiment_id"])),
        "root_service": exp["target_service"],
        "root_metric": exp["target_metric"],
        "root_type": exp["target_fault_type"],
        "symptom_service": exp["symptom_service"],
        "start_ts": float(start_ts),
        "end_ts": float(end_ts),
        "injected_path": [
            "paymentservice.cpu.throttled_usec",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
    }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _phase_values(metrics: list[dict], *, service: str, metric: str, phase: str) -> list[float]:
    values: list[float] = []
    for record in metrics:
        if record.get("service") == service and record.get("metric") == metric and record.get("phase") == phase:
            try:
                values.append(float(record.get("value", 0.0)))
            except (TypeError, ValueError):
                continue
    return values


def _metric_phase_summary(metrics: list[dict]) -> dict:
    payment_baseline = _phase_values(metrics, service="paymentservice", metric="cpu.throttled_usec", phase="baseline")
    payment_faulty = _phase_values(metrics, service="paymentservice", metric="cpu.throttled_usec", phase="faulty")
    frontend_baseline = _phase_values(metrics, service="frontend", metric="request.p99_latency_ms", phase="baseline")
    frontend_faulty = _phase_values(metrics, service="frontend", metric="request.p99_latency_ms", phase="faulty")
    payment_baseline_mean = _mean(payment_baseline)
    payment_faulty_mean = _mean(payment_faulty)
    frontend_baseline_mean = _mean(frontend_baseline)
    frontend_faulty_mean = _mean(frontend_faulty)
    return {
        "paymentservice_baseline_cpu_throttled_usec_mean": payment_baseline_mean,
        "paymentservice_faulty_cpu_throttled_usec_mean": payment_faulty_mean,
        "paymentservice_throttling_lift": max(0.0, payment_faulty_mean - payment_baseline_mean),
        "frontend_baseline_p99_mean": frontend_baseline_mean,
        "frontend_faulty_p99_mean": frontend_faulty_mean,
    }


def _build_evidence(metrics: list[dict], incident: dict) -> tuple[list[dict], dict]:
    baseline_values = _phase_values(metrics, service="paymentservice", metric="cpu.throttled_usec", phase="baseline")
    faulty_values = _phase_values(metrics, service="paymentservice", metric="cpu.throttled_usec", phase="faulty")
    if not baseline_values and not faulty_values:
        return [], {"weak_cpu_evidence": False, "cpu_evidence_reason": "missing_paymentservice_throttled_metric"}

    baseline_mean = _mean(baseline_values)
    faulty_mean = _mean(faulty_values)
    lift = max(0.0, faulty_mean - baseline_mean)
    denom = max(abs(baseline_mean), abs(faulty_mean), 1.0)
    score = max(0.0, min(1.0, lift / denom))
    weak = False
    if score <= 0.0:
        score = 0.05
        weak = True
    return [
        {
            "timestamp": incident["start_ts"],
            "service": "paymentservice",
            "instance": "paymentservice",
            "node": "paymentservice",
            "evidence_type": "CPU",
            "metric": "cpu.throttled_usec",
            "value": float(faulty_mean),
            "evidence_score": float(score),
            "baseline_mean": float(baseline_mean),
            "faulty_mean": float(faulty_mean),
            "lift": float(lift),
            "source": "kubelet_cadvisor",
            "probe_id": "p2a1r_cadvisor_cpu_probe",
            "sampling_rate": 1.0,
            "incident_id": incident["incident_id"],
        }
    ], {"weak_cpu_evidence": weak, "cpu_evidence_reason": "weak_lift" if weak else "throttling_lift"}


def build_data_quality_report(
    metrics: list[dict], metas: list[dict], incident: dict, config: dict, fault_ok: bool, restore_ok: bool
) -> dict:
    services_seen = sorted({str(m["service"]) for m in metrics if m.get("service")})
    metrics_seen = sorted({str(m["metric"]) for m in metrics if m.get("metric")})
    exp = config["experiment"]
    payment_cpu = any(m.get("service") == "paymentservice" and m.get("metric") == "cpu.usage" for m in metrics)
    payment_throttled = any(m.get("service") == "paymentservice" and m.get("metric") == "cpu.throttled_usec" for m in metrics)
    summary = _metric_phase_summary(metrics)
    return {
        "metrics_count": len(metrics),
        "services_seen": services_seen,
        "metrics_seen": metrics_seen,
        "baseline_windows": int(exp["baseline_windows"]),
        "faulty_windows": int(exp["faulty_windows"]),
        "recovery_windows": int(exp["recovery_windows"]),
        "cadvisor_metrics_available": any(meta.get("cadvisor_metrics_available") for meta in metas),
        "paymentservice_cpu_metric_present": payment_cpu,
        "paymentservice_throttled_metric_present": payment_throttled,
        "root_service_metric_coverage_passed": payment_cpu and payment_throttled,
        "frontend_latency_metric_present": any(m.get("service") == "frontend" and m.get("metric") == "request.p99_latency_ms" for m in metrics),
        "cgroup_cpu_stat_available": any(meta.get("cgroup_cpu_stat_available") for meta in metas),
        "fault_injection_succeeded": fault_ok,
        "restore_succeeded": restore_ok,
        "incident_id": incident["incident_id"],
        **summary,
    }


_quality_report = build_data_quality_report


def run_p2a1_cpu_fault_experiment(config_path: str) -> dict:
    """Run P2A-1/P2A-1R CPU fault injection and real metric collection."""

    config = load_simple_yaml(config_path)
    namespace = config["kubernetes"]["namespace"]
    expected_context = config["kubernetes"]["context"]
    exp = config["experiment"]
    fault = config["fault_injection"]
    traffic = config["traffic"]
    output_dir = Path(exp["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    code, context, stderr = run_cmd(["kubectl", "config", "current-context"], timeout=10)
    if code != 0 or context.strip() != expected_context:
        raise RuntimeError(f"unexpected kubectl context: {context.strip()} {stderr}")
    _check_ready(namespace)
    port_forward = _start_port_forward_if_needed(namespace, traffic["frontend_url"], output_dir)

    metrics: list[dict] = []
    window_meta: list[dict] = []
    phase_timestamps: dict[str, list[float]] = {"baseline": [], "faulty": [], "recovery": []}
    fault_log = None
    restore_log = None
    original_resources = None
    fault_ok = False
    restore_ok = False
    window_size_sec = int(exp["window_size_sec"])

    def collect_phase(phase: str, count: int) -> None:
        for idx in range(count):
            records, meta = collect_window_metrics(
                namespace,
                traffic["frontend_url"],
                int(traffic["requests_per_window"]),
                int(traffic["request_timeout_sec"]),
                CPU_INCIDENT_ID,
                window_size_sec=window_size_sec,
            )
            for record in records:
                record["phase"] = phase
            meta["phase"] = phase
            meta["window_index"] = idx
            metrics.extend(records)
            window_meta.append(meta)
            phase_timestamps[phase].append(meta["timestamp"])

    try:
        _save_command_output(["kubectl", "get", "pods", "-n", namespace, "-o", "wide"], output_dir / "kubectl_get_pods_before.txt")
        _save_command_output(["kubectl", "get", "deploy", "-n", namespace], output_dir / "kubectl_get_deploy_before.txt")
        _save_command_output(["kubectl", "get", "svc", "-n", namespace], output_dir / "kubectl_get_svc_before.txt")
        smoke_before = curl_frontend_requests(traffic["frontend_url"], 3, int(traffic["request_timeout_sec"]))
        write_json(output_dir / "frontend_smoke_before.txt", smoke_before)
        if smoke_before["error_rate"] != 0.0:
            raise RuntimeError("frontend smoke before experiment failed")

        collect_phase("baseline", int(exp["baseline_windows"]))

        original_resources = get_deployment_resources(namespace, fault["target_deployment"], fault["target_container"])
        write_json(output_dir / "paymentservice_original_resources.json", original_resources)
        fault_log = inject_cpu_limit_fault(
            namespace,
            fault["target_deployment"],
            fault["target_container"],
            str(fault["cpu_limit_during_fault"]),
            str(fault["memory_limit_during_fault"]),
        )
        fault_ok = True
        write_fault_logs(output_dir, {"fault_injection": fault_log})
        wait_deployment_ready(namespace, fault["target_deployment"], timeout_sec=180)

        collect_phase("faulty", int(exp["faulty_windows"]))
    finally:
        if original_resources is not None:
            try:
                restore_log = restore_deployment_resources(namespace, fault["target_deployment"], fault["target_container"], original_resources)
                wait_deployment_ready(namespace, fault["target_deployment"], timeout_sec=180)
                restore_ok = True
            finally:
                if restore_log is not None:
                    write_fault_logs(output_dir, {"restore": restore_log})

    collect_phase("recovery", int(exp["recovery_windows"]))

    wait_deployment_ready(namespace, fault["target_deployment"], timeout_sec=180)
    _save_command_output(["kubectl", "get", "pods", "-n", namespace, "-o", "wide"], output_dir / "kubectl_get_pods_after.txt")
    _save_command_output(["kubectl", "get", "deploy", "-n", namespace], output_dir / "kubectl_get_deploy_after.txt")
    smoke_after = curl_frontend_requests(traffic["frontend_url"], 3, int(traffic["request_timeout_sec"]))
    write_json(output_dir / "frontend_smoke_after.txt", smoke_after)
    if port_forward is not None:
        port_forward.terminate()
        port_forward = None

    start_ts = phase_timestamps["faulty"][0]
    end_ts = phase_timestamps["faulty"][-1]
    incident = build_incident_record(start_ts, end_ts, config)
    evidence, evidence_meta = _build_evidence(metrics, incident)
    service_graph_result = write_online_boutique_service_graph(output_dir / "service_graph.jsonl")
    quality = build_data_quality_report(metrics, window_meta, incident, config, fault_ok, restore_ok)
    quality.update(evidence_meta)
    metadata = {
        "experiment_id": exp["experiment_id"],
        "output_dir": str(output_dir),
        "metrics_count": len(metrics),
        "evidence_count": len(evidence),
        "incidents_count": 1,
        "service_graph": service_graph_result,
        "window_meta": window_meta,
        "config_path": str(config_path),
        "weak_cpu_evidence": evidence_meta.get("weak_cpu_evidence", False),
        "notes": "P2A-1R real CPU fault metric collection; RCA pipeline not run.",
    }

    write_jsonl(output_dir / "metrics.jsonl", metrics)
    write_jsonl(output_dir / "incidents.jsonl", [incident])
    write_jsonl(output_dir / "evidence.jsonl", evidence)
    write_json(output_dir / "metadata.json", metadata)
    write_json(output_dir / "data_quality_report.json", quality)

    return {
        "output_dir": str(output_dir),
        "metadata": metadata,
        "data_quality_report": quality,
        "metrics_count": len(metrics),
        "services_seen": quality["services_seen"],
        "metrics_seen": quality["metrics_seen"],
    }

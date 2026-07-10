"""P2B-0 real network fault feasibility smoke for Online Boutique."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from proberca.adapters.online_boutique.network_fault import (
    apply_netem_fault,
    clear_netem_fault,
    collect_proc_net_snmp,
    collect_ss_rtt,
    curl_frontend,
    get_pod_netns_pid,
    get_pod_sandbox_id,
    get_target_pod,
    get_tc_qdisc,
    run_cmd,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8")


def _deployment_ready(namespace: str, name: str) -> bool:
    code, stdout, _ = run_cmd(["kubectl", "get", "deploy", "-n", namespace, name, "-o", "json"], timeout=30)
    if code != 0:
        return False
    data = json.loads(stdout)
    status = data.get("status", {})
    return int(status.get("readyReplicas", 0) or 0) >= 1 and int(status.get("availableReplicas", 0) or 0) >= 1


def _maybe_start_frontend_port_forward(namespace: str, frontend_url: str, output_dir: Path) -> subprocess.Popen | None:
    probe = curl_frontend(frontend_url, 1, 3)
    if probe.get("http_ok"):
        return None
    log = (output_dir / "frontend_port_forward.log").open("a", encoding="utf-8")
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"],
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Keep the file handle attached to the process object so it stays open.
    proc._proberca_log_handle = log  # type: ignore[attr-defined]
    time.sleep(5)
    return proc


def _stop_port_forward(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    handle = getattr(proc, "_proberca_log_handle", None)
    if handle is not None:
        handle.close()


def _tcp_counter(snmp: dict[str, Any], key: str) -> int:
    return int(snmp.get("tcp", {}).get(key, 0) or 0)


def _rtt_ms(rtt: dict[str, Any]) -> float | None:
    value = rtt.get("rtt_ms")
    return None if value is None else float(value)


def evaluate_network_fault_feasible(summary: dict[str, Any]) -> tuple[bool, list[str]]:
    checks = [
        (summary.get("netem_applied") is True, "netem_applied != true"),
        (summary.get("netem_restored") is True, "netem_restored != true"),
        (summary.get("frontend_after_http_ok") is True, "frontend_after_http_ok != true"),
        ("netem" in str(summary.get("tc_qdisc_during", "")), "tc_qdisc_during missing netem"),
        ("netem" not in str(summary.get("tc_qdisc_after", "")), "tc_qdisc_after still contains netem"),
    ]
    failed = [reason for ok, reason in checks if not ok]
    return (not failed, failed)


def _assert_online_boutique_ready(namespace: str) -> None:
    for name in ["shippingservice", "frontend", "checkoutservice"]:
        if not _deployment_ready(namespace, name):
            raise RuntimeError(f"required deployment not ready: {name}")


def run_p2b0_network_smoke(config_path: str) -> dict[str, Any]:
    """Run one real tc netem feasibility smoke without running RCA."""

    config = load_simple_yaml(config_path)
    namespace = str(config["kubernetes"]["namespace"])
    kind_node = str(config["kind_node_container"])
    exp = config["experiment"]
    fault = config["fault_injection"]
    output_dir = Path(str(exp["output_dir"]))
    output_dir.mkdir(parents=True, exist_ok=True)

    if run_cmd(["kubectl", "config", "current-context"], timeout=10)[1].strip() != str(config["kubernetes"]["context"]):
        raise RuntimeError("kubectl context mismatch")
    _assert_online_boutique_ready(namespace)

    port_forward_proc = _maybe_start_frontend_port_forward(namespace, str(exp["frontend_url"]), output_dir)
    netem_applied = False
    restore = {"restored": False, "stderr": "not attempted"}
    try:
        target = get_target_pod(namespace, str(fault["target_service"]))
        if not target.get("ready"):
            raise RuntimeError(f"target pod not ready: {target}")
        sandbox_id = get_pod_sandbox_id(kind_node, namespace, str(target["name"]))
        pid = get_pod_netns_pid(kind_node, sandbox_id)
        device = str(fault.get("device", "eth0"))

        tc_before = get_tc_qdisc(kind_node, pid, device)
        before_snmp = collect_proc_net_snmp(kind_node, pid)
        before_rtt = collect_ss_rtt(kind_node, pid)
        frontend_before = curl_frontend(str(exp["frontend_url"]), int(exp["requests_before"]), int(exp["request_timeout_sec"]))

        apply = apply_netem_fault(kind_node, pid, device, int(fault["delay_ms"]), int(fault["jitter_ms"]), float(fault["loss_percent"]))
        netem_applied = bool(apply.get("applied"))
        tc_during = get_tc_qdisc(kind_node, pid, device)
        frontend_during = curl_frontend(str(exp["frontend_url"]), int(exp["requests_during"]), int(exp["request_timeout_sec"]))
        time.sleep(max(0, int(fault.get("duration_sec", 0))))
        during_snmp = collect_proc_net_snmp(kind_node, pid)
        during_rtt = collect_ss_rtt(kind_node, pid)
    finally:
        # Best-effort restore even if apply or during collection fails.
        try:
            if 'pid' in locals():
                restore = clear_netem_fault(kind_node, pid, str(fault.get("device", "eth0")))
        finally:
            pass

    tc_after = get_tc_qdisc(kind_node, pid, device)
    frontend_after = curl_frontend(str(exp["frontend_url"]), int(exp["requests_after"]), int(exp["request_timeout_sec"]))
    after_snmp = collect_proc_net_snmp(kind_node, pid)
    after_rtt = collect_ss_rtt(kind_node, pid)
    _stop_port_forward(port_forward_proc)

    _write_text(output_dir / "tc_qdisc_before.txt", tc_before)
    _write_text(output_dir / "tc_qdisc_during.txt", tc_during)
    _write_text(output_dir / "tc_qdisc_after.txt", tc_after)
    _write_json(output_dir / "network_fault_log.json", apply)
    _write_json(output_dir / "network_restore_log.json", restore)
    _write_json(output_dir / "network_metrics_before.json", {"snmp": before_snmp, "ss_rtt": before_rtt})
    _write_json(output_dir / "network_metrics_during.json", {"snmp": during_snmp, "ss_rtt": during_rtt})
    _write_json(output_dir / "network_metrics_after.json", {"snmp": after_snmp, "ss_rtt": after_rtt})
    _write_json(output_dir / "frontend_network_smoke_before.json", frontend_before)
    _write_json(output_dir / "frontend_network_smoke_during.json", frontend_during)
    _write_json(output_dir / "frontend_network_smoke_after.json", frontend_after)

    retrans_before = _tcp_counter(before_snmp, "RetransSegs")
    retrans_during = _tcp_counter(during_snmp, "RetransSegs")
    retrans_after = _tcp_counter(after_snmp, "RetransSegs")
    summary: dict[str, Any] = {
        "experiment_id": exp["experiment_id"],
        "target_service": fault["target_service"],
        "target_metric": exp["target_metric"],
        "target_fault_type": exp["target_fault_type"],
        "pod_name": target["name"],
        "pod_ip": target["pod_ip"],
        "netns_pid": pid,
        "sandbox_id": sandbox_id,
        "netem_applied": netem_applied,
        "netem_restored": bool(restore.get("restored")),
        "frontend_before_http_ok": bool(frontend_before.get("http_ok")),
        "frontend_during_http_ok": bool(frontend_during.get("http_ok")),
        "frontend_after_http_ok": bool(frontend_after.get("http_ok")),
        "retrans_before": retrans_before,
        "retrans_during": retrans_during,
        "retrans_after": retrans_after,
        "retrans_delta_during": retrans_during - retrans_before,
        "rtt_before_ms": _rtt_ms(before_rtt),
        "rtt_during_ms": _rtt_ms(during_rtt),
        "rtt_after_ms": _rtt_ms(after_rtt),
        "frontend_p99_before_ms": frontend_before.get("p99_latency_ms"),
        "frontend_p99_during_ms": frontend_during.get("p99_latency_ms"),
        "frontend_p99_after_ms": frontend_after.get("p99_latency_ms"),
        "tc_qdisc_before": tc_before,
        "tc_qdisc_during": tc_during,
        "tc_qdisc_after": tc_after,
    }
    feasible, failed = evaluate_network_fault_feasible(summary)
    summary["network_fault_feasible"] = feasible
    summary["failed_checks"] = failed
    metadata = {
        "config_path": str(config_path),
        "output_dir": str(output_dir),
        "note": "P2B-0 network feasibility smoke only; no RCA pipeline and no accuracy output.",
    }
    _write_json(output_dir / "p2b0_network_smoke_summary.json", summary)
    _write_json(output_dir / "p2b0_network_smoke_metadata.json", metadata)
    return {"summary": summary, "metadata": metadata, "output_dir": str(output_dir)}

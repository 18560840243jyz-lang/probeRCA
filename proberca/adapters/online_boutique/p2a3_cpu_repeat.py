"""P2A-3 repeated real Online Boutique CPU fault injection experiments."""

from __future__ import annotations

import copy
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np

from proberca.adapters.online_boutique.metrics import (
    build_cadvisor_service_snapshot,
    curl_frontend_requests,
    get_cadvisor_metrics,
    get_deployments,
    get_pods,
    parse_prometheus_text_metrics,
    run_cmd,
)
from proberca.adapters.online_boutique.p2a1_cpu_experiment import load_simple_yaml, run_p2a1_cpu_fault_experiment
from proberca.adapters.online_boutique.p2a2_real_rca import run_p2a2_real_cpu_rca


CADVISOR_NODE_NAME = "proberca-ob-control-plane"


def load_repeat_config(config_path: str | Path) -> dict[str, Any]:
    """Load P2A-3/P2A-3R repeat config."""

    return load_simple_yaml(config_path)


def _dump_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if any(ch in text for ch in [":", "#", "\n"]) or text.strip() != text or text == "":
        return json.dumps(text, ensure_ascii=False)
    return text


def _dump_yaml(data: Any, indent: int = 0) -> str:
    prefix = " " * indent
    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(_dump_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}{key}: {_dump_scalar(value)}")
        return "\n".join(lines)
    if isinstance(data, list):
        lines = []
        for value in data:
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}-")
                lines.append(_dump_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}- {_dump_scalar(value)}")
        return "\n".join(lines)
    return f"{prefix}{_dump_scalar(data)}"


def write_yaml(path: str | Path, data: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_dump_yaml(data) + "\n", encoding="utf-8")


def _repeat_overrides(config: dict[str, Any] | None) -> dict[str, Any]:
    if not config:
        return {}
    repeat_cfg = config.get("repeat_experiment", {})
    overrides = repeat_cfg.get("controlled_fault_overrides", {})
    return overrides if isinstance(overrides, dict) else {}


def _apply_override(target: dict[str, Any], section: str, key: str, value: Any) -> None:
    target.setdefault(section, {})[key] = value


def make_repeat_fault_config(
    base_config_path: str | Path,
    repeat_index: int,
    repeat_output_dir: str | Path,
    repeat_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create one repeat-specific P2A-1R fault config dictionary."""

    cfg = copy.deepcopy(load_simple_yaml(base_config_path))
    suffix = f"{int(repeat_index):02d}"
    phase = str((repeat_config or {}).get("phase", "P2A-3"))
    cfg["phase"] = phase
    cfg.setdefault("experiment", {})["experiment_id"] = f"ob_cpu_paymentservice_repeat_{suffix}"
    cfg["experiment"]["output_dir"] = str(Path(repeat_output_dir) / "raw")

    overrides = _repeat_overrides(repeat_config)
    if overrides:
        mapping = {
            "cpu_limit_during_fault": ("fault_injection", "cpu_limit_during_fault"),
            "memory_limit_during_fault": ("fault_injection", "memory_limit_during_fault"),
            "window_size_sec": ("experiment", "window_size_sec"),
            "baseline_windows": ("experiment", "baseline_windows"),
            "faulty_windows": ("experiment", "faulty_windows"),
            "recovery_windows": ("experiment", "recovery_windows"),
            "requests_per_window": ("traffic", "requests_per_window"),
        }
        for override_key, (section, key) in mapping.items():
            if override_key in overrides:
                _apply_override(cfg, section, key, overrides[override_key])
    return cfg


def _frontend_smoke(namespace: str, frontend_url: str = "http://127.0.0.1:8080") -> dict[str, Any]:
    result = curl_frontend_requests(frontend_url, 1, 3)
    if result.get("error_rate") == 0.0:
        return {"ok": True, "port_forward_started": False, "summary": result}
    log_handle = None
    proc: subprocess.Popen | None = None
    try:
        log_path = Path("/tmp/proberca_p2a3_frontend_port_forward.log")
        log_handle = log_path.open("a", encoding="utf-8")
        proc = subprocess.Popen(
            ["kubectl", "port-forward", "-n", namespace, "svc/frontend", "8080:80"],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        time.sleep(5)
        result = curl_frontend_requests(frontend_url, 1, 3)
        return {"ok": result.get("error_rate") == 0.0, "port_forward_started": True, "summary": result}
    finally:
        if proc is not None:
            proc.terminate()
        if log_handle is not None:
            log_handle.close()


def ensure_online_boutique_ready(namespace: str) -> dict[str, Any]:
    """Ensure Online Boutique deployments, frontend, paymentservice, and cAdvisor are ready."""

    deployments = get_deployments(namespace)
    not_ready = [item for item in deployments if not item.get("ready")]
    if not deployments or not_ready:
        raise RuntimeError(f"Online Boutique deployments not ready: {not_ready}")
    deploy_names = {str(item.get("name")) for item in deployments}
    for required in ["frontend", "paymentservice"]:
        if required not in deploy_names:
            raise RuntimeError(f"missing required deployment: {required}")
    smoke = _frontend_smoke(namespace)
    if not smoke.get("ok"):
        raise RuntimeError(f"frontend smoke failed: {smoke}")
    code, stdout, stderr = run_cmd(["kubectl", "get", "--raw", f"/api/v1/nodes/{CADVISOR_NODE_NAME}/proxy/metrics/cadvisor"], timeout=30)
    if code != 0 or "container_cpu_cfs_throttled_seconds_total" not in stdout:
        raise RuntimeError(f"cAdvisor API unavailable: {stderr[:200]}")
    return {"deployments_count": len(deployments), "frontend_smoke": smoke, "cadvisor_available": True}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _quality_ok(quality: dict[str, Any], requirements: dict[str, Any]) -> bool:
    mapping = {
        "require_fault_injection_succeeded": "fault_injection_succeeded",
        "require_restore_succeeded": "restore_succeeded",
        "require_cadvisor_metrics_available": "cadvisor_metrics_available",
        "require_root_service_metric_coverage_passed": "root_service_metric_coverage_passed",
        "require_paymentservice_throttled_metric_present": "paymentservice_throttled_metric_present",
        "require_frontend_latency_metric_present": "frontend_latency_metric_present",
    }
    for req, field in mapping.items():
        if requirements.get(req) is True and quality.get(field) is not True:
            return False
    return True


def _service_throttled_seconds(snapshot: dict[str, Any], service: str) -> float:
    total = 0.0
    for key, metrics in snapshot.items():
        if not str(key).startswith(f"{service}/"):
            continue
        value = metrics.get("cpu_cfs_throttled_seconds_total")
        if value is not None:
            total += float(value)
    return total


def measure_recent_service_throttling_usec(namespace: str, service: str, sample_sec: float = 2.0) -> float:
    """Measure recent cAdvisor throttling delta for a service without using root labels for scoring."""

    pods = get_pods(namespace)
    text1 = get_cadvisor_metrics(CADVISOR_NODE_NAME)
    snap1 = build_cadvisor_service_snapshot(parse_prometheus_text_metrics(text1), pods)
    time.sleep(max(0.1, float(sample_sec)))
    text2 = get_cadvisor_metrics(CADVISOR_NODE_NAME)
    snap2 = build_cadvisor_service_snapshot(parse_prometheus_text_metrics(text2), pods)
    delta = _service_throttled_seconds(snap2, service) - _service_throttled_seconds(snap1, service)
    if delta < 0:
        return 0.0
    return float(delta * 1_000_000.0)


def _controlled_metadata(config: dict[str, Any], fault_cfg: dict[str, Any]) -> dict[str, Any]:
    overrides = _repeat_overrides(config)
    return {
        "controlled_config_used": bool(overrides),
        "cpu_limit_during_fault": fault_cfg.get("fault_injection", {}).get("cpu_limit_during_fault"),
        "requests_per_window": fault_cfg.get("traffic", {}).get("requests_per_window"),
        "pre_repeat_cooldown_sec": overrides.get("pre_repeat_cooldown_sec", 0),
        "post_restore_cooldown_sec": overrides.get("post_restore_cooldown_sec", 0),
    }


def _pre_repeat_control(namespace: str, config: dict[str, Any]) -> dict[str, Any]:
    overrides = _repeat_overrides(config)
    info: dict[str, Any] = {"pre_repeat_target_throttling_usec": None, "precheck_warning": ""}
    cooldown = int(overrides.get("pre_repeat_cooldown_sec", 0) or 0)
    if cooldown > 0:
        time.sleep(cooldown)
    threshold = overrides.get("require_pre_repeat_target_throttling_below_usec")
    if threshold is None:
        return info
    service = str(config.get("target", {}).get("service", "paymentservice"))
    measured = measure_recent_service_throttling_usec(namespace, service)
    info["pre_repeat_target_throttling_usec"] = measured
    if measured > float(threshold):
        wait_sec = max(5, cooldown)
        time.sleep(wait_sec)
        measured2 = measure_recent_service_throttling_usec(namespace, service)
        info["pre_repeat_target_throttling_usec"] = measured2
        if measured2 > float(threshold):
            info["precheck_warning"] = f"target throttling remained above threshold: {measured2} > {threshold}"
    return info


def run_single_cpu_repeat(repeat_index: int, config: dict[str, Any]) -> dict[str, Any]:
    """Run one real CPU fault repeat: inject, collect, restore, then RCA."""

    repeat_cfg = config["repeat_experiment"]
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    repeat_dir = base_dir / f"repeat_{int(repeat_index):02d}"
    raw_dir = repeat_dir / "raw"
    rca_dir = repeat_dir / "p1rca"
    repeat_dir.mkdir(parents=True, exist_ok=True)
    fault_cfg = make_repeat_fault_config(repeat_cfg["base_fault_config"], repeat_index, repeat_dir, config)
    config_path = repeat_dir / "repeat_config.yaml"
    write_yaml(config_path, fault_cfg)

    namespace = str(config["kubernetes"]["namespace"])
    row: dict[str, Any] = {
        "repeat_index": int(repeat_index),
        "raw_output_dir": str(raw_dir),
        "rca_output_dir": str(rca_dir),
        "fault_injection_succeeded": False,
        "restore_succeeded": False,
        "root_service_metric_coverage_passed": False,
        "paymentservice_throttled_metric_present": False,
        "predicted_top1_service": None,
        "predicted_top1_metric": None,
        "predicted_root_type": None,
        "metric_rank_debug": None,
        "service_hit_at_1": 0.0,
        "metric_hit_at_1": 0.0,
        "metric_hit_at_3": 0.0,
        "metric_mrr": 0.0,
        "root_type_accuracy": 0.0,
        "path_fidelity": 0.0,
        "paymentservice_throttling_lift_debug": 0.0,
        "frontend_latency_lift_debug": 0.0,
        "status": "failed",
        "failure_reason": "",
    }
    row.update(_controlled_metadata(config, fault_cfg))
    try:
        ensure_online_boutique_ready(namespace)
        row.update(_pre_repeat_control(namespace, config))
        run_p2a1_cpu_fault_experiment(str(config_path))
        quality_path = raw_dir / "data_quality_report.json"
        if not quality_path.exists():
            raise RuntimeError(f"missing raw quality report: {quality_path}")
        quality = _load_json(quality_path)
        for key in [
            "fault_injection_succeeded",
            "restore_succeeded",
            "root_service_metric_coverage_passed",
            "paymentservice_throttled_metric_present",
        ]:
            row[key] = bool(quality.get(key, False))
        row["paymentservice_throttling_lift_debug"] = float(quality.get("paymentservice_throttling_lift", 0.0))
        row["frontend_latency_lift_debug"] = float(quality.get("frontend_faulty_p99_mean", 0.0)) - float(quality.get("frontend_baseline_p99_mean", 0.0))
        if not _quality_ok(quality, config.get("quality_requirements", {})):
            raise RuntimeError(f"raw quality failed: {quality}")
        rca = run_p2a2_real_cpu_rca(str(raw_dir), str(rca_dir), top_k=5)
        summary = rca["summary"]
        for key in [
            "predicted_top1_service",
            "predicted_top1_metric",
            "predicted_root_type",
            "metric_rank_debug",
            "service_hit_at_1",
            "metric_hit_at_1",
            "metric_hit_at_3",
            "metric_mrr",
            "root_type_accuracy",
            "path_fidelity",
            "paymentservice_throttling_lift_debug",
            "frontend_latency_lift_debug",
        ]:
            row[key] = summary.get(key)
        row["status"] = "success"
        row["failure_reason"] = ""
    except Exception as exc:  # noqa: BLE001 - record repeat failure and let caller continue.
        row["failure_reason"] = str(exc)
        try:
            ensure_online_boutique_ready(namespace)
        except Exception as ready_exc:  # noqa: BLE001
            row["failure_reason"] += f"; readiness_after_failure={ready_exc}"
    finally:
        try:
            ensure_online_boutique_ready(namespace)
        except Exception as ready_exc:  # noqa: BLE001
            row["failure_reason"] = (row.get("failure_reason") or "") + f"; readiness_after_repeat={ready_exc}"
        post_cooldown = int(_repeat_overrides(config).get("post_restore_cooldown_sec", 0) or 0)
        if post_cooldown > 0:
            time.sleep(post_cooldown)
    return row


def _mean(values: list[float]) -> float:
    return float(np.mean(np.asarray(values, dtype=float))) if values else 0.0


def _min(values: list[float]) -> float:
    return float(np.min(np.asarray(values, dtype=float))) if values else 0.0


def aggregate_repeat_summary(config: dict[str, Any], per_repeat: list[dict[str, Any]]) -> dict[str, Any]:
    successes = [row for row in per_repeat if row.get("status") == "success"]
    quality_successes = [row for row in per_repeat if row.get("root_service_metric_coverage_passed") and row.get("paymentservice_throttled_metric_present") and row.get("fault_injection_succeeded") and row.get("restore_succeeded")]

    def vals(key: str) -> list[float]:
        return [float(row.get(key, 0.0) or 0.0) for row in successes]

    return {
        "experiment_group_id": str(config["repeat_experiment"]["experiment_group_id"]),
        "repeats_requested": int(config["repeat_experiment"]["repeats"]),
        "repeats_completed": len(per_repeat),
        "repeats_successful_quality": len(quality_successes),
        "repeats_successful_rca": len(successes),
        "service_hit_at_1_mean": _mean(vals("service_hit_at_1")),
        "service_hit_at_1_min": _min(vals("service_hit_at_1")),
        "metric_hit_at_1_mean": _mean(vals("metric_hit_at_1")),
        "metric_hit_at_1_min": _min(vals("metric_hit_at_1")),
        "metric_hit_at_3_mean": _mean(vals("metric_hit_at_3")),
        "metric_hit_at_3_min": _min(vals("metric_hit_at_3")),
        "metric_mrr_mean": _mean(vals("metric_mrr")),
        "metric_mrr_min": _min(vals("metric_mrr")),
        "root_type_accuracy_mean": _mean(vals("root_type_accuracy")),
        "root_type_accuracy_min": _min(vals("root_type_accuracy")),
        "path_fidelity_mean": _mean(vals("path_fidelity")),
        "path_fidelity_min": _min(vals("path_fidelity")),
        "paymentservice_throttling_lift_mean": _mean(vals("paymentservice_throttling_lift_debug")),
        "paymentservice_throttling_lift_min": _min(vals("paymentservice_throttling_lift_debug")),
        "frontend_latency_lift_mean": _mean(vals("frontend_latency_lift_debug")),
        "per_repeat": per_repeat,
    }


def run_p2a3_cpu_repeated_experiment(config_path: str | Path, repeats: int | None = None, sleep_between_repeats_sec: int | None = None) -> dict[str, Any]:
    """Run P2A-3/P2A-3R repeated real CPU fault injection experiments."""

    config = load_repeat_config(config_path)
    repeat_cfg = config["repeat_experiment"]
    if repeats is not None:
        repeat_cfg["repeats"] = int(repeats)
    if sleep_between_repeats_sec is not None:
        repeat_cfg["sleep_between_repeats_sec"] = int(sleep_between_repeats_sec)
    base_dir = Path(str(repeat_cfg["base_output_dir"]))
    base_dir.mkdir(parents=True, exist_ok=True)
    namespace = str(config["kubernetes"]["namespace"])

    per_repeat: list[dict[str, Any]] = []
    for index in range(1, int(repeat_cfg["repeats"]) + 1):
        ensure_online_boutique_ready(namespace)
        row = run_single_cpu_repeat(index, config)
        per_repeat.append(row)
        if index < int(repeat_cfg["repeats"]):
            time.sleep(int(repeat_cfg["sleep_between_repeats_sec"]))

    summary = aggregate_repeat_summary(config, per_repeat)
    failures = [row for row in per_repeat if row.get("status") != "success"]
    metadata = {
        "config_path": str(config_path),
        "base_output_dir": str(base_dir),
        "phase": str(config.get("phase", "P2A-3")),
        "repeats": int(repeat_cfg["repeats"]),
        "sleep_between_repeats_sec": int(repeat_cfg["sleep_between_repeats_sec"]),
        "controlled_fault_overrides": repeat_cfg.get("controlled_fault_overrides", {}),
        "note": "Repeated real CPU fault injection only; not multi-fault accuracy.",
    }
    (base_dir / "p2a3_cpu_repeat_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (base_dir / "p2a3_cpu_repeat_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (base_dir / "p2a3_cpu_repeat_failures.json").write_text(json.dumps({"failures": failures}, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"summary": summary, "metadata": metadata, "failures": failures, "output_dir": str(base_dir)}

import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.p2e_multifault_summary import (
    compute_weighted_overall_metrics,
    evaluate_p2e_multifault_acceptance,
    load_fault_type_summaries,
    write_p2e_multifault_summary,
)
from proberca.cli.check_p2e_multifault_summary import check_p2e_multifault_summary


def _fault_summary(metric_hit_at_1=0.2, metric_hit_at_3=1.0, service_hit_at_1=1.0):
    return {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "service_hit_at_1_mean": service_hit_at_1,
        "service_hit_at_1_min": service_hit_at_1,
        "metric_hit_at_1_mean": metric_hit_at_1,
        "metric_hit_at_1_min": metric_hit_at_1,
        "metric_hit_at_3_mean": metric_hit_at_3,
        "metric_hit_at_3_min": metric_hit_at_3,
        "metric_mrr_mean": 0.8,
        "metric_mrr_min": 0.8,
        "root_type_accuracy_mean": 1.0,
        "root_type_accuracy_min": 1.0,
        "path_fidelity_mean": 1.0,
        "path_fidelity_min": 1.0,
        "per_repeat": [],
    }


def _normalized_faults(metric_hit_at_1=0.2, metric_hit_at_3=1.0):
    base = {}
    for name, fault_type, service, metric in [
        ("cpu", "CPU", "paymentservice", "cpu.throttled_usec"),
        ("network", "Network", "shippingservice", "net.retrans"),
        ("io", "I/O", "redis-cart", "io.write_bytes"),
        ("lock", "Lock", "cartservice", "lock.futex_wait_ms"),
    ]:
        item = _fault_summary(metric_hit_at_1=metric_hit_at_1, metric_hit_at_3=metric_hit_at_3)
        item.update({"fault_type": fault_type, "target_service": service, "target_metric": metric, "limitations": [], "missing_fields": []})
        base[name] = item
    return base


def test_compute_weighted_overall_metrics():
    faults = _normalized_faults(metric_hit_at_1=0.25, metric_hit_at_3=1.0)
    faults["cpu"]["repeats_completed"] = 10
    faults["cpu"]["metric_hit_at_1_mean"] = 0.0
    overall = compute_weighted_overall_metrics(faults)
    assert overall["total_repeats"] == 25
    assert overall["metric_hit_at_3_overall"] == 1.0
    assert overall["service_hit_at_1_overall"] == 1.0
    assert overall["metric_hit_at_1_overall_auxiliary"] < 0.25


def test_metric_hit_at_1_low_but_hit_at_3_high_passes():
    faults = _normalized_faults(metric_hit_at_1=0.0, metric_hit_at_3=1.0)
    overall = compute_weighted_overall_metrics(faults)
    acceptance = evaluate_p2e_multifault_acceptance(overall, faults)
    assert acceptance["p2e_passed"] is True
    assert acceptance["decision"] == "P2E_REAL_MULTIFAULT_PASS"
    assert acceptance["auxiliary_metrics"]["metric_hit_at_1_overall_auxiliary"] == 0.0


def test_fault_type_low_metric_hit_at_3_fails():
    faults = _normalized_faults(metric_hit_at_1=1.0, metric_hit_at_3=1.0)
    faults["network"]["metric_hit_at_3_mean"] = 0.6
    overall = compute_weighted_overall_metrics(faults)
    acceptance = evaluate_p2e_multifault_acceptance(overall, faults)
    assert acceptance["p2e_passed"] is False
    assert "network.metric_hit_at_3_mean < 0.8" in acceptance["failed_checks"]


def _write_fake_real_layout(base: Path):
    paths = {
        "cpu_paymentservice_repeated_controlled/p2a3_cpu_repeat_summary.json": _fault_summary(metric_hit_at_1=0.2, metric_hit_at_3=1.0),
        "network_shippingservice_repeated/p2b1_network_repeat_summary.json": _fault_summary(metric_hit_at_1=1.0, metric_hit_at_3=1.0),
        "io_rediscart_repeated/p2c1_io_repeat_summary.json": _fault_summary(metric_hit_at_1=1.0, metric_hit_at_3=1.0),
        "lock_cartservice_repeated_phaseaware/p2d1r_lock_repeat_summary.json": dict(_fault_summary(metric_hit_at_1=1.0, metric_hit_at_3=1.0), limitation="sidecar_lock_contention_not_original_cartservice_code_bug"),
    }
    for rel, payload in paths.items():
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    accept = base / "cpu_paymentservice_repeated_controlled/p2a4_cpu_top3_acceptance.json"
    accept.write_text(json.dumps({"p2a4_passed": True, "decision": "P2A4_CPU_TOP3_PASS", "failed_checks": []}), encoding="utf-8")


def test_write_p2e_multifault_summary_outputs_json_and_markdown(tmp_path):
    base = tmp_path / "p2"
    _write_fake_real_layout(base)
    output = tmp_path / "out"
    result = write_p2e_multifault_summary(output_dir=str(output), base_dir=str(base))
    assert result["acceptance"]["p2e_passed"] is True
    assert (output / "p2e_multifault_summary.json").exists()
    assert (output / "p2e_multifault_acceptance.json").exists()
    assert (output / "p2e_multifault_report.md").exists()
    assert "metric Hit@1 is auxiliary" in (output / "p2e_multifault_report.md").read_text(encoding="utf-8")
    check = check_p2e_multifault_summary(str(output))
    assert check["passed"] is True


def test_load_fault_type_summaries_records_missing_fields(tmp_path):
    base = tmp_path / "p2"
    _write_fake_real_layout(base)
    data = json.loads((base / "network_shippingservice_repeated/p2b1_network_repeat_summary.json").read_text())
    data.pop("metric_mrr_min")
    (base / "network_shippingservice_repeated/p2b1_network_repeat_summary.json").write_text(json.dumps(data), encoding="utf-8")
    loaded = load_fault_type_summaries(str(base))
    assert loaded["network"]["metric_mrr_min"] == "unknown"
    assert "metric_mrr_min" in loaded["network"]["missing_fields"]


def test_cli_imports():
    assert importlib.import_module("proberca.cli.run_p2e_multifault_summary")
    assert importlib.import_module("proberca.cli.check_p2e_multifault_summary")

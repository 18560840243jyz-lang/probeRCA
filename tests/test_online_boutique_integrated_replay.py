import json
from pathlib import Path

from proberca.adapters.online_boutique import integrated_replay as replay
from proberca.cli.check_b2_integrated_replay import check_b2_integrated_replay


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _make_final_result(root: Path, service="paymentservice", metric="paymentservice.cpu.throttled_usec", root_type="CPU"):
    final = root / "09_final_result"
    metric_name = metric.split(".", 1)[1] if "." in metric else metric
    family = "CPU" if metric_name.startswith("cpu.") else "storage I/O" if metric_name.startswith("io.") else "memory"
    primary = {
        "node_id": metric,
        "service": service,
        "metric": metric_name,
        "metric_family": family,
        "ownership_valid": True,
        "service_matches_node_id": True,
        "score_components": {
            "diagnostic_specificity": 1.0,
            "weak_memory_usage_penalty_applied": metric_name == "memory.usage",
            "node_evidence_support": 1.0,
            "service_family_evidence_support": 1.0,
            "family_global_evidence_support": 0.1,
            "family_global_evidence_weight": 0.10,
            "structured_propagation_support": 0.8,
            "path_edge_support": 0.7,
            "lag_support": 0.5,
            "ownership_valid": True,
            "service_matches_node_id": True,
        },
    }
    result = {
        "alert_window_id": "w1",
        "predicted_top1_service": service,
        "predicted_top1_metric": metric,
        "predicted_root_type": root_type,
        "root_type_source": "primary_metric_family",
        "root_type_uses_labels": False,
        "primary_candidate": primary,
        "service_first": True,
        "primary_metric_conditioned_on_service": True,
        "global_top_metrics_primary": False,
        "top_services": [{"service": service, "rank": 1}],
        "top_metrics": [
            {"node_id": metric, "service": service, "metric": metric_name, "rank": 1},
            {"node_id": f"{service}.cpu.throttle_ratio", "service": service, "metric": "cpu.throttle_ratio", "rank": 2},
            {"node_id": f"{service}.cpu.throttled_periods", "service": service, "metric": "cpu.throttled_periods", "rank": 3},
        ],
        "global_top_metrics_auxiliary": [
            {"node_id": metric, "service": service, "metric": metric_name, "rank": 1, "auxiliary": True},
            {"node_id": "checkoutservice.request.p99_latency_ms", "service": "checkoutservice", "rank": 2, "auxiliary": True},
            {"node_id": "frontend.request.p99_latency_ms", "service": "frontend", "rank": 3, "auxiliary": True},
        ],
        "path": [service, "checkoutservice", "frontend"],
        "confidence": 0.9,
    }
    _write_jsonl(final / "integrated_rca_results.jsonl", [result])
    _write_json(final / "integrated_rca_aggregate.json", {"aggregation_mode": "highest_confidence_window", "result": result})
    prop = root / "05b_structured_propagation"
    _write_json(prop / "structured_propagation_metadata.json", {
        "structured_propagation_model": "structured_multilag_ridge",
        "propagation_model": "structured_multilag_ridge",
        "stable_only": True,
        "propagation_drift_used": False,
        "uses_root_labels": False,
        "uses_incident_start_end": False,
    })
    _write_json(final / "integrated_rca_metadata.json", {
        "uses_root_labels": False,
        "uses_incident_start_end": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "top_service_metric_consistent": True,
        "per_window_results_match_alert_windows": True,
        "root_type_source": "primary_metric_family",
        "root_type_uses_labels": False,
        "service_first_enabled": True,
        "primary_candidate_source": "service_candidate_table",
        "primary_service_source": "service_candidate_table",
        "primary_metric_source": "metric_candidates_within_root_service",
        "primary_metric_conditioned_on_service": True,
        "global_top_metrics_primary": False,
        "service_local_support_used": True,
        "global_family_support_weight_limited": True,
        "ownership_invalid_count": 0,
        "primary_candidate_ownership_valid": True,
        "structured_propagation_enabled": True,
        "structured_propagation_uses_labels": False,
        "structured_propagation_uses_injected_path": False,
        "propagation_drift_used": False,
    })


def test_evaluate_integrated_result_uses_labels_posthoc(tmp_path):
    result_dir = tmp_path / "repeat"
    _make_final_result(result_dir)
    incidents = tmp_path / "raw" / "incidents.jsonl"
    _write_jsonl(incidents, [{
        "root_service": "paymentservice",
        "root_metric": "cpu.throttled_usec",
        "root_type": "CPU throttling",
        "injected_path": [
            "paymentservice.cpu.throttled_usec",
            "checkoutservice.request.p99_latency_ms",
            "frontend.request.p99_latency_ms",
        ],
    }])
    ev = replay.evaluate_integrated_result(str(result_dir), str(incidents))
    assert ev["service_hit_at_1"] == 1.0
    assert ev["metric_hit_at_3"] == 1.0
    assert ev["metric_hit_at_1"] == 1.0
    assert ev["metric_mrr"] == 1.0
    assert ev["root_type_accuracy"] == 1.0
    assert ev["path_fidelity"] == 1.0
    assert ev["labels_used_only_after_result"] is True


def test_root_type_normalization_storage_io(tmp_path):
    result_dir = tmp_path / "repeat"
    _make_final_result(result_dir, service="redis-cart", metric="redis-cart.io.write_bytes", root_type="I/O")
    incidents = tmp_path / "raw" / "incidents.jsonl"
    _write_jsonl(incidents, [{"root_service": "redis-cart", "root_metric": "io.write_bytes", "root_type": "storage I/O"}])
    ev = replay.evaluate_integrated_result(str(result_dir), str(incidents))
    assert ev["root_type_accuracy"] == 1.0


def test_run_p2_integrated_replay_collects_fake_repeats(tmp_path, monkeypatch):
    raw_dirs = []
    for idx in range(1, 3):
        raw = tmp_path / f"raw_{idx}"
        raw.mkdir()
        for name in ["metrics.jsonl", "service_graph.jsonl"]:
            (raw / name).write_text("{}\n", encoding="utf-8")
        _write_jsonl(raw / "incidents.jsonl", [{"root_service": "paymentservice", "root_metric": "cpu.throttled_usec", "root_type": "CPU"}])
        raw_dirs.append(str(raw))

    monkeypatch.setattr(replay, "fault_type_sources", lambda: {"cpu": raw_dirs})

    def fake_run(raw_input_dir, output_dir, debug_evaluate_incidents=False):
        _make_final_result(Path(output_dir))
        return {"metadata": {}, "results": [], "aggregate": None, "debug": None}

    monkeypatch.setattr(replay, "run_integrated_blind_rca", fake_run)
    out = tmp_path / "out"
    result = replay.run_p2_integrated_replay(str(out), debug_evaluate_incidents=True)
    summary = result["summary"]
    assert summary["total_repeats"] == 2
    assert summary["repeats_completed"] == 2
    assert summary["service_hit_at_1_overall"] == 1.0
    assert summary["evaluation_uses_labels_posthoc"] is True


def test_check_b2_integrated_replay_rejects_label_inference(tmp_path):
    root = tmp_path / "b2"
    _write_json(root / "p2_integrated_replay_metadata.json", {})
    _write_json(root / "p2_integrated_replay_summary.json", {
        "total_repeats": 20,
        "repeats_completed": 20,
        "uses_root_labels_for_inference": True,
        "uses_target_config_for_inference": False,
        "uses_injected_path_for_inference": False,
        "uses_incident_start_end_for_inference": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "reinjects_faults": False,
        "evaluation_uses_labels_posthoc": True,
        "per_repeat": [],
    })
    result = check_b2_integrated_replay(str(root))
    assert result["passed"] is False
    assert any("uses_root_labels_for_inference" in item for item in result["failed_checks"])


def test_check_b2_integrated_replay_accepts_structural_summary(tmp_path):
    root = tmp_path / "b2"
    repeat = root / "cpu" / "repeat_01"
    _make_final_result(repeat)
    _write_json(root / "p2_integrated_replay_metadata.json", {})
    _write_json(root / "p2_integrated_replay_summary.json", {
        "total_repeats": 20,
        "repeats_completed": 20,
        "uses_root_labels_for_inference": False,
        "uses_target_config_for_inference": False,
        "uses_injected_path_for_inference": False,
        "uses_incident_start_end_for_inference": False,
        "uses_legacy_evidence": False,
        "runs_old_p1_rca": False,
        "reinjects_faults": False,
        "evaluation_uses_labels_posthoc": True,
        "per_repeat": [{"output_dir": str(repeat)}],
    })
    result = check_b2_integrated_replay(str(root))
    assert result["passed"] is True


def test_service_conditioned_metric_hit_requires_correct_service(tmp_path):
    result_dir = tmp_path / "repeat_service_miss"
    _make_final_result(result_dir, service="adservice", metric="adservice.cpu.throttled_usec", root_type="CPU")
    final = result_dir / "09_final_result"
    aggregate = json.loads((final / "integrated_rca_aggregate.json").read_text())
    aggregate["result"]["global_top_metrics_auxiliary"] = [
        {"node_id": "paymentservice.cpu.throttled_usec", "service": "paymentservice", "auxiliary": True},
        {"node_id": "adservice.cpu.throttled_usec", "service": "adservice", "auxiliary": True},
    ]
    (final / "integrated_rca_aggregate.json").write_text(json.dumps(aggregate), encoding="utf-8")
    incidents = tmp_path / "raw" / "incidents.jsonl"
    _write_jsonl(incidents, [{"root_service": "paymentservice", "root_metric": "cpu.throttled_usec", "root_type": "CPU"}])
    ev = replay.evaluate_integrated_result(str(result_dir), str(incidents))
    assert ev["service_hit_at_1"] == 0.0
    assert ev["service_conditioned_metric_hit_at_3"] == 0.0
    assert ev["global_metric_hit_at_3_auxiliary"] == 1.0
    assert ev["service_metric_pair_hit_at_1"] == 0.0


def test_service_metric_pair_hit_requires_both_service_and_metric(tmp_path):
    result_dir = tmp_path / "repeat_pair"
    _make_final_result(result_dir, service="paymentservice", metric="paymentservice.cpu.throttled_usec", root_type="CPU")
    incidents = tmp_path / "raw" / "incidents.jsonl"
    _write_jsonl(incidents, [{"root_service": "paymentservice", "root_metric": "cpu.throttled_usec", "root_type": "CPU"}])
    ev = replay.evaluate_integrated_result(str(result_dir), str(incidents))
    assert ev["service_metric_pair_hit_at_1"] == 1.0
    assert ev["service_conditioned_metric_hit_at_3"] == 1.0

import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import diagnostic_priority_bonus, score_ipw_semantic_evidence
from proberca.evidence.semantic import metric_specificity_weight
from proberca.features.robust import normalize_dataset
from proberca.inference.ipw_sparse import solve_ipw_sparse_inversion
from proberca.observation.adaptive import ObservationPolicyConfig, simulate_adaptive_observation
from proberca.propagation.ipw import train_ipw_masked_propagation


def _prepare_dataset(output_dir):
    generate_dataset(
        SyntheticConfig(
            output_dir=str(output_dir),
            seed=7,
            baseline_windows=30,
            faulty_windows=30,
            instances_per_service=2,
        )
    )
    normalize_dataset(output_dir)
    simulate_adaptive_observation(output_dir, config=ObservationPolicyConfig(seed=7))
    train_ipw_masked_propagation(output_dir)
    solve_ipw_sparse_inversion(output_dir)


def test_p1d_diagnostic_specificity_weights():
    assert metric_specificity_weight("cpu.throttled_usec") > metric_specificity_weight("cpu.pressure")
    assert metric_specificity_weight("net.retrans") > metric_specificity_weight("net.rtt_ms")
    assert metric_specificity_weight("io.bio_latency_ms") > metric_specificity_weight("io.queue_depth")
    assert metric_specificity_weight("lock.futex_wait_ms") > metric_specificity_weight("request.p99_latency_ms")
    assert metric_specificity_weight("request.p99_latency_ms") < 1.0
    assert diagnostic_priority_bonus("cpu.throttled_usec", 1.0, 2.0) > diagnostic_priority_bonus("cpu.pressure", 1.0, 2.0)
    assert diagnostic_priority_bonus("request.p99_latency_ms", 1.0, 2.0) == 0.0


def test_ipw_semantic_evidence_outputs_and_ranks(tmp_path):
    output_dir = tmp_path / "p1d"
    _prepare_dataset(output_dir)
    result = score_ipw_semantic_evidence(output_dir)

    assert (output_dir / "ipw_semantic_interventions.jsonl").exists()
    assert (output_dir / "ipw_semantic_type_scores.jsonl").exists()
    assert (output_dir / "ipw_semantic_evidence_summary.json").exists()
    assert (output_dir / "ipw_semantic_evidence_metadata.json").exists()

    candidates = read_jsonl(output_dir / "ipw_semantic_interventions.jsonl")
    assert candidates
    for record in candidates:
        assert "semantic_score" in record
        assert "semantic_rank" in record
        assert "evidence_type" in record
        assert "specificity_weight" in record
        assert "semantic_anchor_bonus" in record
        assert "diagnostic_priority_bonus" in record
        assert "top_services" not in record
        assert "top_metrics" not in record
        assert "path" not in record
        assert "RCAResult" not in record
        assert "true_root_semantic_rank_debug" not in record

    summary = result["summary"]
    assert summary["mean_true_root_semantic_rank_debug"] <= 1.25
    assert summary["metric_hit_at_1_debug"] >= 0.75
    assert summary["metric_hit_at_3_debug"] == 1.0
    for item in summary["per_incident"]:
        assert item["true_root_semantic_rank_debug"] <= 2
        incident_candidates = [row for row in candidates if row["incident_id"] == item["incident_id"]]
        assert any(row["semantic_score"] > 0 for row in incident_candidates)
    assert "true_root_semantic_rank_debug" in summary["per_incident"][0]


def test_ipw_semantic_evidence_cli(tmp_path):
    output_dir = tmp_path / "p1d_cli"
    _prepare_dataset(output_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.score_ipw_semantic_evidence", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1D IPW semantic evidence 完成" in completed.stdout
    assert "不包含 path explanation" in completed.stdout

    summary = json.loads((output_dir / "ipw_semantic_evidence_summary.json").read_text(encoding="utf-8"))
    assert summary["mean_true_root_semantic_rank_debug"] <= 1.25
    assert summary["metric_hit_at_1_debug"] >= 0.75

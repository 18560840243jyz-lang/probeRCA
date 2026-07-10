import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.semantic import metric_specificity_weight, score_semantic_evidence
from proberca.features.robust import normalize_dataset
from proberca.inference.sparse import solve_sparse_inversion
from proberca.propagation.stable import train_stable_propagation


def _prepare_dataset(output_dir) -> None:
    generate_dataset(SyntheticConfig(seed=31, output_dir=str(output_dir)))
    normalize_dataset(output_dir)
    train_stable_propagation(output_dir)
    solve_sparse_inversion(output_dir)


def test_semantic_evidence_outputs_and_true_root_improvement(tmp_path) -> None:
    output_dir = tmp_path / "demo"
    _prepare_dataset(output_dir)
    result = score_semantic_evidence(output_dir)

    semantic_path = output_dir / "semantic_interventions.jsonl"
    type_scores_path = output_dir / "semantic_type_scores.jsonl"
    summary_path = output_dir / "semantic_evidence_summary.json"
    metadata_path = output_dir / "semantic_evidence_metadata.json"

    assert semantic_path.exists()
    assert type_scores_path.exists()
    assert summary_path.exists()
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    semantic_records = read_jsonl(semantic_path)
    type_scores = read_jsonl(type_scores_path)
    incidents = read_jsonl(output_dir / "incidents.jsonl")

    assert metadata["incidents_count"] == 4
    assert metadata["expected_candidates_count"] == 4 * 176
    assert metadata["candidates_count"] == metadata["expected_candidates_count"]
    assert metadata["candidates_count_matches_expected"] is True
    assert result["metadata"]["semantic_records_count"] == metadata["semantic_records_count"]

    assert semantic_records
    for record in semantic_records:
        assert "semantic_score" in record
        assert "semantic_rank" in record
        assert "evidence_score" in record
        assert "evidence_type" in record
        assert "path" not in record
        assert "evidence" not in record
        assert "top_services" not in record
        assert "top_metrics" not in record

    by_incident_node = {(row["incident_id"], row["node"]): row for row in semantic_records}
    for incident in incidents:
        root_node = f"{incident['root_service']}.{incident['root_metric']}"
        root_record = by_incident_node[(incident["incident_id"], root_node)]
        assert root_record["semantic_score"] > root_record["sparse_score"]
        assert root_record["semantic_rank"] <= root_record["sparse_rank"]
        assert root_record["semantic_rank"] <= 2

    root_type_candidates = {row["root_type_candidate"] for row in type_scores}
    assert {"CPU", "network", "storage I/O", "lock contention"}.issubset(root_type_candidates)


def test_semantic_evidence_cli(tmp_path) -> None:
    output_dir = tmp_path / "demo-cli"
    _prepare_dataset(output_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.score_semantic_evidence", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "语义证据打分完成" in completed.stdout
    assert "不是最终 RCAResult" in completed.stdout


def test_metric_specificity_weight_orders_diagnostic_metrics():
    assert metric_specificity_weight("cpu.throttled_usec") > metric_specificity_weight("cpu.pressure")
    assert metric_specificity_weight("net.retrans") > metric_specificity_weight("net.rtt_ms")
    assert metric_specificity_weight("io.bio_latency_ms") > metric_specificity_weight("io.queue_depth")
    assert metric_specificity_weight("lock.futex_wait_ms") > metric_specificity_weight("request.p99_latency_ms")
    assert metric_specificity_weight("request.p99_latency_ms") < 1.0

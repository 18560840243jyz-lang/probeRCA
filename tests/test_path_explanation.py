import json
import subprocess
import sys
from pathlib import Path

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.semantic import score_semantic_evidence
from proberca.explain.path import explain_paths
from proberca.features.robust import normalize_dataset
from proberca.inference.sparse import solve_sparse_inversion
from proberca.propagation.stable import train_stable_propagation


def _prepare_dataset(output_dir: Path) -> None:
    generate_dataset(SyntheticConfig(seed=41, output_dir=str(output_dir)))
    normalize_dataset(output_dir)
    train_stable_propagation(output_dir)
    solve_sparse_inversion(output_dir)
    score_semantic_evidence(output_dir)


def test_path_explanation_outputs_and_path_fidelity(tmp_path):
    output_dir = tmp_path / "demo"
    _prepare_dataset(output_dir)

    result = explain_paths(output_dir)

    assert Path(result["path_explanations_path"]).exists()
    assert Path(result["path_explanation_summary_path"]).exists()
    assert Path(result["path_explanation_metadata_path"]).exists()

    metadata = json.loads(Path(result["path_explanation_metadata_path"]).read_text(encoding="utf-8"))
    assert metadata["incidents_count"] == 4
    assert metadata["path_records_count"] > 0
    assert metadata["candidates_explained_count"] >= 4

    records = read_jsonl(output_dir / "path_explanations.jsonl")
    assert records
    for record in records:
        assert "path" in record
        assert "path_score" in record
        assert "candidate_service" in record
        assert "candidate_metric" in record
        assert "symptom_service" in record
        assert "top_services" not in record
        assert "top_metrics" not in record
        assert "RCAResult" not in record

    semantic_records = read_jsonl(output_dir / "semantic_interventions.jsonl")
    incidents = read_jsonl(output_dir / "incidents.jsonl")
    for incident in incidents:
        incident_id = incident["incident_id"]
        top_candidate = next(row for row in semantic_records if row["incident_id"] == incident_id and row["semantic_rank"] == 1)
        top_paths = [row for row in records if row["incident_id"] == incident_id and row["candidate_node"] == top_candidate["node"]]
        assert top_paths
        for path_record in top_paths:
            assert path_record["path"][0] == path_record["candidate_service"]
            if not path_record.get("path_missing"):
                assert path_record["path"][-1] == path_record["symptom_service"]

    summary = json.loads((output_dir / "path_explanation_summary.json").read_text(encoding="utf-8"))
    hits = [
        item["path_fidelity_debug"].get("top_path_intersects_injected_path") is True
        for item in summary["summaries"]
    ]
    assert sum(hits) >= 3


def test_path_explanation_cli(tmp_path):
    output_dir = tmp_path / "demo-cli"
    _prepare_dataset(output_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.explain_paths", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "路径解释完成" in completed.stdout
    assert "不是最终 RCAResult" in completed.stdout

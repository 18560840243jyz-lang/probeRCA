import json
import subprocess
import sys

from proberca.data.io import read_jsonl
from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.features.robust import normalize_dataset
from proberca.inference.sparse import solve_sparse_inversion
from proberca.propagation.stable import train_stable_propagation


def _prepare_dataset(output_dir) -> None:
    generate_dataset(SyntheticConfig(seed=29, output_dir=str(output_dir)))
    normalize_dataset(output_dir)
    train_stable_propagation(output_dir)


def test_sparse_inversion_outputs_and_true_roots(tmp_path) -> None:
    output_dir = tmp_path / "demo"
    _prepare_dataset(output_dir)
    result = solve_sparse_inversion(output_dir)

    interventions_path = output_dir / "sparse_interventions.jsonl"
    summary_path = output_dir / "sparse_inversion_summary.json"
    metadata_path = output_dir / "sparse_inversion_metadata.json"

    assert interventions_path.exists()
    assert summary_path.exists()
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    interventions = read_jsonl(interventions_path)
    incidents = read_jsonl(output_dir / "incidents.jsonl")

    assert metadata["incidents_count"] == 4
    assert metadata["expected_candidates_count"] == 4 * 176
    assert metadata["candidates_count"] == metadata["expected_candidates_count"]
    assert metadata["candidates_count_matches_expected"] is True
    assert result["metadata"]["candidates_count"] == metadata["candidates_count"]

    assert interventions
    for record in interventions:
        assert "intervention_score" in record
        assert "rank" in record
        assert "service" in record
        assert "metric" in record
        assert "path" not in record
        assert "root_type" not in record
        assert "evidence" not in record

    by_incident_node = {(row["incident_id"], row["node"]): row for row in interventions}
    for incident in incidents:
        root_node = f"{incident['root_service']}.{incident['root_metric']}"
        root_record = by_incident_node[(incident["incident_id"], root_node)]
        assert root_record["intervention_score"] > 0
        assert root_record["rank"] <= 5


def test_sparse_inversion_cli(tmp_path) -> None:
    output_dir = tmp_path / "demo-cli"
    _prepare_dataset(output_dir)
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.solve_sparse_inversion", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "稀疏反演完成" in completed.stdout
    assert "不是最终 RCA 结果" in completed.stdout

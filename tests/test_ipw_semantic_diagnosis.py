import subprocess
import sys

from proberca.data.synthetic import SyntheticConfig, generate_dataset
from proberca.evidence.ipw_semantic import score_ipw_semantic_evidence
from proberca.evidence.ipw_semantic_diagnosis import diagnose_ipw_semantic_sibling_errors
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
    score_ipw_semantic_evidence(output_dir)


def test_ipw_semantic_sibling_diagnosis_outputs(tmp_path):
    output_dir = tmp_path / "p1d_diag"
    _prepare_dataset(output_dir)

    result = diagnose_ipw_semantic_sibling_errors(output_dir)

    assert (output_dir / "ipw_semantic_sibling_diagnosis.json").exists()
    assert "failed_top1_incidents" in result
    assert "same_service_sibling_errors" in result
    assert "same_type_sibling_errors" in result
    assert "per_incident_top5" in result
    assert isinstance(result["failed_top1_incidents"], list)


def test_ipw_semantic_sibling_diagnosis_cli(tmp_path):
    output_dir = tmp_path / "p1d_diag_cli"
    _prepare_dataset(output_dir)

    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.diagnose_ipw_semantic", "--input", str(output_dir)],
        check=False,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert "P1D sibling diagnosis 完成" in completed.stdout

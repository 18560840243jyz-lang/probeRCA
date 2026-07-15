from pathlib import Path
import subprocess
import sys

from scripts.check_source_tree_hygiene import find_backup_artifacts


ROOT = Path(__file__).resolve().parents[1]


def test_p11_source_scopes_contain_no_backup_artifacts():
    violations = find_backup_artifacts(ROOT)
    assert violations == (), "\n".join(violations)

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/check_source_tree_hygiene.py"),
            "--root",
            str(ROOT),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "BACKUP_ARTIFACT_COUNT=0"


def test_hygiene_gate_detects_all_generic_backup_patterns_in_stable_order(
    tmp_path: Path,
):
    paths = (
        "Dockerfile.bak",
        "proberca/module.py.backup",
        "requirements/production.lock.txt.save",
        "deploy/kubernetes/base/example.yaml.rej",
        "deploy/kubernetes/test/p11-smoke/config.yaml~",
        "scripts/p11_smoke_example.sh.orig",
        "scripts/.validate_p11_image_reference.py.swp",
        "tests/.test_p11_example.py.swo",
    )
    for relative in reversed(paths):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("backup", encoding="utf-8")

    assert find_backup_artifacts(tmp_path) == tuple(sorted(paths))

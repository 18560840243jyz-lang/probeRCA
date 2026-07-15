from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import p11_release_artifacts as artifacts


def _repo(root: Path) -> Path:
    (root / "requirements").mkdir(parents=True)
    (root / "proberca").mkdir()
    (root / "deploy/kubernetes/base").mkdir(parents=True)
    (root / "deploy/kubernetes/test").mkdir(parents=True)
    (root / "scripts").mkdir()
    (root / "tests").mkdir()
    (root / "Dockerfile").write_text("FROM scratch\nCOPY proberca /app/proberca\n")
    (root / ".dockerignore").write_text("**\n!proberca/**/*.py\n")
    (root / "pyproject.toml").write_text("[tool.pytest.ini_options]\n")
    for name in ("production", "build", "test"):
        (root / f"requirements/{name}.lock.txt").write_text(f"{name}==1 --hash=sha256:{'a'*64}\n")
    (root / "proberca/__init__.py").write_text("VALUE = 1\n")
    (root / "deploy/kubernetes/base/deployment.yaml").write_text("kind: Deployment\n")
    (root / "scripts/p11_smoke_run.sh").write_text("#!/bin/sh\n")
    (root / "scripts/validate_p11_image_reference.py").write_text("VALUE = 1\n")
    (root / "scripts/check_source_tree_hygiene.py").write_text("VALUE = 1\n")
    (root / "scripts/p11_release_artifacts.py").write_text("VALUE = 1\n")
    (root / "tests/test_p11_fixture.py").write_text("def test_ok(): pass\n")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=x", "-c", "user.email=x@y", "commit", "-qm", "fixture"], cwd=root, check=True)
    return root


def _evidence() -> dict:
    return {"p11_test_file_count": 1, "p11_directed_passed": 1,
            "full_regression_passed": 1, "failed": 0, "errors": 0,
            "skipped": 0, "xfail": 0,
            "transaction_contract_version": "live-transaction-v2",
            "smoke_harness_contract_version": "p11-smoke-harness-v2",
            "image_identity_contract": "stage-independent-v1"}


def test_same_source_manifests_are_byte_identical(tmp_path):
    repo = _repo(tmp_path / "repo")
    first = artifacts.build_source_manifests(repo, _evidence())
    second = artifacts.build_source_manifests(repo, _evidence())
    assert {k: artifacts.canonical_bytes(v) for k, v in first.items()} == {k: artifacts.canonical_bytes(v) for k, v in second.items()}


def test_source_byte_change_changes_runtime_fingerprint(tmp_path):
    repo = _repo(tmp_path / "repo")
    first = artifacts.source_fingerprints(artifacts.build_source_manifests(repo, _evidence()))
    (repo / "proberca/__init__.py").write_text("VALUE = 2\n")
    second = artifacts.source_fingerprints(artifacts.build_source_manifests(repo, _evidence()))
    assert first["runtime_source_fingerprint"] != second["runtime_source_fingerprint"]


@pytest.mark.parametrize("location", ["inside", "tmp"])
def test_persistent_root_rejects_repository_or_tmp(tmp_path, location):
    repo = _repo(tmp_path / "repo")
    candidate = repo / "artifacts" if location == "inside" else Path("/tmp/p11-artifacts")
    with pytest.raises(artifacts.ArtifactError):
        artifacts.validate_persistent_root(repo, candidate)


def test_atomic_write_interruption_leaves_no_valid_target(tmp_path, monkeypatch):
    target = tmp_path / "record.json"
    def fail_replace(*_args):
        raise OSError("interrupted")
    monkeypatch.setattr(artifacts.os, "replace", fail_replace)
    with pytest.raises(OSError):
        artifacts.atomic_write(target, b"payload")
    assert not target.exists()


def test_atomic_write_refuses_overwrite_with_different_bytes(tmp_path):
    target = tmp_path / "record.json"
    artifacts.atomic_write(target, b"first")
    artifacts.atomic_write(target, b"first")
    with pytest.raises(artifacts.ArtifactError):
        artifacts.atomic_write(target, b"second")


def test_source_manifest_contract_has_stable_entries(tmp_path):
    repo = _repo(tmp_path / "repo")
    manifests = artifacts.build_source_manifests(repo, _evidence())
    runtime = manifests["runtime_source_manifest.json"]
    assert runtime["schema_version"] == "p11-source-manifest-v2"
    assert [item["path"] for item in runtime["files"]] == sorted(item["path"] for item in runtime["files"])
    assert all(set(item) >= {"path", "type", "executable", "mode", "size", "sha256", "git_state"} for item in runtime["files"])


def test_generator_source_has_no_stage_or_current_identity_hardcoding():
    source = Path(artifacts.__file__).read_text()
    forbidden = ("p11-s3-", "p11-s4fix-", "ca0907cc", "ed05480b", "proberca-ob")
    assert not any(value in source for value in forbidden)
    assert "hash(" not in source


def test_canonical_json_has_no_nondeterministic_identity_fields(tmp_path):
    repo = _repo(tmp_path / "repo")
    raw = artifacts.canonical_bytes(artifacts.build_source_manifests(repo, _evidence())["runtime_source_manifest.json"])
    for token in (str(repo).encode(), b"mtime", b"ctime", b"inode", b"hostname", b"username"):
        assert token not in raw


def test_long_inline_json_argument_is_not_treated_as_a_path():
    value = {"evidence": "x" * 512}
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    assert artifacts._json_argument(raw) == value

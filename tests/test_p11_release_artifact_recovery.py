from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from scripts import p11_release_artifacts as artifacts
from tests.test_p11_durable_release_artifacts import _evidence, _repo


def _canonical(path: Path, value: object) -> str:
    raw = artifacts.canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _bundle(root: Path, repo: Path) -> Path:
    manifests = artifacts.build_source_manifests(repo, _evidence())
    root.mkdir(parents=True)
    for name, value in manifests.items():
        _canonical(root / name, value)
    fps = artifacts.source_fingerprints(manifests)
    identity = {
        "schema_version": "p11-image-identity-v2", "source_manifest_schema_version": "p11-source-manifest-v2",
        "HEAD": "a" * 40, "runtime_source_fingerprint": fps["runtime_source_fingerprint"],
        "validation_source_fingerprint": fps["validation_source_fingerprint"], "test_evidence_fingerprint": fps["test_evidence_fingerprint"],
        "image_tag": "proberca:src-" + fps["runtime_source_fingerprint"][:16],
        "image_id": "sha256:" + "b" * 64, "OCI_labels": {"org.opencontainers.image.revision": "a" * 40,
        "io.proberca.source-fingerprint": fps["runtime_source_fingerprint"], "io.proberca.schema-version": "generation_v5"},
        "architecture": "amd64", "OS": "linux", "image_user": "65534", "entrypoint": ["python3", "-m", "proberca.cli.live"],
        "lock_fingerprints": {}, "test_evidence": _evidence(), "transaction_contract_version": "live-transaction-v2",
        "smoke_harness_contract_version": "p11-smoke-harness-v2", "artifact_persistence_contract": "p11-durable-artifacts-v1",
        "ready_for_node_import": True,
    }
    identity_fp = _canonical(root / "image_identity.json", identity)
    node = {"schema_version": "p11-node-image-evidence-v2", "image_identity_fingerprint": identity_fp,
            "nodes": [{"node_name": "dynamic-node", "architecture": "amd64", "os": "linux", "runtime": "containerd",
            "image_tag": identity["image_tag"], "runtime_image_id": identity["image_id"], "runtime_manifest_digest": "sha256:" + "c"*64,
            "runtime_config_id": identity["image_id"], "source_fingerprint": identity["runtime_source_fingerprint"], "revision": identity["HEAD"],
            "runtime_query_succeeded": True, "identity_verified": True, "verification_method": "containerd_runtime_query"}]}
    node_fp = _canonical(root / "node_image_evidence.json", node)
    binding = artifacts.build_release_binding(identity, identity_fp, node, node_fp, manifests, _evidence())
    _canonical(root / "release_binding.json", binding)
    return root


def test_verify_bundle_passes_after_unrelated_tmp_is_deleted(tmp_path):
    repo = _repo(tmp_path / "repo")
    bundle = _bundle(tmp_path / "durable", repo)
    transient = tmp_path / "temporary"; transient.mkdir(); (transient / "x").write_text("x"); shutil.rmtree(transient)
    assert artifacts.verify_bundle(bundle, repo=repo, verify_docker=False)["ready_for_s4"] is True


@pytest.mark.parametrize("name", ["runtime_source_manifest.json", "image_identity.json", "node_image_evidence.json", "release_binding.json"])
def test_verify_rejects_tampered_bundle_file(tmp_path, name):
    repo = _repo(tmp_path / "repo"); bundle = _bundle(tmp_path / "durable", repo)
    value = json.loads((bundle / name).read_bytes()); value["tampered"] = True; _canonical(bundle / name, value)
    with pytest.raises(artifacts.ArtifactError):
        artifacts.verify_bundle(bundle, repo=repo, verify_docker=False)


@pytest.mark.parametrize("field", ["image_id", "runtime_source_fingerprint"])
def test_verify_rejects_wrong_image_identity(tmp_path, field):
    repo = _repo(tmp_path / "repo"); bundle = _bundle(tmp_path / "durable", repo)
    value = json.loads((bundle / "image_identity.json").read_bytes()); value[field] = "sha256:" + "d"*64 if field == "image_id" else "d"*64
    _canonical(bundle / "image_identity.json", value)
    with pytest.raises(artifacts.ArtifactError): artifacts.verify_bundle(bundle, repo=repo, verify_docker=False)


def test_ready_flag_cannot_bypass_broken_binding(tmp_path):
    repo = _repo(tmp_path / "repo"); bundle = _bundle(tmp_path / "durable", repo)
    value = json.loads((bundle / "release_binding.json").read_bytes()); value["ready_for_s4"] = True; value["image_id"] = "sha256:" + "e"*64
    _canonical(bundle / "release_binding.json", value)
    with pytest.raises(artifacts.ArtifactError): artifacts.verify_bundle(bundle, repo=repo, verify_docker=False)


def test_historical_release_is_not_overwritten(tmp_path):
    target = tmp_path / "release"; target.mkdir(); (target / "record").write_text("old")
    with pytest.raises(artifacts.ArtifactError): artifacts.publish_candidate(tmp_path / "missing", target)


def test_sensitive_markers_are_rejected(tmp_path):
    repo = _repo(tmp_path / "repo"); bundle = _bundle(tmp_path / "durable", repo)
    (bundle / "leak.txt").write_text("Authorization: Bearer actual-value")
    with pytest.raises(artifacts.ArtifactError): artifacts.verify_no_sensitive_content(bundle)


def test_node_and_identity_fingerprints_are_cross_checked(tmp_path):
    repo = _repo(tmp_path / "repo"); bundle = _bundle(tmp_path / "durable", repo)
    node = json.loads((bundle / "node_image_evidence.json").read_bytes()); node["image_identity_fingerprint"] = "f"*64
    _canonical(bundle / "node_image_evidence.json", node)
    with pytest.raises(artifacts.ArtifactError): artifacts.verify_bundle(bundle, repo=repo, verify_docker=False)

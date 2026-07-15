from __future__ import annotations

from pathlib import Path


def test_source_fingerprint_includes_production_diff_but_excludes_tests(tmp_path):
    from proberca.live.identity import compute_source_fingerprint

    (tmp_path / "proberca").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "proberca" / "runtime.py").write_text("one", encoding="utf-8")
    (tmp_path / "tests" / "test_runtime.py").write_text("one", encoding="utf-8")
    first = compute_source_fingerprint(tmp_path, head_revision="abc")
    (tmp_path / "tests" / "test_runtime.py").write_text("two", encoding="utf-8")
    assert compute_source_fingerprint(tmp_path, head_revision="abc") == first
    (tmp_path / "proberca" / "runtime.py").write_text("two", encoding="utf-8")
    assert compute_source_fingerprint(tmp_path, head_revision="abc") != first


def test_source_fingerprint_excludes_credentials_and_runtime(tmp_path):
    from proberca.live.identity import compute_source_fingerprint

    (tmp_path / "proberca").mkdir()
    (tmp_path / "proberca" / "live.py").write_text("live", encoding="utf-8")
    first = compute_source_fingerprint(tmp_path, head_revision="abc")
    (tmp_path / "token").write_text("secret", encoding="utf-8")
    (tmp_path / "checkpoint").mkdir()
    (tmp_path / "checkpoint" / "CURRENT").write_text("runtime", encoding="utf-8")
    assert compute_source_fingerprint(tmp_path, head_revision="abc") == first


def test_status_exposes_credential_free_build_identity():
    from proberca.live.health import LiveHealthState

    state = LiveHealthState(code_revision="abc", source_fingerprint="f" * 64,
                            schema_version="1.0", image_digest="sha256:" + "d" * 64)
    status = state.status()
    assert status["build"]["code_revision"] == "abc"
    assert status["build"]["source_fingerprint"] == "f" * 64
    assert "token" not in str(status).lower()
    assert "kubeconfig" not in str(status).lower()


def test_deployment_identity_mismatch_is_rejected():
    from proberca.live.identity import ImageIdentityMismatchError, verify_runtime_identity
    import pytest

    with pytest.raises(ImageIdentityMismatchError):
        verify_runtime_identity("a" * 64, "b" * 64)



def test_source_fingerprint_includes_image_build_definition(tmp_path):
    from proberca.live.identity import compute_source_fingerprint

    (tmp_path / "proberca").mkdir()
    (tmp_path / "proberca" / "runtime.py").write_text("runtime", encoding="utf-8")
    (tmp_path / "Dockerfile").write_text("FROM one", encoding="utf-8")
    first = compute_source_fingerprint(tmp_path, head_revision="abc")
    (tmp_path / "Dockerfile").write_text("FROM two", encoding="utf-8")
    assert compute_source_fingerprint(tmp_path, head_revision="abc") != first

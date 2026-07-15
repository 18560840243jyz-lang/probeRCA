from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIGEST_FROM = re.compile(r"^FROM\s+[^\s]+@sha256:[0-9a-f]{64}$", re.MULTILINE)

def test_root_dockerfile_uses_digest_lock_and_explicit_copy():
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert DIGEST_FROM.search(text)
    assert "latest" not in text.lower() and "COPY ." not in text and "ADD ." not in text
    assert "requirements/production.lock.txt" in text and "--require-hashes" in text
    assert "pip install" in text and "--no-deps" in text and "--no-build-isolation" in text
    assert re.search(r"^USER\s+(?!0|root\b).+$", text, re.MULTILINE)
    assert 'ENTRYPOINT ["python3", "-m", "proberca.cli.live"]' in text

def test_root_dockerfile_does_not_resolve_named_packages():
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    for name in ("numpy", "scipy", "pyyaml", "pyarrow", "kubernetes", "requests"):
        assert not re.search(rf"pip install[^\n]*\b{name}([<>=]|\s|$)", text, re.IGNORECASE)
    assert "pip install -U" not in text

def test_smoke_dockerfile_is_digest_pinned_and_context_limited():
    text = (ROOT / "deploy/kubernetes/test/p11-smoke/Dockerfile").read_text(encoding="utf-8")
    assert DIGEST_FROM.search(text)
    assert "COPY ." not in text and "ADD ." not in text and "pip install" not in text
    assert re.search(r"^USER\s+(?!0|root\b).+$", text, re.MULTILINE)

def test_no_project_dockerfile_uses_latest_copy_dot_or_upgrade():
    dockerfiles = [path for path in ROOT.rglob("Dockerfile") if "external" not in path.parts]
    assert dockerfiles
    for path in dockerfiles:
        text = path.read_text(encoding="utf-8")
        assert "latest" not in text.lower(), path
        assert "COPY ." not in text and "ADD ." not in text and "pip install -U" not in text, path


def test_all_runtime_python_sources_are_readable_by_non_root_image_user():
    unreadable = [
        path.as_posix()
        for path in Path("proberca").rglob("*.py")
        if not (path.stat().st_mode & 0o004)
    ]
    assert unreadable == []

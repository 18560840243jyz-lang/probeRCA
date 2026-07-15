from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SENTINEL = "proberca:0.0.0-image-must-be-overridden"

def _validate(value: str, *, allow_sentinel: bool = False):
    args = [sys.executable, str(ROOT / "scripts/validate_p11_image_reference.py"), value]
    if allow_sentinel:
        args.append("--allow-sentinel")
    return subprocess.run(args, text=True, capture_output=True, check=False)

def test_base_manifest_uses_non_deployable_sentinel_and_marker():
    text = (ROOT / "deploy/kubernetes/base/deployment.yaml").read_text(encoding="utf-8")
    assert f"image: {SENTINEL}" in text
    assert 'proberca.io/immutable-image-required: "true"' in text
    assert "proberca:latest" not in text

def test_image_policy_rejects_latest_empty_and_unoverridden_sentinel():
    assert _validate("proberca:latest").returncode != 0
    assert _validate("").returncode != 0
    assert _validate(SENTINEL).returncode != 0

def test_single_reference_policy_is_digest_only():
    assert _validate("registry.invalid/proberca@sha256:" + "d" * 64).returncode == 0
    unbound_tag = "proberca:release-" + "a" * 16
    assert _validate(unbound_tag).returncode != 0

def test_base_validation_may_explicitly_allow_sentinel():
    assert _validate(SENTINEL, allow_sentinel=True).returncode == 0

def test_smoke_apply_path_validates_image_before_kubectl():
    text = (ROOT / "scripts/p11_smoke_run.sh").read_text(encoding="utf-8")
    assert text.index("validate_p11_image_reference.py") < text.index("kubectl apply")

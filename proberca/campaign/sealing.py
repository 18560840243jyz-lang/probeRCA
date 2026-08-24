"""Public-key sealing for Test metadata; plaintext is never written to disk."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from .model import canonical_json


class PrivateManifestSealingError(RuntimeError):
    pass


def seal_private_manifest(
    payload: Any,
    *,
    recipient_certificate: Path,
    output_path: Path,
    openssl_executable: str = "openssl",
) -> None:
    certificate = Path(recipient_certificate).resolve()
    output = Path(output_path).resolve()
    if not certificate.is_file():
        raise PrivateManifestSealingError("Test recipient certificate does not exist")
    if output.exists():
        raise PrivateManifestSealingError("refusing to overwrite sealed Test manifest")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    command = [
        openssl_executable, "cms", "-encrypt", "-binary", "-aes-256-gcm",
        "-outform", "DER", "-recip", str(certificate), "-out", str(temporary),
    ]
    encoded = (canonical_json(payload) + "\n").encode("utf-8")
    completed = subprocess.run(
        command, input=encoded, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        temporary.unlink(missing_ok=True)
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise PrivateManifestSealingError(f"OpenSSL CMS sealing failed: {message}")
    if not temporary.is_file() or temporary.stat().st_size == 0:
        temporary.unlink(missing_ok=True)
        raise PrivateManifestSealingError("OpenSSL produced no sealed Test manifest")
    os.replace(temporary, output)

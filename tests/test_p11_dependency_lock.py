from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HASH = re.compile(r"--hash=sha256:([0-9a-f]{64})")

def _read(name: str):
    path = ROOT / "requirements" / name
    first, *rest = path.read_text(encoding="utf-8").splitlines()
    prefix = "# proberca-lock-metadata: "
    assert first.startswith(prefix)
    metadata = json.loads(first[len(prefix):])
    entries, current = [], ""
    for line in rest:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        current += line.strip().removesuffix("\\").strip() + " "
        if not line.rstrip().endswith("\\"):
            match = re.match(r"^([A-Za-z0-9_.-]+)==([^ ]+)", current)
            assert match, current
            hashes = HASH.findall(current)
            assert hashes, current
            entries.append((match.group(1).lower().replace("_", "-"), match.group(2), tuple(hashes)))
            current = ""
    assert not current
    return metadata, entries

def test_production_lock_is_exact_hashed_unique_and_excludes_pytest():
    metadata, entries = _read("production.lock.txt")
    names = [entry[0] for entry in entries]
    assert names == sorted(names) and len(names) == len(set(names))
    assert "pytest" not in names
    assert (metadata["python_version"], metadata["target_os"], metadata["target_architecture"]) == ("3.10", "linux", "x86_64")
    assert len(metadata["source_input_fingerprint"]) == 64
    assert len(metadata["lock_fingerprint"]) == 64

def test_test_lock_is_exact_hashed_and_contains_production_closure():
    _, production = _read("production.lock.txt")
    _, test = _read("test.lock.txt")
    prod_versions = {name: version for name, version, _ in production}
    test_versions = {name: version for name, version, _ in test}
    assert "pytest" in test_versions
    assert prod_versions.items() <= test_versions.items()

def test_lock_fingerprint_matches_canonical_payload():
    for name in ("production.lock.txt", "test.lock.txt"):
        metadata, entries = _read(name)
        payload = json.dumps([{"hashes": list(h), "name": n, "version": v} for n, v, h in entries], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        assert metadata["lock_fingerprint"] == hashlib.sha256(payload).hexdigest()

def test_lock_generator_is_deterministic_by_construction():
    script = (ROOT / "scripts/generate_dependency_locks.py").read_text(encoding="utf-8")
    assert "sort_keys=True" in script and "sha256" in script and "--report" in script

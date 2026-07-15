#!/usr/bin/env python3
"""Resolve and render deterministic hashed locks from pyproject direct pins."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

SCHEMA_VERSION = "proberca-dependency-lock-v1"

def _canonical_json(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()

def _source_fingerprint(kind: str, requirements: list[str]) -> str:
    payload = {"kind": kind, "python_version": "3.10", "requirements": sorted(requirements), "schema_version": SCHEMA_VERSION, "target_architecture": "x86_64", "target_os": "linux"}
    return hashlib.sha256(_canonical_json(payload)).hexdigest()

def _resolve(resolver_python: Path, requirements: list[str], report: Path, index_url: str, timeout_sec: int) -> None:
    environment = os.environ.copy()
    environment["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    environment["PIP_INDEX_URL"] = index_url
    command = [str(resolver_python), "-m", "pip", "install", "--dry-run", "--ignore-installed", "--report", str(report), *requirements]
    subprocess.run(command, env=environment, check=True, timeout=timeout_sec)

def _entries(report: dict) -> list[dict]:
    result = []
    for item in report.get("install", []):
        metadata = item["metadata"]
        archive = item.get("download_info", {}).get("archive_info", {})
        hashes = archive.get("hashes") or {}
        sha256 = hashes.get("sha256")
        if not sha256 and str(archive.get("hash", "")).startswith("sha256="):
            sha256 = archive["hash"].split("=", 1)[1]
        if not sha256 or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise RuntimeError(f"missing sha256 artifact hash for {_canonical_name(metadata['name'])}")
        result.append({"hashes": [sha256], "name": _canonical_name(metadata["name"]), "version": metadata["version"]})
    result.sort(key=lambda item: item["name"])
    names = [item["name"] for item in result]
    if len(names) != len(set(names)):
        raise RuntimeError("resolver report contains duplicate canonical distributions")
    return result

def _render(kind: str, requirements: list[str], report_path: Path, output_path: Path) -> dict:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    entries = _entries(report)
    lock_fingerprint = hashlib.sha256(_canonical_json(entries)).hexdigest()
    metadata = {"lock_fingerprint": lock_fingerprint, "pip_version": report.get("pip_version", "unknown"), "python_version": "3.10", "schema_version": SCHEMA_VERSION, "source_input_fingerprint": _source_fingerprint(kind, requirements), "target_architecture": "x86_64", "target_os": "linux"}
    lines = ["# proberca-lock-metadata: " + _canonical_json(metadata).decode("ascii")]
    for item in entries:
        lines.append(f"{item['name']}=={item['version']} \\")
        for index, artifact_hash in enumerate(item["hashes"]):
            suffix = " \\" if index + 1 < len(item["hashes"]) else ""
            lines.append(f"    --hash=sha256:{artifact_hash}{suffix}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return metadata

def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--resolver-python", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--index-url", default=os.environ.get("PIP_INDEX_URL", "https://pypi.org/simple"))
    parser.add_argument("--timeout-sec", type=int, default=600)
    args = parser.parse_args(argv)
    project_root = args.project_root.resolve()
    output_dir = (args.output_dir or project_root / "requirements").resolve()
    project = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))
    production = list(project["project"]["dependencies"])
    test = production + list(project["project"]["optional-dependencies"]["test"])
    build = list(project["build-system"]["requires"])
    args.report_dir.mkdir(parents=True, exist_ok=True)
    for kind, requirements in (("build", build), ("production", production), ("test", test)):
        report = args.report_dir / f"{kind}.json"
        _resolve(args.resolver_python, requirements, report, args.index_url, args.timeout_sec)
        metadata = _render(kind, requirements, report, output_dir / f"{kind}.lock.txt")
        count = len(_entries(json.loads(report.read_text(encoding="utf-8"))))
        print(f"{kind} packages={count} fingerprint={metadata['lock_fingerprint']}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

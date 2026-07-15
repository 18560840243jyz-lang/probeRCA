#!/usr/bin/env python3
"""Create and verify durable, stage-independent P11 release artifacts."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
from typing import Iterable, Mapping, Sequence

SOURCE_SCHEMA = "p11-source-manifest-v2"
IMAGE_SCHEMA = "p11-image-identity-v2"
NODE_SCHEMA = "p11-node-image-evidence-v2"
BINDING_SCHEMA = "p11-release-binding-v2"
PERSISTENCE_CONTRACT = "p11-durable-artifacts-v1"
TRANSACTION_CONTRACT = "live-transaction-v2"
SMOKE_CONTRACT = "p11-smoke-harness-v2"
IDENTITY_CONTRACT = "stage-independent-v1"
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_FILES = (
    "runtime_source_manifest.json",
    "validation_source_manifest.json",
    "test_evidence_manifest.json",
    "image_identity.json",
    "node_image_evidence.json",
    "release_binding.json",
)


class ArtifactError(RuntimeError):
    pass


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists():
        if path.read_bytes() == data:
            return
        raise ArtifactError(f"refusing to overwrite different artifact: {path.name}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def atomic_json(path: Path, value: object) -> str:
    raw = canonical_bytes(value)
    atomic_write(path, raw)
    return sha256_bytes(raw)


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_persistent_root(repo: Path, artifact_root: Path) -> Path:
    repo = repo.resolve()
    artifact_root = artifact_root.resolve()
    if _inside(artifact_root, repo):
        raise ArtifactError("artifact root must be outside the repository")
    if _inside(artifact_root, Path("/tmp")):
        raise ArtifactError("artifact root must not be under /tmp")
    return artifact_root


def _run(
    command: Sequence[str], *, timeout: int = 30, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command), cwd=cwd, check=True, capture_output=True,
            text=True, timeout=timeout,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        detail = getattr(error, "stderr", "") or getattr(error, "stdout", "")
        raise ArtifactError(f"command failed: {' '.join(command)}: {detail}") from error


def _git_sets(repo: Path) -> tuple[set[str], set[str]]:
    tracked = set(filter(None, _run(("git", "ls-files", "-z"), cwd=repo).stdout.split("\0")))
    modified: set[str] = set()
    for command in (("git", "diff", "--name-only", "-z"),
                    ("git", "diff", "--cached", "--name-only", "-z")):
        modified.update(filter(None, _run(command, cwd=repo).stdout.split("\0")))
    return tracked, modified


def _runtime_paths(repo: Path) -> tuple[Path, ...]:
    fixed = (
        repo / "Dockerfile", repo / ".dockerignore", repo / "pyproject.toml",
        repo / "requirements/production.lock.txt",
        repo / "requirements/build.lock.txt",
    )
    package = tuple(sorted((repo / "proberca").rglob("*.py")))
    paths = fixed + package
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise ArtifactError(f"runtime source is missing: {missing[0]}")
    non_python = [
        path for path in (repo / "proberca").rglob("*")
        if path.is_file() and path.suffix != ".py"
        and "__pycache__" not in path.parts
    ]
    if non_python:
        raise ArtifactError(
            "non-Python package data requires an explicit runtime contract: "
            + ", ".join(path.relative_to(repo).as_posix() for path in non_python)
        )
    return paths


def _validation_paths(repo: Path) -> tuple[Path, ...]:
    paths: set[Path] = {repo / "requirements/test.lock.txt"}
    for root in (repo / "deploy/kubernetes/base", repo / "deploy/kubernetes/test"):
        if root.exists():
            paths.update(path for path in root.rglob("*") if path.is_file() or path.is_symlink())
    scripts = repo / "scripts"
    paths.update(scripts.glob("p11_smoke_*.sh"))
    for name in (
        "validate_p11_image_reference.py", "check_source_tree_hygiene.py",
        "p11_release_artifacts.py",
    ):
        paths.add(scripts / name)
    paths.update((repo / "tests").glob("test_p11*.py"))
    return tuple(sorted(path for path in paths if path.exists()))


def _test_paths(repo: Path) -> tuple[Path, ...]:
    paths = {
        repo / "pyproject.toml", repo / "requirements/test.lock.txt",
        repo / "scripts/check_source_tree_hygiene.py",
        repo / "scripts/validate_p11_image_reference.py",
        repo / "scripts/p11_release_artifacts.py",
    }
    paths.update((repo / "tests").glob("test_p11*.py"))
    return tuple(sorted(path for path in paths if path.exists()))


def _entry(repo: Path, path: Path, tracked: set[str], modified: set[str]) -> dict:
    relative = path.relative_to(repo).as_posix()
    metadata = path.lstat()
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISLNK(metadata.st_mode):
        target_text = os.readlink(path)
        resolved = (path.parent / target_text).resolve()
        if not _inside(resolved, repo.resolve()):
            raise ArtifactError(f"symlink escapes repository: {relative}")
        raw = target_text.encode("utf-8")
        entry_type = "symlink"
    elif stat.S_ISREG(metadata.st_mode):
        raw = path.read_bytes()
        entry_type = "regular"
    else:
        raise ArtifactError(f"unsupported source file type: {relative}")
    state = (
        "untracked" if relative not in tracked
        else "tracked_modified" if relative in modified
        else "tracked_clean"
    )
    result = {
        "path": relative,
        "type": entry_type,
        "executable": bool(mode & 0o111),
        "mode": f"{mode:04o}",
        "size": metadata.st_size,
        "sha256": sha256_bytes(raw),
        "git_state": state,
    }
    if entry_type == "symlink":
        result["symlink_target"] = target_text
    return result


def _manifest(repo: Path, source_set: str, paths: Iterable[Path],
              tracked: set[str], modified: set[str], **extra: object) -> dict:
    files = sorted(
        (_entry(repo, path, tracked, modified) for path in set(paths)),
        key=lambda item: item["path"],
    )
    counts = Counter(item["git_state"] for item in files)
    result = {
        "schema_version": SOURCE_SCHEMA,
        "source_set": source_set,
        "files": files,
        "file_count": len(files),
        "total_bytes": sum(item["size"] for item in files),
        "state_counts": {
            key: counts.get(key, 0)
            for key in ("tracked_clean", "tracked_modified", "untracked")
        },
    }
    result.update(extra)
    return result


def build_source_manifests(repo: Path, test_evidence: Mapping[str, object]) -> dict[str, dict]:
    repo = repo.resolve()
    tracked, modified = _git_sets(repo)
    runtime = _manifest(repo, "runtime", _runtime_paths(repo), tracked, modified)
    runtime_fingerprint = sha256_bytes(canonical_bytes(runtime))
    validation = _manifest(
        repo, "validation", _validation_paths(repo), tracked, modified,
        inputs={"runtime_source_fingerprint": runtime_fingerprint},
    )
    evidence = dict(test_evidence)
    for field in ("failed", "errors", "skipped", "xfail"):
        if evidence.get(field) != 0:
            raise ArtifactError(f"test evidence is not clean: {field}")
    tests = _manifest(
        repo, "test-evidence", _test_paths(repo), tracked, modified,
        test_evidence=evidence,
    )
    return {
        "runtime_source_manifest.json": runtime,
        "validation_source_manifest.json": validation,
        "test_evidence_manifest.json": tests,
    }


def source_fingerprints(manifests: Mapping[str, Mapping[str, object]]) -> dict[str, str]:
    return {
        "runtime_source_fingerprint": sha256_bytes(canonical_bytes(manifests["runtime_source_manifest.json"])),
        "validation_source_fingerprint": sha256_bytes(canonical_bytes(manifests["validation_source_manifest.json"])),
        "test_evidence_fingerprint": sha256_bytes(canonical_bytes(manifests["test_evidence_manifest.json"])),
    }


def write_source_manifests(repo: Path, output_dir: Path,
                           test_evidence: Mapping[str, object],
                           *, enforce_persistent: bool = True) -> dict[str, str]:
    if enforce_persistent:
        validate_persistent_root(repo, output_dir)
    manifests = build_source_manifests(repo, test_evidence)
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, value in manifests.items():
        atomic_json(output_dir / name, value)
    fingerprints = source_fingerprints(manifests)
    atomic_json(output_dir / "source_fingerprints.json", fingerprints)
    return fingerprints


def _load_canonical(path: Path, schema: str | None = None) -> tuple[dict, str]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactError(f"cannot read artifact {path.name}: {error}") from error
    if not isinstance(value, dict) or raw != canonical_bytes(value):
        raise ArtifactError(f"artifact is not canonical: {path.name}")
    if schema is not None and value.get("schema_version") != schema:
        raise ArtifactError(f"wrong schema for {path.name}")
    return value, sha256_bytes(raw)


def load_source_manifests(directory: Path) -> dict[str, dict]:
    return {
        name: _load_canonical(directory / name, SOURCE_SCHEMA)[0]
        for name in (
            "runtime_source_manifest.json", "validation_source_manifest.json",
            "test_evidence_manifest.json",
        )
    }


def create_image_identity(bundle_dir: Path, image_tag: str, repo: Path) -> dict:
    manifests = load_source_manifests(bundle_dir)
    fingerprints = source_fingerprints(manifests)
    expected_tag = "proberca:src-" + fingerprints["runtime_source_fingerprint"][:16]
    if image_tag != expected_tag:
        raise ArtifactError("image tag is not derived from runtime source identity")
    inspected = json.loads(_run(("docker", "image", "inspect", image_tag)).stdout)[0]
    labels = inspected.get("Config", {}).get("Labels") or {}
    head = _run(("git", "rev-parse", "HEAD"), cwd=repo).stdout.strip()
    if labels.get("org.opencontainers.image.revision") != head:
        raise ArtifactError("image revision label mismatch")
    if labels.get("io.proberca.source-fingerprint") != fingerprints["runtime_source_fingerprint"]:
        raise ArtifactError("image source label mismatch")
    if labels.get("io.proberca.schema-version") != "generation_v5":
        raise ArtifactError("image schema label mismatch")
    user = str(inspected.get("Config", {}).get("User", ""))
    entrypoint = inspected.get("Config", {}).get("Entrypoint")
    if user in ("", "0", "root") or entrypoint != ["python3", "-m", "proberca.cli.live"]:
        raise ArtifactError("image runtime contract mismatch")
    test_evidence = manifests["test_evidence_manifest.json"]["test_evidence"]
    identity = {
        "schema_version": IMAGE_SCHEMA,
        "source_manifest_schema_version": SOURCE_SCHEMA,
        "HEAD": head,
        **fingerprints,
        "image_tag": image_tag,
        "image_id": inspected["Id"],
        "repo_digest": (inspected.get("RepoDigests") or [None])[0],
        "OCI_labels": labels,
        "architecture": inspected["Architecture"],
        "OS": inspected["Os"],
        "image_size": inspected["Size"],
        "image_user": user,
        "entrypoint": entrypoint,
        "lock_fingerprints": {
            name: sha256_file(repo / f"requirements/{name}.lock.txt")
            for name in ("production", "build", "test")
        },
        "test_evidence": test_evidence,
        "transaction_contract_version": TRANSACTION_CONTRACT,
        "smoke_harness_contract_version": SMOKE_CONTRACT,
        "image_identity_contract": IDENTITY_CONTRACT,
        "artifact_persistence_contract": PERSISTENCE_CONTRACT,
        "ready_for_node_import": True,
    }
    return identity


def _node_ready(node: Mapping[str, object]) -> bool:
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in node.get("status", {}).get("conditions", [])
    )


def create_node_evidence(identity_path: Path, context: str,
                         kind_cluster: str) -> dict:
    identity, identity_fingerprint = _load_canonical(identity_path, IMAGE_SCHEMA)
    if identity.get("ready_for_node_import") is not True:
        raise ArtifactError("image identity is not ready for node import")
    current = _run(("kubectl", "config", "current-context")).stdout.strip()
    if current != context:
        raise ArtifactError("Kubernetes context mismatch")
    nodes_value = json.loads(_run(("kubectl", "--context", context, "get", "nodes", "-o", "json")).stdout)
    ready = {item["metadata"]["name"]: item for item in nodes_value.get("items", []) if _node_ready(item)}
    kind_nodes = set(filter(None, _run(("kind", "get", "nodes", "--name", kind_cluster)).stdout.splitlines()))
    if not ready or set(ready) != kind_nodes:
        raise ArtifactError("Ready Kubernetes nodes do not match kind runtime nodes")
    records = []
    for node_name in sorted(ready):
        reference = identity["image_tag"]
        runtime_reference = reference if "/" in reference else "docker.io/library/" + reference
        listing = _run(("docker", "exec", node_name, "ctr", "-n", "k8s.io", "images", "ls", f"name=={runtime_reference}"))
        lines = [line for line in listing.stdout.splitlines() if runtime_reference in line]
        if len(lines) != 1:
            raise ArtifactError(f"exact image tag missing on node: {node_name}")
        digests = re.findall(r"sha256:[0-9a-f]{64}", lines[0])
        if not digests:
            raise ArtifactError(f"runtime manifest digest missing on node: {node_name}")
        manifest_digest = digests[0]
        manifest = json.loads(_run(("docker", "exec", node_name, "ctr", "-n", "k8s.io", "content", "get", manifest_digest)).stdout)
        config_digest = str(manifest.get("config", {}).get("digest", ""))
        if not DIGEST_RE.fullmatch(config_digest):
            raise ArtifactError("runtime config digest is invalid")
        config = json.loads(_run(("docker", "exec", node_name, "ctr", "-n", "k8s.io", "content", "get", config_digest)).stdout)
        labels = config.get("config", {}).get("Labels") or {}
        architecture = str(config.get("architecture", ""))
        os_name = str(config.get("os", "")).lower()
        verified = (
            labels.get("io.proberca.source-fingerprint") == identity["runtime_source_fingerprint"]
            and labels.get("org.opencontainers.image.revision") == identity["HEAD"]
            and architecture == identity["architecture"]
            and os_name == str(identity["OS"]).lower()
        )
        if not verified:
            raise ArtifactError(f"runtime image identity mismatch on node: {node_name}")
        records.append({
            "node_name": node_name,
            "architecture": architecture,
            "os": os_name,
            "runtime": str(ready[node_name]["status"]["nodeInfo"]["containerRuntimeVersion"]).split(":", 1)[0],
            "image_tag": reference,
            "runtime_tag": runtime_reference,
            "runtime_image_id": manifest_digest,
            "runtime_manifest_digest": manifest_digest,
            "runtime_config_id": config_digest,
            "source_fingerprint": labels["io.proberca.source-fingerprint"],
            "revision": labels["org.opencontainers.image.revision"],
            "runtime_query_succeeded": True,
            "identity_verified": True,
            "verification_method": "containerd_runtime_query",
        })
    return {
        "schema_version": NODE_SCHEMA,
        "cluster_type": "kind",
        "image_identity_fingerprint": identity_fingerprint,
        "nodes": records,
    }


def _verify_manifest_shape(manifest: Mapping[str, object]) -> None:
    files = manifest.get("files")
    if not isinstance(files, list) or files != sorted(files, key=lambda item: item["path"]):
        raise ArtifactError("source manifest paths are not canonical")
    if len({item["path"] for item in files}) != len(files):
        raise ArtifactError("source manifest contains duplicate paths")
    if manifest.get("file_count") != len(files):
        raise ArtifactError("source manifest file count mismatch")
    if manifest.get("total_bytes") != sum(item["size"] for item in files):
        raise ArtifactError("source manifest byte count mismatch")
    counts = Counter(item["git_state"] for item in files)
    expected = {key: counts.get(key, 0) for key in ("tracked_clean", "tracked_modified", "untracked")}
    if manifest.get("state_counts") != expected:
        raise ArtifactError("source manifest state count mismatch")


def _verify_identity(identity: Mapping[str, object], manifests: Mapping[str, Mapping[str, object]]) -> None:
    fingerprints = source_fingerprints(manifests)
    for key, value in fingerprints.items():
        if identity.get(key) != value:
            raise ArtifactError(f"image identity {key} mismatch")
    labels = identity.get("OCI_labels") or {}
    if labels.get("io.proberca.source-fingerprint") != fingerprints["runtime_source_fingerprint"]:
        raise ArtifactError("identity source label mismatch")
    if labels.get("org.opencontainers.image.revision") != identity.get("HEAD"):
        raise ArtifactError("identity revision label mismatch")
    if not DIGEST_RE.fullmatch(str(identity.get("image_id", ""))):
        raise ArtifactError("identity image ID is invalid")
    if identity.get("image_tag") != "proberca:src-" + fingerprints["runtime_source_fingerprint"][:16]:
        raise ArtifactError("identity image tag mismatch")
    if identity.get("ready_for_node_import") is not True:
        raise ArtifactError("identity readiness mismatch")
    if identity.get("artifact_persistence_contract") != PERSISTENCE_CONTRACT:
        raise ArtifactError("identity persistence contract mismatch")


def _verify_node(node: Mapping[str, object], identity: Mapping[str, object], identity_fp: str) -> None:
    if node.get("image_identity_fingerprint") != identity_fp:
        raise ArtifactError("node evidence identity fingerprint mismatch")
    nodes = node.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ArtifactError("node evidence is empty")
    names: set[str] = set()
    for item in nodes:
        name = str(item.get("node_name", ""))
        if not name or name in names:
            raise ArtifactError("node evidence has duplicate or empty node")
        names.add(name)
        if item.get("identity_verified") is not True or item.get("runtime_query_succeeded") is not True:
            raise ArtifactError("node runtime query is not verified")
        if item.get("image_tag") != identity.get("image_tag"):
            raise ArtifactError("node image tag mismatch")
        if item.get("source_fingerprint") != identity.get("runtime_source_fingerprint") or item.get("revision") != identity.get("HEAD"):
            raise ArtifactError("node image labels mismatch")
        if item.get("architecture") != identity.get("architecture") or str(item.get("os", "")).lower() != str(identity.get("OS", "")).lower():
            raise ArtifactError("node platform mismatch")
        for key in ("runtime_image_id", "runtime_manifest_digest", "runtime_config_id"):
            if not DIGEST_RE.fullmatch(str(item.get(key, ""))):
                raise ArtifactError(f"node {key} is invalid")


def build_release_binding(identity: Mapping[str, object], identity_fp: str,
                          node: Mapping[str, object], node_fp: str,
                          manifests: Mapping[str, Mapping[str, object]],
                          test_evidence: Mapping[str, object]) -> dict:
    _verify_identity(identity, manifests)
    _verify_node(node, identity, identity_fp)
    fingerprints = source_fingerprints(manifests)
    if dict(test_evidence) != manifests["test_evidence_manifest.json"].get("test_evidence"):
        raise ArtifactError("test evidence does not match test manifest")
    if any(test_evidence.get(field) != 0 for field in ("failed", "errors", "skipped", "xfail")):
        raise ArtifactError("test evidence contains non-passing outcomes")
    return {
        "schema_version": BINDING_SCHEMA,
        "ready_for_s4": True,
        "image_identity_fingerprint": identity_fp,
        "node_image_evidence_fingerprint": node_fp,
        **fingerprints,
        "image_tag": identity["image_tag"],
        "image_id": identity["image_id"],
        "OCI_labels": identity["OCI_labels"],
        "runtime_source_unchanged": True,
        "image_unchanged": True,
        "image_supply_policy_version": "p11-image-supply-policy-v2",
        "image_identity_contract": IDENTITY_CONTRACT,
        "transaction_contract_version": TRANSACTION_CONTRACT,
        "smoke_harness_contract_version": SMOKE_CONTRACT,
        "artifact_persistence_contract": PERSISTENCE_CONTRACT,
        "supported_actions": ["apply", "render"],
        "supported_stack_profiles": ["bounded", "live"],
        "supported_supply_modes": ["registry_digest", "verified_local_import"],
        "bounded_contract": {"live_runner_count": 0, "bounded_job_count": 1, "restart_policy": "Never", "backoff_limit": 0},
        "live_contract": {"live_runner_count": 1, "bounded_job_count": 0},
        "local_identity_contract": {"identity_record_required": True, "release_binding_required": True, "node_runtime_evidence_required": True, "image_pull_policy": "Never", "third_party_digest_required": True},
        "latest_test_evidence": dict(test_evidence),
        "blocking_issues": [],
        "windows_run": False,
    }


def verify_no_sensitive_content(root: Path) -> None:
    patterns = (
        re.compile(br"Authorization:\s*Bearer\s+\S+", re.I),
        re.compile(br"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
        re.compile(br"(?:password|passwd)\s*[:=]\s*[^\s<>{}\[\]]+", re.I),
        re.compile(br"https?://[^/\s:@]+:[^/\s@]+@"),
    )
    for path in sorted(root.rglob("*")):
        if path.is_file():
            raw = path.read_bytes()
            if any(pattern.search(raw) for pattern in patterns):
                raise ArtifactError(f"sensitive content detected: {path.name}")


def verify_bundle(bundle_dir: Path, *, repo: Path | None = None,
                  verify_docker: bool = True) -> dict:
    for name in _REQUIRED_FILES:
        if not (bundle_dir / name).is_file():
            raise ArtifactError(f"bundle file is missing: {name}")
    manifests = load_source_manifests(bundle_dir)
    for manifest in manifests.values():
        _verify_manifest_shape(manifest)
    if repo is not None:
        evidence = manifests["test_evidence_manifest.json"]["test_evidence"]
        rebuilt = build_source_manifests(repo, evidence)
        for name, value in rebuilt.items():
            if canonical_bytes(value) != canonical_bytes(manifests[name]):
                raise ArtifactError(f"source manifest no longer matches repository: {name}")
    identity, identity_fp = _load_canonical(bundle_dir / "image_identity.json", IMAGE_SCHEMA)
    _verify_identity(identity, manifests)
    if verify_docker:
        inspected = json.loads(_run(("docker", "image", "inspect", identity["image_tag"])).stdout)[0]
        if inspected["Id"] != identity["image_id"] or (inspected.get("Config", {}).get("Labels") or {}) != identity["OCI_labels"]:
            raise ArtifactError("local Docker image identity mismatch")
    node, node_fp = _load_canonical(bundle_dir / "node_image_evidence.json", NODE_SCHEMA)
    _verify_node(node, identity, identity_fp)
    binding, _binding_fp = _load_canonical(bundle_dir / "release_binding.json", BINDING_SCHEMA)
    expected = build_release_binding(
        identity, identity_fp, node, node_fp, manifests,
        manifests["test_evidence_manifest.json"]["test_evidence"],
    )
    if binding != expected:
        raise ArtifactError("release binding does not match derived contract")
    verify_no_sensitive_content(bundle_dir)
    return binding


def publish_candidate(candidate: Path, release: Path) -> None:
    if release.exists():
        if not candidate.is_dir():
            raise ArtifactError("candidate release does not exist")
        candidate_files = {p.relative_to(candidate): sha256_file(p) for p in candidate.rglob("*") if p.is_file()}
        release_files = {p.relative_to(release): sha256_file(p) for p in release.rglob("*") if p.is_file()}
        if candidate_files != release_files:
            raise ArtifactError("refusing to overwrite a different historical release")
        return
    if not candidate.is_dir():
        raise ArtifactError("candidate release does not exist")
    temporary = release.parent / f".{release.name}.publishing"
    if temporary.exists():
        raise ArtifactError("stale publishing directory exists")
    shutil.copytree(candidate, temporary, symlinks=True)
    for path in temporary.rglob("*"):
        if path.is_file():
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
    os.replace(temporary, release)
    _fsync_directory(release.parent)


def record_s4_evidence(bundle_dir: Path, scenario: str, source: Path,
                       summary: Mapping[str, object]) -> Path:
    if scenario not in ("three-window", "transient-empty"):
        raise ArtifactError("unsupported S4 evidence scenario")
    target = bundle_dir / "s4" / scenario
    if target.exists():
        raise ArtifactError("S4 evidence already exists")
    verify_no_sensitive_content(source)
    temporary = target.parent / f".{scenario}.recording"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, temporary, symlinks=True)
    atomic_json(temporary / "acceptance_summary.json", dict(summary))
    os.replace(temporary, target)
    _fsync_directory(target.parent)
    return target


def _json_argument(value: str) -> dict:
    stripped = value.lstrip()
    if stripped.startswith("{"):
        raw: str | bytes = value
    else:
        path = Path(value)
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise ArtifactError(f"cannot read JSON argument: {error}") from error
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ArtifactError("JSON argument must be an object")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("source-manifests")
    source.add_argument("--repo", type=Path, required=True)
    source.add_argument("--output-dir", type=Path, required=True)
    source.add_argument("--test-evidence", required=True)
    image = sub.add_parser("image-identity")
    image.add_argument("--repo", type=Path, required=True)
    image.add_argument("--bundle-dir", type=Path, required=True)
    image.add_argument("--image-tag", required=True)
    node = sub.add_parser("node-evidence")
    node.add_argument("--identity-record", type=Path, required=True)
    node.add_argument("--context", required=True)
    node.add_argument("--kind-cluster", required=True)
    node.add_argument("--output", type=Path, required=True)
    binding = sub.add_parser("release-binding")
    binding.add_argument("--bundle-dir", type=Path, required=True)
    verify = sub.add_parser("verify-bundle")
    verify.add_argument("--bundle-dir", type=Path, required=True)
    verify.add_argument("--repo", type=Path)
    verify.add_argument("--no-docker", action="store_true")
    record = sub.add_parser("record-s4-evidence")
    record.add_argument("--bundle-dir", type=Path, required=True)
    record.add_argument("--scenario", required=True)
    record.add_argument("--source", type=Path, required=True)
    record.add_argument("--summary", required=True)
    args = parser.parse_args(argv)
    if args.command == "source-manifests":
        print(json.dumps(write_source_manifests(args.repo, args.output_dir, _json_argument(args.test_evidence)), sort_keys=True))
    elif args.command == "image-identity":
        identity = create_image_identity(args.bundle_dir, args.image_tag, args.repo)
        print(atomic_json(args.bundle_dir / "image_identity.json", identity))
    elif args.command == "node-evidence":
        evidence = create_node_evidence(args.identity_record, args.context, args.kind_cluster)
        print(atomic_json(args.output, evidence))
    elif args.command == "release-binding":
        manifests = load_source_manifests(args.bundle_dir)
        identity, identity_fp = _load_canonical(args.bundle_dir / "image_identity.json", IMAGE_SCHEMA)
        node_value, node_fp = _load_canonical(args.bundle_dir / "node_image_evidence.json", NODE_SCHEMA)
        value = build_release_binding(identity, identity_fp, node_value, node_fp, manifests, manifests["test_evidence_manifest.json"]["test_evidence"])
        print(atomic_json(args.bundle_dir / "release_binding.json", value))
    elif args.command == "verify-bundle":
        value = verify_bundle(args.bundle_dir, repo=args.repo, verify_docker=not args.no_docker)
        print(json.dumps({"ready_for_s4": value["ready_for_s4"]}, sort_keys=True))
    else:
        target = record_s4_evidence(args.bundle_dir, args.scenario, args.source, _json_argument(args.summary))
        print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

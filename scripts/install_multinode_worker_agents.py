#!/usr/bin/env python3
"""Install one immutable Git archive on each pinned campaign node."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import tempfile
from pathlib import Path

import yaml


def _run(arguments: list[str], **kwargs) -> subprocess.CompletedProcess:
    completed = subprocess.run(
        arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=kwargs.pop("text", True), check=False, **kwargs,
    )
    if completed.returncode != 0:
        stderr = completed.stderr if isinstance(completed.stderr, str) else \
            completed.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"command failed closed: {stderr.strip()}")
    return completed


def _ssh_base(node: dict, known_hosts: Path) -> list[str]:
    return [
        "ssh", "-T", "-p", str(int(node["port"])),
        "-i", str(Path(node["identity_file"]).resolve()),
        "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes",
        "-o", f"UserKnownHostsFile={known_hosts.resolve()}",
        f"{node['user']}@{node['host']}",
    ]


def install(repository: Path, inventory_path: Path) -> dict:
    repository = repository.resolve()
    inventory = yaml.safe_load(inventory_path.read_text(encoding="utf-8"))
    if inventory.get("schema_version") != "probeRCA-multinode-node-inventory-v1":
        raise ValueError("unsupported node inventory")
    known_hosts = Path(inventory["known_hosts_file"])
    if not known_hosts.is_file():
        raise ValueError("pinned known_hosts file is missing")
    sha = _run(["git", "-C", str(repository), "rev-parse", "HEAD"]).stdout.strip()
    if len(sha) != 40:
        raise RuntimeError("repository HEAD is not a full Git SHA")
    if _run([
        "git", "-C", str(repository), "status", "--porcelain",
        "--untracked-files=no",
    ]).stdout.strip():
        raise RuntimeError("worker agents can only be installed from a clean tracked tree")
    with tempfile.TemporaryDirectory(prefix="proberca-worker-release-") as temporary:
        archive = Path(temporary) / f"proberca-{sha}.tar"
        with archive.open("wb") as stream:
            completed = subprocess.run(
                ["git", "-C", str(repository), "archive", "--format=tar", "HEAD"],
                stdout=stream, stderr=subprocess.PIPE, check=False,
            )
        if completed.returncode != 0:
            raise RuntimeError("git archive failed")
        archive_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
        results = []
        for node in inventory["nodes"]:
            remote_archive = f"/tmp/proberca-{sha}-{archive_sha[:12]}.tar"
            scp = [
                "scp", "-P", str(int(node["port"])),
                "-i", str(Path(node["identity_file"]).resolve()),
                "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
                "-o", "StrictHostKeyChecking=yes",
                "-o", f"UserKnownHostsFile={known_hosts.resolve()}",
                str(archive), f"{node['user']}@{node['host']}:{remote_archive}",
            ]
            _run(scp)
            ssh = _ssh_base(node, known_hosts)
            release = f"/opt/proberca/releases/{sha}"
            _run(ssh + ["sudo", "-n", "mkdir", "-p", release])
            _run(ssh + ["sudo", "-n", "tar", "-xf", remote_archive, "-C", release])
            _run(ssh + [
                "sudo", "-n", "ln", "-sfn", release, "/opt/proberca/current",
            ])
            _run(ssh + ["rm", "-f", remote_archive])
            verification = _run(ssh + [
                "sudo", "-n", "env", "PYTHONPATH=/opt/proberca/current",
                "python3", "-m", "py_compile",
                "/opt/proberca/current/scripts/multinode_fault_agent.py",
                "/opt/proberca/current/scripts/multinode_fault_actor.py",
            ])
            results.append({
                "node_id": node["node_id"], "git_sha": sha,
                "archive_sha256": archive_sha,
                "installed": verification.returncode == 0,
            })
    return {"git_sha": sha, "archive_sha256": archive_sha, "nodes": results}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--nodes", type=Path, required=True)
    arguments = parser.parse_args()
    import json
    print(json.dumps(install(arguments.repository, arguments.nodes), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

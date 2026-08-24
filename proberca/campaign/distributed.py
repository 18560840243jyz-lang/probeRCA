"""Start identical one-second worker collectors and merge their sealed output."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import subprocess
import tarfile
import time
import shutil
from pathlib import Path
from typing import Any, Callable

import yaml

from .model import fingerprint
from .multinode_merge import merge_worker_archives


class DistributedCollectionError(RuntimeError):
    pass


def _ssh(node: dict[str, Any], known_hosts: Path) -> list[str]:
    return [
        "ssh", "-T", "-p", str(int(node["port"])),
        "-i", str(Path(node["identity_file"]).resolve()),
        "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes",
        "-o", f"UserKnownHostsFile={known_hosts.resolve()}",
        f"{node['user']}@{node['host']}",
    ]


def _run(arguments: list[str], *, timeout: int) -> subprocess.CompletedProcess:
    completed = subprocess.run(
        arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, timeout=timeout, check=False,
    )
    if completed.returncode != 0:
        raise DistributedCollectionError(
            "distributed worker command failed: " + completed.stderr.strip()
        )
    return completed


def collect_distributed_dataset(
    *,
    repository: Path,
    node_inventory: Path,
    case_id: str,
    window_count: int,
    output_root: Path,
    first_window_start_ns: int | None = None,
    on_capture_started: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    """Collect on all Workers; the callback may schedule an independent injector."""

    if window_count <= 0:
        raise ValueError("distributed window count must be positive")
    inventory = yaml.safe_load(node_inventory.read_text(encoding="utf-8"))
    workers = [item for item in inventory["nodes"] if item["role"] == "worker"]
    if len(workers) != 3:
        raise DistributedCollectionError("distributed collection requires three Workers")
    known_hosts = Path(inventory["known_hosts_file"])
    if not known_hosts.is_file():
        raise DistributedCollectionError("pinned known_hosts file is missing")
    git_sha = _run(
        ["git", "-C", str(repository.resolve()), "rev-parse", "HEAD"],
        timeout=10,
    ).stdout.strip()
    now = time.time_ns()
    if first_window_start_ns is None:
        first_window_start_ns = ((now + 30_000_000_000 + 999_999_999)
                                 // 1_000_000_000) * 1_000_000_000
    if first_window_start_ns <= now + 10_000_000_000 \
            or first_window_start_ns % 1_000_000_000:
        raise DistributedCollectionError(
            "shared first boundary must be epoch-aligned and at least 10 seconds ahead"
        )
    dataset_id = fingerprint({
        "case_id": case_id, "git_sha": git_sha,
        "first_window_start_ns": first_window_start_ns,
        "window_count": window_count,
        "node_inventory_sha256": hashlib.sha256(node_inventory.read_bytes()).hexdigest(),
    })
    remote_root = f"/var/lib/proberca-campaign/staging/{dataset_id}"
    timeout = window_count + 1200

    def collect(node: dict[str, Any]) -> tuple[str, str]:
        base = _ssh(node, known_hosts)
        worker = node["node_id"]
        command = base + [
            "sudo", "-n", "env", "PYTHONPATH=/opt/proberca/current",
            "python3", "-m", "proberca.cli.collect_final",
            "--source-config", "/etc/proberca/live-collector.yaml",
            "--collection-contract", "/opt/proberca/current/configs/final_collection_contract.yaml",
            "--burst-config", "/etc/proberca/live-burst.yaml",
            "--output", f"{remote_root}/normal",
            "--burst-output", f"{remote_root}/burst",
            "--raw-primitives-output", f"{remote_root}/primitives",
            "--raw-events-output", f"{remote_root}/raw-events",
            "--windows", str(window_count), "--dataset-id", dataset_id,
            "--first-window-start-ns", str(first_window_start_ns),
        ]
        result = _run(command, timeout=timeout)
        archive = f"/tmp/{dataset_id}-{worker}.tar"
        _run(base + [
            "sudo", "-n", "tar", "-cf", archive,
            "-C", "/var/lib/proberca-campaign/staging", dataset_id,
        ], timeout=600)
        _run(base + [
            "sudo", "-n", "chown", f"{node['user']}:{node['user']}", archive,
        ], timeout=30)
        return worker, result.stdout.strip()

    output = output_root.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if on_capture_started is not None:
        on_capture_started(first_window_start_ns, dataset_id)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(collect, node): node for node in workers}
        worker_results = {}
        for future in concurrent.futures.as_completed(futures):
            worker, result = future.result()
            worker_results[worker] = result
    for node in workers:
        worker = node["node_id"]
        archive_name = f"{dataset_id}-{worker}.tar"
        local_archive = output / archive_name
        scp = [
            "scp", "-P", str(int(node["port"])),
            "-i", str(Path(node["identity_file"]).resolve()),
            "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
            "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={known_hosts.resolve()}",
            f"{node['user']}@{node['host']}:/tmp/{archive_name}", str(local_archive),
        ]
        _run(scp, timeout=1200)
        worker_root = output / "workers" / worker
        worker_root.mkdir(parents=True)
        with tarfile.open(local_archive, "r") as archive:
            members = archive.getmembers()
            expected_prefix = f"{dataset_id}/"
            if any(
                member.name != dataset_id and not member.name.startswith(expected_prefix)
                for member in members
            ):
                raise DistributedCollectionError("worker tar path escapes Dataset ID")
            destination = worker_root.resolve()
            for member in members:
                relative = Path(member.name)
                resolved = (destination / relative).resolve()
                if (
                    relative.is_absolute() or ".." in relative.parts
                    or destination not in resolved.parents and resolved != destination
                    or member.issym() or member.islnk()
                    or member.isdev() or member.isfifo()
                ):
                    raise DistributedCollectionError(
                        "worker tar contains an unsafe member"
                    )
            archive.extractall(worker_root)
        local_archive.unlink()
        _run(_ssh(node, known_hosts) + ["rm", "-f", f"/tmp/{archive_name}"], timeout=30)
    normals = [output / "workers" / item["node_id"] / dataset_id / "normal"
               for item in workers]
    bursts = [output / "workers" / item["node_id"] / dataset_id / "burst"
              for item in workers]
    merge = merge_worker_archives(
        normal_roots=normals, burst_roots=bursts,
        normal_output=output / "normal", burst_output=output / "burst",
    )
    primitive_root = output / "primitives"
    primitive_root.mkdir()
    raw_event_root = output / "raw-events"
    raw_event_root.mkdir()
    for item in workers:
        worker = item["node_id"]
        source = output / "workers" / worker / dataset_id / "primitives"
        shutil.move(str(source), str(primitive_root / worker))
        source = output / "workers" / worker / dataset_id / "raw-events"
        shutil.move(str(source), str(raw_event_root / worker))
    shutil.rmtree(output / "workers")
    # Only after all local archives have been copied, verified, merged, and
    # rearranged may the bounded remote staging copy be removed.
    for node in workers:
        _run(_ssh(node, known_hosts) + [
            "sudo", "-n", "rm", "-rf", "--", remote_root,
        ], timeout=120)
    report = {
        "schema_version": "probeRCA-distributed-collection-report-v1",
        "case_id": case_id, "git_sha": git_sha, "dataset_id": dataset_id,
        "first_window_start_ns": first_window_start_ns,
        "window_count": window_count, "worker_results": worker_results,
        "merge": merge,
    }
    (output / "distributed-collection-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    return report

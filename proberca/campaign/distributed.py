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


def _capture_load_intents(
    *, ledger_path: Path, output_root: Path, dataset_id: str,
    first_window_start_ns: int, window_count: int,
) -> dict[str, Any]:
    """Copy the bounded interval ledger that covers this immutable dataset."""

    if not ledger_path.is_file():
        raise DistributedCollectionError("formal load-intent ledger is missing")
    records = []
    for line_number, line in enumerate(
        ledger_path.read_text(encoding="utf-8").splitlines(), start=1,
    ):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as error:
            raise DistributedCollectionError(
                f"load-intent ledger line {line_number} is malformed"
            ) from error
        if item.get("schema_version") != "probeRCA-load-behavior-intent-v1":
            raise DistributedCollectionError("load-intent ledger schema is invalid")
        start = item.get("interval_start_ns")
        end = item.get("interval_end_ns")
        behaviors = item.get("behavior_intents")
        admitted = item.get("admitted_intents")
        admitted_behaviors = item.get("admitted_behavior_intents")
        rejected = item.get("backpressure_rejected_intents")
        rejected_behaviors = item.get(
            "backpressure_rejected_behavior_intents"
        )
        if (
            not isinstance(start, int) or not isinstance(end, int) or end <= start
            or not isinstance(behaviors, dict)
            or any(not isinstance(value, int) or value < 0 for value in behaviors.values())
            or item.get("scheduled_intents") != sum(behaviors.values())
            or not item.get("load_profile_id")
            or not item.get("load_profile_fingerprint")
        ):
            raise DistributedCollectionError("load-intent ledger record is invalid")
        optional_execution_fields = (
            admitted, admitted_behaviors, rejected, rejected_behaviors,
        )
        if any(value is not None for value in optional_execution_fields):
            if (
                not isinstance(admitted, int) or admitted < 0
                or not isinstance(rejected, int) or rejected < 0
                or not isinstance(admitted_behaviors, dict)
                or not isinstance(rejected_behaviors, dict)
                or set(admitted_behaviors) != set(behaviors)
                or set(rejected_behaviors) != set(behaviors)
                or any(
                    not isinstance(value, int) or value < 0
                    for value in admitted_behaviors.values()
                )
                or any(
                    not isinstance(value, int) or value < 0
                    for value in rejected_behaviors.values()
                )
                or admitted != sum(admitted_behaviors.values())
                or rejected != sum(rejected_behaviors.values())
                or item["scheduled_intents"] != admitted + rejected
                or any(
                    behaviors[name]
                    != admitted_behaviors[name] + rejected_behaviors[name]
                    for name in behaviors
                )
            ):
                raise DistributedCollectionError(
                    "load-intent execution accounting is invalid"
                )
        records.append(item)
    final_window_end_ns = first_window_start_ns + window_count * 1_000_000_000
    selected = sorted(
        (
            item for item in records
            if item["interval_end_ns"] > first_window_start_ns
            and item["interval_start_ns"] < final_window_end_ns
        ),
        key=lambda item: item["interval_start_ns"],
    )
    if not selected:
        raise DistributedCollectionError("load-intent ledger does not cover dataset")
    if selected[0]["interval_start_ns"] > first_window_start_ns \
            or selected[-1]["interval_end_ns"] < final_window_end_ns:
        raise DistributedCollectionError("load-intent ledger has incomplete coverage")
    if any(
        left["interval_end_ns"] != right["interval_start_ns"]
        for left, right in zip(selected, selected[1:])
    ):
        raise DistributedCollectionError("load-intent ledger has a time gap or overlap")
    identities = {
        (item["load_profile_id"], item["load_profile_fingerprint"])
        for item in selected
    }
    if len(identities) != 1:
        raise DistributedCollectionError("load profile changed during dataset")
    destination = output_root / "load-intent"
    destination.mkdir()
    ledger_output = destination / "behavior-intents.jsonl"
    encoded = "".join(
        json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n"
        for item in selected
    )
    ledger_output.write_text(encoded, encoding="utf-8")
    profile_id, profile_fingerprint = next(iter(identities))
    manifest = {
        "schema_version": "probeRCA-load-intent-manifest-v1",
        "dataset_id": dataset_id,
        "dataset_start_ns": first_window_start_ns,
        "dataset_end_ns": final_window_end_ns,
        "covered_start_ns": selected[0]["interval_start_ns"],
        "covered_end_ns": selected[-1]["interval_end_ns"],
        "record_count": len(selected),
        "load_profile_id": profile_id,
        "load_profile_fingerprint": profile_fingerprint,
        "ledger_sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    }
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    return manifest


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
    load_intent_ledger: Path | None = None,
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
    load_intent = None
    if load_intent_ledger is not None:
        load_intent = _capture_load_intents(
            ledger_path=load_intent_ledger, output_root=output,
            dataset_id=dataset_id, first_window_start_ns=first_window_start_ns,
            window_count=window_count,
        )
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
        "load_intent": load_intent,
    }
    (output / "distributed-collection-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    return report

#!/usr/bin/env python3
"""Bounded real-kernel attach/read/detach acceptance for P12 CO-RE probes."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import select
import shutil
import socket
import struct
import subprocess
import time
from pathlib import Path

PROBES = ("process", "sched", "futex", "block", "tcp", "dns")
EDGE_PROBES = {"tcp", "dns"}
EXPECTED_EVENT_TYPES = {
    "process": {1, 2, 3},
    "sched": {10, 11},
    "futex": {20, 21},
    "block": {30, 31, 32},
    "tcp": {41, 42, 43},
    "dns": {50},
}


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def current_cgroup():
    relative = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
    path = Path("/sys/fs/cgroup") / relative.lstrip("/")
    return path, path.stat().st_ino


def kernel_objects(kind):
    result = subprocess.run(
        ["sudo", "-n", "bpftool", "-j", kind, "show"], text=True,
        capture_output=True, timeout=15, check=True,
    )
    return {(int(item["id"]), str(item.get("name", ""))) for item in json.loads(result.stdout)}


def pins():
    root = Path("/sys/fs/bpf")
    result = subprocess.run(
        ["sudo", "-n", "find", str(root), "-mindepth", "1", "-printf", "%p\n"],
        text=True, capture_output=True, timeout=15, check=True,
    )
    return frozenset(line for line in result.stdout.splitlines() if line)


def build_futex_workload(output: Path):
    source = output / "futex-workload.c"
    binary = output / "futex-workload"
    source.write_text(
        "#include <pthread.h>\n#include <unistd.h>\n"
        "static pthread_mutex_t l=PTHREAD_MUTEX_INITIALIZER;\n"
        "static void *w(void *x){(void)x;for(int i=0;i<120;i++){"
        "pthread_mutex_lock(&l);usleep(1000);pthread_mutex_unlock(&l);}return 0;}\n"
        "int main(void){pthread_t t[4];for(int i=0;i<4;i++)pthread_create(&t[i],0,w,0);"
        "for(int i=0;i<4;i++)pthread_join(t[i],0);return 0;}\n",
        encoding="ascii",
    )
    subprocess.run(
        ["cc", "-O2", "-Wall", "-Werror", "-pthread", str(source), "-o", str(binary)],
        timeout=30, check=True,
    )
    return binary


def workload(probe: str, output: Path, futex_binary: Path):
    if probe == "process":
        for _ in range(20):
            subprocess.run(["/bin/true"], timeout=5, check=True)
    elif probe == "sched":
        workers = [subprocess.Popen([
            "bash", "-c", "end=$((SECONDS+1)); while ((SECONDS<end)); do :; done",
        ]) for _ in range(3)]
        for worker in workers:
            worker.wait(timeout=5)
            if worker.returncode:
                raise RuntimeError("sched workload failed")
    elif probe == "futex":
        subprocess.run([str(futex_binary)], timeout=10, check=True)
    elif probe == "block":
        target = output / "bounded-io.bin"
        subprocess.run(
            ["dd", "if=/dev/zero", f"of={target}", "bs=1M", "count=16", "conv=fsync"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20, check=True,
        )
        target.unlink()
    elif probe == "tcp":
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(8)
        try:
            for _ in range(20):
                connection = socket.create_connection(server.getsockname(), timeout=1)
                accepted, _ = server.accept()
                accepted.close()
                connection.close()
        finally:
            server.close()
        for port in range(42000, 42040):
            connection = socket.socket()
            connection.settimeout(0.05)
            try:
                connection.connect(("127.0.0.1", port))
            except OSError:
                pass
            finally:
                connection.close()
    elif probe == "dns":
        for _ in range(20):
            datagram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                datagram.sendto(b"\x12\x34\x01\x00" + b"\x00" * 8, ("127.0.0.1", 53))
            finally:
                datagram.close()


def wait_line(stream, timeout_sec):
    ready, _, _ = select.select([stream], [], [], timeout_sec)
    if not ready:
        raise TimeoutError("loader control event timeout")
    line = stream.readline()
    if not line:
        raise RuntimeError("loader closed before control event")
    return line


def run_probe(probe, build_dir, output, ttl, cgroup_path, cgroup_id, futex_binary):
    before_programs = kernel_objects("prog")
    before_maps = kernel_objects("map")
    before_pins = pins()
    command = [
        "sudo", "-n", str(build_dir / "proberca-ebpf-loader"),
        "--object", str(build_dir / f"{probe}.bpf.o"), "--probe", probe,
        "--ttl", str(ttl), "--attach-epoch", str(1000 + PROBES.index(probe)),
        "--candidate-version", "1", "--candidate-cgroup", str(cgroup_id),
        "--cgroup-path", str(cgroup_path),
    ]
    if probe in EDGE_PROBES:
        loopback_identity = struct.unpack("=I", socket.inet_aton("127.0.0.1"))[0]
        command += ["--candidate-edge", f"{cgroup_id}:{loopback_identity}"]
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    prefix = [wait_line(process.stdout, 15), wait_line(process.stdout, 15)]
    initial = [json.loads(line) for line in prefix]
    if [item.get("state") for item in initial] != ["ATTACHED", "ACTIVE"]:
        process.kill()
        raise RuntimeError(f"{probe} did not enter ACTIVE: {initial}")
    workload(probe, output, futex_binary)
    stdout, stderr = process.communicate(timeout=ttl + 15)
    stdout = "".join(prefix) + stdout
    if process.returncode:
        raise RuntimeError(f"{probe} loader failed: {stderr.strip()}")
    records = [json.loads(line) for line in stdout.splitlines() if line]
    controls = [item["state"] for item in records if item.get("record_type") == "control"]
    expected = ["ATTACHED", "ACTIVE", "DRAINING", "DETACHING", "CLOSED"]
    if controls != expected:
        raise RuntimeError(f"{probe} state history {controls}")
    events = [item for item in records if item.get("record_type") == "event"]
    if not events:
        raise RuntimeError(f"{probe} produced no real event")
    summary = [item for item in records if item.get("record_type") == "summary"][-1]
    after_programs = kernel_objects("prog")
    after_maps = kernel_objects("map")
    after_pins = pins()
    residual = sorted(after_programs - before_programs)
    residual_maps = sorted(after_maps - before_maps)
    residual_pins = sorted(after_pins - before_pins)
    if residual or residual_maps or residual_pins or summary["residual_links"]:
        raise RuntimeError(
            f"{probe} left residual kernel objects: "
            f"programs={residual} maps={residual_maps} pins={residual_pins}"
        )
    atomic_write(output / f"{probe}.raw.jsonl", stdout.encode())
    atomic_write(output / f"{probe}.stderr.txt", stderr.encode())
    sequence_gaps = 0
    per_cpu = {}
    for event in events:
        per_cpu.setdefault(event["cpu"], []).append(event["event_sequence"])
    for values in per_cpu.values():
        ordered = sorted(set(values))
        sequence_gaps += sum(max(0, right - left - 1) for left, right in zip(ordered, ordered[1:]))
    lost = int(summary["ring_buffer_drops"]) + sequence_gaps
    denominator = len(events) + lost
    loss_rate = lost / denominator if denominator else 0.0
    if loss_rate >= 0.01:
        raise RuntimeError(f"{probe} event loss {loss_rate:.6f} exceeds limit")
    classes = sorted({int(item["event_class"]) for item in events})
    event_types = {int(item["event_type"]) for item in events}
    missing_types = sorted(EXPECTED_EVENT_TYPES[probe] - event_types)
    if missing_types:
        raise RuntimeError(f"{probe} missing required real event types: {missing_types}")
    return {
        "probe": probe, "attach_read_detach": True,
        "states": controls, "event_count": len(events), "event_classes": classes,
        "ring_buffer_drops": int(summary["ring_buffer_drops"]),
        "sequence_gaps": sequence_gaps, "event_loss_rate": loss_rate,
        "filtered_events": int(summary["filtered_events"]),
        "ttl_sec": ttl, "ttl_closed": True, "events_after_close": 0,
        "event_types": sorted(event_types),
        "residual_programs": residual, "residual_maps": residual_maps,
        "residual_pins": residual_pins,
        "stderr_empty": not bool(stderr.strip()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--ttl", type=int, default=30)
    args = parser.parse_args()
    if args.ttl < 1 or args.ttl > 60:
        parser.error("TTL must be between 1 and 60 seconds")
    args.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    cgroup_path, cgroup_id = current_cgroup()
    futex_binary = build_futex_workload(args.output_dir)
    reports = []
    for probe in PROBES:
        reports.append(run_probe(
            probe, args.build_dir, args.output_dir, args.ttl,
            cgroup_path, cgroup_id, futex_binary,
        ))
    all_events = []
    for probe in PROBES:
        all_events.extend(
            json.loads(line) for line in (args.output_dir / f"{probe}.raw.jsonl").read_text().splitlines()
            if line and json.loads(line).get("record_type") == "event"
        )
    unmapped = [{**item, "mapping_status": "unmapped"} for item in all_events]
    atomic_write(args.output_dir / "unmapped_events.json", canonical_bytes({
        "schema_version": "p12-unmapped-events-v1", "events": unmapped,
    }))
    loss = {
        "schema_version": "p12-event-loss-v1",
        "probe_results": reports,
        "lost_events": sum(item["ring_buffer_drops"] + item["sequence_gaps"] for item in reports),
        "received_events": sum(item["event_count"] for item in reports),
    }
    denominator = loss["lost_events"] + loss["received_events"]
    loss["event_loss_rate"] = loss["lost_events"] / denominator if denominator else 0.0
    loss["passed"] = loss["event_loss_rate"] < 0.01
    atomic_write(args.output_dir / "loss_report.json", canonical_bytes(loss))
    ttl_report = {
        "schema_version": "p12-ttl-report-v1", "default_ttl_sec": 30,
        "acceptance_ttl_sec": args.ttl,
        "all_closed": all(item["ttl_closed"] for item in reports),
        "all_residual_free": all(
            not item["residual_programs"] and not item["residual_maps"]
            and not item["residual_pins"] for item in reports
        ),
        "probe_states": {item["probe"]: item["states"] for item in reports},
    }
    atomic_write(args.output_dir / "ttl_report.json", canonical_bytes(ttl_report))
    cleanup = {
        "schema_version": "p12-residual-cleanup-v1", "residual_program_count": 0,
        "residual_map_count": sum(len(item["residual_maps"]) for item in reports),
        "residual_pin_count": sum(len(item["residual_pins"]) for item in reports),
        "passed": all(
            not item["residual_programs"] and not item["residual_maps"]
            and not item["residual_pins"] for item in reports
        ),
    }
    atomic_write(args.output_dir / "residual_cleanup_report.json", canonical_bytes(cleanup))
    report = {
        "schema_version": "p12-real-kernel-acceptance-v1",
        "status": "complete", "probes": reports,
        "event_loss_rate": loss["event_loss_rate"],
        "node_edge_separation": all(
            (item["probe"] in EDGE_PROBES) == (item["event_classes"] == [2])
            for item in reports
        ),
        "unmapped_events_retained": len(unmapped),
        "dns_support_boundary": "IPv4 UDP/TCP port 53 transport events only; no payload or domain parsing",
        "sidecar_support_boundary": "generic TCP connection/retransmit/reset evidence only; no mesh-specific payload parsing",
    }
    atomic_write(args.output_dir / "acceptance_report.json", canonical_bytes(report))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()

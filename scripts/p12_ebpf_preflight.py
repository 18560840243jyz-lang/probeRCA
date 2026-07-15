#!/usr/bin/env python3
"""Generate a fail-closed P12 kernel capability report."""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path

STATUSES = {
    "supported", "permission_missing", "kernel_feature_missing",
    "dependency_missing", "attach_failed", "verifier_rejected", "runtime_failed",
}
TOOLS = ("clang", "llvm-config", "bpftool", "pahole", "make", "cmake", "pkg-config")


def command(argv, *, sudo=False):
    prefix = ["sudo", "-n"] if sudo else []
    return subprocess.run(prefix + list(argv), text=True, capture_output=True, timeout=20)


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sudo = command(["true"], sudo=True)
    blockers = []
    status = "supported"
    if sudo.returncode:
        status = "permission_missing"
        blockers.append("passwordless sudo is required for verifier/load/attach acceptance")
    missing = [name for name in TOOLS if shutil.which(name) is None]
    libbpf = command(["pkg-config", "--exists", "libbpf"])
    if missing or libbpf.returncode:
        status = "dependency_missing"
        blockers.extend([f"missing tool: {name}" for name in missing])
        if libbpf.returncode:
            blockers.append("libbpf development package is unavailable")
    btf = Path("/sys/kernel/btf/vmlinux").is_file()
    cgroup_v2 = command(["findmnt", "-n", "-t", "cgroup2", "/sys/fs/cgroup"]).returncode == 0
    bpffs = command(["findmnt", "-n", "-t", "bpf", "/sys/fs/bpf"]).returncode == 0
    if not btf or not cgroup_v2 or not bpffs:
        status = "kernel_feature_missing"
        blockers.extend(
            name for name, present in (("BTF", btf), ("cgroup_v2", cgroup_v2), ("bpffs", bpffs))
            if not present
        )
    feature_payload = {}
    if status == "supported":
        feature = command(["bpftool", "-j", "feature", "probe", "kernel"], sudo=True)
        if feature.returncode:
            status = "runtime_failed"
            blockers.append("bpftool kernel feature probe failed")
        else:
            feature_payload = json.loads(feature.stdout)
    capsh = command(["capsh", "--print"], sudo=True)
    capabilities = capsh.stdout.lower()
    required_caps = {
        name: name.lower() in capabilities
        for name in ("CAP_BPF", "CAP_PERFMON", "CAP_NET_ADMIN", "CAP_SYS_ADMIN")
    }
    tracepoints = {}
    for name in (
        "sched/sched_switch", "sched/sched_process_fork", "sched/sched_process_exec",
        "sched/sched_process_exit", "syscalls/sys_enter_futex", "syscalls/sys_exit_futex",
        "block/block_rq_issue", "block/block_rq_complete", "tcp/tcp_retransmit_skb",
        "tcp/tcp_receive_reset", "tcp/tcp_send_reset", "sock/inet_sock_set_state",
    ):
        tracepoints[name] = command(["test", "-e", str(Path("/sys/kernel/tracing/events", name))], sudo=True).returncode == 0
    if not all(tracepoints.values()):
        status = "kernel_feature_missing"
        blockers.extend(f"missing tracepoint: {name}" for name, value in tracepoints.items() if not value)
    report = {
        "schema_version": "p12-capability-report-v1",
        "status": status, "blocking_issues": blockers,
        "kernel": {"release": platform.release(), "architecture": platform.machine()},
        "privilege": {
            "sudo_noninteractive": sudo.returncode == 0,
            "capabilities": required_caps,
            "unprivileged_bpf_disabled": Path("/proc/sys/kernel/unprivileged_bpf_disabled").read_text().strip(),
            "lockdown": Path("/sys/kernel/security/lockdown").read_text().strip()
            if Path("/sys/kernel/security/lockdown").exists() else "unavailable",
        },
        "mounts": {"bpffs": bpffs, "cgroup_v2": cgroup_v2},
        "toolchain": {
            name: (command([name, "--version"]).stdout or command([name, "--version"]).stderr).splitlines()[:1]
            if shutil.which(name) else [] for name in TOOLS
        } | {"libbpf": libbpf.returncode == 0},
        "kernel_features": {
            "btf": btf, "core": btf and shutil.which("clang") is not None,
            "ring_buffer": bool(feature_payload.get("map_types", {}).get("have_ringbuf_map_type")),
            "tracepoints": tracepoints,
            "kprobe": "kprobe" in json.dumps(feature_payload).lower(),
            "fentry_fexit": "fentry" in json.dumps(feature_payload).lower(),
            "tc": required_caps["CAP_NET_ADMIN"],
            "network_namespace": required_caps["CAP_SYS_ADMIN"],
        },
        "kind_kernel_relationship": "kind nodes share the host Linux kernel while using isolated container namespaces",
    }
    if report["status"] not in STATUSES:
        raise RuntimeError("invalid capability status")
    atomic_write(args.output, canonical_bytes(report))
    print(json.dumps(report, sort_keys=True))
    raise SystemExit(0 if status == "supported" else 2)


if __name__ == "__main__":
    main()

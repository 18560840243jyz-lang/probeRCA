#!/usr/bin/env python3
"""Small fault/probe actor used by the final single-VM data-only pilot."""

from __future__ import annotations

import argparse
import json
import mmap
import os
import random
import signal
import socket
import struct
import threading
import time
from pathlib import Path


STOP = threading.Event()
COUNTERS: dict[str, int] = {}


def increment(name: str, value: int = 1) -> None:
    COUNTERS[name] = COUNTERS.get(name, 0) + value


def stop(_signal_number, _frame) -> None:
    STOP.set()


def join_cgroup(path: str | None) -> None:
    if not path:
        return
    target = Path(path).resolve()
    if not str(target).startswith("/sys/fs/cgroup/"):
        raise RuntimeError("refusing non-cgroup target")
    (target / "cgroup.procs").write_text(
        f"{os.getpid()}\n", encoding="ascii"
    )


def wait_until(deadline: float) -> None:
    while not STOP.is_set() and time.monotonic() < deadline:
        STOP.wait(min(0.25, max(0.0, deadline - time.monotonic())))


def memory_actor(byte_count: int, deadline: float) -> None:
    region = mmap.mmap(-1, byte_count)
    for offset in range(0, byte_count, 4096):
        region[offset:offset + 1] = b"x"
        if STOP.is_set():
            break
    increment("bytes_touched", byte_count)
    wait_until(deadline)
    region.close()


def io_actor(path: Path, maximum_bytes: int, deadline: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = b"\0" * (1024 * 1024)
    descriptor = os.open(
        path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600
    )
    try:
        offset = 0
        writes_since_sync = 0
        while not STOP.is_set() and time.monotonic() < deadline:
            os.pwrite(descriptor, block, offset)
            increment("bytes_written", len(block))
            offset += len(block)
            writes_since_sync += 1
            if offset >= maximum_bytes:
                offset = 0
            if writes_since_sync >= 8:
                os.fdatasync(descriptor)
                increment("fdatasync")
                writes_since_sync = 0
        os.fdatasync(descriptor)
    finally:
        os.close(descriptor)


def futex_actor(thread_count: int, hold_ms: float, deadline: float) -> None:
    threading.stack_size(256 * 1024)
    mutex = threading.Lock()

    def holder() -> None:
        while not STOP.is_set() and time.monotonic() < deadline:
            with mutex:
                time.sleep(hold_ms / 1000.0)
                increment("lock_holds")
            time.sleep(0.001)

    def waiter() -> None:
        while not STOP.is_set() and time.monotonic() < deadline:
            acquired = mutex.acquire(timeout=0.5)
            if acquired:
                increment("lock_acquires")
                mutex.release()

    threads = [threading.Thread(target=holder, daemon=True)]
    threads.extend(
        threading.Thread(target=waiter, daemon=True)
        for _ in range(thread_count)
    )
    for thread in threads:
        thread.start()
    wait_until(deadline)
    STOP.set()
    for thread in threads:
        thread.join(timeout=1)


def localnet_actor(thread_count: int, deadline: float) -> None:
    threading.stack_size(256 * 1024)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def client() -> None:
        while not STOP.is_set() and time.monotonic() < deadline:
            candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            candidate.settimeout(0.05)
            result = candidate.connect_ex(("127.0.0.1", port))
            increment("connect_success" if result == 0 else "connect_failure")
            candidate.close()

    threads = [
        threading.Thread(target=client, daemon=True)
        for _ in range(thread_count)
    ]
    for thread in threads:
        thread.start()
    wait_until(deadline)
    STOP.set()
    for thread in threads:
        thread.join(timeout=1)
    server.close()


def tcp_probe(host: str, port: int, interval: float, deadline: float) -> None:
    while not STOP.is_set() and time.monotonic() < deadline:
        candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        candidate.settimeout(0.2)
        result = candidate.connect_ex((host, port))
        increment("connect_success" if result == 0 else "connect_failure")
        candidate.close()
        STOP.wait(interval)


def dns_question(name: str) -> bytes:
    labels = name.rstrip(".").split(".")
    return b"".join(
        bytes([len(label)]) + label.encode("ascii") for label in labels
    ) + b"\0" + struct.pack("!HH", 1, 1)


def dns_probe(
    host: str, name: str, interval: float, deadline: float,
) -> None:
    question = dns_question(name)
    while not STOP.is_set() and time.monotonic() < deadline:
        transaction_id = random.randrange(0, 65536)
        message = struct.pack(
            "!HHHHHH", transaction_id, 0x0100, 1, 0, 0, 0
        ) + question
        candidate = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        candidate.settimeout(0.25)
        try:
            candidate.sendto(message, (host, 53))
            response, _address = candidate.recvfrom(4096)
            if len(response) >= 12:
                flags = struct.unpack("!H", response[2:4])[0]
                rcode = flags & 0xF
                increment("dns_success" if rcode == 0 else "dns_failure")
            else:
                increment("dns_failure")
        except socket.timeout:
            increment("dns_timeout")
        except OSError:
            increment("dns_failure")
        finally:
            candidate.close()
        STOP.wait(interval)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        required=True,
        choices=("memory", "io", "futex", "localnet", "tcp", "dns"),
    )
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--cgroup")
    parser.add_argument("--bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--file", type=Path)
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--hold-ms", type=float, default=50.0)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--interval", type=float, default=0.02)
    parser.add_argument(
        "--dns-name",
        default="productcatalogservice.online-boutique.svc.cluster.local.",
    )
    arguments = parser.parse_args()
    if arguments.duration <= 0:
        raise SystemExit("duration must be positive")
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    join_cgroup(arguments.cgroup)
    started = time.time_ns()
    deadline = time.monotonic() + arguments.duration
    print(json.dumps({
        "event": "actor_started",
        "mode": arguments.mode,
        "pid": os.getpid(),
        "started_at_ns": started,
    }, sort_keys=True), flush=True)
    try:
        if arguments.mode == "memory":
            memory_actor(arguments.bytes, deadline)
        elif arguments.mode == "io":
            if arguments.file is None:
                raise RuntimeError("io actor requires --file")
            io_actor(arguments.file, arguments.bytes, deadline)
        elif arguments.mode == "futex":
            futex_actor(arguments.threads, arguments.hold_ms, deadline)
        elif arguments.mode == "localnet":
            localnet_actor(arguments.threads, deadline)
        elif arguments.mode == "tcp":
            if not arguments.host or not arguments.port:
                raise RuntimeError("tcp actor requires host and port")
            tcp_probe(
                arguments.host, arguments.port,
                arguments.interval, deadline,
            )
        elif arguments.mode == "dns":
            if not arguments.host:
                raise RuntimeError("dns actor requires host")
            dns_probe(
                arguments.host, arguments.dns_name,
                arguments.interval, deadline,
            )
    finally:
        print(json.dumps({
            "counters": COUNTERS,
            "event": "actor_finished",
            "finished_at_ns": time.time_ns(),
            "mode": arguments.mode,
        }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

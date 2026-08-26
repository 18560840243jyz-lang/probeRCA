#!/usr/bin/env python3
"""Bounded worker-side actors used only by allow-listed campaign profiles."""

from __future__ import annotations

import argparse
import mmap
import multiprocessing
import os
import signal
import socket
import threading
import time
from pathlib import Path


STOP = threading.Event()


def _stop(_number, _frame) -> None:
    STOP.set()


def _join_cgroup(value: str) -> None:
    path = Path(value).resolve()
    root = Path("/sys/fs/cgroup").resolve()
    if root not in path.parents or not (path / "cgroup.procs").is_file():
        raise RuntimeError("actor cgroup path is invalid")
    (path / "cgroup.procs").write_text(f"{os.getpid()}\n", encoding="ascii")


def _cpu(workers: int, deadline: float, duty_cycle: float) -> None:
    def work(stop: multiprocessing.Event, end: float, duty: float) -> None:
        value = 1
        period_seconds = 0.1
        while not stop.is_set() and time.monotonic() < end:
            period_start = time.monotonic()
            busy_until = min(end, period_start + period_seconds * duty)
            while not stop.is_set() and time.monotonic() < busy_until:
                value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
            remaining = period_start + period_seconds - time.monotonic()
            if remaining > 0:
                stop.wait(remaining)

    process_stop = multiprocessing.Event()
    processes = [
        multiprocessing.Process(
            target=work, args=(process_stop, deadline, duty_cycle),
        )
        for _ in range(workers)
    ]
    for process in processes:
        process.start()
    while not STOP.is_set() and time.monotonic() < deadline:
        STOP.wait(0.25)
    process_stop.set()
    for process in processes:
        process.join(timeout=2)
        if process.is_alive():
            process.kill()


def _memory(byte_count: int, deadline: float) -> None:
    region = mmap.mmap(-1, byte_count)
    try:
        while not STOP.is_set() and time.monotonic() < deadline:
            for offset in range(0, byte_count, 4096):
                region[offset] = (region[offset] + 1) & 0xFF
                if STOP.is_set():
                    break
            STOP.wait(0.05)
    finally:
        region.close()


def _io(
    path: Path, file_bytes: int, deadline: float, bytes_per_second: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = b"\0" * (1024 * 1024)
    with path.open("w+b", buffering=0) as stream:
        offset = 0
        written = 0
        started = time.monotonic()
        while not STOP.is_set() and time.monotonic() < deadline:
            stream.seek(offset)
            stream.write(block)
            offset = (offset + len(block)) % file_bytes
            stream.flush()
            os.fsync(stream.fileno())
            written += len(block)
            if bytes_per_second > 0:
                target = started + written / bytes_per_second
                remaining = target - time.monotonic()
                if remaining > 0:
                    STOP.wait(remaining)


def _futex(threads: int, deadline: float) -> int:
    """Maintain observable futex contention without busy spinning.

    A single lock held for the whole fault interval only completes its futex
    waits during cleanup.  The normal BPF counter is cumulative over completed
    waits, so that shape incorrectly makes the intervention look inactive for
    almost the entire FAULT_ACTIVE phase.  Condition waiters are therefore
    woken together at a bounded cadence and immediately re-enter their wait.
    At least one real futex wait cycle completes throughout the active phase,
    while the actor remains sleeping rather than consuming a CPU core.
    """
    condition = threading.Condition()
    ready = 0
    completed_waits = 0

    def wait() -> None:
        nonlocal ready, completed_waits
        with condition:
            ready += 1
            condition.notify_all()
            while not STOP.is_set() and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                condition.wait(timeout=min(0.5, remaining))
                completed_waits += 1

    workers = [threading.Thread(target=wait) for _ in range(threads)]
    for worker in workers:
        worker.start()
    startup_deadline = time.monotonic() + 2.0
    with condition:
        while ready < threads:
            remaining = startup_deadline - time.monotonic()
            if remaining <= 0:
                STOP.set()
                condition.notify_all()
                break
            condition.wait(timeout=remaining)
    if ready < threads:
        for worker in workers:
            worker.join(timeout=2)
        raise RuntimeError("futex actor waiter did not start")
    while not STOP.is_set() and time.monotonic() < deadline:
        STOP.wait(min(0.25, max(0.0, deadline - time.monotonic())))
        with condition:
            condition.notify_all()
    with condition:
        condition.notify_all()
    for worker in workers:
        worker.join(timeout=2)
        if worker.is_alive():
            raise RuntimeError("futex actor waiter did not stop")
    return completed_waits


def _local_socket(threads: int, deadline: float) -> None:
    # Connections intentionally target an unbound loopback port.  The actor is
    # joined to the application cgroup and network namespace by the worker
    # agent, so the resulting operations are attributable and bounded.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    def work() -> None:
        while not STOP.is_set() and time.monotonic() < deadline:
            candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            candidate.settimeout(0.05)
            candidate.connect_ex(("127.0.0.1", port))
            candidate.close()

    workers = [threading.Thread(target=work) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=(
        "cpu", "memory", "io", "futex", "local_socket",
    ))
    parser.add_argument("--duration", required=True, type=int)
    parser.add_argument("--cgroup", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--duty-cycle", type=float, default=1.0)
    parser.add_argument("--bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--bytes-per-second", type=int, default=0)
    parser.add_argument("--file", type=Path)
    arguments = parser.parse_args()
    if arguments.duration <= 0 or arguments.duration > 3600:
        raise SystemExit("actor duration must be in 1..3600 seconds")
    if arguments.workers <= 0 or arguments.workers > 128:
        raise SystemExit("actor worker count must be in 1..128")
    if not 0.0 < arguments.duty_cycle <= 1.0:
        raise SystemExit("actor duty cycle must be in (0, 1]")
    if arguments.bytes <= 0:
        raise SystemExit("actor byte count must be positive")
    if arguments.bytes_per_second < 0:
        raise SystemExit("actor byte rate cannot be negative")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    _join_cgroup(arguments.cgroup)
    deadline = time.monotonic() + arguments.duration
    if arguments.mode == "cpu":
        _cpu(arguments.workers, deadline, arguments.duty_cycle)
    elif arguments.mode == "memory":
        _memory(arguments.bytes, deadline)
    elif arguments.mode == "io":
        if arguments.file is None:
            raise RuntimeError("io actor requires --file")
        _io(
            arguments.file, arguments.bytes, deadline,
            arguments.bytes_per_second,
        )
    elif arguments.mode == "futex":
        _futex(arguments.workers, deadline)
    else:
        _local_socket(arguments.workers, deadline)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

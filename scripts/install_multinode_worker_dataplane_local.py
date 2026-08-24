#!/usr/bin/env python3
"""Install the local eBPF/Burst/exporter stack on one real Worker."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path


def _run(arguments: list[str], *, cwd: Path | None = None) -> None:
    completed = subprocess.run(
        arguments, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"worker dataplane install failed: {' '.join(arguments)}: "
            + completed.stderr.strip()
        )


def _copy(source: Path, target: Path, mode: int) -> None:
    temporary = target.with_suffix(target.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    os.chmod(temporary, mode)
    os.replace(temporary, target)


def install(repository: Path, rendered: Path) -> None:
    if os.geteuid() != 0:
        raise RuntimeError("worker dataplane installer requires root")
    repository = repository.resolve()
    rendered = rendered.resolve()
    required = (
        "primitive-exporter.yaml", "live-collector.yaml", "live-burst.yaml",
        "proberca-final-primitive-exporter.service",
    )
    if any(not (rendered / name).is_file() for name in required):
        raise RuntimeError("rendered worker config set is incomplete")
    build = Path("/tmp/proberca-final-bpf-build")
    bpftool = shutil.which("bpftool")
    if bpftool is None:
        raise RuntimeError("bpftool is required on every Worker")
    _run([
        "make", "-f", "Makefile.final", f"BPFTOOL={bpftool}",
        f"BUILD_DIR={build}",
    ], cwd=repository)
    library = Path("/usr/local/lib/proberca-final")
    library.mkdir(parents=True, exist_ok=True)
    for name, mode in (
        ("proberca-final-ebpf-loader", 0o755),
        ("final_normal.bpf.o", 0o644),
        ("proberca-final-burst-loader", 0o755),
        ("final_burst.bpf.o", 0o644),
    ):
        _copy(build / name, library / name, mode)
    config_root = Path("/etc/proberca")
    config_root.mkdir(parents=True, exist_ok=True)
    for name in required[:3]:
        _copy(rendered / name, config_root / name, 0o640)
    host_fault_cgroup = Path("/sys/fs/cgroup/proberca-campaign-host-fault")
    host_fault_cgroup.mkdir(exist_ok=True)
    if not (host_fault_cgroup / "cgroup.procs").is_file():
        raise RuntimeError("could not create the isolated host fault cgroup")
    units = Path("/etc/systemd/system")
    _copy(
        repository / "deploy/final-dataplane/proberca-final-ebpf.service",
        units / "proberca-final-ebpf.service", 0o644,
    )
    _copy(
        repository / "deploy/final-dataplane/proberca-final-burst.service",
        units / "proberca-final-burst.service", 0o644,
    )
    _copy(
        rendered / "proberca-final-primitive-exporter.service",
        units / "proberca-final-primitive-exporter.service", 0o644,
    )
    _run(["systemctl", "daemon-reload"])
    for name in (
        "proberca-final-ebpf.service", "proberca-final-burst.service",
        "proberca-final-primitive-exporter.service",
    ):
        _run(["systemctl", "enable", "--now", name])
        _run(["systemctl", "is-active", "--quiet", name])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--rendered", type=Path, required=True)
    arguments = parser.parse_args()
    install(arguments.repository, arguments.rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

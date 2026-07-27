#!/usr/bin/env python3
"""Install the final ProbeRCA primitive producer on the frozen single VM.

This installer changes deployment state only.  It does not collect experiment
windows, run control-plane code, or inject faults.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml


REQUIRED_REPOSITORY = Path("/home/jyz/probeRCA")
PROMETHEUS_CONFIG = Path("/etc/prometheus/prometheus.yml")
PROMETHEUS_JOB_NAME = "proberca-final-primitives"


def _run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    return subprocess.run(arguments, check=True, **kwargs)


def _require_root() -> None:
    if os.geteuid() != 0:
        raise SystemExit("install_final_dataplane.py must run as root")


def _find_bpftool() -> str:
    candidates = [
        shutil.which("bpftool"),
        "/usr/lib/linux-tools-5.15.0-185/bpftool",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            result = subprocess.run(
                [candidate, "version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode == 0:
                return candidate
    raise SystemExit("a working bpftool executable is required")


def _load_mapping(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} must contain a YAML mapping")
    return payload


def _atomic_copy(source: Path, destination: Path, mode: int) -> None:
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}-",
        dir=str(destination.parent),
    )
    os.close(descriptor)
    temporary = Path(name)
    try:
        shutil.copyfile(source, temporary)
        os.chmod(temporary, mode)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _install_prometheus_job(repository: Path) -> None:
    configuration = _load_mapping(PROMETHEUS_CONFIG)
    scrape_configs = configuration.get("scrape_configs")
    if not isinstance(scrape_configs, list):
        raise SystemExit("Prometheus scrape_configs must be a list")
    desired = _load_mapping(
        repository
        / "deploy/final-dataplane/prometheus-scrape-job.yaml"
    )
    matches = [
        item for item in scrape_configs
        if isinstance(item, dict)
        and item.get("job_name") == PROMETHEUS_JOB_NAME
    ]
    if matches and matches != [desired]:
        raise SystemExit(
            "an incompatible proberca-final-primitives job already exists"
        )
    if not matches:
        scrape_configs.append(desired)
    rendered = yaml.safe_dump(
        configuration, sort_keys=False, allow_unicode=True
    )
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(PROMETHEUS_CONFIG.parent),
        prefix=".proberca-prometheus-",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        _run(["promtool", "check", "config", str(temporary)])
        os.chmod(temporary, PROMETHEUS_CONFIG.stat().st_mode & 0o777)
        os.replace(temporary, PROMETHEUS_CONFIG)
    finally:
        temporary.unlink(missing_ok=True)


def install(repository: Path) -> None:
    _require_root()
    repository = repository.resolve()
    if repository != REQUIRED_REPOSITORY:
        raise SystemExit(
            f"this frozen VM deployment requires {REQUIRED_REPOSITORY}"
        )
    bpftool = _find_bpftool()
    build_directory = Path("/tmp/proberca-final-bpf-build")
    _run([
        "make", "-f", "Makefile.final",
        f"BPFTOOL={bpftool}",
        f"BUILD_DIR={build_directory}",
    ], cwd=repository)

    install_directory = Path("/usr/local/lib/proberca-final")
    install_directory.mkdir(parents=True, exist_ok=True)
    exporter_config = _load_mapping(
        repository / "configs/final_primitive_exporter.example.yaml"
    )
    kubelet_ca_destination = Path(
        exporter_config["kubelet_ca_path"]
    )
    if kubelet_ca_destination != install_directory / "kubelet.crt":
        raise SystemExit(
            "final kubelet CA path must remain inside the install directory"
        )
    with tempfile.TemporaryDirectory(
        prefix="proberca-kubelet-ca-"
    ) as directory:
        kubelet_chain = Path(directory) / "kubelet.crt"
        _run([
            "docker", "cp",
            (
                f"{exporter_config['kind_node_container']}:"
                "/var/lib/kubelet/pki/kubelet.crt"
            ),
            str(kubelet_chain),
        ])
        if (
            kubelet_chain.read_text(encoding="ascii").count(
                "-----BEGIN CERTIFICATE-----"
            ) < 2
        ):
            raise SystemExit(
                "kind kubelet certificate chain lacks its pinned CA"
            )
        _atomic_copy(
            kubelet_chain, kubelet_ca_destination, 0o644
        )
    _atomic_copy(
        build_directory / "proberca-final-ebpf-loader",
        install_directory / "proberca-final-ebpf-loader",
        0o755,
    )
    _atomic_copy(
        build_directory / "final_normal.bpf.o",
        install_directory / "final_normal.bpf.o",
        0o644,
    )
    _atomic_copy(
        build_directory / "proberca-final-burst-loader",
        install_directory / "proberca-final-burst-loader",
        0o755,
    )
    _atomic_copy(
        build_directory / "final_burst.bpf.o",
        install_directory / "final_burst.bpf.o",
        0o644,
    )

    unit_directory = Path("/etc/systemd/system")
    for name in (
        "proberca-final-ebpf.service",
        "proberca-final-burst.service",
        "proberca-final-primitive-exporter.service",
    ):
        _atomic_copy(
            repository / "deploy/final-dataplane" / name,
            unit_directory / name,
            0o644,
        )
    _install_prometheus_job(repository)
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "apply", "-f",
        str(repository / "deploy/final-dataplane/beyla.yaml"),
    ])
    _run([
        "kubectl",
        "--kubeconfig", "/home/jyz/.kube/config",
        "--context", "kind-proberca-ob",
        "-n", "proberca-observe",
        "rollout", "status", "daemonset/proberca-beyla",
        "--timeout=120s",
    ])
    _run(["systemctl", "daemon-reload"])
    _run([
        "systemctl", "enable",
        "proberca-final-ebpf.service",
        "proberca-final-burst.service",
        "proberca-final-primitive-exporter.service",
    ])
    _run(["systemctl", "restart", "proberca-final-ebpf.service"])
    _run(["systemctl", "restart", "proberca-final-burst.service"])
    _run([
        "systemctl", "restart",
        "proberca-final-primitive-exporter.service",
    ])
    _run(["systemctl", "reload", "prometheus.service"])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-ebpf.service",
    ])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-burst.service",
    ])
    _run([
        "systemctl", "is-active", "--quiet",
        "proberca-final-primitive-exporter.service",
    ])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repository",
        type=Path,
        default=REQUIRED_REPOSITORY,
    )
    arguments = parser.parse_args()
    install(arguments.repository)
    print("final data-plane primitive producer installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

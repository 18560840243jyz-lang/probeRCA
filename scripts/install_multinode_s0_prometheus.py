#!/usr/bin/env python3
"""Install the isolated S0 Prometheus used by the one-time campaign."""

from __future__ import annotations

import argparse
import os
import pwd
import shutil
import subprocess
from pathlib import Path


def _run(arguments: list[str]) -> None:
    completed = subprocess.run(
        arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("S0 Prometheus install failed: " + completed.stderr.strip())


def install(config: Path) -> None:
    if os.geteuid() != 0:
        raise RuntimeError("S0 Prometheus installer requires root")
    executable = shutil.which("prometheus")
    if executable is None:
        raise RuntimeError("a pinned Prometheus binary must be installed on S0")
    try:
        pwd.getpwnam("prometheus")
    except KeyError:
        _run(["useradd", "--system", "--home", "/var/lib/prometheus", "prometheus"])
    target = Path("/etc/proberca/prometheus.yaml")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    shutil.copyfile(config, temporary)
    os.chmod(temporary, 0o644)
    os.replace(temporary, target)
    storage = Path("/var/lib/proberca-campaign/prometheus")
    storage.mkdir(parents=True, exist_ok=True)
    user = pwd.getpwnam("prometheus")
    os.chown(storage, user.pw_uid, user.pw_gid)
    unit = Path("/etc/systemd/system/proberca-campaign-prometheus.service")
    unit.write_text(f"""[Unit]
Description=ProbeRCA isolated multi-node campaign Prometheus
After=network-online.target

[Service]
Type=simple
User=prometheus
ExecStart={executable} --config.file=/etc/proberca/prometheus.yaml --storage.tsdb.path={storage} --storage.tsdb.retention.time=36h --storage.tsdb.retention.size=80GB --web.listen-address=0.0.0.0:9090
Restart=on-failure
RestartSec=2s
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
""", encoding="utf-8")
    _run(["systemctl", "daemon-reload"])
    _run(["systemctl", "enable", "--now", unit.stem])
    _run(["systemctl", "is-active", "--quiet", unit.stem])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    arguments = parser.parse_args()
    install(arguments.config.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_real_kernel_process_probe_attach_read_detach(tmp_path):
    build_dir = tmp_path / "build"
    env = {**os.environ, "BUILD_DIR": str(build_dir)}
    build = subprocess.run(
        ["make", "-f", "Makefile.p12", "all"], cwd=ROOT, env=env,
        text=True, capture_output=True, timeout=180,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    relative = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
    cgroup_id = os.stat(Path("/sys/fs/cgroup") / relative.lstrip("/")).st_ino
    command = [
        "sudo", "-n", str(build_dir / "proberca-ebpf-loader"),
        "--object", str(build_dir / "process.bpf.o"),
        "--probe", "process", "--ttl", "2", "--attach-epoch", "12001",
        "--candidate-version", "1", "--candidate-cgroup", str(cgroup_id),
        "--cgroup-path", "/sys/fs/cgroup",
    ]
    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    prefix = [process.stdout.readline(), process.stdout.readline()]
    assert [json.loads(line)["state"] for line in prefix] == ["ATTACHED", "ACTIVE"]
    subprocess.run(["/bin/true"], check=True, timeout=5)
    stdout, stderr = process.communicate(timeout=15)
    stdout = "".join(prefix) + stdout
    assert process.returncode == 0, stderr
    records = [json.loads(line) for line in stdout.splitlines() if line]
    states = [item.get("state") for item in records if item.get("record_type") == "control"]
    assert states == ["ATTACHED", "ACTIVE", "DRAINING", "DETACHING", "CLOSED"]
    events = [item for item in records if item.get("record_type") == "event"]
    assert events, records
    assert all(item["probe_name"] == "process" for item in events)
    summary = [item for item in records if item.get("record_type") == "summary"][-1]
    assert summary["ring_buffer_drops"] == 0
    assert summary["residual_links"] == 0


def test_all_probe_families_attach_read_detach_with_real_kernel(tmp_path):
    build_dir = tmp_path / "build-all"
    output_dir = tmp_path / "acceptance"
    env = {**os.environ, "BUILD_DIR": str(build_dir)}
    build = subprocess.run(
        ["make", "-f", "Makefile.p12", "all"], cwd=ROOT, env=env,
        text=True, capture_output=True, timeout=180,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    run = subprocess.run(
        [
            "python3", "-B", "scripts/p12_real_kernel_acceptance.py",
            "--build-dir", str(build_dir), "--output-dir", str(output_dir),
            "--ttl", "1",
        ],
        cwd=ROOT, text=True, capture_output=True, timeout=180,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    report = json.loads((output_dir / "acceptance_report.json").read_text())
    assert report["status"] == "complete"
    assert report["node_edge_separation"] is True
    assert report["event_loss_rate"] < 0.01
    assert {item["probe"] for item in report["probes"]} == {
        "process", "sched", "futex", "block", "tcp", "dns",
    }
    assert all(item["attach_read_detach"] for item in report["probes"])
    assert all(not item["residual_programs"] for item in report["probes"])
    assert all(not item["residual_maps"] for item in report["probes"])
    assert all(not item["residual_pins"] for item in report["probes"])

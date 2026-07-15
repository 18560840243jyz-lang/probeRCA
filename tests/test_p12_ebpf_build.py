from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_p12_source_tree_has_separate_core_probes_and_libbpf_loader():
    required = {
        "bpf/common/event.h", "bpf/common/maps.h", "bpf/common/filters.h",
        "bpf/process/process.bpf.c", "bpf/sched/sched.bpf.c",
        "bpf/futex/futex.bpf.c", "bpf/block/block.bpf.c",
        "bpf/tcp/tcp.bpf.c", "bpf/dns/dns.bpf.c",
        "bpf/user/proberca_ebpf_loader.c", "Makefile.p12",
    }
    assert not [path for path in sorted(required) if not (ROOT / path).is_file()]


def test_p12_build_is_reproducible_and_uses_core_libbpf_ringbuf(tmp_path):
    build_dir = tmp_path / "build"
    env = {**os.environ, "BUILD_DIR": str(build_dir)}
    result = subprocess.run(
        ["make", "-f", "Makefile.p12", "all"], cwd=ROOT, env=env,
        text=True, capture_output=True, timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    objects = sorted(build_dir.glob("*.bpf.o"))
    assert {path.name for path in objects} == {
        "block.bpf.o", "dns.bpf.o", "futex.bpf.o", "process.bpf.o",
        "sched.bpf.o", "tcp.bpf.o",
    }
    loader = build_dir / "proberca-ebpf-loader"
    assert loader.is_file() and os.access(loader, os.X_OK)
    abi = subprocess.run(
        [str(loader), "--print-abi"], text=True, capture_output=True, timeout=10,
    )
    assert abi.returncode == 0, abi.stderr
    assert '"event_size":136' in abi.stdout
    for path in objects:
        sections = subprocess.run(
            ["llvm-readelf", "-S", str(path)], text=True,
            capture_output=True, timeout=10, check=True,
        ).stdout
        assert ".BTF" in sections and ".BTF.ext" in sections
        assert "maps" in sections and "license" in sections


def test_production_loader_does_not_poll_events_with_bpftool():
    source = (ROOT / "proberca/collectors/ebpf/loader.py").read_text()
    c_source = (ROOT / "bpf/user/proberca_ebpf_loader.c").read_text()
    assert "bpftool" not in c_source
    assert "ring_buffer__poll" in c_source
    assert "ring_buffer__new" in c_source
    assert "bpftool" not in source.replace("bpftool is never used for event reads", "")

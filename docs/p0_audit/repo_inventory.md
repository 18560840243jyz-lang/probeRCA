# P0 Repository Inventory

Audit timestamp: `20260710T055900Z`
Project path: `/home/jyz/probeRCA`

## Git Baseline

Initial mandatory command sequence was run from `/home/jyz/probeRCA` before creating audit directories. The path is not a Git repository.

Evidence: `artifacts/p0_audit/logs/initial_git_20260710T055900Z.log`

| Command | Exit | Result |
|---|---:|---|
| `pwd` | 0 | `/home/jyz/probeRCA` |
| `git status --short` | 128 | `fatal: not a git repository` |
| `git branch --show-current` | 128 | `fatal: not a git repository` |
| `git rev-parse HEAD` | 128 | `fatal: not a git repository` |
| `git log -1 --oneline` | 128 | `fatal: not a git repository` |
| `git diff --stat` | nonzero | git usage output because root has no `.git` |
| `git diff` | not reached in first strict `&&` command; re-recorded in log as non-repo failure context |

Nested Git repository detected only at `external/microservices-demo/.git`. The ProbeRCA project root itself has no Git metadata, so P0 cannot prove pre-existing vs P0-created file changes through `git diff`.

## Environment

Evidence: `artifacts/p0_audit/logs/environment_20260710T055900Z.log`

- OS: Ubuntu 22.04.5 LTS
- Kernel: `Linux jyzz 5.15.0-185-generic x86_64`
- CPU/memory: 4 CPU, 16 GiB RAM
- User: `jyz`, groups include `sudo`, `docker`, `lxd`
- Shell: remote login shell `/bin/bash`
- Python: `/usr/bin/python3`, Python 3.10.12
- pip: `/usr/bin/pip`, pip 22.0.2
- Virtualenv: none (`VIRTUAL_ENV` empty)
- `pip check`: fails: `pygobject 3.42.1 requires pycairo, which is not installed`
- Go/Rust/Cargo: not installed
- clang/LLVM: Ubuntu clang 14.0.0, LLVM target list includes BPF
- bpftool: not found for kernel 5.15.0-185
- libbpf: `/lib/x86_64-linux-gnu/libbpf.so.0` exists; `pkg-config` not installed
- Docker: 29.1.3
- containerd: 2.2.1
- kubectl client: v1.28.2
- Kubernetes: kind control plane reachable at `https://127.0.0.1:35371`; `kubectl get nodes` returned a running control-plane node
- Capabilities: current shell `CapEff=0000000000000000`; no effective privileged caps
- BTF: `/sys/kernel/btf/vmlinux` exists
- cgroup: cgroup v2 (`cgroup2fs`)
- bpffs: mounted at `/sys/fs/bpf`, mode `700`, owned by root

## Directory Tree Summary

Evidence: `artifacts/p0_audit/logs/repo_scan_20260710T055900Z.log`

Top-level entries:

```text
AGENTS.md
artifacts/
configs/
data/
docs/
experiments/
external/
proberca/
pyproject.toml
.pytest_cache/
README.md
scripts/
skills/
tests/
```

Key package directories:

```text
proberca/adapters/online_boutique/
proberca/cli/
proberca/data/
proberca/eval/
proberca/evidence/
proberca/explain/
proberca/features/
proberca/graph/
proberca/inference/
proberca/observation/
proberca/propagation/
```

## Languages and Dependencies

Evidence: `pyproject.toml`, `artifacts/p0_audit/logs/code_structure_20260710T055900Z.log`

- Primary language: Python.
- Python package name: `proberca`.
- Declared dependencies: `numpy`, `pyyaml`, `pytest`.
- No `[project.scripts]` / console-script entrypoint in `pyproject.toml`.
- No root Makefile and no CI workflow found.
- No Go source under ProbeRCA root excluding `external/microservices-demo/.git`.
- No Rust/Cargo project found.
- No eBPF source (`*.bpf.c` or `*bpf*.c`) found under ProbeRCA root excluding nested external Git metadata.

## Build Entry

- Python package uses setuptools via `pyproject.toml`.
- No explicit build command in README or Makefile.
- Compile smoke command executed: `python3 -m compileall -q proberca tests scripts`; it failed due syntax error in `proberca/cli/audit_cpu_distinguishability.py:403`.

## Test Entry

Evidence: `artifacts/p0_audit/logs/test_discovery_20260710T055900Z.log`

- Official pytest config: `[tool.pytest.ini_options] testpaths = ["tests"], pythonpath = ["."], addopts = "-q"`.
- Official full test command inferred from config: `python3 -m pytest`.
- README also documents many phase-specific module commands such as `python3 -m proberca.cli.check_p0_freeze`, `check_p1_freeze`, `check_p2a2_real_rca`, and Online Boutique smoke/fault commands.
- No CI config found.

## Runtime and CLI Entrypoints

There is no installed CLI console script. Runtime entrypoints are Python module files under `proberca/cli/`, for example:

- `proberca.cli.run_p0_experiment`
- `proberca.cli.run_p1a_observation`
- `proberca.cli.run_p1b_ipw_propagation`
- `proberca.cli.run_p1c_sparse_inversion`
- `proberca.cli.run_p2_integrated_replay`
- `proberca.cli.run_integrated_blind_rca`
- checkers such as `proberca.cli.check_b2_integrated_replay`

Evidence: `artifacts/p0_audit/logs/code_structure_20260710T055900Z.log` and package file list.

## Data Input/Output Formats

Current implementation is JSON/JSONL centered, not final-scheme Parquet/JSONL dataset layout.

Evidence:

- `proberca/data/schema.py:10` `MetricRecord` uses timestamp/service/instance/metric/value/incident_id/source.
- `proberca/data/schema.py:24` `EvidenceRecord` uses incident/service/evidence_type/value/source.
- `proberca/data/schema.py:41` `IncidentRecord` stores root labels.
- `proberca/data/schema.py:55` `RCAResult` stores incident/root_service/root_metric/rank/score/path.
- `proberca/data/io.py` provides JSON/JSONL helpers.

Large existing result files include `metrics.jsonl`, `normalized_metrics.jsonl`, `sampling_log.jsonl`, `ipw_stable_propagation_model.json`, `calibrated_residuals.jsonl`, and many `p2_*_summary.json` files.

## Deployment and Fault Injection Entry

Scripts under `scripts/online_boutique/` include:

- `00_check_env.sh`
- `01_prepare_repo.sh`
- `02_create_kind_cluster.sh`
- `03_deploy_online_boutique.sh`
- `04_smoke_test_frontend.sh`
- `99_cleanup_kind_cluster.sh`
- `run_p2a1_cpu_fault.sh`
- `run_p2b0_network_smoke.sh`
- `run_p2c0_io_smoke.sh`
- `run_p2d0_lock_smoke.sh`

These scripts were not executed in P0 because several are deployment/fault-injection paths and P0 forbids destructive injection.

## eBPF Build/Load Entry

No eBPF source or loader entry was found in the ProbeRCA root. eBPF validation level is therefore `NOT_IMPLEMENTED_NO_BPF_SOURCE` for compile/verifier/attach/event/detach.

Evidence: `artifacts/p0_audit/logs/other_language_ebpf_20260710T055900Z.log`, `artifacts/p0_audit/logs/ebpf_status_20260710T055900Z.log`.

## Data, Result, Cache, Model Directories

Important existing artifact directories:

- `data/p1_single_vm/demo*`
- `data/p1_single_vm/audit_quick`, `audit_full`
- `data/p2_online_boutique/a1_*` through `a9_*`
- `data/p2_online_boutique/b1*`, `b2*`, `blind_rerun`, `final_blind_audit`, fault-type repeated/smoke directories
- `.pytest_cache/`
- many `__pycache__/` directories

Largest files are JSONL/model/result artifacts, not source. Evidence: `artifacts/p0_audit/logs/repo_scan_20260710T055900Z.log`.

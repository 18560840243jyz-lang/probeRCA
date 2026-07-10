# P0 Baseline Test Report

Audit timestamp: `20260710T055900Z`
Working directory: `/home/jyz/probeRCA`

## Test Discovery

Evidence: `artifacts/p0_audit/logs/test_discovery_20260710T055900Z.log`

- Official pytest config is in `pyproject.toml`: `testpaths=["tests"]`, `pythonpath=["."]`, `addopts="-q"`.
- No Makefile found.
- No CI workflow found.
- README contains phase-specific `python3 -m proberca.cli.check_*` commands and Online Boutique deploy/fault scripts.
- `pytest --collect-only` collected 214 tests.
- No pytest skip/xfail marker usage found, except a test name containing the word `skip` (`test_compute_fs_delta_and_negative_skip`).

## Summary Table

| Test group | Command | Passed | Failed | Errors | Skipped | XFail | Exit code | Duration | Log file | Assessment |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| Import smoke | `python3 - <<PY import key modules PY` | N/A | 0 | 0 | 0 | 0 | 0 | 0s | `artifacts/p0_audit/logs/python_import_smoke_20260710T055900Z.log` | PASS |
| Compileall | `python3 -m compileall -q proberca tests scripts` | N/A | 1 | 0 | 0 | 0 | 1 | 0s | `artifacts/p0_audit/logs/python_compileall_20260710T055900Z.log` | FAIL: syntax error |
| pip check | `pip check` | N/A | 1 | 0 | 0 | 0 | 1 | 0s | `artifacts/p0_audit/logs/pip_check_20260710T055900Z.log` | FAIL: missing pycairo for pygobject |
| Test collect | `python3 -m pytest --collect-only` | 214 collected | 0 | 0 | 0 | 0 | 0 | 1s | `artifacts/p0_audit/logs/pytest_collect_20260710T055900Z.log` | PASS |
| Core unit subset | `python3 -m pytest tests/test_schema.py ... tests/test_counterfactual_explanation.py` | 31 | 0 | 0 | 0 | 0 | 0 | 42s | `artifacts/p0_audit/logs/pytest_unit_core_20260710T055900Z.log` | PASS |
| Full pytest | `python3 -m pytest` | 214 | 0 | 0 | 0 | 0 | 0 | 533s | `artifacts/p0_audit/logs/pytest_all_20260710T055900Z.log` | PASS; slow path includes P0 audit CLI |
| Ruff | `ruff check .` | N/A | N/A | N/A | N/A | N/A | 127 | N/A | `artifacts/p0_audit/logs/ruff_check_20260710T055900Z.log` | BLOCKED: ruff not installed |
| Mypy | `mypy proberca` | N/A | N/A | N/A | N/A | N/A | 127 | N/A | `artifacts/p0_audit/logs/mypy_check_20260710T055900Z.log` | BLOCKED: mypy not installed |
| Flake8 | `flake8 proberca tests` | N/A | N/A | N/A | N/A | N/A | 127 | N/A | `artifacts/p0_audit/logs/flake8_check_20260710T055900Z.log` | BLOCKED: flake8 not installed |
| Go test | `go test ./...` | N/A | N/A | N/A | N/A | N/A | 127 | N/A | `artifacts/p0_audit/logs/go_test_all_20260710T055900Z.log` | NOT_APPLICABLE/BLOCKED: no Go files and Go not installed |
| Cargo test | `cargo test --all-targets` | N/A | N/A | N/A | N/A | N/A | 127 | N/A | `artifacts/p0_audit/logs/cargo_test_all_20260710T055900Z.log` | NOT_APPLICABLE/BLOCKED: no Cargo project |
| eBPF validation | existing build/load target discovery | 0 | 0 | 0 | 0 | 0 | N/A | N/A | `artifacts/p0_audit/logs/ebpf_status_20260710T055900Z.log` | NOT_IMPLEMENTED_NO_BPF_SOURCE |

## Failure Details

### Compileall failure

Command: `python3 -m compileall -q proberca tests scripts`
Exit code: 1

Failure:

```text
*** Error compiling 'proberca/cli/audit_cpu_distinguishability.py'...
  File "proberca/cli/audit_cpu_distinguishability.py", line 403
    component_sections.append('### ' + r['repeat'] + ' component deltas
                                                     ^
SyntaxError: unterminated string literal (detected at line 403)
```

Possible reason: source file contains an unterminated string literal.
P0 action: not fixed.
Blocks: packaging/static compile clean gate.

### pip check failure

Command: `pip check`
Exit code: 1

Failure:

```text
pygobject 3.42.1 requires pycairo, which is not installed.
```

Possible reason: system Python environment is not dependency-clean.
P0 action: no install/upgrade performed.
Blocks: clean environment reproducibility, but did not block current pytest.

## Full Test Notes

`python3 -m pytest` passed all 214 collected tests in 532.33s. During execution, the slowest observed phase was a subprocess spawned by `tests/test_p0_audit.py`:

```text
/usr/bin/python3 -m proberca.cli.run_p0_audit --output /tmp/.../audit-cli --quick
```

This subprocess generated multi-seed/noise/semantic-ablation artifacts under pytest temp directories and then completed successfully. Evidence was collected through process inspection while the test was running.

## eBPF Validation Level

| Stage | Status | Evidence |
|---|---|---|
| Compile | NOT_IMPLEMENTED_NO_BPF_SOURCE | `ebpf_status_20260710T055900Z.log` |
| Verifier | NOT_IMPLEMENTED_NO_BPF_SOURCE | same |
| Attach | NOT_IMPLEMENTED_NO_BPF_SOURCE | same |
| Event receive | NOT_IMPLEMENTED_NO_BPF_SOURCE | same |
| Detach | NOT_IMPLEMENTED_NO_BPF_SOURCE | same |

Environment also lacks `bpftool`, current shell has no effective capabilities, and `/sys/fs/bpf` is root-owned mode 700. These are environment risks for later eBPF validation, not P0 failures because no eBPF source exists yet.

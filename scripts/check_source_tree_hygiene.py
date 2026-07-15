#!/usr/bin/env python3
"""Reject editor and patch backup artifacts from P11 source scopes."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


_BACKUP_SUFFIXES = (".orig", ".bak", ".backup", ".save", ".rej")
_SWAP_SUFFIXES = (".swp", ".swo")


def _is_backup_artifact(path: Path) -> bool:
    name = path.name
    return (
        name.endswith(_BACKUP_SUFFIXES)
        or name.endswith("~")
        or (name.startswith(".") and name.endswith(_SWAP_SUFFIXES))
    )


def _iter_scope_candidates(root: Path) -> Iterable[Path]:
    for path in root.iterdir():
        if path.is_file() or path.is_symlink():
            yield path

    for relative in (
        "proberca",
        "requirements",
        "deploy/kubernetes/base",
        "deploy/kubernetes/test/p11-smoke",
    ):
        scope = root / relative
        if scope.exists():
            yield from (
                path
                for path in scope.rglob("*")
                if path.is_file() or path.is_symlink()
            )

    scripts = root / "scripts"
    if scripts.exists():
        yield from (
            path
            for path in scripts.rglob("*")
            if path.is_file() or path.is_symlink()
        )

    tests = root / "tests"
    if tests.exists():
        yield from (
            path
            for path in tests.rglob("*")
            if (
                (path.is_file() or path.is_symlink())
                and path.name.lstrip(".").startswith("test_p11")
            )
        )


def find_backup_artifacts(root: Path) -> tuple[str, ...]:
    root = root.resolve()
    violations = {
        path.relative_to(root).as_posix()
        for path in _iter_scope_candidates(root)
        if _is_backup_artifact(path)
    }
    return tuple(sorted(violations))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    args = parser.parse_args()
    violations = find_backup_artifacts(args.root)
    if violations:
        print("backup artifacts found:")
        for path in violations:
            print(path)
        return 1
    print("BACKUP_ARTIFACT_COUNT=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

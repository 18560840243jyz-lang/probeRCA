from __future__ import annotations

import ast
from pathlib import Path

import tomli

ROOT = Path(__file__).resolve().parents[1]
IMPORT_TO_DISTRIBUTION = {"kubernetes": "kubernetes", "numpy": "numpy", "pyarrow": "pyarrow", "requests": "requests", "scipy": "scipy", "yaml": "pyyaml"}

def _project():
    return tomli.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

def _name(requirement: str) -> str:
    return requirement.split(";", 1)[0].split("[", 1)[0].split("=", 1)[0].split("<", 1)[0].split(">", 1)[0].strip().lower()

def test_production_dependencies_are_bounded_and_exclude_pytest():
    dependencies = _project()["project"]["dependencies"]
    assert "pytest" not in {_name(item) for item in dependencies}
    assert all(("==" in item) or (">=" in item and "<" in item) for item in dependencies)

def test_build_system_is_exactly_pinned():
    requirements = _project()["build-system"]["requires"]
    assert requirements
    assert all("==" in item and not any(token in item for token in (">", "<", "~=")) for item in requirements)

def test_test_dependencies_are_isolated():
    dependencies = _project()["project"]["optional-dependencies"]["test"]
    assert any(_name(item) == "pytest" for item in dependencies)
    assert all("==" in item for item in dependencies)

def test_package_discovery_is_limited_to_proberca():
    include = _project()["tool"]["setuptools"]["packages"]["find"]["include"]
    assert include == ["proberca*"]

def test_all_production_third_party_imports_are_declared():
    imported = set()
    for path in (ROOT / "proberca").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".")[0])
    declared = {_name(item) for item in _project()["project"]["dependencies"]}
    assert {IMPORT_TO_DISTRIBUTION[name] for name in imported & IMPORT_TO_DISTRIBUTION.keys()} == declared

def test_smoke_metric_programs_use_only_standard_library():
    payload = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "deploy/kubernetes/test/p11-smoke").glob("metrics-*-configmap.yaml"))
    forbidden = ("numpy", "scipy", "yaml", "requests", "kubernetes", "pyarrow")
    assert not any(f"import {name}" in payload or f"from {name}" in payload for name in forbidden)

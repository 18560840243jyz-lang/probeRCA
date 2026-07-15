from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def _rules(path: Path) -> tuple[str, ...]:
    return tuple(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#"))

def test_production_context_is_an_allowlist():
    rules = _rules(ROOT / ".dockerignore")
    assert rules[0] == "**"
    for required in ("!Dockerfile", "!pyproject.toml", "!requirements/", "!requirements/production.lock.txt", "!proberca/", "!proberca/**/*.py"):
        assert required in rules

def test_sensitive_and_runtime_categories_are_not_allowlisted():
    rules = _rules(ROOT / ".dockerignore")
    exceptions = {rule[1:] for rule in rules if rule.startswith("!")}
    forbidden = ("tests", ".git", ".kube", "kubeconfig", "token", ".venv", "venv", "checkpoint", "generation", "output", "diagnostic")
    assert not any(any(part in item.lower() for part in forbidden) for item in exceptions)
    assert "!proberca/replay/output.py" not in rules
    assert "!proberca/**/*.py" in rules

def test_production_required_source_remains_visible():
    rules = _rules(ROOT / ".dockerignore")
    assert "!proberca/**/*.py" in rules
    assert (ROOT / "proberca/replay/output.py").is_file()
    assert (ROOT / "proberca/cli/live.py").is_file()

def test_smoke_context_has_its_own_allowlist():
    context = ROOT / "deploy/kubernetes/test/p11-smoke"
    rules = _rules(context / ".dockerignore")
    assert rules[0] == "**"
    assert "!Dockerfile" in rules and "!metrics_app.py" in rules
    assert not any(rule.startswith("!../") for rule in rules)

def test_dockerfile_copy_sources_are_within_each_allowlist():
    root_text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY pyproject.toml" in root_text
    assert "COPY requirements/production.lock.txt" in root_text
    assert "COPY proberca" in root_text
    smoke_text = (ROOT / "deploy/kubernetes/test/p11-smoke/Dockerfile").read_text(encoding="utf-8")
    assert "metrics_app.py /app/metrics_app.py" in smoke_text

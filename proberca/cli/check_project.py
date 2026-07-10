"""Project scaffold checks for probeRCA P0."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "p0_single_vm.yaml"

ALLOWED_ENABLED_MODULES = {
    "robust_normalization",
    "stable_propagation",
    "sparse_inversion",
    "semantic_evidence",
    "path_explanation",
}

REQUIRED_DISABLED_MODULES = {
    "real_ebpf",
    "kubernetes",
    "prometheus",
    "beyla",
    "clickhouse",
    "bandit",
    "optional_drift",
    "shapley",
    "ui",
    "gnn",
    "transformer",
    "llm",
}

REQUIRED_MEMORY_FILES = [
    "AGENTS.md",
    "skills/proberca/SKILL.md",
    "docs/PROJECT_CONTEXT.md",
    "docs/IMPLEMENTATION_PLAN.md",
    "docs/DECISIONS.md",
    "docs/VM_SYNC.md",
]


def _failures() -> list[str]:
    failures: list[str] = []

    if not CONFIG_PATH.exists():
        return [f"缺少配置文件：{CONFIG_PATH.relative_to(PROJECT_ROOT)}"]

    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    if config.get("project_mode") != "single_vm_pseudo_distributed":
        failures.append("project_mode 必须是 single_vm_pseudo_distributed。")

    enabled_modules = set(config.get("enabled_modules") or [])
    unknown_enabled = enabled_modules - ALLOWED_ENABLED_MODULES
    if unknown_enabled:
        failures.append(f"enabled_modules 包含非 P0 模块：{sorted(unknown_enabled)}")

    disabled_modules = set(config.get("disabled_modules") or [])
    missing_disabled = REQUIRED_DISABLED_MODULES - disabled_modules
    if missing_disabled:
        failures.append(f"disabled_modules 缺少禁用模块：{sorted(missing_disabled)}")

    for file_name in REQUIRED_MEMORY_FILES:
        if not (PROJECT_ROOT / file_name).exists():
            failures.append(f"缺少项目记忆文件：{file_name}")

    return failures


def main() -> int:
    """Run project status checks and print Chinese output."""

    failures = _failures()
    print("probeRCA P0 Step 1 项目状态检查")
    print(f"项目根目录：{PROJECT_ROOT}")
    print(f"配置文件：{CONFIG_PATH.relative_to(PROJECT_ROOT)}")

    if failures:
        print("检查失败：")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("检查通过：当前配置属于 single VM pseudo-distributed P0 scaffold。")
    print("已确认 enabled_modules 仅包含 P0 允许模块。")
    print("已确认 forbidden modules 保持禁用。")
    print("已确认项目记忆文件存在。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

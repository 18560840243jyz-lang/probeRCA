"""Environment check for probeRCA P0 Step 1 scaffold."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = [
    "AGENTS.md",
    "skills/proberca/SKILL.md",
    "docs/PROJECT_CONTEXT.md",
    "docs/IMPLEMENTATION_PLAN.md",
    "docs/DECISIONS.md",
    "docs/VM_SYNC.md",
    "configs/p0_single_vm.yaml",
    "proberca/data/schema.py",
    "proberca/graph/schema.py",
]

REQUIRED_MODULES = ["numpy", "yaml", "pytest"]


def main() -> int:
    failures: list[str] = []

    if sys.version_info < (3, 10):
        failures.append(f"Python 版本需要 >= 3.10，当前版本：{sys.version.split()[0]}")
    else:
        print(f"Python 版本检查通过：{sys.version.split()[0]}")

    for module_name in REQUIRED_MODULES:
        if importlib.util.find_spec(module_name) is None:
            failures.append(f"缺少 Python 模块：{module_name}")
        else:
            print(f"依赖检查通过：{module_name}")

    for file_name in REQUIRED_FILES:
        if not (PROJECT_ROOT / file_name).exists():
            failures.append(f"缺少关键文件：{file_name}")
        else:
            print(f"关键文件存在：{file_name}")

    if failures:
        print("环境检查失败：")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("环境检查通过：probeRCA P0 Step 1 scaffold ready。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

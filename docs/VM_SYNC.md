# VM Sync Rule

## Why

当前代码开发和实验可能发生在不同环境中。

Windows local machine
中文解释：Windows 本机。

single VM
中文解释：单机虚拟机。

为了防止项目规则丢失，AGENTS.md、skills/proberca/SKILL.md 和 docs/ 下的项目记忆文件必须跟随仓库同步。

## Required Files

虚拟机实验目录必须包含：

- AGENTS.md
- skills/proberca/SKILL.md
- docs/PROJECT_CONTEXT.md
- docs/IMPLEMENTATION_PLAN.md
- docs/DECISIONS.md
- docs/VM_SYNC.md

## Recommended Sync Methods

git
中文解释：版本控制工具。

rsync
中文解释：增量同步工具。

scp
中文解释：基于 SSH 的文件复制工具。

当前阶段不强制配置 git remote，但必须保证虚拟机运行目录和本机 Codex 修改目录内容一致。

## Rule

不允许只复制 Python 文件。

必须同步整个仓库。

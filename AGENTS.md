# probeRCA Project Rules

## Project Goal

probeRCA 当前目标是在单机虚拟机上完成伪分布式验证，并逐步形成 label-safe blind RCA pipeline。

single VM pseudo-distributed verification
中文解释：单机虚拟机伪分布式验证，即在一台虚拟机上模拟多个服务、多个实例、多个指标和多个故障类型。

当前不是生产级多机分布式系统，也不是 production-ready 系统。

## Current Phase

P0 已冻结。
P1 已冻结。
A1-A10 已完成 label-safe modular repair and final blind audit。
当前 B 阶段目标是：integrated blind RCA pipeline -> replay -> future re-injection。

当前 B1/B1R 只允许做 integrated pipeline smoke 和 final result schema 修复。
B2 才允许做 20 次已有 raw metrics 全量 replay。
B3 才允许重新真实注入故障。

## Frozen Logic Rules

- 不允许修改 P0/P1 freeze snapshot。
- 不允许修改 P0/P1 冻结逻辑。
- 不允许修改 P1 scoring logic。
- 不允许为了 debug 指标修改 A2-A9 算法输出。

## Label Safety Rules

- 禁止使用 legacy target-aware `evidence.jsonl` 做 blind claim。
- 禁止把 legacy P2E 100% 称为 blind RCA。
- 禁止使用 `root_service`、`root_metric`、`root_type`、`target_service`、`target_metric`、`target_fault_type`、`injected_path` 参与推理、排序、构图、证据生成、传播学习、稀疏反演或反事实解释。
- `incidents.jsonl` 只能在结果生成后用于 debug evaluation，不得反向影响结果。

## Current Implementation Category

当前实现是 stable-only probeRCA modular prototype。
中文解释：稳定传播版 probeRCA 模块化原型。

A3-A9 是 label-safe module previews。B1/B1R 正在把这些模块整合成端到端 blind RCA schema。

## Still Not Production Ready

当前仍未实现：

- production real eBPF/libbpf/CO-RE activation。
- Prometheus / Beyla / ClickHouse / OTel / Alertmanager production stack。
- multi-node Kubernetes production validation。
- propagation drift。
- production UI。

## Mandatory Behavior

每次执行开发任务前，必须先读取：

AGENTS.md
skills/proberca/SKILL.md
docs/PROJECT_CONTEXT.md
docs/IMPLEMENTATION_PLAN.md
docs/DECISIONS.md

每次只完成当前任务，不擅自扩展范围。

新开发必须遵循 review-before-next-step。
中文解释：每一轮修复或新增模块后必须做 review/audit，再进入下一步。

如果必须新增依赖，必须说明原因。默认不新增依赖。

不允许为了“看起来能跑”而加入 fallback。
不允许用 mock 掩盖核心算法错误。
不允许写“看起来能跑但不符合方案”的代码。

## Repository Location and VM Rule

项目根目录固定为 `/home/jyz/probeRCA`。

所有开发必须在单机虚拟机项目目录中进行，不要在 Windows 本机写代码。

当前虚拟机没有 `python` 命令，只有 `python3`。所有 Python 命令统一使用 `python3`。

## Completion Report Requirement

每次完成任务后必须输出：

- 修改了哪些文件。
- 为什么这些修改属于当前阶段。
- 如何验证。
- 是否偏离当前任务边界。
- 是否存在未解决问题。

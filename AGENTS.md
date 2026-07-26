# probeRCA Project Rules

## Project Goal

probeRCA 当前目标是在单机虚拟机上完成伪分布式验证，并逐步形成 label-safe blind RCA pipeline。

single VM pseudo-distributed verification
中文解释：单机虚拟机伪分布式验证，即在一台虚拟机上模拟多个服务、多个实例、多个指标和多个故障类型。

当前不是生产级多机分布式系统，也不是 production-ready 系统。

## Current Phase

P0 已冻结。
P1 已冻结。

当前唯一有效的新方案是：

`skills/proberca/SKILL.md` 中保存的 ProbeRCA-BPF 最终定稿版。

当前阶段是最终数据面/控制面分离实现的阻断项修复与 Healthy-only dry run
准备。旧 A/B 阶段内容只保留为历史兼容背景，不再作为新实现方案。

当前禁止真实故障注入。只有完成最终契约修复、测试、真实采集器迁移、
Healthy-only 契约 dry run 和单事故 Pilot 审计后，才可进入正式采集。

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

当前实现是 Final ProbeRCA-BPF two-plane architecture checkpoint。
中文解释：最终 ProbeRCA-BPF 数据面/控制面分离架构检查点。

它尚不是正式实验基线。旧混合 `ProbeRCAEngine` 只用于历史回归，
不是最终方案入口。

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

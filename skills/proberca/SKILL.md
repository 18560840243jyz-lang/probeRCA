---
name: proberca
description: Keep Codex aligned with the probeRCA single-VM pseudo-distributed verification plan, frozen P0/P1 rules, label-safe blind RCA repair chain, and B-stage integrated pipeline boundaries.
---

# probeRCA Local Development Skill

## Purpose

This skill keeps probeRCA implementation consistent with the current single-VM validation plan.

中文解释：本 skill 用于指导 Codex 在 probeRCA 项目中保持方案一致性，防止后续开发中忘记冻结边界、标签安全边界或当前 B 阶段范围。

## Current Status

P0 已冻结。
P1 已冻结。
A1-A10 已完成 label-safe modular repair and final blind audit。

当前 B 阶段目标：

1. B1/B1R integrated blind RCA pipeline smoke and final result schema repair。
2. B2 replay over existing raw metrics。
3. B3 future real re-injection。

当前不是 production-ready。

## Frozen Logic Rules

- Do not modify P0/P1 frozen logic.
- Do not modify P1 scoring logic.
- Do not edit freeze snapshots to hide failures.

## Label Safety Rules

Never use these labels for inference, ranking, graph building, evidence generation, propagation learning, sparse inversion, or counterfactual explanation:

- `root_service`
- `root_metric`
- `root_type`
- `target_service`
- `target_metric`
- `target_fault_type`
- `injected_path`

`incidents.jsonl` may be used only after output generation for debug evaluation.

Legacy target-aware `evidence.jsonl` must not be used for blind RCA claims.
Legacy P2E 100% must not be described as blind RCA.

## Current Core Technical Line

- alert gate
- blind evidence
- candidate subgraph
- adaptive probe policy preview
- IPW-masked online RLS
- evidence channel and calibrated residual
- graph sparse inversion
- counterfactual explanation
- integrated blind RCA result schema

## Not Implemented / Not Claimed

- real eBPF/libbpf/CO-RE activation
- Prometheus/Beyla/ClickHouse/OTel/Alertmanager production stack
- multi-node Kubernetes production validation
- propagation drift
- production UI

## Mandatory Behavior

Before each development task, read:

- AGENTS.md
- docs/PROJECT_CONTEXT.md
- docs/IMPLEMENTATION_PLAN.md
- docs/DECISIONS.md
- skills/proberca/SKILL.md

Use `python3`, not `python`.

Work only in `/home/jyz/probeRCA` when the task targets the VM project.

Do not expand beyond the current user task. Review before moving to the next stage.

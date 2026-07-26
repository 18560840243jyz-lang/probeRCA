# probeRCA Project Context

## Authoritative Current Scheme

当前唯一有效的新方案全文保存在：

`skills/proberca/SKILL.md`

该方案固定采用数据面/控制面分离、服务级 Healthy `A_s`、指标级
Healthy masked Ridge `A_v`、只扣除跨指标传播、Burst 只调整对应组惩罚，
以及单次非负 Sparse-Group FISTA。不得恢复综合关系强度变量或反事实重复求解。

下文关于 P0/P1 和早期 synthetic 路线的内容只表示历史背景，不覆盖上述最终方案。

## Problem

probeRCA 解决微服务系统故障后的根因定位问题。

目标是在大量服务和指标中定位：

service
中文解释：服务。

metric
中文解释：指标。

root-cause type
中文解释：根因类型。

path
中文解释：解释路径。

## Current Environment

当前环境是单机虚拟机。

single VM
中文解释：单台虚拟机。

pseudo-distributed
中文解释：伪分布式，即在一台机器上模拟多服务、多实例、多指标和多故障。

当前不是完整生产系统。

## Current Core Line

stable propagation
中文解释：稳定传播。

sparse root-cause intervention
中文解释：稀疏根因干预。

semantic eBPF evidence
中文解释：语义化 eBPF 证据。注意当前 P0 不做真实 eBPF，只做 evidence schema 和模拟证据。

path explanation
中文解释：路径解释。

## Why Synthetic Data First

synthetic pseudo-distributed data
中文解释：合成伪分布式数据。

先用合成伪分布式数据，是为了在单机上验证算法主线，避免一开始陷入 Kubernetes、真实 eBPF、Prometheus、Beyla、ClickHouse 等工程问题。

## Migration Plan

P0 通过后再做 P1。

P1 通过后再接真实观测系统。

单机验证通过后，再租服务器跑真实分布式实验。

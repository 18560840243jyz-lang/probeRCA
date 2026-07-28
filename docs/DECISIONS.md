# probeRCA Decisions

- 先单机伪分布式，不租服务器。
- 先 P0，不做 P1/P2。
- 先 synthetic data，不接真实 Kubernetes。
  synthetic data 中文解释：合成数据。
  Kubernetes 中文解释：容器编排系统。
- 先离线算法，不做在线流系统。
- 先 JSONL / CSV 文件，不做 ClickHouse。
  ClickHouse 中文解释：列式数据库。
- 先规则化 semantic evidence，不做真实 eBPF。
  semantic evidence 中文解释：语义证据。
  eBPF 中文解释：Linux 内核观测技术。
- 先路径解释，不做 Shapley。
  Shapley 中文解释：贡献值解释方法。
- 稳定传播优先，不默认图漂移。
- 根因输出必须包含 service、metric、type、path。
  service 中文解释：服务。
  metric 中文解释：指标。
  type 中文解释：根因类型。
  path 中文解释：解释路径。
- 不允许为了通过测试而加入与方案无关的假逻辑。
- 不允许提前实现禁用模块。
- 项目记忆文件必须跟随仓库保存。
- 虚拟机实验目录必须包含 AGENTS.md 和 skills/proberca/SKILL.md。
- Windows 本机和单机虚拟机不能维护两套不一致的 probeRCA 代码。
- 后续以仓库根目录为唯一可信上下文。
- 如果本机和虚拟机内容不一致，必须先同步，再继续实验。

## P0 Freeze Decision

- P0 has passed G1 gate.
  中文解释：P0 已通过 G1 决策门。
- P0 is frozen as the baseline for P1.
  中文解释：P0 冻结为 P1 的基础版本。
- Freeze snapshot is stored under docs/p0_freeze_snapshot.
  中文解释：冻结快照保存在 docs/p0_freeze_snapshot。
- P1 cannot modify P0 outputs silently.
  中文解释：P1 不能静默修改 P0 输出逻辑。
- Any change to P0 scoring logic must rerun full audit and G1 gate.
  中文解释：任何 P0 打分逻辑变更都必须重跑完整审计和 G1 决策门。
- Disk space must be checked before running multi-seed experiments.
  中文解释：跑多 seed 实验前必须检查磁盘空间。
- Large intermediate files must be cleaned after audit runs.
  中文解释：审计后必须清理大体量中间文件。


## P1 Freeze Decision

- P1 has passed P1 gate.
  中文解释：P1 已通过 P1 决策门。
- P1 is frozen as the partial-observation RCA baseline.
  中文解释：P1 冻结为部分观测根因定位基线。
- Freeze snapshot is stored under docs/p1_freeze_snapshot.
  中文解释：冻结快照保存在 docs/p1_freeze_snapshot。
- P1 scoring logic cannot be changed silently.
  中文解释：P1 打分逻辑不能被静默修改。
- Any change to P1 scoring logic must rerun full P1 audit and P1 gate.
  中文解释：任何 P1 打分逻辑变更都必须重跑完整 P1 审计和 P1 决策门。
- P2 must not modify P0/P1 frozen outputs silently.
  中文解释：P2 不能静默修改 P0/P1 已冻结输出。

## P2 Real Experiment Metric Policy

P2 真实实验主指标采用 `metric_hit_at_3`，并同时报告 `service_hit_at_1`、`root_type_accuracy` 和 `path_fidelity`。`metric_hit_at_1` 只作为辅助指标报告，不作为 P2 真实实验通过门槛。P2A-3R 在 Top3 口径下通过，但不是 exact metric Top1 成功。后续 network / IO / lock 真实注入也采用同一口径。多故障总体准确率必须同时报告：`service_hit_at_1`、`metric_hit_at_3`、auxiliary `metric_hit_at_1`、`root_type_accuracy`、`path_fidelity`。

## P2D-1R Phase-Aware Lock Collection Decision

P2D-1 failed because lock metrics existed only in faulty windows and therefore did not enter P1 robust baseline normalization. P2D-1R changes the real sidecar collection protocol so baseline/faulty/recovery windows all emit real lock measurements. Baseline lock metrics are real idle sidecar measurements, not fake baseline zeros. P1 scoring and P0/P1 frozen logic remain unchanged.

## P2E Real Multi-Fault Summary

- CPU / Network / I/O / Lock real repeated experiments have been summarized under `data/p2_online_boutique/multifault_summary`.
- P2 primary metrics are service Hit@1, metric Hit@3, root type accuracy, and path fidelity.
- metric Hit@1 is an auxiliary metric and is reported, not used as a P2 real-experiment pass threshold.
- CPU exact Top1 instability is reported explicitly: CPU exact metric Hit@1 is unstable, while metric Hit@3 is stable.
- Lock sidecar limitation is reported explicitly: lock contention comes from a cartservice Pod sidecar and is not an original cartservice business-code bug.
- P2E passing does not imply Prometheus/Beyla/ClickHouse integration and does not imply multi-node production Kubernetes deployment.

## A1 Evidence De-leak

A1 Evidence De-leak implemented as blind evidence generation protocol.
中文解释：A1 已实现 blind evidence 生成协议，但尚未做 blind RCA rerun。

Legacy P2 `evidence.jsonl` remains target-aware evidence and must not be used for blind RCA claims. A1 writes separate `blind_evidence.jsonl` and `blind_evidence_metadata.json` from all observed service.metric lift without using root labels, target configuration, or injected paths.

## A2 Blind P2 Rerun

A2 Blind P2 Rerun uses existing real raw metrics and A1 blind evidence to rerun the frozen P1 RCA pipeline without new fault injection.
中文解释：A2 使用已有真实 raw metrics 和 A1 blind evidence 重跑冻结 P1 RCA pipeline，不重新注入故障。

A2 does not use legacy target-aware `evidence.jsonl` from raw experiment directories. It still uses `incident.start_ts` and `incident.end_ts` as the alert window; A3 will implement the true Alert Gate.

## A3 Alert Gate

A3 Alert Gate implements metrics-driven alert event detection and alert window construction.
中文解释：A3 实现基于 metrics 的告警事件检测和告警窗口构造。

A3 does not run RCA, does not reinject faults, does not modify P1 scoring, and does not use incident start/end or root labels for detection. Incidents may be used only after detection for debug overlap evaluation.


## A4 Candidate Subgraph Safety Decision

Candidate graph construction must not use root labels, target config, injected paths, or incident start/end timestamps. Incidents are allowed only for post-build debug coverage. Graph direction assumptions must be recorded in candidate metadata.

## A5 Adaptive Probe Policy Safety Decision

Adaptive probe selection must use alert windows, candidate subgraphs, blind evidence, and metric availability only. Root labels, target configuration, injected paths, and incident start/end timestamps are forbidden for policy decisions. Debug coverage using incidents is allowed only after policy generation.

## A6 IPW-masked RLS Safety Decision

A6 learning must consume A5 sampling probabilities and observation masks, use online RLS updates, and avoid root labels, target config, injected paths, and incident windows. Incidents are debug-only after learning.

## A7 Evidence Channel Decision

A7 uses only A2 blind evidence, A5 probe policy, and A6 IPW-masked RLS residuals to construct `C h_t`. It explicitly avoids root labels, target labels, injected paths, and incident start/end for channel construction. Because A6 raw residual scale can be large, A8 must consume `calibrated_residuals.jsonl`, not raw residuals.


## A8 Graph Sparse Inversion Decision

A8 consumes `calibrated_residuals.jsonl` from A7 and refuses uncalibrated raw residual-only input. The solver uses L1, graph total variation, and service group-lasso penalties through ADMM. Debug incident labels may be read only after output generation.

## A8R Graph Sparse Inversion Repair

A8R repairs A8 sparse inversion by reducing metric-level edge explosion, using positive top-k calibrated residual aggregation, adding blind-evidence signal support, using automatic sparse regularization, applying post-sparsify, and improving ADMM convergence. The repair does not use root labels, target labels, injected paths, or incident start/end times for inversion. A8R remains a preview and is not a P2E acceptance result.

## A9 Counterfactual Explanation

A9 implements counterfactual explanation preview for A8R sparse candidates. For top metric and service candidates it re-optimizes graph sparse inversion with the candidate removed and reports `Delta L = L(u^{-v}) - L(u_hat)`. A9 does not use root labels, target labels, injected paths, or incident start/end times for explanation generation. It does not run old P1 RCA and does not reinject faults. Debug metrics are post-hoc diagnostics only, not P2E acceptance.

## B1 Integrated Blind RCA Pipeline

B1 integrates A3-A9 into a single end-to-end blind RCA smoke pipeline over existing raw metrics and service graph data. It uses A3 alert windows, alert-window blind evidence, A4 candidates, A5 policy preview, A6 IPW-masked RLS, A7 calibrated residuals, A8R graph sparse inversion, and A9 counterfactual explanation to write an integrated RCA result schema.

B1 does not reinject faults, does not run the old P1 RCA pipeline, does not modify P1 scoring logic, does not use legacy target-aware evidence, and does not use root/target labels or injected paths for inference. B1 is a single smoke integration step; B2 is the full 20-repeat replay and B3 is future real reinjection.

## B1R Integrated Final Result Repair

B1R repairs B1 final RCA result assembly. The final result now uses a metric-level `metric_candidate_table` as the primary candidate source, derives `top1_service`, `top1_metric`, and `predicted_root_type` from the same primary candidate, aggregates `top_services` from metric candidates, and writes one RCA result per alert window. B1R does not run B2 replay, does not reinject faults, does not run the old P1 RCA pipeline, and does not use root/target labels or legacy target-aware evidence.

## B2 Integrated Replay Decision

B2 is a full replay over existing raw metrics using the B1R integrated pipeline. Incident labels are permitted only after final result generation for evaluation. B2 results must be reported honestly and must not be merged with A2 official blind rerun claims.

## B2R Integrated Ranking Repair Decision

B2R may adjust integrated final candidate ranking using static metric diagnostic specificity and blind-evidence support. This is allowed because it is a label-free semantic prior over metric names, not a repeat-specific root-label rule. `memory.usage` is weak diagnostic evidence unless supported by stronger memory signals, while CPU throttling metrics are high-specificity CPU diagnostics. Root labels, target labels, injected paths, and incident start/end timestamps remain forbidden for inference and ranking. B2R remains replay over existing raw metrics and is not B3 reinjection.

## B2S Service-first RCA Repair

B2R fixed CPU metric-family and root-type recognition, but CPU service Hit@1 remained 0.0. B2S changes the final integrated RCA schema from metric-first to service-first. The primary root service now comes from `service_candidate_table`; the primary root metric is selected only within that root service. Global metric ranking remains available only as `global_top_metrics_auxiliary` and is not a primary RCA result.

B2S adds service-conditioned evaluation fields: `service_conditioned_metric_hit_at_3`, `global_metric_hit_at_3_auxiliary`, and `service_metric_pair_hit_at_1`. Labels remain post-hoc evaluation only. B2S does not use root labels, target labels, injected paths, incident start/end timestamps, or legacy target-aware evidence for inference. B2S is still replay over existing raw metrics; B3 is the future real reinjection stage.

## B2M Service-Metric Ownership Mapping Repair

B2S already switched the integrated RCA output to a service-first hierarchy, but CPU service localization remained weak. B2M adds explicit service-metric ownership mapping so each service's own resource metric remains tied to that service throughout final assembly, for example `paymentservice.cpu.throttled_usec`, `adservice.cpu.throttled_usec`, and `checkoutservice.cpu.throttled_usec`.

Evidence support is now separated into node-level, service-family-level, and family-global-level support. Family-global evidence is kept as a weak fallback only, with `family_global_evidence_weight = 0.10`, so global CPU evidence cannot by itself make all CPU services equivalent. Primary RCA candidates must pass ownership checks, and final metadata records `service_local_support_used`, `global_family_support_weight_limited`, `ownership_invalid_count`, and `primary_candidate_ownership_valid`.

B2M remains an existing-raw-metrics replay. It does not use root labels, target labels, injected paths, or incident start/end during inference, and it is not B3 real re-injection.

## B2P Normal Propagation Audit and Repair

B2M ruled out service-metric ownership loss as the main CPU failure mode. CPU service localization remained weak, so B2P adds a stable-only structured multi-lag propagation support stage. This stage learns label-free parent sets and lagged propagation weights from existing raw metrics, alert windows, candidate nodes, and probe-policy sampling probabilities. It does not implement propagation drift.

The parent set is structure constrained rather than fully connected: self-lag, same-service resource -> request, same-service request -> request, callee resource/request -> caller request, and request-chain propagation. It does not use root labels, target labels, injected paths, or incident start/end.

The integrated pipeline now emits `05b_structured_propagation/` with structured parent sets, propagation edges, predictions, residuals, and metadata. Final service scoring consumes `structured_propagation_support`, `path_edge_support`, and `lag_support`; if structured support is unavailable, fallback is explicit in score components. B2P remains an existing-raw-metrics replay, not B3 real re-injection.

## Final Data-Plane / Control-Plane Separation Decision

The final ProbeRCA-BPF path must collect and seal all input windows before the RCA control algorithm consumes them. `proberca/dataplane` must not import or execute control-plane code, and `proberca/controlplane` must not invoke collectors or mutate a sealed archive. The legacy mixed `ProbeRCAEngine` path remains only for frozen historical regression compatibility and is not the canonical final-scheme entrypoint.

Final normal metrics are service-level, node-level, or directed service-pair aggregates exactly as declared in `configs/final_collection_contract.yaml`; incomplete entity metric sets fail closed. Ground-truth, target configuration, injection paths, and expected-root fields are forbidden across the boundary. Burst evidence is collected after Hard in a distinct following window, is required to be independent from residual metrics, and may only reduce the matching `(entity, root category)` group penalty. The final path does not subtract Burst evidence from residuals, add a direct evidence ranking term, introduce a composite relation-strength variable, or perform counterfactual repeat solves.

## Final DNS Attribution and Aggregation Decision

DNS attribution is resolved before Pod-to-Service aggregation. The frozen
single-VM policy is `configs/final_dns_aggregation_policy.yaml`; its ID and
SHA-256 are part of collection-contract v3 and the aggregation fingerprint.
Existing collection-contract v2 archives remain readable for Replay, but new
v3 archives reject an unconfigured or all-zero DNS policy.

The formal DNS edge still exposes exactly three metrics:

- `dns_query_count` counts closed logical transactions admitted by policy;
- `dns_latency_p95` merges successful-transaction histograms only;
- `dns_failure_rate` uses the frozen terminal failure counters.

Application, DNS-sidecar, and diagnostic containers remain separate. Metadata
probes and other `record_only` qname classes are retained in policy audit
output but do not enter the formal business DNS edge. Unknown roles, qname
classes, outcomes, incomplete transaction identity, and inconsistent terminal
counter partitions fail closed.

The current normal BPF implementation handles UDP pending state across
one-second windows, qname/qtype identity, retries, terminal outcomes, and
successful-only latency. Two engineering items remain explicit readiness
blockers rather than silent approximations:

1. a locally sealed per-logical-transaction normal ledger is not yet wired
   into the final collection archive; the formal map path retains cumulative
   per-qname counters and audit series;
2. UDP truncation followed by TCP fallback is detected but not yet reassembled,
   so any observed `TC=1` makes the DNS snapshot Not Ready.

Healthy Pilot and fault injection must remain disabled until the required DNS
scope has real application-container coverage and these blockers are either
implemented or explicitly excluded by a newly reviewed scheme revision.

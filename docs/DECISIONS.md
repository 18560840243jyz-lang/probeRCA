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

Burst calibration is Healthy-only and target-scoped. References are keyed by
`(entity_id, channel_id)`; observations from different services, hosts, or TCP
edges are never pooled into one baseline. For event-count channels, a real
exposure produces `count/exposure`; zero exposure retains the one-second count
and never divides by a numerical epsilon. The frozen rare-event threshold is
the maximum of the configured resolution floor and the configured Healthy
upper quantile. These rules are label-independent and are frozen before fault
data is evaluated.

## Formal single-VM fault-trial qualification

Archive integrity and intervention validity are separate gates. A sealed,
aligned `9/4/3` archive proves that data were recorded correctly; it does not
prove that the declared fault affected the application or the corresponding
formal root coordinate. Before an experiment enters an accuracy denominator,
an after-seal qualification report must prove all of the following:

- the injector acted on the real application/resource/communication path,
  rather than producing an isolated signal in an unrelated helper;
- the declared formal root metric or its exact formal mechanism is observed;
- the abnormal phase contains a Confirmed Hard business symptom under the
  frozen `5 x 3` rule;
- topology and runtime identity remain stable and formal Pods do not restart;
- Normal/Burst archives remain aligned and no second fault category was
  intentionally introduced.

The injector manifest and expected root may be used only by this post-seal
qualification and the final evaluator. They remain forbidden inputs to alert
detection, candidate construction, propagation, residuals, Burst calibration,
FISTA, and ranking. A trial that fails qualification is reported as an invalid
intervention and must be recollected; it is neither counted as an RCA miss nor
made valid by lowering alert thresholds. Per-class accuracy must report both
the attempted-trial count and the qualified-trial denominator.

Final `A_v` models cross-metric propagation only. Its semantic mask excludes
the target coordinate itself at every lag; a target with no legal cross-metric
parents is Ready with an empty coefficient set and an exact zero propagation
contribution. P95 modeling validity remains governed by
`latency_min_samples`, while alert eligibility additionally requires
`ceil(1 / (1 - quantile))` samples. Observations between those two thresholds
remain available to Baseline and `A_v` but cannot contribute latency to
Soft/Hard scores.

## Final Formal Root-Cause Scope and Experimental DNS Decision

The formal ProbeRCA-BPF paper scope contains only:

- service root-cause entities;
- host root-cause entities;
- directed TCP edge entities identified as
  `(src_service -> dst_service, TCP)`.

For the frozen single-VM Online Boutique experiment, that scope is exactly
11 Service-backed business services, one host, and 15 directed TCP edges.
It contains 100 required root coordinates
(`11 * 6 + 1 * 4 + 15 * 2`). `kube-dns` is infrastructure and
`loadgenerator` is an experiment load producer; neither is alert-eligible,
root-eligible, or required for Readiness.

The final normal metric contract is `9/4/3`: nine metrics per service, four
metrics per host, and three metrics per directed TCP edge
(`count`, `latency_p95`, and `failure_rate`). TCP edge alert state is
independent from its endpoint services. Formal service and host roots also have
a label-blind, Healthy-calibrated configured-root change channel. It uses a
strictly-prior 30-window rolling median, freezes per-coordinate change
thresholds from CALIBRATING data, and applies a two-window transient prefilter
before the common entity state machine. Metric names and all thresholds come
from frozen configuration; entity or fault-name exceptions are forbidden.
Thresholds are keyed by the complete `(entity, metric)` coordinate; a noisy
service cannot raise the threshold for another service carrying the same
metric name.
Formal Lock and LocalNet ratios participate only after their existing validity
and exposure gates pass. Sparse or missing event windows are not converted
into zeros or promoted into this channel. Soft remains score `>=3` for three
consecutive one-second windows. Score `>=5` for two consecutive one-second
windows is a **Hard Candidate** diagnostic only. A **Confirmed Hard** requires
score `>=5` for three consecutive one-second windows; only Confirmed Hard enters
the Hard state, starts formal RCA, or fails Healthy Validation. A single formal
service, host, or TCP edge may independently produce either state;
multi-entity corroboration is not required.

The directed TCP edge path remains:

```text
independent edge alert
  -> candidate scope
  -> healthy A_v cross-metric propagation subtraction
  -> TCP edge root residual
  -> TCP Burst group-penalty adjustment
  -> one non-negative Sparse-Group FISTA solve
  -> (src_service -> dst_service, TCP)
```

DNS is no longer a formal root-cause category. It is excluded from
`required_candidate_scope`, Baseline and `A_v` Readiness denominators,
Soft/Hard Alert, root residual coordinates, theta/FISTA groups, formal Burst
evidence, the fault matrix, and paper evaluation.

DNS transaction attribution and aggregation code is retained only as
`experimental / optional / not evaluated in the formal paper scope`. It is
disabled by default and may run only through an explicit experimental
configuration. Enabling or disabling that experimental producer must not
change the formal `9/4/3` contract, formal Dataset ID inputs, or Readiness
denominator.

Legacy collection-contract v2/v3 DNS archives remain readable for Replay.
Their original archive and contract fingerprints remain provenance. DNS
coordinates and DNS Burst records from those archives are marked
`excluded_from_formal_rca` and cannot enter calibration, alerting,
propagation, residual construction, penalty adjustment, FISTA, or ranking.

## Formal TCP Projection and Series Lifecycle Decision

The live data plane freezes the exact directed TCP edge set before collection.
Raw exporters and Prometheus may continue to observe additional dynamic TCP
series for diagnostics, but the formal `9/4/3` archive projects them out before
aggregation. An out-of-scope edge cannot alter formal topology identity or
block a formal window; a missing frozen edge still fails closed.

## Formal TCP Attempt and Pre-transaction Failure Decision

Beyla observes completed application transactions, but a TCP connection that
closes in `SYN_SENT` or `SYN_RECV` has no completed transaction and therefore
cannot be represented by Beyla alone. The formal Normal path consequently
combines two independent, label-free cumulative sources before the `9/4/3`
window aggregation:

- completed application request, error, timeout, and latency-histogram
  counters from Beyla;
- an always-on BPF cumulative pre-connection-failure counter keyed by source
  cgroup and destination address/port, joined to the frozen service identity.

The formal edge request count is completed transactions plus pre-connection
failures. The failure numerator is completed errors/timeouts plus those
pre-connection failures. Latency P95 remains defined only over completed
application transactions; the exporter therefore exposes an internal latency
observation count solely to validate the histogram. This internal component is
not a fourth formal edge metric, root coordinate, alert coordinate, or FISTA
variable. Burst events are not read to construct any Normal metric.

The BPF counter is unsampled and cumulative. It stores no incident label and no
per-event Normal archive. An unmappable cgroup or destination remains outside
the formal aggregation rather than being guessed, and frozen-edge projection
continues to decide which service pairs enter the archive.

When an in-scope cumulative counter or histogram series exists at only one of
the two exact window boundaries, its delta is unknowable. The affected metric
is retained as `value=null`, `valid=false`, with
`invalid_reason=series_lifecycle_transition`; neither zero nor a previous value
is imputed. The next window recovers only after both boundaries are present.
Negative deltas remain hard lifecycle violations.

## Healthy Load Qualification and Freeze Decision

The single-VM healthy workload is not frozen before it has been qualified. Three
configuration-defined profiles preserving the same traffic composition are run
independently for 300 healthy one-second windows at approximately 25%, 40%, and
55% of the previous aggregate load. All profiles are evaluated even when a lower
profile fails. Qualification uses the production Baseline and per-target `A_v`
Readiness implementations: each coordinate's observed valid-sample or valid-row
rate is projected from 300 to 600 calibration windows and compared with twice
that coordinate's exact minimum. There is no flat global coverage count.

A profile fails on any Confirmed Hard episode, topology/runtime change, Pod
restart, or projected required coordinate that cannot meet its formal minimum.
Soft and Hard Candidate episodes are diagnostic. From all passing profiles, the
highest load is selected and its complete parameters and fingerprint are frozen.
The selected single-VM profile is `single-vm-qualified-55`, fingerprint
`d3f0bffc6c26cb4d3f837d66eb62b5e377bae78b97522c5cefa75f3f0c2ee848`.

The frozen profile preserves request paths and phase-distributes independent
clients. Delayed calls do not replay missed deadlines as a catch-up burst. The
same fingerprint is required for the final Healthy Pilot and subsequent formal
single-VM fault experiments. Any future workload change creates a new profile
fingerprint and requires a new independent Healthy Pilot; it does not authorize
changes to RCA mathematics, Soft/Hard scores, or P95 validity rules.

## Stable Runtime Identity Decision

Runtime identity is defined by semantic identities: formal service association,
owner UID, Pod UID, full container ID, image ID, and node placement. Kubernetes
`resourceVersion`, object `generation`, observation time, and serialization order
are excluded because status-only controller updates can change them without
changing the running workload. Runtime changes during Calibration, Healthy
Validation, a Normal/abnormal phase, or the interval between READY and a fault
trial fail closed. A container replacement changes cgroup-local counter epochs
and may change the healthy distribution even when the Pod UID is unchanged;
therefore a same-Pod runtime rebind cannot reuse frozen Baseline, `A_s`, or
`A_v`. The live runtime fingerprint must exactly match the Healthy calibration
fingerprint. Any runtime, Pod UID, owner, image, node, service association,
topology, or frozen configuration change requires a fresh Healthy Pilot.

## Local-socket exposure and frozen-model decision

`local_socket_failure_rate` is defined only over meaningful completed socket
operations. Routine non-blocking `accept` outcomes (`EAGAIN`/`EWOULDBLOCK`) and
interrupted/restartable accepts are neither failures nor exposure. Its archived
`sample_count` is the real socket-operation denominator, not the number of raw
component series. A ratio with at least one meaningful operation is a valid
healthy-model observation and may enter Baseline and `A_v`. It is eligible as
current-window root evidence in residual construction and FISTA only when
`failure_min_requests` is satisfied. This separates sparse modeling coverage
from incident-time attribution reliability without filling or rewriting data.

After the independent Healthy Validation reaches READY, the formal Baseline,
`A_s`, and `A_v` snapshot is immutable. Additional healthy raw windows may be
archived for diagnostics, but cannot update the model used by formal fault
experiments. A workload, scope, contract, topology, runtime-identity, or model
configuration fingerprint change requires a new Healthy Pilot. There is no
same-Pod runtime-rebind exception for a frozen formal model.

## Stable service-memory intervention decision

The formal service-memory intervention must create sustained reclaim pressure
without restarting the target container. For the single-VM profile it touches
192 MiB in `recommendationservice` and sets `memory.high` to 224 MiB. The former
256/256 MiB profile caused three consecutive liveness timeouts and a container
restart and is invalid as an RCA trial. A 30-second qualification of the new
profile produced 718 `memory.events high`, zero OOM/oom_kill, no not-Ready
sample, and unchanged Pod UID, container ID, and restart count. A restart
invalidates the trial; it is never treated as a successful memory fault.

## Bounded Burst runtime-log decision

The always-on Burst loader writes a transient runtime transport log, not a
research archive. Each loader start creates a fresh log epoch instead of
appending across deployments, and the formal service enforces a 4 GiB hard
limit. Reaching that limit terminates the loader so systemd starts a new epoch;
an overlapping collection detects the truncation and fails closed rather than
losing or fabricating a target window.

After exact boundaries have been captured, the live Burst reader consumes the
log sequentially only through the checkpoint needed by the current one-second
window. It retains at most the configured event-record bound plus the latest
checkpoint/look-ahead state, then discards already aggregated events. It must
not load the entire Healthy interval into Python objects. This changes only
runtime retention and transport; the sealed Burst window schema, channels,
sampling profile, event-loss calculation, and RCA evidence semantics remain
unchanged.

## Formal instrumentation-epoch decision

Beyla's process attachment is runtime identity scoped. A container may remain
Ready and serve real RPCs after a restart while an older long-lived Beyla
instance exposes only discovery metadata or probe traffic for the replacement
process. Treating that partial source as a valid zero would corrupt the formal
service baseline.

The formal installer therefore creates one deterministic instrumentation epoch
before a Healthy Pilot or fault campaign: restart the Beyla DaemonSet, restart
the 11 formal workload Deployments in the frozen dependency order while Beyla
is active, and wait for each dependency before restarting its callers. After
the services are stable, restart the frozen load-generator Deployments so their
long-lived HTTP/gRPC channels cannot retain pre-epoch connections. The
primitive exporter starts last and must pass complete service plus 15-edge
coverage. Load generators remain traffic sources and are never formal RCA
entities; experimental DNS workloads are not started. A later application
runtime-identity change still
invalidates an active collection; the installer rule is preparation, not an
online repair that hides changes during an experiment.

## Continuous fault-phase capture decision

A formal fault trial uses one uninterrupted exact one-second acquisition axis
for its pre-fault and fault windows. The data-plane collector emits an atomic
marker immediately after the configured pre-fault boundary is captured; only
then may the external orchestrator activate the fault. Acquisition continues
without waiting for Prometheus range queries, aggregation, archive sealing, or
signal qualification. The final marker deactivates the fault after the last
required boundary has been captured.

After the combined Normal/Burst capture is sealed and verified, the data-only
orchestrator creates two immutable contiguous archive slices with new opaque
Dataset IDs and sequences rebased to start at one. The slice boundary must be
exactly contiguous in wall time, and Normal/Burst timestamps must remain equal.
Fault labels remain only in the external experiment manifest and never enter
metric records, Burst evidence, alerting, propagation, or FISTA. A trial with a
time gap, overlapping phases, a missing lifecycle marker, or activation before
the exact boundary fails closed. Separate collection commands for the two
phases are not valid for formal RCA because their query/seal interval creates
an unobserved gap and falsely makes non-adjacent samples appear adjacent to
lagged models.

## One-time multi-node evidence-acquisition decision

The rented four-node campaign is an evidence acquisition run, not an online
accuracy gate. Load qualification and each episode are accepted only by
objective data-plane integrity, capacity, coverage, direct intervention evidence,
identity stability, cleanup, and archive SHA checks. Current Soft, Hard, READY,
`A_s`/`A_v`, FISTA rank, and RCA correctness are recorded but cannot stop the
campaign. This exception does not relax READY for ordinary online RCA operation
or bounded algorithm smoke tests; it keeps collection and later analysis separate.

## Terminal TCP-failure effectiveness decision

The formal TCP-failure injector remains a continuous, directional, port-scoped
TCP RST intervention. A gRPC client may stop issuing connection attempts after
the first failures because of protocol backoff. Those later windows retain the
data-plane meaning `value=null`, `valid=false`, `invalid_reason=no_exposure`;
they are never rewritten as failures or zeroes.

Injector effectiveness is therefore evaluated once per episode: after a fixed
two-second in-flight drain grace, the next 15 seconds must contain at least
three attempts in three positive failure windows and a cumulative failure ratio
of at least 0.5. The full active phase must contain at least five direct
error/timeout increments and RST filter hits. During
the remainder of the active phase, a durable five-second behavior-intent ledger
must prove continuing demand for the target edge. After exact cleanup, at least
30 consecutive exposed windows must return to the empirical Healthy-Pre failure
range. Missing demand evidence produces
`NOT_EVALUABLE_INSUFFICIENT_DEMAND`, not a fabricated PASS. A successful episode
is `PASS_TERMINAL_FAILURE_WITH_EXPECTED_BACKOFF`.

The frozen configuration is `configs/final_multinode_campaign.yaml`. With a
supported Host NIC intervention it generates 3 Healthy datasets, 12 injector
Pilots, 111 formal fault episodes, and 9 controls: 135 core datasets. The 37
concrete coordinates each have three independent repeats. A fixed seed randomly
assigns one repeat to Validation and two to Test and creates a non-adjacent fixed
order. If and only if Host NIC is proven unsupported before the formal manifest
is generated, that coordinate and its Pilot are removed, producing 131 core
datasets; TCP loss is never relabeled as Host NIC.

The multi-node formal scope is 11 services, 3 workers, and 15 directed TCP edges.
It therefore has 108 theoretical root coordinates and 156 formal records per
one-second window under the unchanged `9/4/3` contract. Four overhead runs are
outside the 135/131 core count. Synthetic data are generated later, locally, and
are not substitutes for these real episodes.

Test fault and control episodes use opaque IDs. Their target, mechanism, repeat,
injector/effectiveness metadata, and labels are absent from the public manifest.
The private manifest is streamed directly from memory into OpenSSL CMS public-key
encryption; the private key remains off the rented cluster. Public commitments
use HMAC-SHA256 with an off-cluster secret. Code and configuration are frozen,
all Test predictions are sealed once, and only then may labels be decrypted and
scored.

Phase labels use actual lifecycle timestamps, not the plan alone. Windows fully
inside effect-confirmed through cleanup-start are `FAULT_ACTIVE`; application and
cleanup boundary windows are transitions and are excluded from primary accuracy.
Campaign resume is bound to the public manifest fingerprint and immutable
original order. A failed attempt is never accepted or counted complete. When an
independent safety report proves exact injector cleanup, unchanged Pod UIDs and
container restart counts, and all business Pods Ready, the controller may mark
that attempt `DEFERRED`, continue the remaining original-order cases, and retry
all deferred cases in original order at the tail under the same case ID and a
new attempt number. If restoration cannot be proved, collection stops. Deferred
cases remain mandatory for the final real `135/135`; three failures of the same
case still stop the campaign for operator review.

The four-node implementation uses Worker-local primitive exporter, Beyla,
eBPF/Burst, and node exporter processes. S0 Prometheus scrapes all three Workers;
the Workers start from one future epoch boundary and their archives are merged
only after all three seal successfully. In addition to merged Normal/Burst, each
dataset preserves per-Worker cumulative primitive boundaries and filtered raw
eBPF event/checkpoint streams. This is the minimum evidence needed to change
offline aggregation after the rented machines are released; it does not change
the formal 9/4/3 feature contract.

The pre-rental and physical gates are intentionally distinct. A local/VM report
may set `pre_rent_code_ready=true` after tests, deterministic generation, CMS,
resume, fake-target, upload/readback, and restore rehearsal. It may never claim
cloud NIC feasibility or real Pilot effectiveness. Only the rented environment
can freeze the highest objectively qualified 25/40/55 load profile, run all 12
real direct-evidence Pilots, verify exact image/placement and node capacity, and
produce `formal_campaign_go=true`.

TCP latency injection is implemented as a selective prio+netem child qdisc in
the caller Pod network namespace, with a destination IP/TCP-port flower filter,
pure delay, and exact qdisc cleanup. TCP failure is a separate terminal-failure
mechanism: a dedicated caller-Pod-netns OUTPUT chain matches the destination
Service IP/TCP port and executes `REJECT --reject-with tcp-reset`, with rule-hit
evidence and exact chain removal. Random netem loss was rejected because TCP
retransmission converted it into latency without activating the formal failure
coordinate. The old `tc action netem` construction is invalid and is not part of
the formal implementation. Host NIC netem is allowed only when the Pilot proves
the host interface qdisc is safely replaceable and the formal NIC drop/error
coordinate rises; otherwise Host NIC is removed before manifest generation
without TCP substitution.

## Incident-local resource residual decision

Frozen Healthy Baseline and `A_v` remain unchanged after READY. Service and
host resource alerts already compare the current standardized observation with
a strictly prior rolling level so that a long-lived, pre-incident level shift
does not itself seed a new incident. Root selection uses the same label-blind
change semantics for those configured service/host resource coordinates: after
subtracting frozen cross-metric `A_v`, subtract the median residual from the
configured rolling window ending before the complete Soft run. The Soft windows
themselves are never part of that reference.

This is not baseline refitting, clipping, zero fill, or label use. The frozen
Baseline, `A_s`, `A_v`, family scales, thresholds, raw archive, and Burst
evidence remain unchanged. Directed TCP edge residuals and all non-resource
coordinates keep the original frozen cross-metric residual exactly. If fewer
than the frozen baseline minimum number of valid prior samples exist, the
offset is not applied. Every applied offset and its sample count is recorded in
the RCA model metadata.

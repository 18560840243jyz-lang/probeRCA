# A10 Final Blind Audit

## Executive Summary

- A10_final_passed: true
- implementation_category: stable-only probeRCA modular prototype
- architecture_completeness: partial online pipeline / label-safe module previews
- final_label_leakage_risk_new_pipeline: LOW
- blind_rca_status: PASS for A2 blind evidence rerun; A3-A9 are label-safe previews but not integrated final RCA pipeline
- production_readiness: NOT_PRODUCTION_READY

## What Changed Since A0

A0 found HIGH leakage risk in legacy target-aware P2 evidence. A1-A9 added blind evidence, alert gate, candidate subgraph, adaptive probe policy, true IPW-masked RLS preview, evidence channel calibration, graph sparse inversion, and counterfactual explanation. The system is now a stable-only modular prototype with label-safe previews, not a production full probeRCA system.

## Official Blind Result

Official source: **A2 Blind P2 Rerun**.

- total_repeats: 20
- service_hit_at_1_overall: 0.9
- metric_hit_at_3_overall: 1.0
- root_type_accuracy_overall: 0.9
- path_fidelity_overall: 1.0
- auxiliary metric_hit_at_1: 0.7
- auxiliary metric_mrr: 0.8416666666666666

Per-fault type is recorded in `final_blind_audit_summary.json`.

## Debug-only Module Preview

A8R debug: service Hit@1 = 0.95, metric Hit@3 = 0.75, root type = 0.75.

A9 debug: service Hit@1 = 0.95, metric Hit@3 = 0.75, root type = 0.5.

These are debug-only module preview metrics and cannot be used as formal P2E acceptance.

## Label Leakage Audit

A10 scan result:

- final_label_leakage_risk_new_pipeline: LOW
- suspicious_count: 0
- allowed_count: 205
- known_window_dependency_count: 10

Suspicious locations: []

Known window dependency locations are documented in `docs/audits/A10_FINAL_LABEL_LEAKAGE_SCAN.txt`; they describe A1/A2 window-aware limitations and are not used by A3-A9 preview ranking.

## Architecture Completeness

Implemented as preview modules:

- alert gate
- candidate subgraph
- adaptive probe policy
- IPW-masked RLS
- evidence channel
- graph sparse inversion
- counterfactual explanation

Still not complete:

- A3-A9 integration into final RCA schema
- real eBPF activation
- Prometheus / ClickHouse / OTel / Alertmanager production stack
- multi-node Kubernetes validation
- propagation drift (stable-only by design)
- production UI

## Claims Allowed

- 真实故障注入和真实指标采集已完成。
- A2 在 blind evidence protocol 下完成 20 次已有真实 raw metrics 的 RCA rerun。
- A2 official blind result: service Hit@1 0.9, metric Hit@3 1.0, root type 0.9, path fidelity 1.0。
- A3-A9 已实现 label-safe module previews。
- Stable propagation only，未实现 propagation drift。

## Claims Forbidden

- 不能说完整生产级 probeRCA 已验证。
- 不能说完全端到端 alert-to-RCA integrated pipeline 已完成。
- 不能说真实 eBPF/libbpf/CO-RE 已落地。
- 不能说多机 Kubernetes 已验证。
- 不能说 propagation drift 已实现。
- 不能把 legacy target-aware P2E 100% 当成 blind RCA。
- 不能把 A8R/A9 debug metrics 当正式 P2E acceptance。
- 不能说 CPU metric-level counterfactual 已解决。

## Remaining Risks

- A2 I/O service/root type 只有 0.6。
- A8R/A9 CPU metric Hit@3 debug 仍为 0.0。
- A9 root type debug 下降到 0.5。
- A3-A9 尚未集成为最终 online RCA schema。
- A5 是 policy preview，不是真实 probe activation。
- A6 使用 expected observation mask，不是真实随机采样流。
- A10 不代表 production-ready。
- A1/A2 official blind rerun 仍是 window-aware：使用已有 incident windows；A3 只作为模块 preview，尚未接入最终 RCA。

## A10 Review Verdict

- A10_final_passed: true
- failed_checks: []
- final_label_leakage_risk_new_pipeline: LOW
- official_blind_result_source: A2 Blind P2 Rerun
- production_readiness: NOT_PRODUCTION_READY
- remaining_risks: see above
- next_work_after_a10: ['Integrate A3 alert windows, A4 candidates, A5 probe policy, A6 RLS, A7 calibrated residuals, A8R sparse inversion, and A9 counterfactual explanations into one end-to-end blind RCA schema.', 'Run an A11/A-final integrated rerun without incident start/end as RCA input.', 'Replace expected probe masks with real probe activation or stream replay.', 'Add production observability storage only after the single-VM module chain is stable.']

## Final Verdict

A10 passes as a final blind audit if the structural checks pass and no suspicious label leakage appears in the A3-A9 new module pipeline. This result does not make the system production-ready and does not convert debug preview metrics into formal P2E acceptance.

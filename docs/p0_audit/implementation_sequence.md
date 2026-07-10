# P0-Based Implementation Sequence

This plan maps the final ProbeRCA-BPF Edge Shock scheme onto the current repository. It does not start P1 and does not authorize implementation without review.

## P1 Data Contract and Indexes

- Current reusable modules: `proberca/data/io.py`, `proberca/data/schema.py`, node-id helpers in `service_metric_identity.py`, candidate node index logic in `ipw_rls_online.py:124`.
- Modify: `proberca/data/schema.py`, data IO helpers, tests under `tests/test_schema.py`.
- Add: schemas for `node_metrics`, `edge_metrics`, `burst_events`, `topology_snapshots`, `alerts`, `incident_labels`, stable node/edge/shock indexes.
- Dependencies: none.
- Required tests: schema roundtrip, edge metrics cannot be encoded as node `net`, labels forbidden in inference input, stable index determinism.
- Acceptance criteria: final dataset contract exists and all current stage readers either adapt or are explicitly legacy.
- Known blockers: root not a Git repo.
- Rollback boundary: schema additions and adapters only; no solver changes.

## P2 Aggregation, Baseline, Alert FSM

- Current reusable modules: `features/robust.py`, `adapters/online_boutique/alert_gate.py`.
- Modify: alert/baseline modules and tests.
- Add: online health buffer, Soft/Hard/Recovery FSM, freeze flags.
- Dependencies: P1 schema.
- Required tests: health-only baseline update, incident freeze, soft does not output RCA, recovery hysteresis.
- Acceptance criteria: explicit state machine drives candidate preparation and hard freeze.
- Known blockers: existing alert gate is event/window oriented, not lifecycle oriented.
- Rollback boundary: keep legacy alert gate as adapter until migrated.

## P3 Topology and Candidate Graph

- Current reusable modules: `candidate_subgraph.py`, `topology.py`, `structured_multilag.build_structured_parent_sets`.
- Modify: topology parsing/candidate graph modules.
- Add: live topology snapshot reader, `E_call/E_imp/E_host/E_resource`, candidate `S_c/V_c/G_c` builder.
- Dependencies: P1/P2.
- Required tests: upstream/downstream hops, cohost/shared-resource inclusion, no full connection.
- Acceptance criteria: candidate graph is derived from topology snapshots, not hardcoded Online Boutique names.
- Known blockers: current topology is static adapter output.
- Rollback boundary: adapter-specific static graphs remain fixtures only.

## P4 Service-Level Propagation

- Current reusable modules: `propagation/ipw_rls_online.py` RLS core.
- Modify: propagation package.
- Add: full-system service-level `A_s^(l)` RLS with structural parent mask, snapshots, restore, freeze.
- Dependencies: P2/P3.
- Required tests: allowed parent mask, RLS update only under health gate, hard freeze snapshot restore.
- Acceptance criteria: `A_s` is service-level and separated from metric `A_v`.
- Known blockers: current RLS is candidate-node oriented.
- Rollback boundary: no change to inference until P6.

## P5 Metric-Level Propagation

- Current reusable modules: `structured_multilag.py` parent-set and ridge fitting.
- Modify: metric propagation module.
- Add: candidate-only `A_v^(l)`, parent metric type constraints, shock metric exclusion from normal propagation.
- Dependencies: P3/P4.
- Required tests: candidate-only fitting, masked parent types, no shock swallowing, hard freeze.
- Acceptance criteria: produces node prediction `z_hat_v` for P6.
- Known blockers: current code mixes preview and final semantics.
- Rollback boundary: output snapshot contract compatible with P6.

## P6 Residuals and Dictionaries

- Current reusable modules: residual exports from `ipw_rls_online.py`; sparse matrix style can be added new.
- Modify: new final RCA input builder; do not reuse evidence-adjusted residual as final residual.
- Add: `r_v`, `r_e`, `r_tilde`, `U`, `X_prop`, `X_shock`, node/edge/shock indexes.
- Dependencies: P1/P5.
- Required tests: exact matrix shapes/values, parent-anomaly-zero shock detection, edge residual preservation.
- Acceptance criteria: final `RCAInput` can be built without labels or evidence subtraction.
- Known blockers: no current edge residual source.
- Rollback boundary: keep legacy graph sparse path intact until P8 replacement is validated.

## P7 Evidence, Quality, Competition Penalties

- Current reusable modules: `evidence/evidence_channel.py`, propagation support helpers.
- Modify: evidence channel to produce `h_v`, `h_prop`, `h_shock`, `W` without residual subtraction.
- Add: effective penalty calculators for `lambda_u_eff`, `lambda_delta_eff`, `lambda_xi_eff`.
- Dependencies: P6.
- Required tests: monotonic penalty behavior, low observation quality never increases confidence, shock evidence penalizes conflicting node/prop explanations.
- Acceptance criteria: evidence only affects penalties/ranking, not residual pre-subtraction.
- Known blockers: no burst/shock evidence input until P12; use contract fixtures first.
- Rollback boundary: legacy evidence calibration remains marked legacy.

## P8 Weighted Sparse-Group FISTA

- Current reusable modules: `soft_threshold`/`group_shrink` concepts from `sparse.py` and `graph_sparse_inversion.py`.
- Modify/add: final solver module under `proberca/inference/`.
- Add: weighted sparse-group FISTA over `u/delta/xi`, warm start, objective/KKT diagnostics.
- Dependencies: P6/P7.
- Required tests: prox unit tests, convergence on known convex cases, no fallback to Lasso/score ranking.
- Acceptance criteria: solver returns nonzero `u`, `delta`, `xi` with objective trace and convergence status.
- Known blockers: current ADMM graph sparse solver is not mathematically equivalent.
- Rollback boundary: legacy solver still available for historical replay only.

## P9 Ranking, Mode, Counterfactual, Path, Report

- Current reusable modules: `integrated_pipeline.py`, `counterfactual_explanation.py`, path explainers.
- Modify: final report builder and final counterfactual over `u/delta/xi` objective.
- Add: mode/subtype classification, unified candidate ranking, confidence, identifiability, ambiguous handling, final JSON schema.
- Dependencies: P8.
- Required tests: node self, propagated-edge, exogenous-edge-shock, ambiguous, symptoms not primary root.
- Acceptance criteria: final report contains required fields and no propagated symptom as `primary_root`.
- Known blockers: current report is service/metric-only.
- Rollback boundary: legacy P2 reports remain in legacy namespace.

## P10 Offline Replay Closed Loop

- Current reusable modules: `integrated_replay.py`, eval metrics.
- Modify: replay runner to use final core `RCAInput`/solver/report.
- Add: replay manifest validation and online/replay equivalence.
- Dependencies: P9.
- Required tests: same data yields same report online/replay; labels only used in evaluator.
- Acceptance criteria: no parallel easier algorithm path for experiments.
- Known blockers: historical data may lack edge/burst fields.
- Rollback boundary: old datasets explicitly marked legacy/not final.

## P11 K8s and Always-On Collection

- Current reusable modules: `online_boutique/metrics.py` as temporary adapter reference.
- Modify/add: collector/adapter modules for Prometheus/K8s metadata and low-cost flow summaries.
- Dependencies: P1/P3.
- Required tests: cgroup/pod/service mapping, edge metrics emitted separately, coverage/event-loss fields populated.
- Acceptance criteria: 1s node/edge metrics produced without hardcoded service names.
- Known blockers: no eBPF agent yet; current env has no effective caps.
- Rollback boundary: collector can run in dry-readonly mode.

## P12 eBPF Burst

- Current reusable modules: none for eBPF; only adapter scripts.
- Add: eBPF source, loader, burst controller, event parser, event-loss accounting.
- Dependencies: P11/P7.
- Required tests: COMPILE_PASS, VERIFIER_PASS, ATTACH_PASS, EVENT_RECEIVE_PASS, DETACH_PASS separately.
- Acceptance criteria: candidate-scoped 30s burst events map to service/edge and feed `h_v/h_shock`.
- Known blockers: bpftool missing, shell lacks caps; test runner must be privileged.
- Rollback boundary: burst failure must report blocked/failure, never success with empty data.

## P13 Single-VM Real Experiment

- Current reusable modules: Online Boutique scripts and adapters, historical P2 fault modules.
- Modify: experiment runner to use final replay/online core and final schemas.
- Dependencies: P12.
- Required tests: 8 fault classes x approximately 10 runs, labels separate, metrics computed.
- Acceptance criteria: engineering gates measured, not assumed.
- Known blockers: current scripts include real injection and must be reviewed before execution.
- Rollback boundary: no tuning on final evaluation set after freeze.

## P14 Parameter Freeze

- Current reusable modules: `check_p0_freeze`, `check_p1_freeze` patterns.
- Add: final config freeze manifest with hashes.
- Dependencies: P13.
- Required tests: frozen config cannot drift; shock templates frozen.
- Acceptance criteria: thresholds/lambdas/templates recorded before four-server eval.
- Known blockers: no final parameters until P13 completes.
- Rollback boundary: freeze creates docs/artifacts only.

## P15 Four-Server Evaluation

- Current reusable modules: none complete; Online Boutique external repo may be reused.
- Add: multi-VM deployment/evaluation orchestration.
- Dependencies: P14.
- Required tests: real cross-node TCP/DNS shock, DaemonSet agent, overhead, event loss.
- Acceptance criteria: uses frozen version only; no repeated tuning on real test set.
- Known blockers: infrastructure and privileged eBPF capabilities.
- Rollback boundary: isolated evaluation outputs.

## P16 Paper Experiments and Artifact

- Current reusable modules: eval metrics and summaries.
- Add: reproducible artifact manifest, final tables, replay scripts.
- Dependencies: P15.
- Required tests: artifact replay from manifest, no label leakage, result provenance complete.
- Acceptance criteria: paper claims match logs, configs, seeds, commit/version, and final code path.
- Known blockers: root repo currently lacks Git metadata.
- Rollback boundary: docs/results only.

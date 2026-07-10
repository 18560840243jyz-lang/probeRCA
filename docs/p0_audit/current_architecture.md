# P0 Current Architecture

Audit timestamp: `20260710T055900Z`

## Executive Summary

Current ProbeRCA is a Python research scaffold centered on synthetic/P1 JSONL experiments and Online Boutique preview/replay adapters. It has real tests and substantial code, but it does not implement the final Edge Shock model:

```text
r_tilde = U u + X_prop delta + X_shock xi + eps
```

The current main RCA path is node/service-metric residual generation, evidence calibration, graph sparse ADMM over node variables, counterfactual re-optimization over forbidden nodes, and final integrated Online Boutique result assembly. It is not a weighted sparse-group FISTA over `u/delta/xi`, and it has no `X_shock` dictionary or edge residual vector.

## Main Entrypoints

No console scripts are declared. Execution is via `python3 -m proberca.cli.<module>`.

Primary tested/integrated paths:

- Synthetic/P1: `run_p1a_observation`, `run_p1b_ipw_propagation`, `run_p1c_sparse_inversion`, `run_p1d_semantic_evidence`, `run_p1e_path_explanation`, `run_p1f_result`.
- Online Boutique P2 preview/replay: `run_p2_alert_preview`, `run_p2_candidate_preview`, `run_p2_ipw_rls_preview`, `run_p2_evidence_channel_preview`, `run_p2_graph_sparse_preview`, `run_p2_counterfactual_preview`, `run_p2_integrated_replay`.
- Integrated Online Boutique: `proberca/adapters/online_boutique/integrated_pipeline.py:889` `run_integrated_blind_rca`.

Evidence: `artifacts/p0_audit/logs/code_structure_20260710T055900Z.log`.

## Data Flow

### Current Data Records

- `MetricRecord`: `proberca/data/schema.py:10` with fields `timestamp`, `service`, `instance`, `metric`, `value`, `incident_id`, `source`.
- `EvidenceRecord`: `proberca/data/schema.py:24` with `incident_id`, `service`, `evidence_type`, `value`, `source`, optional `target_service`.
- `IncidentRecord`: `proberca/data/schema.py:41` with `root_service`, `root_metric`, `start_ts`, `end_ts`.
- `RCAResult`: `proberca/data/schema.py:55` with root service/metric, score, rank, and path.

Assessment: current schema is node-centric and incident-centric. There is no first-class `edge_metrics` schema with `src_service`, `dst_service`, `edge_metric`, timestamp, value, coverage, and event loss. `EvidenceRecord.target_service` is not equivalent to a time-aligned edge metric series.

### Online Boutique Metrics

`proberca/adapters/online_boutique/metrics.py` collects Kubernetes/container metrics through external commands:

- `run_cmd`: line 24, wraps subprocess command execution.
- `_infer_service_from_pod_name`: line 41, infers service from pod names.
- `get_pods`: line 49, calls `kubectl`.
- `collect_window_metrics`: line 445, collects a metrics window.

Assessment: this is adapter-level collection, not final always-on eBPF/Prometheus/K8s collection with 1s node/edge contract.

## Topology Flow

`proberca/adapters/online_boutique/topology.py:39` writes a fixed Online Boutique service graph. Candidate graph logic exists under `candidate_subgraph.py`, and structured propagation parses service graph edges in `proberca/propagation/structured_multilag.py:159`.

Assessment: topology is partially represented as service graph JSONL. It is not a live Kubernetes topology snapshot stream with pod/cgroup/socket-to-service mapping.

## Alert Flow

`proberca/adapters/online_boutique/alert_gate.py` implements alert event detection:

- `detect_alert_events`: line 153.
- `build_alert_windows`: line 243.
- `write_alert_outputs`: line 286.

It computes robust z-score style alert events and windows. It does not implement the final explicit Soft Alert / Hard Alert / Recovery state machine. It does not freeze `A_s`, `A_v`, or health baselines after Hard Alert because those concepts are not integrated as an online state machine.

Status by final scheme:

- Soft Alert: `PARTIAL` as alert-window preparation only.
- Hard Alert: `PARTIAL` as threshold/window events only.
- Recovery: `NOT_IMPLEMENTED`.
- Hard-after-freeze behavior: `NOT_IMPLEMENTED` in online sense.

## Propagation Learning

### IPW Online RLS Preview

`proberca/propagation/ipw_rls_online.py` contains a real online masked RLS preview:

- `RLSConfig`: line 36.
- `build_parent_sets`: line 198.
- `OnlineIPWMaskedRLS`: line 309.
- `predict`: line 353.
- `update`: line 361.
- `run`: line 409.
- `export_edges`, `export_residuals`, `export_predictions`: lines 427-437.

Assessment: this is a reusable partial component for masked RLS/residual generation, but it is candidate-node oriented and not the final full-system `A_s` + candidate `A_v` split with online freeze semantics.

### Structured Multilag Ridge

`proberca/propagation/structured_multilag.py` implements structured parent sets and multilag ridge:

- `StructuredPropagationConfig`: line 28.
- `build_structured_parent_sets`: line 216.
- `fit_multilag_ridge_for_target`: line 264.
- `fit_structured_multilag_propagation`: line 338.
- `compute_service_to_symptom_propagation_support`: line 416.

Assessment: this is useful for P5-style candidate metric propagation, but it is not the final online service-level `A_s` RLS plus candidate metric-level `A_v` with frozen model snapshots. It also does not separate edge-shock metrics from ordinary metric propagation at final-scheme rigor.

## Residual Flow

Current residuals are node/service-metric residual records, mainly from IPW RLS and evidence calibration:

- IPW RLS residual export: `proberca/propagation/ipw_rls_online.py:434` `export_residuals`.
- Evidence channel consumes `ipw_rls_residuals.jsonl`: `proberca/evidence/evidence_channel.py:244`.
- Evidence channel creates `raw_adjusted_residual`: `proberca/evidence/evidence_channel.py:375-379`.
- Calibrated residuals are written by `build_evidence_channel`: `proberca/evidence/evidence_channel.py:475-508`.

Final-scheme violation: `evidence_channel.py` estimates an evidence effect and subtracts it from raw residual before sparse inversion. The final scheme explicitly says not to subtract eBPF root evidence from residuals before joint inversion.

Missing: first-class edge residual `r_e = z_e` and concatenated `r_tilde = concat(r_v, r_e)`.

## Inference and Solver

### Old Sparse Inversion

`proberca/inference/sparse.py` exposes:

- `SparseInversionConfig`: line 16.
- `soft_threshold`: line 58.
- `group_shrink`: line 68.
- `solve_sparse_inversion_for_incident`: line 133.
- `solve_sparse_inversion`: line 226.

This path aggregates residual statistics and ranks node interventions. It is not final convex joint sparse-group FISTA.

### IPW Sparse Inversion

`proberca/inference/ipw_sparse.py` exposes:

- `IPWSparseInversionConfig`: line 20.
- `compute_candidate_score`: line 129.
- `solve_ipw_sparse_inversion`: line 178.

This remains candidate scoring over node residuals/evidence, not `u/delta/xi` joint optimization.

### Graph Sparse ADMM

`proberca/inference/graph_sparse_inversion.py` implements graph sparse ADMM:

- `GraphSparseConfig`: line 26.
- `build_graph_incidence`: line 347.
- `soft_threshold`: line 359.
- `group_shrink`: line 363.
- `solve_graph_sparse_admm`: line 421.
- `compute_objective`: line 499.
- `build_sparse_rankings`: line 544.
- `run_graph_sparse_inversion`: line 573.

Assessment: this is a real optimizer, but it optimizes one node variable vector `x` with graph incidence/TV-like terms and service groups. It is not the final objective over `u`, `delta`, and `xi`; it does not construct `U`, `X_prop`, or `X_shock`; and it is ADMM, not weighted sparse-group FISTA.

## Evidence Flow

`proberca/evidence/evidence_channel.py` builds node evidence vectors:

- `compute_evidence_vector_for_node`: line 265.
- `estimate_evidence_effect`: line 330.
- `calibrate_residuals`: line 364.
- `build_evidence_channel`: line 418.

Assessment: evidence is node/family/service evidence. It does not expose final separate `h_v`, `h_prop`, `h_shock` channels or final `lambda_u_eff`, `lambda_delta_eff`, `lambda_xi_eff` competition. It may be reusable for P7 after redesign.

## Counterfactual Flow

`proberca/explain/counterfactual_explanation.py` performs real re-optimization:

- `load_reconstruction_inputs`: line 141.
- `solve_counterfactual_with_forbidden_nodes`: line 174.
- `compute_metric_counterfactuals`: line 237.
- `compute_service_counterfactuals`: line 272.
- `run_counterfactual_explanation`: line 318.

Assessment: counterfactual is real for current graph sparse node model, but not final `u/delta/xi` objective deletion over node, propagated edge, and shock candidates.

## Integrated Output Flow

`proberca/adapters/online_boutique/integrated_pipeline.py` assembles final results:

- `build_metric_candidate_table`: line 174.
- `select_primary_candidate`: line 332.
- `build_service_candidate_table`: line 478.
- `select_root_service`: line 599.
- `build_final_result_from_stages`: line 707.
- `run_integrated_blind_rca`: line 889.

`proberca/adapters/online_boutique/integrated_replay.py` evaluates replay outputs:

- `evaluate_integrated_result`: line 142.
- `run_single_integrated_replay`: line 235.
- `run_p2_integrated_replay`: line 301.

Assessment: output is a stage-assembled Online Boutique RCA result with service/metric candidates and evidence summaries. It is not the final unified report schema with `primary_root` supporting node, propagated-edge, exogenous-edge-shock, ambiguous, `root_edge`, `edge_subtype`, `counterfactual_delta_loss`, `identifiability`, runtime, and data quality.

## eBPF Path

No eBPF source or loader exists in the ProbeRCA root. Current Online Boutique metrics are collected via `kubectl`, `crictl`, kubelet summary/cAdvisor, and curl samples. eBPF compile/verifier/attach/event/detach are all `NOT_IMPLEMENTED_NO_BPF_SOURCE`.

Evidence: `artifacts/p0_audit/logs/other_language_ebpf_20260710T055900Z.log`, `artifacts/p0_audit/logs/ebpf_status_20260710T055900Z.log`.

## Online vs Replay Reuse

The most integrated current path is replay-oriented and stage-based. `run_integrated_blind_rca` reuses stage functions, and `run_p2_integrated_replay` evaluates result directories. There is no always-on online service that shares a core final `RCAInput` solver with offline replay. Status: `PARTIAL` for reuse of stage functions; `NOT_IMPLEMENTED` for final online/replay shared core.

## Historical Results

`data/` contains many prior JSON/JSONL outputs. Some have metadata and seed-like directories; many are preview/replay artifacts. P0 did not delete or regenerate them. They cannot by themselves prove final Edge Shock correctness because the current code has no `X_shock`/edge residual/eBPF burst implementation.

Evidence: `artifacts/p0_audit/logs/results_artifacts_20260710T055900Z.log`.

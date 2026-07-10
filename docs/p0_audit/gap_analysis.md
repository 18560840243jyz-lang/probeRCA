# P0 Gap Analysis Against Final Edge Shock ProbeRCA-BPF

Status enum used exactly as requested: `IMPLEMENTED_AND_VERIFIED`, `IMPLEMENTED_NOT_VERIFIED`, `PARTIAL`, `STUB_ONLY`, `TEST_ONLY`, `DEGRADED`, `NOT_IMPLEMENTED`, `BLOCKED`, `UNKNOWN`.

| Requirement | Current status | Evidence | Correctness assessment | Missing or incorrect parts | Severity | Recommended phase | Required tests |
|---|---|---|---|---|---|---|---|
| Data contract | PARTIAL | `proberca/data/schema.py:10,24,41,55` | JSONL dataclasses exist for metric/evidence/incident/result | No final `node_metrics`, `edge_metrics`, `burst_events`, `topology_snapshots`, config, labels contract | P0 | P1 | Schema validation for all six datasets |
| Node/edge metric separation | PARTIAL | `MetricRecord` line 10; no `EdgeMetricRecord`; `EvidenceRecord.target_service` line 24 | Node metrics exist; edge evidence exists only as evidence-like records | No service-pair edge metric time series | P0 | P1 | Unit tests rejecting edge metrics placed in service `net` node |
| Stable indexes | PARTIAL | `ipw_rls_online.build_candidate_node_index:124`; integrated stage tables | Some node id/index construction exists | No unified stable node/edge/shock indexes | P0 | P1 | Index roundtrip and replay determinism tests |
| Health baseline | PARTIAL | `alert_gate.robust_zscore_series:109`; `structured_multilag.robust_normalize_panel:128` | Robust baseline utilities exist | No online health buffer updated only under health gate | P0 | P2 | Baseline freeze/update state-machine tests |
| Soft Alert | PARTIAL | `alert_gate.detect_alert_events:153`, `build_alert_windows:243` | Alert windowing exists | No explicit Soft Alert state used to prepare `S_c/V_c/G_c` and burst config | P0 | P2 | Soft-only test must not output root |
| Hard Alert | PARTIAL | `alert_gate.detect_alert_events:153` | Threshold events exist | No final hard state machine with freeze and burst trigger | P0 | P2 | Hard transition freezes models and starts candidate burst |
| Recovery | NOT_IMPLEMENTED | No recovery state in alert symbols | Missing | No `A_t<1.5` for 30 windows recovery behavior | P1 | P2 | Recovery hysteresis tests |
| Hard-after-freeze | NOT_IMPLEMENTED | No integrated freeze in `ipw_rls_online` or `structured_multilag` | Missing | Hard alert does not freeze baseline/`A_s`/`A_v` in online core | P0 | P2/P4/P5 | Regression preventing updates after hard |
| Candidate subgraph | PARTIAL | `candidate_subgraph.py`; `structured_multilag.build_structured_parent_sets:216` | Candidate graph utilities exist | Not final `S_c = symptoms + ancestors + descendants + cohost + shared resource` over live topology | P1 | P3 | Graph hop/resource/cohost tests |
| Service-level RLS `A_s` | PARTIAL | `ipw_rls_online.OnlineIPWMaskedRLS:309` | RLS exists for candidate nodes | No full-system service-level `A_s` separated from metric `A_v` | P0 | P4 | Full service graph parent-mask RLS tests |
| Metric-level masked Ridge/RLS `A_v` | PARTIAL | `structured_multilag.fit_multilag_ridge_for_target:264`; `fit_structured_multilag_propagation:338` | Structured multilag fitting exists | Not integrated with Soft/Hard lifecycle; no explicit exclusion of shock metrics | P0 | P5 | Candidate-only masked parent tests including shock exclusion |
| Node residual `r_v` | PARTIAL | `ipw_rls_online.export_residuals:434`; `evidence_channel.calibrate_residuals:364` | Node residuals exist | Evidence channel adjusts residual before inversion | P0 | P6 | Verify `r_v=z_v-z_hat_v` and no evidence subtraction |
| Edge residual `r_e` | NOT_IMPLEMENTED | No `edge_residual` search hit; no edge metric schema | Missing | `r_e=z_e` absent | P0 | P6 | Edge residual vector construction tests |
| Joint residual `r_tilde` | NOT_IMPLEMENTED | No final `r_tilde`/concat implementation | Missing | Cannot run Edge Shock joint inversion | P0 | P6 | Shape/index tests for concat node+edge residuals |
| `U=[I;0]` | NOT_IMPLEMENTED | No final `U` dictionary; graph ADMM uses `x` | Missing | Node dictionary absent | P0 | P6 | Sparse matrix shape/value test |
| `X_prop` | NOT_IMPLEMENTED | No `X_prop` symbol | Missing | Propagated-edge dictionary absent | P0 | P6 | Parent lag column semantics tests |
| `X_shock` | NOT_IMPLEMENTED | No `X_shock`/`shock_dictionary` symbol | Missing | Edge Shock dictionary absent | P0 | P6 | Parent-anomaly-zero shock detection test |
| Edge Shock template | NOT_IMPLEMENTED | No shock metrics contract found | Missing | No TCP/DNS/sidecar/proxy shock mapping | P0 | P6/P7 | Template coverage tests for required metrics |
| `h_v` | PARTIAL | `evidence_channel.compute_evidence_vector_for_node:265` | Node evidence-like vectors exist | Not named/structured as final `h_v` | P1 | P7 | Evidence normalization tests |
| `h_prop` | PARTIAL | `structured_multilag.compute_service_to_symptom_propagation_support:416` | Propagation support exists | Not final per propagated-edge evidence vector | P1 | P7 | Learned edge support tests |
| `h_shock` | NOT_IMPLEMENTED | No shock evidence channel | Missing | No TCP/DNS/sidecar shock evidence | P0 | P7 | Burst-to-shock evidence tests |
| Observation quality `W` | PARTIAL | Evidence/channel metadata has counts; no final W | Quality metadata exists in pieces | No unified weight vector for joint residual | P1 | P7 | Low coverage must reduce confidence/increase penalty |
| Three effective penalties | NOT_IMPLEMENTED | No `lambda_u_eff`, `lambda_delta_eff`, `lambda_xi_eff` | Missing | No competition among node/prop/shock | P0 | P7 | Penalty monotonicity tests |
| Weighted sparse-group FISTA | NOT_IMPLEMENTED | `graph_sparse_inversion.solve_graph_sparse_admm:421`; no FISTA | Current solver is ADMM over node vector | Final FISTA objective over `u/delta/xi` absent | P0 | P8 | KKT/objective/prox convergence tests |
| `u/delta/xi` outputs | NOT_IMPLEMENTED | No `xi`; current `x`/candidate scores only | Missing | No three variable output | P0 | P8 | Nonzero group extraction tests |
| Fault mode | PARTIAL | Integrated pipeline root type utilities line 78/629 | Node/root-type classification exists | No final `self/edge/ambiguous` primary root semantics | P1 | P9 | Mode classification tests |
| Edge subtype | NOT_IMPLEMENTED | No `edge_subtype` output | Missing | No `propagated-edge` vs `exogenous-edge-shock` | P0 | P9 | Subtype F1 tests |
| Ranking | PARTIAL | `graph_sparse_inversion.build_sparse_rankings:544`; integrated candidate tables | Current node/service ranking exists | No unified node/prop-edge/shock ranking | P1 | P9 | Candidate union ranking tests |
| Confidence | PARTIAL | `integrated_pipeline._result_confidence:703`; `ipw_semantic` confidence fields | Confidence exists heuristically | Not final score ratio + evidence - missing formula | P1 | P9 | Confidence formula tests |
| Counterfactual | PARTIAL | `counterfactual_explanation.solve_counterfactual_with_forbidden_nodes:174` | Real reoptimization for node graph model | Not final deletion over node, propagated edge, shock variables | P1 | P9 | Delta-loss tests per object type |
| Path | PARTIAL | `integrated_pipeline.build_path_explanation:636`; `explain/ipw_path.py` | Path support exists | No edge-shock-origin path semantics | P1 | P9 | Path fidelity tests for shock edge |
| Identifiability | NOT_IMPLEMENTED | No final identifiability formula | Missing | No CF/path/coherence/lag entropy score | P1 | P9 | Ambiguity/coherence tests |
| Ambiguous output | PARTIAL | Some low-confidence fields exist | Partial | No final primary_root null/status ambiguous contract | P1 | P9 | Low-confidence no forced top1 test |
| Unified report schema | PARTIAL | `integrated_pipeline.build_final_result_from_stages:707` | Produces stage result | Missing final `primary_root`, `root_edge`, `ranked_candidates`, `runtime`, data quality schema | P1 | P9 | JSON schema golden tests |
| Online/replay shared core | PARTIAL | `integrated_pipeline.run_integrated_blind_rca:889`; `integrated_replay.run_p2_integrated_replay:301` | Stage functions are reused | No final common `RCAInput` online/replay solver core | P0 | P10 | Same input online/replay equivalence test |
| eBPF always-on | NOT_IMPLEMENTED | No BPF source; `metrics.py` uses kubectl/crictl/cAdvisor | Missing | No always-on eBPF agent | P0 | P11 | Compile/verifier/attach/event tests |
| eBPF burst | NOT_IMPLEMENTED | No BPF source/loader | Missing | No candidate-scoped burst hooks | P0 | P12 | Burst TTL and event mapping tests |
| K8s mapping | PARTIAL | `metrics.get_pods:49`; pod name inference line 41 | K8s pod/service adapter exists | No pid/cgroup/socket/5tuple mapping | P0 | P11/P12 | cgroup/socket-to-service mapping tests |
| Event loss | NOT_IMPLEMENTED | No ring buffer/event loss implementation | Missing | No event-loss quality channel | P0 | P12 | Ring buffer loss accounting tests |
| Single-machine experiment | PARTIAL | Many P1/P2 data dirs and tests | Synthetic/replay and some Online Boutique artifacts exist | Not final complete Edge Shock single VM experiment | P1 | P13 | 8 fault classes, labels separate, no leakage |
| Parameter freeze | PARTIAL | `check_p0_freeze`, `check_p1_freeze` exist | Freeze checks exist for old phases | Not final freeze of Edge Shock parameters before 4-server | P1 | P14 | Frozen config hash/version tests |
| Four-server evaluation | NOT_IMPLEMENTED | No 4-server deployment/eval path found | Missing | No frozen four-server evaluation | P1 | P15 | Multi-node CNI/TCP/DNS shock tests |

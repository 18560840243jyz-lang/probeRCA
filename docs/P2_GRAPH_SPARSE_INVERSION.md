# P2 Graph Sparse Inversion

## Goal

Use A7 calibrated residuals to solve sparse root-cause intervention `u_t`, replacing simplified residual-lift ranking with graph-constrained sparse inversion.

## Objective

`min_u 1/2 ||r-u||_2^2 + lambda1 ||u||_1 + lambda2 sum_(i,j) w_ij |u_i-u_j| + lambda3 sum_s ||u_Ms||_2`

中文解释：

- `r` is the calibrated residual aggregate signal.
- `u` is the root-cause intervention vector.
- L1 encourages sparsity.
- Graph total variation encourages related graph nodes to stay coherent.
- Group-lasso jointly models service-level and metric-level sparsity.

## Solver

A8 uses ADMM with separate proximal updates for L1 sparsity, graph total variation, and service-group shrinkage. It consumes A7 `calibrated_residuals.jsonl` and refuses inputs that do not confirm calibrated residual production.

## Inputs

- A4 candidate graph
- A7 `calibrated_residuals.jsonl`

## Outputs

- `sparse_interventions.jsonl`
- `metric_scores.jsonl`
- `service_scores.jsonl`
- `graph_sparse_objective_trace.jsonl`
- `graph_sparse_metadata.json`

## Label Safety

Root labels, target labels, injected paths, and incident start/end are not used for inversion. `incidents.jsonl` may be used only after inversion for debug ranking.

## Limitations

- A8 is a preview and is not yet integrated into the final online RCA result schema.
- A9 adds counterfactual explanation.
- A10 performs final blind audit and paper-ready summary.

## A8R Repair Notes

A8R repairs the graph sparse inversion preview without using root labels, target labels, injected path, or incident start/end times for solving.

Changes:
- Metric graph edge explosion is reduced by label-free metric-level edge rules and a per-node degree cap.
- Residual aggregation now uses `positive_topk_mean` over A7 `calibrated_residual` values only.
- Request/load metrics receive a symptom-family penalty so symptom latency does not automatically dominate root-resource metrics.
- Blind evidence / A7 h-value support can boost residual signal as an unlabeled support term.
- `auto_lambda` and adaptive group lambda derive regularization from unlabeled residual signal distributions.
- Post-sparsify limits nonzero intervention nodes for interpretability.
- Adaptive rho and a higher ADMM iteration budget improve convergence.
- The structural check now enforces sparse nonzero ratio and calibrated-residual consumption.

# P2 Candidate Subgraph

## Goal

A4 builds a local candidate subgraph from the A3 alert symptom service. The goal is to reduce global search and unrelated metric noise before later RCA integration.

## Scope

A4 only outputs candidate graph artifacts. It does not run RCA, does not re-inject faults, and does not change P1 scoring logic or the A2 blind rerun result.

## Inputs

- A3 `alert_windows.jsonl`
- raw `metrics.jsonl`
- raw `service_graph.jsonl`
- raw `metadata.json` when available

## Candidate Rules

Candidate services are selected from:

- the alert `symptom_service`
- reverse k-hop upstream neighborhood from the symptom service
- forward 1-hop context neighborhood
- resource neighbors if node, pod, namespace, or host labels are available

The builder records the service graph direction assumption in metadata. Online Boutique raw graphs may use caller-to-callee direction, so upstream dependency traversal can follow downstream graph edges when that convention is detected.

## Label Safety

`root_service`, `root_metric`, `root_type`, `target_service`, `target_metric`, `target_fault_type`, `injected_path`, and incident `start_ts/end_ts` are not used for graph construction. `incidents.jsonl` may only be used after construction for debug coverage, such as checking whether the root service was covered.

## Outputs

Per alert window:

- `candidate_services.jsonl`
- `candidate_metric_nodes.jsonl`
- `candidate_edges.jsonl`
- `candidate_subgraph_metadata.json`

Per repeat:

- `repeat_candidate_summary.json`
- optional `candidate_debug_evaluation.json`

Full preview:

- `p2_candidate_preview_summary.json`

## Limitations

- Graph direction depends on the `service_graph.jsonl` convention and is recorded as an assumption.
- Co-host/resource neighbors depend on whether raw metrics or metadata include node, pod, namespace, or host fields.
- A4 is not connected to the RCA pipeline yet.
- A4 does not fix I/O blind performance.

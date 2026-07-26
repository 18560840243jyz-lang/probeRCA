# Final collection-only data plane

The canonical live collection entry point is:

```text
proberca-collect-final
```

It performs only:

```text
raw Prometheus/eBPF-map primitives + dedicated Burst ring events
  -> Kubernetes identity resolution
  -> exact 1-second 9/4/3/3 normal aggregation
  -> topology-version proof
  -> typed raw Burst count/exposure or continuous samples
  -> paired write-once normal and raw-Burst archives
```

It does not import or execute alerting, propagation, candidate selection,
Ridge/RLS, residual construction, FISTA, diagnosis, fault injection, or
evaluation labels. Analysis remains a separate later command:

```text
proberca-analyze-collection
```

## Frozen aggregation rules

`proberca.dataplane.final_aggregation.FinalWindowAggregator` is the only
canonical final aggregator.

- Every monotonic counter series must have exactly one sample at both
  boundaries of the half-open 1-second window.
- A counter decrease rejects the window. It is never hidden by summing other
  Pods, containers, interfaces, or flows.
- Counter differences are computed per series before any cross-series sum.
- Failure, throttle, memory, I/O, futex, and local-socket ratios are recomputed
  from summed numerator and denominator components.
- P95 is accepted only from complete cumulative histograms with identical
  bucket layouts. Bucket deltas are merged before the P95 is selected.
- Duplicate, stale, ambiguous, pre-aggregated, wrong-unit, or wrong-kind
  inputs reject the complete window.
- A genuinely empty request/edge histogram is represented only as
  `sample_count=0, coverage=0`. Its finite zero placeholder preserves the
  frozen tensor shape but is not an observation and is never a forward fill.
- The cumulative histogram `+Inf` delta must equal its corresponding request
  or query counter delta whenever observations exist.

The output is exactly:

- 9 metrics for every monitored service;
- 4 metrics for every host that runs a monitored service;
- 3 metrics for every known directed TCP edge;
- 3 metrics for every known directed DNS edge.

Known edges remain in the topology during an idle second. Their count and
failure rate are zero and their empty latency has zero coverage. This keeps
traffic sparsity from masquerading as a deployment-layout change.

## Raw exporter contract

`proberca-export-final-primitives` is the canonical raw producer. Its frozen
configuration is `configs/final_primitive_exporter.example.yaml`. It:

- discovers exact ready Kubernetes Pod/container identities;
- reads only required cAdvisor, CoreDNS, node_exporter, and Beyla cumulative
  primitives;
- reads cgroup v2 CPU/memory/PSI/task/thread primitives;
- snapshots the always-on `bpf/final_normal` cgroup, futex, socket, and DNS
  maps;
- exports cumulative counters, cumulative buckets, and gauges with one
  explicit epoch-second timestamp.

The independent sources are fetched concurrently so a full snapshot completes
inside the frozen 1-second period. Normal-path BPF data stays in maps; it does
not stream fine-grained events to userspace.

`configs/final_live_collector.example.yaml` binds all 34 raw components to
this exporter. It does not query cAdvisor or node_exporter directly, so the
source labels and aggregation boundary are fixed in one auditable adapter.

Prometheus is a transport for source primitives, not the final aggregator.
The source adapter rejects `rate`, `irate`, `increase`, `delta`,
`histogram_quantile`, and cross-series reductions. Counter and histogram
samples must have exact window-boundary timestamps. This prevents an exporter
or recording rule from silently changing the mathematical contract.

## Topology and identity

Each window is bracketed by two complete Kubernetes inventory revisions.
Collection fails if the relevant structure fingerprint or ready-container
runtime identity fingerprint changes. The API server's global list
`resourceVersion` watermarks are recorded for provenance but are not treated
as layout identity because unrelated cluster events advance them. Every
service is resolved through Pod UID and ready container runtime identity, and
every known edge has complete metrics for both endpoint services. The sealed
topology therefore covers the entire window rather than only its end
timestamp.

## Burst boundary

`proberca-final-burst-loader` is a dedicated 31-program eBPF producer. It
writes an append-only JSONL event stream plus 250 ms loss checkpoints. The
`low` sampling profile is the Healthy default; `full` is the high-resolution
profile. Every sampled event carries its sampling divisor, so event counts
and exposure denominators are corrected without treating sampled counts as
complete counts.

The live adapter freezes 29 Burst channel types:

- event-count channels store an integral count plus exposure. OOM, backlog
  overflow, accept/connect failure, retransmission, RTO, RST, DNS timeout,
  rcode failure, and the other frozen count channels never consume a
  median/MAD Healthy baseline;
- continuous channels store one window value with no exposure. Scheduler,
  reclaim, block, futex/socket wait, softirq, RTT, and DNS latency channels
  are the only channels that later consume a Healthy reference distribution.

This distinction is enforced when each raw sample is created and again when
the control plane converts a sealed raw sample into evidence. An event-count
channel without exposure, a continuous channel with exposure, an unknown
channel, or a mode/calibration mismatch fails closed.

The data plane does not calculate Burst anomaly strength. It writes
`burst-windows.jsonl` and `burst-manifest.json` as a separate sealed archive
sharing the normal archive's dataset ID and 1-second window sequence. The
later control plane applies the rare-event threshold or continuous
median/MAD calibration and quality weight. Raw Burst source IDs are checked
against normal residual source IDs before either window is committed.

## Healthy-only collection

Install the pinned Beyla DaemonSet, final BPF loader, raw exporter systemd
services, and Prometheus scrape job:

```bash
sudo env PYTHONPATH=/home/jyz/.local/lib/python3.10/site-packages \
  python3 scripts/install_final_dataplane.py
```

Then collect without fault injection:

```bash
proberca-collect-final \
  --source-config configs/final_live_collector.example.yaml \
  --collection-contract configs/final_collection_contract.yaml \
  --burst-config configs/final_live_burst.example.yaml \
  --output artifacts/healthy-collection \
  --burst-output artifacts/healthy-burst \
  --windows 30
```

This command does not inject a fault. If any required normal or Burst source
is absent, it exits without producing a sealed pair. Only matching,
successfully sealed archives may later be passed to the control plane.

The single-VM Healthy paired dry run on 2026-07-26 sealed consecutive
normal/Burst windows with one shared dataset ID and exact sequence/time
alignment. Every normal window contained 12 complete service sets, one host
set, and 16 stable directed edge sets. Every matching raw-Burst window
contained all 29 channel types across the same 12 services, one host, 15 TCP
edges, and one DNS edge. Ring-buffer loss was zero and the minimum identity
mapping quality was one.

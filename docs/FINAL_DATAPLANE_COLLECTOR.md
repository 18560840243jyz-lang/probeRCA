# Final collection-only data plane

The canonical live collection entry point is:

```text
proberca-collect-final
```

It performs only:

```text
raw Prometheus/eBPF-map primitives
  -> Kubernetes identity resolution
  -> exact 1-second 9/4/3/3 aggregation
  -> topology-version proof
  -> optional independent Burst evidence
  -> write-once sealed collection archive
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
- Missing, duplicate, stale, ambiguous, pre-aggregated, wrong-unit, or
  wrong-kind inputs reject the complete window. No source value is filled with
  zero.

The output is exactly:

- 9 metrics for every monitored service;
- 4 metrics for every host that runs a monitored service;
- 3 metrics for every active directed TCP edge;
- 3 metrics for every active directed DNS edge.

## Raw exporter contract

The example source mapping is
`configs/final_live_collector.example.yaml`. It binds the 34 required raw
components to Prometheus selectors. Some inputs are standard cAdvisor or node
exporter counters; the service PSI/futex/local-socket and TCP/DNS primitives
must come from the always-on eBPF/cgroup-map exporter.

Prometheus is a transport for source primitives, not the final aggregator.
The source adapter rejects `rate`, `irate`, `increase`, `delta`,
`histogram_quantile`, and cross-series reductions. Counter and histogram
samples must have exact window-boundary timestamps. This prevents an exporter
or recording rule from silently changing the mathematical contract.

## Topology and identity

Each window is bracketed by two complete Kubernetes inventory revisions.
Collection fails if the structure fingerprint, required resource-version
vector, or runtime identity fingerprint changes. Every service is resolved
through Pod UID and ready container runtime identity, and every active edge
must have complete metrics for both endpoint services. The sealed topology
therefore covers the entire window rather than only its end timestamp.

## Burst boundary

`BurstEvidenceCollector` accepts a different set of opaque source records.
It applies the frozen rare-event or continuous normalization, multiplies
strength by coverage/loss/mapping quality exactly once, and writes the result
as independent Burst evidence. Any overlap with normal residual sources fails
closed.

## Healthy-only collection

After the required exporters are deployed and the example mapping is adjusted
to their exact label set:

```bash
proberca-collect-final \
  --source-config configs/final_live_collector.example.yaml \
  --collection-contract configs/final_collection_contract.yaml \
  --output artifacts/healthy-collection \
  --windows 30
```

This command does not inject a fault. If any required source is absent, it
exits without producing a sealed archive. Only a successfully sealed archive
may later be passed to the control plane.

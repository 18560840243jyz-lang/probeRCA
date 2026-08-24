# ProbeRCA Multi-node Campaign Implementation Plan

## Scope

This plan prepares the repository before renting four servers. It changes the
experiment orchestration boundary, not the `9/4/3` data contract, Baseline,
`A_s`, `A_v`, Burst, Soft/Hard thresholds, or Sparse-Group FISTA.

## Frozen campaign

- S0: Kubernetes control, Prometheus, one open-loop load generator, orchestration,
  and archive upload. It is excluded from formal root causes.
- Worker-1/2/3: fixed placement from `configs/final_multinode_campaign.yaml`.
- Formal scope: 11 services, 3 workers, 15 directed TCP edges, 108 theoretical
  root coordinates, and 156 `9/4/3` records per second.
- Core count with Host NIC: 3 Healthy + 12 Pilot + 111 fault + 9 control = 135.
- Pre-manifest unsupported Host NIC branch: 3 + 11 + 108 + 9 = 131.
- Four 300-second overhead experiments are reported separately.
- 120 synthetic groups are generated locally after the rented machines stop.

## Implementation milestones

1. Deterministic public/private campaign generation and exact count validation.
2. Fixed-seed per-coordinate Validation/Test assignment and non-adjacent order.
3. Public-key Test metadata sealing and one-way prediction-before-label protocol.
4. Actual-time phase labels and transition-window exclusion.
5. Crash-safe state bound to manifest fingerprint, case order, Dataset ID, SHA,
   and retry attempt.
6. Data-only load qualification and episode integrity gates.
7. Multi-node remote injector adapters and effectiveness checks for every Pilot.
8. One complete local/VM dry run with fake targets, then four-server preflight.
9. Object-storage upload, independent SHA verification, and offline restore test.

## Pre-rental deliverables

The repository must provide and test all of the following before any server is
rented:

- a vendored, SHA-verified Online Boutique manifest plus exact application,
  load-generator, and Beyla image digests;
- deterministic fixed placement rendering for the 11 formal services, with no
  default loadgenerator, DNS wrapper, sidecar, or experimental DNS workload;
- one local Worker data plane per Worker and one S0 Prometheus configuration;
- synchronized three-Worker collection, strict 156-record merge, and aligned
  Normal/Burst sealing;
- preservation of three per-Worker cumulative primitive archives and three
  filtered raw eBPF event/checkpoint archives for offline reaggregation;
- allow-listed, journaled, exact-cleanup injector actors, immutable cgroup/netns
  target resolution, actual-time phase records, and direct effectiveness gates;
- 25/40/55 objective qualification reports and highest-qualified load-profile
  freezing without Soft/Hard/READY/RCA gates;
- crash-safe campaign order/resume, opaque Test IDs, CMS private metadata,
  prediction-before-decryption, object-store readback, and offline restore;
- one non-privileged fake-target rehearsal and the complete repository test
  suite, producing a `pre_rent_code_ready` report.

The exact physical execution order and commands are frozen in
`docs/MULTINODE_CAMPAIGN_RUNBOOK.md`. Worker archives are explicit projected
archives (8/7/0 caller-owned TCP edges) and are not control-plane inputs until
the strict three-Worker merge has produced the complete 156-record archive.
Qualification control scope and Baseline/A_v row projections are generated from
the campaign topology and semantic mask; operators do not hand-write coordinate
lists or equate the Baseline denominator with all 156 physical records.

The code rehearsal cannot prove cloud NIC support, kernel/driver compatibility,
actual clock offsets, exact cloud storage credentials, real workload capacity,
or real intervention effectiveness. These remain explicit physical gates and
must not be reported as passed before the four rented nodes exist.

## Rent-server Go/No-Go

`pre_rent_code_ready` requires all repository tests, deterministic manifest generation in both
Host NIC branches, working CMS encryption/decryption with the private key held
outside the cluster, a dry-run resume after an injected interruption, and working
fake injector/effectiveness/cleanup protocol rehearsal. It also requires the
filesystem object-store adapter and successful offline restoration.

`formal_campaign_go` is a separate, post-rental gate. It requires the four
physical node reports, <=10 ms clock offset, swap disabled, required CPU/memory
and disk, Worker node-exporter/Beyla/eBPF readiness, exact image digests and Pod
placement, independent object-store upload/readback, one frozen objectively
qualified load profile, and 12 accepted real Pilots. Simulated Pilot evidence is
rejected by the registry freezer.

Until the pre-rental report is complete, a matrix preview is not permission to
rent servers. Conversely, `pre_rent_code_ready` is permission to provision the
environment, not permission to start the formal campaign; physical gates must
still produce `formal_campaign_go=true`.

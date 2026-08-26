# ProbeRCA four-node campaign runbook

This is the operator order for the one-time 135/131-dataset acquisition.  It
does not turn simulated evidence into a physical pass.  Every command is run
from one clean, fixed Git SHA; cloud credentials, the CMS private key, and the
HMAC commitment secret stay outside the rented cluster.

## 0. Pre-rental code gate

Run the full repository test suite and then:

```bash
python3 scripts/run_multinode_campaign_rehearsal.py \
  --repository "$PWD" \
  --output /data/pre-rent-rehearsal-$(git rev-parse --short HEAD) \
  --repository-tests-passed
```

`pre_rent_code_ready=true` permits provisioning only.  It must still report
`formal_campaign_go=false`, because real nodes, real object storage, real load
qualification, and real injector Pilots do not exist yet.

## 1. Physical inventory and immutable release

1. Provision S0 plus Worker-1/2/3 in one private network.  Create the Kubernetes
   cluster using the provider's supported procedure; cloud-specific bootstrap is
   deliberately not hidden in this repository.
2. Copy `configs/final_multinode_nodes.example.yaml` outside the repository,
   replace every `CHANGE_ME`, record exact Kubernetes node names and the safe
   fault interface, pin SSH host keys, and chmod the SSH key/config appropriately.
3. Run `scripts/preflight_multinode_campaign.py`.  Do not continue on clock,
   swap, cgroup-v2, BTF, CPU/RAM/disk, SSH, node-name, image-digest, endpoint, or
   placement failure.
4. Run `scripts/install_multinode_worker_agents.py` from a clean SHA.  Workers
   receive a Git archive at `/opt/proberca/releases/<sha>`; no dirty source is
   copied.

## 2. Render and deploy the formal data plane

```bash
python3 scripts/render_multinode_workloads.py --help
python3 scripts/generate_multinode_dataplane_configs.py --help
python3 scripts/install_multinode_cluster_workloads.py --help
python3 scripts/install_multinode_worker_dataplane_local.py --help
python3 scripts/install_multinode_s0_prometheus.py --help
```

Use those CLIs with the frozen node inventory and image lock.  The three Worker
projections own 8, 7, and 0 caller-owned TCP edges respectively.  Each Worker
archive contains global topology but only its local metrics.  Only the merge is
a complete formal archive: 11 services, 3 hosts, 15 directed TCP edges, and 156
records per window.  Projected archives are rejected by the control plane.

## 3. Objective Load Qualification

Run all three profile IDs for exactly 300 aligned windows, separated by the
configured cooldown.  For each profile:

1. Install that profile with `install_multinode_campaign_load.py`.
2. Collect and merge a 300-window dataset.
3. Generate its profile-bound control config:

```bash
python3 scripts/generate_multinode_control_config.py \
  --campaign configs/final_multinode_campaign.yaml \
  --base-control configs/final_control.yaml \
  --candidate-profile-id multi-node-open-loop-25 \
  --output /private/qualification/25-control.yaml
```

4. Save a five-field objective telemetry JSON, measured over the same exact
   boundaries:

```json
{
  "measured_rps": 24.8,
  "business_error_rate": 0.0002,
  "worker_cpu_p95": 0.61,
  "worker_memory_p95": 0.68,
  "pod_restart_delta": 0
}
```

5. Build, evaluate, and freeze without supplying Soft, Hard, READY, FISTA, or
   RCA fields:

```bash
python3 scripts/build_multinode_qualification_observation.py --help
python3 scripts/evaluate_multinode_load_qualification.py --help
python3 scripts/freeze_multinode_load_profile.py --help
```

The builder derives Normal/Burst alignment, data-source gaps, topology/runtime
stability, all 15 observed TCP edges, and per-coordinate Baseline/A_v row
projections directly from the sealed archive.  The Baseline denominator is the
108 roots plus their actual semantic parents; it is intentionally not hard-coded
to the 156 physical records.  Freeze the highest profile that passes every
objective gate.

## 4. Twelve real injector Pilots

Use `run_multinode_campaign_episode.py` once for every candidate injector.  The
coordinate JSON and resulting `injection-session.json` are private operator
files outside `dataset_root`.  The script resolves the current container/cgroup
or source netns, starts all three collectors at one future epoch second, applies
at +60 seconds, cleans at +120 seconds, and records actual lifecycle times.

Seal the data-only Pilot, calculate its SHA, then run
`evaluate_multinode_pilot.py` with direct contamination observations.  Finally
pass all 12 reports to `freeze_multinode_injectors.py`.  Simulated reports,
missing contamination dimensions, a changed runtime binding, ineffective
intervention, or inexact cleanup cannot freeze the registry.  If Host NIC is
unsupported, change the campaign flag before manifest generation and use the
131-dataset branch; never substitute TCP loss.

For `service_cpu`, the evaluator derives `cpu_throttle_ratio` contamination
from the sealed Pilot rather than trusting an operator Boolean. Its reference
is the same Pilot's label-blind `HEALTHY_PRE` segment, using the configured
median-plus-robust-scale bound. A quota mutation, an active median above that
bound, more than two consecutive excess windows, or an active-minus-reference
excess fraction above five percentage points invalidates the Pilot. One or two
isolated excess windows are preserved as raw `nr_throttled/nr_periods` evidence
and produce `PASS_WITH_INCIDENTAL_SECONDARY_SIGNAL`; they do not turn a unique
CPU-usage intervention into a CPU-throttle intervention. This Pilot-only gate
does not use the final Healthy control-plane model, Soft/Hard, or RCA output.

## 5. Freeze the executable campaign

Generate public/private manifests only after the load and injector registries
are frozen.  Stream the private manifest into CMS encryption.  Keep the private
key off-cluster.  The public manifest and `CampaignState` fix the exact order;
failed attempts are never accepted. A case may move to the deferred tail only
after an independent report proves exact cleanup, unchanged Pod/runtime
identity and restart counts, and all business Pods Ready. Remaining cases keep
their original relative order; deferred cases retry in their original order
under the same case ID with a new attempt. Any unproved cleanup stops the
campaign, and rented nodes cannot be released until every deferred case is
completed and the real count is 135/135.

For Test cases, the trusted off-cluster controller reads the private coordinate
only to invoke `run_multinode_campaign_episode.py`.  It must place private
injection/effectiveness evidence outside the dataset.  Test data are sealed with
only the opaque `Txxxxxx` ID and public commitments.

## 6. Collect, seal, upload, and resume

For every case:

1. Run the one-case episode CLI (or label-free collector for Healthy/control).
2. Verify three Worker projected archives merge into one complete v3 archive.
3. Verify the three raw primitive and three filtered raw-event/checkpoint
   archives exist and share Dataset ID and boundaries.
4. Apply direct episode integrity/effectiveness gates.  Current RCA output is
   never an acquisition gate.
5. Seal with `seal_campaign_dataset.py`.
6. Upload immutably with `upload_campaign_dataset.py`; require independent
   object readback SHA success before deleting local staging.
7. Commit the Dataset ID/SHA into `CampaignState` and continue.  On interruption,
   reload the same manifest/state and retry the incomplete case only.

## 7. Test isolation and shutdown gate

Freeze all Test predictions once before decrypting any Test label.  Then decrypt
off-cluster and score; never tune and rerun Test.  Before releasing the servers:

- all 135/131 core datasets exist with unique IDs and valid SHA;
- three Healthy datasets contain 1800 windows; all 132/128 episodes contain
  180 windows; Normal/Burst boundaries align exactly;
- every fault has private direct-effectiveness evidence;
- object-store readback and a second local copy pass SHA;
- at least ten randomly selected restores pass, including Healthy, Validation,
  and Test; and
- one offline machine can load/replay restored archives without Kubernetes.

Only then may the rented machines be released.  Synthetic groups are generated
later on the local workstation and are not part of this physical shutdown gate.

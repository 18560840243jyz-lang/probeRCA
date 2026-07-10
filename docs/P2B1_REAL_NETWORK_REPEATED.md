# P2B-1 Real Network Repeated Injection

P2B-1 is the repeated real network fault injection experiment for Google Online Boutique on the single-VM kind deployment.

This stage repeatedly injects `tc netem` delay/loss into the `shippingservice` Pod network namespace, collects real network counters and frontend request latency, restores the qdisc, and runs the frozen P1 RCA pipeline on each real repeat dataset.

This is not synthetic data. Each repeat performs a fresh real `tc netem` injection and produces its own raw dataset and P1 RCA output.

This is not multi-fault overall accuracy. It only represents repeated `shippingservice` network instability experiments.

Primary metrics follow `docs/P2_REAL_EXPERIMENT_METRICS.md`:
- `service_hit_at_1`
- `metric_hit_at_3`
- `root_type_accuracy`
- `path_fidelity`

Auxiliary metrics:
- `metric_hit_at_1`
- `metric_mrr`

Output directory:

```bash
data/p2_online_boutique/network_shippingservice_repeated
```

Run:

```bash
bash scripts/online_boutique/run_p2b1_network_repeated.sh
```

After P2B-1, IO and lock real injection experiments are still required before any multi-fault real accuracy summary.

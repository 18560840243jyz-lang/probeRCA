# P2C-0 Real IO Fault Smoke

P2C-0 validates whether a real I/O stress fault can be injected into Online Boutique `redis-cart` on the single-VM kind deployment.

This stage only verifies feasibility. It does not run the RCA pipeline and does not report I/O accuracy.

The smoke uses `kubectl exec` to run `dd` write pressure inside the `redis-cart` Pod and observes kubelet/cAdvisor filesystem metrics such as write bytes, write ops, and I/O time.

Output directory:

```bash
data/p2_online_boutique/io_rediscart_smoke_001
```

Run:

```bash
bash scripts/online_boutique/run_p2c0_io_smoke.sh
```

If P2C-0 passes, the next step is P2C-1 repeated real I/O fault injection. P2C-0 does not use Prometheus, Beyla, or ClickHouse.

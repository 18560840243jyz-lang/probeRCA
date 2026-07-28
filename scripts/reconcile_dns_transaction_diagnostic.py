from __future__ import annotations

import argparse
import json
from pathlib import Path

from proberca.dataplane.dns_diagnostic import reconcile


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile a dedicated DNS diagnostic Pod's application logs, "
            "decoded PCAP, and full-sampling ProbeRCA eBPF events."
        )
    )
    parser.add_argument("--application", type=Path, required=True)
    parser.add_argument("--tcpdump-text", type=Path, required=True)
    parser.add_argument("--ebpf", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pod-ip", required=True)
    parser.add_argument("--dns-cluster-ip", required=True)
    parser.add_argument("--pod-uid", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--netns", required=True)
    args = parser.parse_args()
    result = reconcile(
        application_path=args.application,
        tcpdump_text_path=args.tcpdump_text,
        ebpf_path=args.ebpf,
        output_dir=args.output,
        pod_ip=args.pod_ip,
        dns_cluster_ip=args.dns_cluster_ip,
        pod_uid=args.pod_uid,
        service=args.service,
        netns=args.netns,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

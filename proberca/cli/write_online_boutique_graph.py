"""Write the P2A-0 Online Boutique service graph."""

from __future__ import annotations

import argparse

from proberca.adapters.online_boutique.topology import write_online_boutique_service_graph


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write Google Online Boutique service graph JSONL.")
    parser.add_argument("--output", default="data/p2_online_boutique/deploy_smoke/service_graph.jsonl")
    args = parser.parse_args(argv)

    result = write_online_boutique_service_graph(args.output)
    print("Online Boutique 服务拓扑已写出")
    print(f"output path：{result['output_path']}")
    print(f"services count：{result['services_count']}")
    print(f"edges count：{result['edges_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

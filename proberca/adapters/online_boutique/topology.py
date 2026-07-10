"""Service topology helpers for Google Online Boutique P2A-0."""

from __future__ import annotations

import json
from pathlib import Path

ONLINE_BOUTIQUE_SERVICES = [
    "frontend",
    "checkoutservice",
    "paymentservice",
    "cartservice",
    "productcatalogservice",
    "recommendationservice",
    "shippingservice",
    "currencyservice",
    "emailservice",
    "adservice",
    "redis-cart",
]

ONLINE_BOUTIQUE_SERVICE_GRAPH = [
    ("frontend", "checkoutservice"),
    ("frontend", "recommendationservice"),
    ("frontend", "productcatalogservice"),
    ("frontend", "cartservice"),
    ("checkoutservice", "paymentservice"),
    ("checkoutservice", "shippingservice"),
    ("checkoutservice", "emailservice"),
    ("checkoutservice", "cartservice"),
    ("checkoutservice", "productcatalogservice"),
    ("checkoutservice", "currencyservice"),
    ("recommendationservice", "productcatalogservice"),
    ("cartservice", "redis-cart"),
    ("adservice", "frontend"),
]


def write_online_boutique_service_graph(output_path: str | Path) -> dict:
    """Write Online Boutique service graph as JSONL records compatible with probeRCA."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for source, target in ONLINE_BOUTIQUE_SERVICE_GRAPH:
            record = {
                "source": source,
                "target": target,
                "src": source,
                "dst": target,
                "edge_type": "call",
                "weight": 1.0,
                "source_system": "google_online_boutique",
            }
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_path": str(path),
        "services_count": len(ONLINE_BOUTIQUE_SERVICES),
        "edges_count": len(ONLINE_BOUTIQUE_SERVICE_GRAPH),
    }

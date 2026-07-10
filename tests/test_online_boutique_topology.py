import json
import subprocess
import sys

from proberca.adapters.online_boutique.topology import (
    ONLINE_BOUTIQUE_SERVICES,
    ONLINE_BOUTIQUE_SERVICE_GRAPH,
    write_online_boutique_service_graph,
)


def test_online_boutique_services_include_core_services():
    for service in ["frontend", "checkoutservice", "paymentservice", "cartservice", "redis-cart"]:
        assert service in ONLINE_BOUTIQUE_SERVICES


def test_online_boutique_graph_include_core_edges():
    edges = set(ONLINE_BOUTIQUE_SERVICE_GRAPH)
    assert ("frontend", "checkoutservice") in edges
    assert ("checkoutservice", "paymentservice") in edges
    assert ("cartservice", "redis-cart") in edges


def test_write_online_boutique_service_graph(tmp_path):
    output = tmp_path / "service_graph.jsonl"
    result = write_online_boutique_service_graph(output)
    assert output.exists()
    lines = output.read_text(encoding="utf-8").splitlines()
    assert lines
    assert result["edges_count"] == len(lines)
    for line in lines:
        record = json.loads(line)
        assert "source" in record
        assert "target" in record
        assert "edge_type" in record


def test_write_online_boutique_graph_cli(tmp_path):
    output = tmp_path / "service_graph.jsonl"
    completed = subprocess.run(
        [sys.executable, "-m", "proberca.cli.write_online_boutique_graph", "--output", str(output)],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    assert output.exists()
    assert "services count" in completed.stdout

import importlib
import json
from pathlib import Path

from proberca.adapters.online_boutique.network_fault import parse_proc_net_snmp
from proberca.adapters.online_boutique.p2b1_network_repeat import _network_delta_records, build_network_incident
from proberca.cli.check_p2b1_network_repeated import check_p2b1_network_repeated


def test_network_delta_records_from_proc_net_snmp():
    before_text = """
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs
Tcp: 1 200 120000 -1 3 4 0 0 1 100 200 2
"""
    after_text = """
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs
Tcp: 1 200 120000 -1 3 4 0 0 1 130 260 7
"""
    prev = {"tcp": parse_proc_net_snmp(before_text)}
    curr = {"tcp": parse_proc_net_snmp(after_text)}
    rows = _network_delta_records(prev, curr, {"service": "shippingservice", "pod_name": "shippingservice-x"}, 123.0, "faulty", "inc")
    values = {row["metric"]: row["value"] for row in rows}
    assert values["net.retrans"] == 5.0
    assert values["net.out_segs"] == 60.0
    assert values["net.in_segs"] == 30.0


def test_build_network_incident_repeat_id():
    incident = build_network_incident(3, 10.0, 20.0)
    assert incident["incident_id"] == "ob-network-shippingservice-repeat-03"
    assert incident["root_service"] == "shippingservice"
    assert incident["root_metric"] == "net.retrans"


def test_check_p2b1_network_repeated_pass_and_fail(tmp_path: Path):
    summary = {
        "repeats_completed": 5,
        "repeats_successful_quality": 5,
        "repeats_successful_rca": 5,
        "service_hit_at_1_mean": 0.8,
        "metric_hit_at_3_mean": 0.8,
        "root_type_accuracy_mean": 1.0,
        "path_fidelity_mean": 0.8,
    }
    (tmp_path / "p2b1_network_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert check_p2b1_network_repeated(tmp_path)["passed"] is True
    summary["metric_hit_at_3_mean"] = 0.5
    (tmp_path / "p2b1_network_repeat_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    result = check_p2b1_network_repeated(tmp_path)
    assert result["passed"] is False
    assert result["failed_checks"]


def test_cli_imports():
    importlib.import_module("proberca.cli.run_p2b1_network_repeated")
    importlib.import_module("proberca.cli.check_p2b1_network_repeated")

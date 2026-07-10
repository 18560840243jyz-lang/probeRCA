import subprocess
import sys

from proberca.adapters.online_boutique.network_fault import parse_proc_net_snmp, parse_ss_rtt
from proberca.adapters.online_boutique.p2b0_network_smoke import evaluate_network_fault_feasible


def test_parse_proc_net_snmp_tcp_fields():
    text = """
Ip: Forwarding DefaultTTL InReceives
Ip: 1 64 10
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs
Tcp: 1 200 120000 -1 3 4 0 0 1 100 120 7
"""
    parsed = parse_proc_net_snmp(text)
    assert parsed["RetransSegs"] == 7
    assert parsed["OutSegs"] == 120
    assert parsed["InSegs"] == 100
    assert parsed["ActiveOpens"] == 3
    assert parsed["PassiveOpens"] == 4


def test_parse_ss_rtt():
    text = "ESTAB 0 0 10.0.0.1:123 10.0.0.2:456\n\t cubic rtt:12.3/1.2 ato:40\nESTAB 0 0 a b\n\t cubic rtt:20/2"
    parsed = parse_ss_rtt(text)
    assert parsed["available"] is True
    assert parsed["samples"] == 2
    assert parsed["rtt_ms"] == 16.15


def test_network_fault_feasible_pass_and_fail():
    summary = {
        "netem_applied": True,
        "netem_restored": True,
        "frontend_after_http_ok": True,
        "tc_qdisc_during": "qdisc netem 8001: root refcnt 2 limit 1000 delay 200ms",
        "tc_qdisc_after": "qdisc noqueue 0: root refcnt 2",
    }
    passed, failed = evaluate_network_fault_feasible(summary)
    assert passed is True
    assert failed == []
    summary["tc_qdisc_after"] = "qdisc netem 8001: root"
    passed, failed = evaluate_network_fault_feasible(summary)
    assert passed is False
    assert "tc_qdisc_after still contains netem" in failed


def test_p2b0_cli_import_only():
    completed = subprocess.run(
        [sys.executable, "-c", "import proberca.cli.run_p2b0_network_smoke as r; import proberca.cli.check_p2b0_network_smoke as c; assert callable(r.main); assert callable(c.main)"],
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout

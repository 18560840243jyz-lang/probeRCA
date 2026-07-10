from proberca.adapters.online_boutique.service_metric_identity import (
    assert_or_repair_node_ownership,
    make_node_id,
    split_node_id,
    validate_node_ownership,
)


def test_make_and_split_node_id_preserves_hyphen_service():
    assert make_node_id("paymentservice", "cpu.throttled_usec") == "paymentservice.cpu.throttled_usec"
    assert split_node_id("redis-cart.io.write_bytes") == ("redis-cart", "io.write_bytes")


def test_validate_node_ownership_detects_service_mismatch():
    row = {"node_id": "paymentservice.cpu.throttled_usec", "service": "adservice", "metric": "cpu.throttled_usec"}
    ownership = validate_node_ownership(row)
    assert ownership["ownership_valid"] is False
    assert ownership["ownership_issue"] == "service_mismatch_node_id"


def test_assert_or_repair_prefers_node_id_without_labels():
    row = {"node_id": "paymentservice.cpu.throttled_usec", "service": "adservice", "metric": "cpu.throttled_usec"}
    repaired = assert_or_repair_node_ownership(row)
    assert repaired["service"] == "paymentservice"
    assert repaired["metric"] == "cpu.throttled_usec"
    assert repaired["ownership_valid"] is True
    assert repaired["ownership_repaired"] is True
    assert repaired["ownership_repair_source"] == "node_id"

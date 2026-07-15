from __future__ import annotations

import ast
from pathlib import Path


def target(file_name, function_name):
    return f"tests/{file_name}.py::{function_name}"


GROUPS = [
    (range(1, 11), target("test_p11_config_security", "test_kubernetes_config_requires_explicit_cluster_and_namespace_scope")),
    (range(11, 29), target("test_p11_watch_inventory", "test_410_relist_marks_not_ready_then_atomically_replaces_cache")),
    (range(29, 40), target("test_p11_watch_inventory", "test_deleted_uid_is_tombstoned_and_recreated_name_is_new_identity")),
    (range(40, 51), target("test_p11_mapping_topology", "test_owner_chain_uses_uid_and_never_name_heuristics")),
    (range(51, 67), target("test_p11_mapping_topology", "test_endpoint_ready_policy_excludes_not_ready_and_deduplicates_slices")),
    (range(67, 75), target("test_p11_mapping_topology", "test_multi_service_membership_fails_without_explicit_service_label")),
    (range(75, 88), target("test_p11_mapping_topology", "test_pvc_pv_csi_and_hostnetwork_resources_are_explicit_and_stable")),
    (range(88, 97), target("test_p11_mapping_topology", "test_topology_uses_explicit_call_provider_and_preserves_host_relation")),
    (range(97, 109), target("test_p11_adversarial", "test_live_topology_is_p3_round_trip_compatible_and_order_deterministic")),
    (range(109, 125), target("test_p11_prometheus_adapter", "test_prometheus_range_is_half_open_and_end_sample_is_excluded")),
    (range(125, 142), target("test_p11_prometheus_adapter", "test_edge_mapping_requires_exact_source_destination_and_protocol")),
    (range(142, 153), target("test_p11_live_leader", "test_epoch_scheduler_never_repeats_or_zero_fills_missed_windows")),
    (range(153, 165), target("test_p11_live_leader", "test_live_runner_only_calls_canonical_engine_after_topology_and_metrics")),
    (range(165, 177), target("test_p11_live_leader", "test_two_lease_contenders_have_at_most_one_leader_and_loss_stops_commit")),
    (range(177, 187), target("test_p11_live_leader", "test_readiness_requires_sync_prometheus_leader_and_writable_state")),
    (range(187, 195), target("test_p11_client_retention_deploy", "test_checkpoint_retention_keeps_current_and_previous_and_never_reports")),
    (range(195, 211), target("test_p11_client_retention_deploy", "test_kubernetes_manifests_are_least_privilege_and_non_privileged")),
    (range(211, 224), target("test_p11_client_retention_deploy", "test_live_cli_has_no_plaintext_token_label_or_stage_bypass_arguments")),
    (range(224, 233), target("test_p11_live_leader", "test_scheduler_snapshot_resume_continues_next_sequence")),
    (range(233, 245), target("test_p11_adversarial", "test_live_production_sources_have_no_labels_hardcoding_kubectl_or_legacy_solver")),
]

COVERAGE = {number: test_name for numbers, test_name in GROUPS for number in numbers}


def test_all_244_p11_requirements_map_to_real_test_functions():
    assert set(COVERAGE) == set(range(1, 245))
    parsed = {}
    for requirement, value in COVERAGE.items():
        file_name, function_name = value.split("::", 1)
        path = Path(file_name)
        assert path.is_file(), f"requirement {requirement}: missing {path}"
        functions = parsed.setdefault(path, {
            node.name for node in ast.parse(path.read_text(encoding="utf-8")).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))})
        assert function_name in functions, f"requirement {requirement}: missing {value}"

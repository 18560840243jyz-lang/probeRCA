from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
import yaml

from proberca.campaign.generator import (
    CampaignConfigError,
    build_campaign_plan,
    load_campaign_config,
)
from proberca.campaign.injectors import (
    InjectorRegistryError,
    load_injector_registry,
)
from proberca.campaign.gates import (
    EpisodeIntegrityObservation,
    QualificationObservation,
    evaluate_episode_integrity,
    evaluate_qualification,
)
from proberca.campaign.evaluation import (
    BlindEvaluationProtocol,
    EvaluationProtocolError,
)
from proberca.campaign.phases import FaultLifecycle, classify_window_phase
from proberca.campaign.sealing import (
    PrivateManifestSealingError,
    seal_private_manifest,
)
from proberca.campaign.state import CampaignState, CampaignStateError
from proberca.campaign.model import fingerprint
from proberca.campaign.restore import (
    RestoreVerificationError,
    compare_restored_copy,
    verify_sha256s,
)
from proberca.campaign.execution import (
    InjectorExecutionError,
    TargetBinding,
    freeze_injector_registry,
    run_injector_pilot,
)
from proberca.campaign.fake_agent import FakeWorkerAgent
from proberca.campaign.orchestrator import (
    CampaignExecutionError,
    CampaignExecutor,
    SealedCaseResult,
)
from proberca.campaign.preflight import (
    CampaignPreflightObservation,
    evaluate_campaign_preflight,
)
from proberca.campaign.remote import RemoteAgentError, RemoteNode, SSHAgentClient
from proberca.campaign.storage import (
    ArchiveStoreError,
    FilesystemArchiveStore,
    restore_dataset,
    upload_and_verify_dataset,
)
from proberca.campaign.target_resolver import resolve_case_target
from proberca.campaign.scheduled_injection import run_scheduled_injection
from proberca.campaign.effectiveness import evaluate_fault_effectiveness
from proberca.campaign.effectiveness import build_real_pilot_report
from proberca.campaign.load_profile import (
    LoadProfileError,
    freeze_load_profile,
)
from proberca.campaign.qualification import (
    QualificationReportError,
    build_qualification_report,
)
from proberca.campaign.control_config import (
    build_multinode_control_config,
    candidate_profile_fingerprint,
)
from proberca.campaign.qualification_observation import (
    QualificationObservationError,
    build_archive_qualification_observation,
)
from proberca.controlplane.config import FinalControlConfig
from proberca.campaign.worker_agent import WorkerAgent, WorkerAgentError
from proberca.dataplane.primitive_exporter import (
    FinalPrimitiveExporterConfig,
    MULTINODE_PRIMITIVE_EXPORTER_SCHEMA_VERSION,
)
from proberca.dataplane.burst_live import FinalLiveBurstConfig
from proberca.dataplane.collector import (
    FinalLiveCollectorConfig,
    MULTINODE_COLLECTOR_CONFIG_SCHEMA_VERSION,
)
from proberca.campaign.multinode_merge import merge_worker_archives
from proberca.data.schema import (
    METRIC_RECORD_SCHEMA_VERSION,
    EdgeMetricRecord,
    NodeMetricRecord,
    ServiceNodePlacement,
    TopologyEdge,
    TopologySnapshot,
)
from proberca.dataplane.archive import (
    CollectionArchive,
    CollectionArchiveWriter,
    PROJECTED_COLLECTION_ARCHIVE_SCHEMA_VERSION,
)
from proberca.dataplane.burst_archive import (
    BurstArchive,
    BurstArchiveWriter,
    RawBurstWindow,
)
from proberca.dataplane.contracts import CollectedWindow


REPOSITORY = Path(__file__).resolve().parents[1]
CONFIG = REPOSITORY / "configs/final_multinode_campaign.yaml"
SECRET = b"campaign-test-secret-that-is-longer-than-32-bytes"


def _script_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, REPOSITORY / "scripts" / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _plan(nic=True):
    config = copy.deepcopy(load_campaign_config(CONFIG))
    config["host_nic"]["supported"] = nic
    return build_campaign_plan(config, commitment_secret=SECRET)


def test_supported_nic_campaign_has_frozen_135_dataset_matrix():
    plan = _plan(True)
    assert plan.summary == {
        "matrix_status": "VALID",
        "rent_server_go_no_go": "NO_GO_PENDING_INJECTOR_AND_RESTORE_PREFLIGHT",
        "host_nic_supported": True,
        "formal_coordinate_count": 37,
        "formal_fault_count": 111,
        "validation_fault_count": 37,
        "test_fault_count": 74,
        "pilot_count": 12,
        "healthy_count": 3,
        "control_count": 9,
        "core_dataset_count": 135,
        "overhead_experiment_count": 4,
        "synthetic_group_count": 120,
        "public_manifest_fingerprint": plan.public_manifest["manifest_fingerprint"],
    }
    assert plan.public_manifest["expected_formal_record_count_per_window"] == 156
    host_metrics = {
        case["coordinate"]["metric"]
        for case in plan.private_manifest["cases"]
        if case.get("coordinate") and case["coordinate"]["entity_kind"] == "host"
    }
    assert host_metrics == {"cpu_psi", "memory_psi", "io_psi", "nic_drop_error_rate"}
    assert not host_metrics & {"host_cpu_psi", "host_memory_psi", "host_io_psi"}


def test_campaign_rejects_root_labels_outside_formal_metric_catalog():
    config = copy.deepcopy(load_campaign_config(CONFIG))
    config["formal_fault_matrix"]["host"][0]["metric"] = "host_cpu_psi"
    with pytest.raises(
        CampaignConfigError, match="outside the formal 9/4/3 roots",
    ):
        build_campaign_plan(config, commitment_secret=SECRET)


def test_unsupported_nic_branch_removes_coordinate_without_tcp_substitution():
    plan = _plan(False)
    assert plan.summary["formal_coordinate_count"] == 36
    assert plan.summary["formal_fault_count"] == 108
    assert plan.summary["validation_fault_count"] == 36
    assert plan.summary["test_fault_count"] == 72
    assert plan.summary["pilot_count"] == 11
    assert plan.summary["core_dataset_count"] == 131
    private_text = json.dumps(plan.private_manifest)
    assert "nic_drop_error_rate" not in private_text
    assert "host_nic" not in private_text
    assert "tcp_failure" in private_text


def test_generation_is_deterministic_and_coordinate_repeats_are_not_adjacent():
    first = _plan(True)
    second = _plan(True)
    assert first.public_manifest == second.public_manifest
    assert first.private_manifest == second.private_manifest
    cases = first.private_manifest["cases"]
    fault_cases = [case for case in cases if case["case_kind"] == "fault"]
    for left, right in zip(cases, cases[1:]):
        if left["case_kind"] == right["case_kind"] == "fault":
            assert left["coordinate"]["coordinate_id"] != right["coordinate"]["coordinate_id"]
    grouped = {}
    for case in fault_cases:
        grouped.setdefault(case["coordinate"]["coordinate_id"], []).append(case)
    assert set(map(len, grouped.values())) == {3}
    assert all(
        sum(case["split"] == "validation" for case in group) == 1
        and sum(case["split"] == "test" for case in group) == 2
        for group in grouped.values()
    )


def test_public_test_manifest_hides_fault_and_control_metadata():
    plan = _plan(True)
    public_test = [
        case for case in plan.public_manifest["cases"]
        if case["split"] == "test" and case["case_id"].startswith("T")
    ]
    assert len(public_test) == 77
    assert all(set(case) == {
        "case_id", "split", "duration_seconds", "storage_class",
        "private_metadata_hmac_sha256",
    } for case in public_test)
    assert all(len(case["private_metadata_hmac_sha256"]) == 64 for case in public_test)
    assert len(plan.public_manifest["randomization_commitment_hmac_sha256"]) == 64
    assert plan.public_manifest["randomization_commitment_hmac_sha256"] != fingerprint({
        "master_seed": 20260824,
    })
    assert "master_seed" not in json.dumps(plan.public_manifest)
    public_text = json.dumps(public_test)
    for forbidden in (
        "coordinate", "fault", "control", "mechanism", "target", "repeat",
        "injector", "frontend", "worker-1",
    ):
        assert forbidden not in public_text


def test_test_private_manifest_is_sealed_from_memory_without_plaintext_file(tmp_path, monkeypatch):
    certificate = tmp_path / "recipient.pem"
    certificate.write_text("certificate", encoding="utf-8")
    output = tmp_path / "private.cms"
    captured = {}

    class Completed:
        returncode = 0
        stdout = b""
        stderr = b""

    def fake_run(command, *, input, stdout, stderr, check):
        captured["command"] = command
        captured["input"] = input
        Path(command[command.index("-out") + 1]).write_bytes(b"sealed-cms")
        return Completed()

    monkeypatch.setattr("proberca.campaign.sealing.subprocess.run", fake_run)
    seal_private_manifest(
        {"root_service": "secret"}, recipient_certificate=certificate,
        output_path=output,
    )
    assert output.read_bytes() == b"sealed-cms"
    assert b"root_service" in captured["input"]
    assert not list(tmp_path.glob("*.json"))
    assert captured["command"][1:6] == [
        "cms", "-encrypt", "-binary", "-aes-256-gcm", "-outform",
    ]
    with pytest.raises(PrivateManifestSealingError, match="overwrite"):
        seal_private_manifest(
            {"root_service": "secret"}, recipient_certificate=certificate,
            output_path=output,
        )


def test_actual_fault_times_define_transition_and_full_active_windows():
    lifecycle = FaultLifecycle(
        planned_start_ns=10, apply_command_start_ns=12,
        effect_confirmed_ns=15, planned_end_ns=30,
        cleanup_command_start_ns=31, cleanup_confirmed_ns=34,
    )
    assert classify_window_phase(0, 10, lifecycle) == "HEALTHY_PRE"
    assert classify_window_phase(11, 13, lifecycle) == "TRANSITION_APPLY"
    assert classify_window_phase(14, 16, lifecycle) == "TRANSITION_APPLY"
    assert classify_window_phase(15, 16, lifecycle) == "FAULT_ACTIVE"
    assert classify_window_phase(30, 31, lifecycle) == "FAULT_ACTIVE"
    assert classify_window_phase(31, 32, lifecycle) == "TRANSITION_CLEANUP"
    assert classify_window_phase(33, 35, lifecycle) == "TRANSITION_CLEANUP"
    assert classify_window_phase(34, 35, lifecycle) == "RECOVERY"


def test_campaign_state_resumes_same_order_and_retries_same_case(tmp_path):
    path = tmp_path / "state.json"
    state = CampaignState(path, "a" * 64, ["A", "B"])
    assert state.next_case_id() == "A"
    assert state.start("A") == 1
    state.fail("A", "injector_not_effective")
    assert state.next_case_id() == "A"
    assert state.start("A") == 2
    state.finish("A", dataset_id="dataset-a", sha256="b" * 64)
    assert state.next_case_id() == "B"
    resumed = CampaignState(path, "a" * 64, ["A", "B"])
    assert resumed.next_case_id() == "B"
    with pytest.raises(CampaignStateError, match="order changed"):
        CampaignState(path, "a" * 64, ["B", "A"])
    with pytest.raises(CampaignStateError, match="manifest changed"):
        CampaignState(path, "c" * 64, ["A", "B"])
    corrupted = json.loads(path.read_text(encoding="utf-8"))
    corrupted["cases"]["B"]["status"] = "complete"
    path.write_text(json.dumps(corrupted), encoding="utf-8")
    with pytest.raises(CampaignStateError, match="fingerprint mismatch"):
        CampaignState(path, "a" * 64, ["A", "B"])


def test_campaign_collection_gates_do_not_include_soft_hard_ready_or_rca():
    config = load_campaign_config(CONFIG)
    qualification = config["load_qualification"]
    assert qualification["control_plane_results_are_gates"] is False
    gates = json.dumps(qualification["objective_gates"], sort_keys=True).lower()
    for forbidden in ("soft", "hard", "ready", "fista", "rca"):
        assert forbidden not in gates
    assert config["formal_scope"]["metric_contract"] == {
        "service": 9, "host": 4, "tcp_edge": 3,
    }
    assert config["formal_scope"]["dns_formal_enabled"] is False


def test_qualification_uses_only_objective_data_and_coverage_gates():
    config = load_campaign_config(CONFIG)
    observation = QualificationObservation(
        profile_id="multi-node-open-loop-55",
        target_arrival_rate_rps=55,
        measured_rps=54,
        normal_burst_aligned=True,
        source_gap_count=0,
        pod_restart_delta=0,
        topology_change_count=0,
        runtime_identity_change_count=0,
        business_error_rate=0.0005,
        worker_cpu_p95=0.60,
        worker_memory_p95=0.70,
        observed_tcp_edges=("a->b", "b->c"),
        required_tcp_edges=("a->b", "b->c"),
        projected_baseline_rows={"x": 600},
        projected_av_rows={"x": 1200},
        required_baseline_rows={"x": 600},
        required_av_rows={"x": 1000},
    )
    result = evaluate_qualification(
        observation, config["load_qualification"]["objective_gates"],
    )
    assert result["qualified"] is True
    overloaded = QualificationObservation(
        **{**observation.__dict__, "worker_cpu_p95": 0.75}
    )
    result = evaluate_qualification(
        overloaded, config["load_qualification"]["objective_gates"],
    )
    assert result["qualified"] is False
    assert result["reasons"] == ["worker_cpu_pressure"]


def test_physical_qualification_report_rejects_control_plane_gates():
    config = load_campaign_config(CONFIG)
    edges = tuple(sorted(
        f"{item['src']}->{item['dst']}" for item in config["formal_tcp_edges"]
    ))
    av_rows = {f"root-{index}": 300 for index in range(108)}
    baseline_rows = {
        **av_rows,
        **{f"parent-{index}": 300 for index in range(20)},
    }
    payload = {
        "profile_id": "multi-node-open-loop-40",
        "target_arrival_rate_rps": 40.0,
        "measured_rps": 39.0,
        "normal_burst_aligned": True,
        "source_gap_count": 0,
        "pod_restart_delta": 0,
        "topology_change_count": 0,
        "runtime_identity_change_count": 0,
        "business_error_rate": 0.0,
        "worker_cpu_p95": 0.5,
        "worker_memory_p95": 0.5,
        "observed_tcp_edges": edges,
        "required_tcp_edges": edges,
        "projected_baseline_rows": baseline_rows,
        "projected_av_rows": av_rows,
        "required_baseline_rows": {key: 100 for key in baseline_rows},
        "required_av_rows": {key: 100 for key in av_rows},
    }
    report = build_qualification_report(config, payload)
    assert report["qualified"] is True
    assert report["profile_fingerprint"] == fingerprint(
        config["load_qualification"]["profiles"][1]
    )
    with pytest.raises(QualificationReportError, match="cannot gate"):
        build_qualification_report(config, {**payload, "hard": 0})
    incomplete = copy.deepcopy(payload)
    incomplete["projected_baseline_rows"].pop("root-0")
    incomplete["required_baseline_rows"].pop("root-0")
    with pytest.raises(QualificationReportError, match="omits an A_v target"):
        build_qualification_report(config, incomplete)


def test_multinode_control_config_is_derived_as_11_3_15_and_108():
    campaign = load_campaign_config(CONFIG)
    base = FinalControlConfig.from_dict(yaml.safe_load((
        REPOSITORY / "configs/final_control.yaml"
    ).read_text(encoding="utf-8")))
    profile = campaign["load_qualification"]["profiles"][1]
    profile_fingerprint = candidate_profile_fingerprint(profile)
    control = build_multinode_control_config(
        campaign_config=campaign,
        base_control_config=base,
        load_profile_id=profile["profile_id"],
        load_profile_fingerprint=profile_fingerprint,
    )
    assert control.load_profile_id == "multi-node-open-loop-40"
    assert control.load_profile_fingerprint == profile_fingerprint
    assert len(control.calibration_required_root_coordinates) == 108
    assert len(control.formal_service_entity_ids) == 11
    assert len(control.formal_host_entity_ids) == 3
    assert len(control.formal_tcp_edge_entity_ids) == 15
    assert all(
        item.startswith("proberca-multinode-formal::")
        for item in control.calibration_required_root_coordinates
    )


def test_campaign_entrypoints_are_directly_executable():
    names = (
        "build_multinode_qualification_observation.py",
        "collect_multinode_campaign_dataset.py",
        "evaluate_multinode_load_qualification.py",
        "evaluate_multinode_pilot.py",
        "freeze_multinode_injectors.py",
        "freeze_multinode_load_profile.py",
        "generate_final_multinode_campaign.py",
        "generate_multinode_control_config.py",
        "generate_multinode_dataplane_configs.py",
        "install_multinode_campaign_load.py",
        "install_multinode_cluster_workloads.py",
        "merge_multinode_campaign_archives.py",
        "preflight_multinode_campaign.py",
        "render_multinode_workloads.py",
        "restore_campaign_dataset.py",
        "run_multinode_campaign_episode.py",
        "run_multinode_campaign_rehearsal.py",
        "seal_campaign_dataset.py",
        "upload_campaign_dataset.py",
    )
    for name in names:
        completed = subprocess.run(
            [sys.executable, str(REPOSITORY / "scripts" / name), "--help"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False,
        )
        assert completed.returncode == 0, (name, completed.stderr)


def test_post_collection_pilot_report_is_real_and_freezable():
    profile = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )["profiles"][0]
    effectiveness = {
        "dataset_id": "d" * 64,
        "profile_id": profile["profile_id"],
        "criterion_passed": True,
        "criterion_failures": [],
        "contamination_passed": True,
        "missing_contamination_checks": [],
        "contaminated_dimensions": [],
        "cleanup_passed": True,
        "baseline_evidence": {"primary_value": 1.0},
        "active_evidence": {"primary_value": 2.0},
        "accepted": True,
    }
    session = {
        "dataset_id": "d" * 64, "session_id": "s" * 64,
        "target": {"entity_kind": "service", "entity_id": "frontend"},
        "apply_confirmed_ns": 1, "cleanup_confirmed_ns": 2,
        "baseline_state_fingerprint": "a" * 64,
        "restored_state_fingerprint": "a" * 64,
        "execution_error": None,
    }
    report = build_real_pilot_report(
        effectiveness_report=effectiveness, injection_session=session,
        profile=profile, dataset_sha256="b" * 64,
    )
    assert report["simulated"] is False
    assert report["accepted"] is True
    assert report["baseline_state_fingerprint"] == \
        report["restored_state_fingerprint"]
    assert report["report_fingerprint"] == fingerprint({
        key: value for key, value in report.items()
        if key != "report_fingerprint"
    })


def test_one_episode_schedules_private_fault_without_label_leak(monkeypatch, tmp_path):
    module = _script_module(
        "run_multinode_campaign_episode_test",
        "run_multinode_campaign_episode.py",
    )
    nodes = REPOSITORY / "configs/final_multinode_nodes.example.yaml"
    output = tmp_path / "dataset"
    private = tmp_path / "private" / "session.json"
    target = TargetBinding(
        node_id="worker-1", entity_kind="service", entity_id="frontend",
        runtime_identity_fingerprint="a" * 64, attributes={},
    )
    monkeypatch.setattr(module, "_agent", lambda _inventory: object())
    monkeypatch.setattr(module, "_kubectl_json", lambda *_args: [])
    monkeypatch.setattr(module, "resolve_case_target", lambda **_kwargs: target)
    observed = {}

    def fake_collection(**kwargs):
        first = 100_000_000_000
        kwargs["on_capture_started"](first, "d" * 64)
        return {
            "dataset_id": "d" * 64, "window_count": 180,
            "first_window_start_ns": first,
        }

    def fake_injection(**kwargs):
        observed.update(kwargs)
        return {
            "schema_version": "probeRCA-scheduled-injection-v1",
            "target": kwargs["target"].as_dict(),
            "cleanup_passed": True,
        }

    monkeypatch.setattr(module, "collect_distributed_dataset", fake_collection)
    monkeypatch.setattr(module, "run_scheduled_injection", fake_injection)
    report = module.run_episode(
        repository=REPOSITORY, nodes_path=nodes, campaign_path=CONFIG,
        registry_path=(
            REPOSITORY / "configs/final_multinode_injector_candidates.yaml"
        ),
        case_id="P001", window_count=180, output=output,
        kubeconfig=tmp_path / "kubeconfig", context="formal",
        coordinate={
            "entity_kind": "service", "entity_id": "frontend",
            "metric": "cpu_usage_rate",
        },
        profile_id="service-cpu-v1", allow_candidate_registry=True,
        private_evidence_output=private,
    )
    assert report["fault_scheduled"] is True
    assert observed["apply_target_ns"] == 160_000_000_000
    assert observed["cleanup_target_ns"] == 220_000_000_000
    assert private.is_file()
    assert not output.exists()
    assert "frontend" not in json.dumps(report)
    with pytest.raises(ValueError, match="cannot be stored"):
        module.run_episode(
            repository=REPOSITORY, nodes_path=nodes, campaign_path=CONFIG,
            registry_path=(
                REPOSITORY / "configs/final_multinode_injector_candidates.yaml"
            ),
            case_id="T000001", window_count=180, output=output,
            kubeconfig=tmp_path / "kubeconfig", context="formal",
            coordinate={"entity_kind": "service", "entity_id": "frontend"},
            profile_id="service-cpu-v1", allow_candidate_registry=True,
            private_evidence_output=output / "session.json",
        )


def test_episode_integrity_gate_ignores_rca_but_requires_effect_and_cleanup():
    observation = EpisodeIntegrityObservation(
        normal_burst_aligned=True,
        window_count=180,
        expected_window_count=180,
        dataset_id_matches=True,
        sha256_verified=True,
        source_timestamps_valid=True,
        pod_restart_delta=0,
        topology_change_count=0,
        runtime_identity_change_count=0,
        injector_effective=True,
        non_target_contamination=False,
        cleanup_confirmed=True,
        collection_services_active=True,
    )
    assert evaluate_episode_integrity(observation)["accepted"] is True
    failed = EpisodeIntegrityObservation(
        **{**observation.__dict__, "injector_effective": False}
    )
    result = evaluate_episode_integrity(failed)
    assert result["accepted"] is False
    assert result["failed_checks"] == ["injector_effective"]


def test_test_protocol_forbids_label_reveal_before_prediction_seal(tmp_path):
    protocol = BlindEvaluationProtocol(tmp_path)
    with pytest.raises(EvaluationProtocolError, match="before predictions"):
        protocol.attest_labels_revealed(label_manifest_sha256="a" * 64)
    protocol.freeze(git_sha="b" * 40, config_fingerprint="c" * 64)
    prediction_fingerprint = protocol.seal_predictions([
        {"case_id": "T000001", "top_k": ["opaque-root-1"]},
    ])
    assert len(prediction_fingerprint) == 64
    protocol.attest_labels_revealed(label_manifest_sha256="d" * 64)
    protocol.record_score({"top1_accuracy": 0.5, "count": 2})
    assert json.loads((tmp_path / "test-protocol-state.json").read_text())["stage"] == "SCORED"
    assert (tmp_path / "sealed-test-predictions.json").is_file()
    assert (tmp_path / "test-score.json").is_file()


def test_load_profiles_have_reproducible_open_loop_rate_and_frozen_mix():
    config = load_campaign_config(CONFIG)
    profiles = config["load_qualification"]["profiles"]
    assert [item["target_arrival_rate_rps"] for item in profiles] == [25, 40, 55]
    assert [item["scale_percent"] for item in profiles] == [25, 40, 55]
    for item in profiles:
        assert item["workers"] == 8
        assert item["behavior_weights"] == {
            "browse_search_list": 40,
            "detail_recommendation_ad_currency": 25,
            "cart": 20,
            "checkout": 15,
        }


def test_load_profile_freeze_selects_highest_objective_qualified_profile():
    config = load_campaign_config(CONFIG)
    reports = []
    for profile in config["load_qualification"]["profiles"]:
        core = {
            "profile_id": profile["profile_id"],
            "qualified": profile["scale_percent"] in {25, 40},
            "reasons": [] if profile["scale_percent"] in {25, 40}
            else ["worker_cpu_pressure"],
        }
        reports.append({**core, "report_fingerprint": fingerprint(core)})
    frozen = freeze_load_profile(config, reports)
    assert frozen["selected_profile"]["scale_percent"] == 40
    assert len(frozen["load_profile_fingerprint"]) == 64
    reports[0]["qualified"] = False
    with pytest.raises(LoadProfileError, match="fingerprint"):
        freeze_load_profile(config, reports)


def test_open_loop_arrival_axis_does_not_depend_on_request_completion(monkeypatch):
    load = _script_module("multinode_open_loop_load_test", "multinode_open_loop_load.py")

    class Clock:
        value = 100.0

        def __call__(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds

    config = load.LoadConfig(
        base_url="http://frontend:80",
        target_arrival_rate_rps=10,
        workers=2,
        maximum_pending=64,
        request_timeout_sec=1,
        duration_sec=1,
        seed=77,
        behavior_weights={
            "browse_search_list": 40,
            "detail_recommendation_ad_currency": 25,
            "cart": 20,
            "checkout": 15,
        },
    )
    monkeypatch.setattr(
        load, "BEHAVIORS", {name: (lambda *_args: None) for name in load.EXPECTED_BEHAVIORS},
    )
    first_targets = []
    first_clock = Clock()
    first = load.run_open_loop(
        config, clock=first_clock, sleeper=first_clock.sleep,
        arrival_hook=lambda _index, target: first_targets.append(target),
        event_sink=lambda _event: None,
    )
    second_clock = Clock()
    second_targets = []
    second = load.run_open_loop(
        config, clock=second_clock, sleeper=second_clock.sleep,
        arrival_hook=lambda _index, target: second_targets.append(target),
        event_sink=lambda _event: None,
    )
    assert first_targets == second_targets
    assert all(left < right for left, right in zip(first_targets, first_targets[1:]))
    assert first["submitted"] == second["submitted"] == len(first_targets)
    assert first["failed"] == second["failed"] == 0


def test_open_loop_pending_queue_fails_closed_instead_of_dropping_arrivals():
    load = _script_module("multinode_open_loop_load_backpressure", "multinode_open_loop_load.py")

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds

    class Pending:
        def done(self):
            return False

    class Executor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, *_args):
            return Pending()

    config = load.LoadConfig(
        base_url="http://frontend:80", target_arrival_rate_rps=100,
        workers=1, maximum_pending=1, request_timeout_sec=1,
        duration_sec=1, seed=1,
        behavior_weights={
            "browse_search_list": 40,
            "detail_recommendation_ad_currency": 25,
            "cart": 20,
            "checkout": 15,
        },
    )
    clock = Clock()
    with pytest.raises(load.LoadBackpressureError, match="pending_limit"):
        load.run_open_loop(
            config, clock=clock, sleeper=clock.sleep,
            executor_factory=Executor, event_sink=lambda _event: None,
        )


def test_multinode_load_installer_uses_one_formal_source_and_profile(monkeypatch, tmp_path):
    installer = _script_module(
        "install_multinode_campaign_load_test", "install_multinode_campaign_load.py",
    )
    commands = []

    def capture(arguments, **kwargs):
        command = tuple(str(item) for item in arguments)
        commands.append((command, kwargs.get("input")))
        if "create" in command and "configmap" in command:
            return SimpleNamespace(stdout="apiVersion: v1\nkind: ConfigMap\n")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(installer, "_run", capture)
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("test", encoding="utf-8")
    installer.install(
        config=CONFIG, profile_id="multi-node-open-loop-40",
        kubeconfig=kubeconfig, context="campaign",
    )
    flattened = [command for command, _input in commands]
    assert sum("create" in command and "configmap" in command for command in flattened) == 1
    set_env = next(command for command in flattened if "set" in command and "env" in command)
    assert "TARGET_ARRIVAL_RATE_RPS=40" in set_env
    assert "WORKERS=8" in set_env
    assert "LOAD_PROFILE_ID=multi-node-open-loop-40" in set_env
    assert any(item.startswith("LOAD_PROFILE_FINGERPRINT=") for item in set_env)
    assert sum("scale" in command for command in flattened) == 1
    assert all("healthy-checkout" not in " ".join(command) for command in flattened)
    assert all("healthy-rpc" not in " ".join(command) for command in flattened)


def test_formal_manifest_requires_frozen_injectors_and_load_profile(tmp_path):
    candidate_path = REPOSITORY / "configs/final_multinode_injector_candidates.yaml"
    with pytest.raises(InjectorRegistryError, match="requires a frozen"):
        load_injector_registry(candidate_path, require_frozen=True)
    candidate = load_injector_registry(candidate_path, require_frozen=False)
    assert candidate["status"] == "candidate"
    assert _plan(True).public_manifest["executable"] is False

    frozen = copy.deepcopy(candidate)
    frozen.pop("registry_fingerprint")
    frozen["status"] = "frozen"
    for profile in frozen["profiles"]:
        profile["pilot_evidence"] = {
            "dataset_id": "pilot-" + profile["profile_id"],
            "sha256": "a" * 64,
            "effectiveness_passed": True,
            "cleanup_passed": True,
        }
    frozen_path = tmp_path / "frozen.yaml"
    frozen_path.write_text(yaml.safe_dump(frozen), encoding="utf-8")
    loaded = load_injector_registry(frozen_path, require_frozen=True)
    config = load_campaign_config(CONFIG)
    reports = []
    for profile in config["load_qualification"]["profiles"]:
        core = {"profile_id": profile["profile_id"], "qualified": True}
        reports.append({**core, "report_fingerprint": fingerprint(core)})
    load_profile = freeze_load_profile(config, reports)
    with pytest.raises(CampaignConfigError, match="requires both"):
        build_campaign_plan(
            config, commitment_secret=SECRET, frozen_injectors=loaded,
        )
    plan = build_campaign_plan(
        config, commitment_secret=SECRET, frozen_injectors=loaded,
        frozen_load_profile=load_profile,
    )
    assert plan.public_manifest["executable"] is True
    assert plan.public_manifest["injector_registry_fingerprint"] == \
        loaded["registry_fingerprint"]
    assert plan.private_manifest["frozen_injector_registry"]["status"] == "frozen"
    assert plan.public_manifest["load_profile_id"] == "multi-node-open-loop-55"
    assert plan.public_manifest["load_profile_fingerprint"] == \
        load_profile["load_profile_fingerprint"]
    assert plan.summary["rent_server_go_no_go"] == "NO_GO_PENDING_RESTORE_PREFLIGHT"


def test_offline_restore_verifies_every_listed_file_and_rejects_escape(tmp_path):
    source = tmp_path / "source"
    restored = tmp_path / "restored"
    expected_digest = None
    for root in (source, restored):
        (root / "normal").mkdir(parents=True)
        (root / "normal/windows.jsonl").write_text("one\ntwo\n", encoding="utf-8")
        digest = hashlib.sha256((root / "normal/windows.jsonl").read_bytes()).hexdigest()
        expected_digest = digest
        (root / "SHA256SUMS").write_text(
            f"{digest}  normal/windows.jsonl\n", encoding="utf-8",
        )
    assert verify_sha256s(source) == {
        "normal/windows.jsonl": expected_digest,
    }
    assert compare_restored_copy(source, restored) == {
        "verified_file_count": 1, "byte_identical": True,
    }
    (restored / "normal/windows.jsonl").write_text("changed\n", encoding="utf-8")
    with pytest.raises(RestoreVerificationError, match="SHA-256 mismatch"):
        compare_restored_copy(source, restored)
    (source / "SHA256SUMS").write_text(
        f"{'a' * 64}  ../outside\n", encoding="utf-8",
    )
    with pytest.raises(RestoreVerificationError, match="escapes"):
        verify_sha256s(source)


def _target():
    return TargetBinding(
        node_id="worker-1", entity_kind="service", entity_id="frontend",
        runtime_identity_fingerprint="1" * 64,
        attributes={"cgroup_path": "/sys/fs/cgroup/example"},
    )


def test_pilot_uses_direct_evidence_and_restores_exact_state():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    profile = registry["profiles"][0]
    agent = FakeWorkerAgent()
    report = run_injector_pilot(
        profile=profile, target=_target(), client=agent,
        dataset_id="pilot-service-cpu", dataset_sha256="a" * 64,
        simulated=True,
    )
    assert report["accepted"] is True
    assert report["effectiveness_passed"] is True
    assert report["contamination_passed"] is True
    assert report["cleanup_passed"] is True
    assert report["baseline_state_fingerprint"] == \
        report["restored_state_fingerprint"]
    serialized = json.dumps(report).lower()
    assert "top_k" not in serialized
    assert "soft_alert" not in serialized
    assert "hard_alert" not in serialized


def test_real_pilot_cannot_fall_back_to_simulated_agent_measurement():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    with pytest.raises(
        InjectorExecutionError, match="independent direct-evidence reader",
    ):
        run_injector_pilot(
            profile=registry["profiles"][0], target=_target(),
            client=FakeWorkerAgent(), dataset_id="pilot-service-cpu",
            dataset_sha256="a" * 64, simulated=False,
        )


def test_scheduled_injection_uses_actual_times_and_always_restores_state():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    clock = SimpleNamespace(value=1_000_000_000)

    def clock_ns():
        return clock.value

    def sleep(seconds):
        clock.value += int(seconds * 1_000_000_000)

    report = run_scheduled_injection(
        profile=registry["profiles"][0], target=_target(),
        client=FakeWorkerAgent(), dataset_id="a" * 64,
        apply_target_ns=2_000_000_000,
        cleanup_target_ns=3_000_000_000,
        clock_ns=clock_ns, sleeper=sleep,
    )
    assert report["accepted"] is True
    assert report["apply_command_start_ns"] == 2_000_000_000
    assert report["cleanup_command_start_ns"] == 3_000_000_000
    assert report["cleanup_passed"] is True


def test_scheduled_injection_cleans_up_after_apply_failure():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    delegate = FakeWorkerAgent()

    class FailingClient:
        def invoke(self, node_id, action, payload):
            if action == "apply":
                delegate.invoke(node_id, action, payload)
                raise RuntimeError("failure after mutation")
            return delegate.invoke(node_id, action, payload)

    clock = SimpleNamespace(value=1_000_000_000)
    with pytest.raises(InjectorExecutionError, match="failure after mutation"):
        run_scheduled_injection(
            profile=registry["profiles"][0], target=_target(),
            client=FailingClient(), dataset_id="b" * 64,
            apply_target_ns=2_000_000_000,
            cleanup_target_ns=3_000_000_000,
            clock_ns=lambda: clock.value,
            sleeper=lambda seconds: setattr(
                clock, "value", clock.value + int(seconds * 1_000_000_000)
            ),
        )
    state = delegate.invoke("worker-1", "snapshot", {
        "target": _target().as_dict(),
    })
    pristine = FakeWorkerAgent().invoke("worker-1", "snapshot", {
        "target": _target().as_dict(),
    })
    assert state["state_fingerprint"] == pristine["state_fingerprint"]


def test_direct_effectiveness_uses_actual_phases_without_rca(monkeypatch, tmp_path):
    dataset_id = "c" * 64
    records = []
    for sequence in range(1, 181):
        start = (sequence - 1) * 1_000_000_000
        value = 5.0 if 62 <= sequence <= 120 else 1.0
        metric = SimpleNamespace(
            metric_name="cpu_usage_rate", service_name="frontend",
            scope="service", valid=True, value=value,
        )
        records.append(SimpleNamespace(
            window_start_ns=start, window_end_ns=start + 1_000_000_000,
            node_metrics=(metric,), edge_metrics=(),
        ))

    class FakeNormal:
        @staticmethod
        def iter_windows():
            return iter(records)
    FakeNormal.dataset_id = dataset_id

    class FakeRaw:
        @staticmethod
        def iter_windows():
            return iter(())
    FakeRaw.manifest = {"dataset_id": dataset_id}

    monkeypatch.setattr(
        "proberca.campaign.effectiveness.CollectionArchive.load",
        lambda _path: FakeNormal(),
    )
    monkeypatch.setattr(
        "proberca.campaign.effectiveness.RawPrimitiveArchive",
        lambda _path: FakeRaw(),
    )
    report = evaluate_fault_effectiveness(
        normal_root=tmp_path / "normal",
        primitive_roots=[tmp_path / name for name in ("w1", "w2", "w3")],
        coordinate={
            "entity_kind": "service", "entity_id": "frontend",
            "metric": "cpu_usage_rate",
        },
        profile={
            "profile_id": "service-cpu-v1",
            "effectiveness_criterion": {
                "metric": "cpu_usage_rate",
                "direct_counter_lift_required": True,
            },
            "contamination_checks": ["pod_restart"],
        },
        injection_session={
            "planned_apply_ns": 60_000_000_000,
            "apply_command_start_ns": 60_000_000_000,
            "apply_confirmed_ns": 61_000_000_000,
            "planned_cleanup_ns": 120_000_000_000,
            "cleanup_command_start_ns": 120_000_000_000,
            "cleanup_confirmed_ns": 121_000_000_000,
            "cleanup_result": {}, "cleanup_passed": True,
        },
        contamination={"pod_restart": False},
    )
    assert report["accepted"] is True
    assert report["baseline_evidence"]["primary_value"] == 1.0
    assert report["active_evidence"]["primary_value"] == 5.0
    serialized = json.dumps(report).lower()
    assert "top_k" not in serialized
    assert "fista" not in serialized


def test_simulated_pilots_can_never_freeze_formal_injectors():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    reports = []
    for index, profile in enumerate(registry["profiles"]):
        target = TargetBinding(
            node_id="worker-1", entity_kind=(
                "tcp_edge" if profile["mechanism"].startswith("tcp_")
                else "host" if profile["mechanism"].startswith("host_")
                else "service"
            ),
            entity_id=(
                "frontend->cartservice"
                if profile["mechanism"].startswith("tcp_") else "target"
            ),
            runtime_identity_fingerprint=f"{index + 1:064x}", attributes={},
        )
        reports.append(run_injector_pilot(
            profile=profile, target=target, client=FakeWorkerAgent(),
            dataset_id=f"pilot-{index}", dataset_sha256="b" * 64,
            simulated=True,
        ))
    assert all(item["accepted"] for item in reports)
    with pytest.raises(InjectorExecutionError, match="simulated"):
        freeze_injector_registry(registry, reports)


def test_real_accepted_pilots_freeze_every_profile_without_changing_candidate():
    registry = load_injector_registry(
        REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    reports = []
    for profile in registry["profiles"]:
        report = {
            "profile_id": profile["profile_id"], "simulated": False,
            "accepted": True, "dataset_id": "pilot-" + profile["profile_id"],
            "sha256": "c" * 64, "report_fingerprint": "d" * 64,
            "effectiveness_passed": True, "contamination_passed": True,
            "cleanup_passed": True,
        }
        reports.append(report)
    frozen = freeze_injector_registry(registry, reports)
    assert registry["status"] == "candidate"
    assert frozen["status"] == "frozen"
    assert all(item["pilot_evidence"] for item in frozen["profiles"])


def test_injector_registry_rejects_unused_intensity_fields(tmp_path):
    path = REPOSITORY / "configs/final_multinode_injector_candidates.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    service_io = next(
        item for item in payload["profiles"] if item["mechanism"] == "service_io"
    )
    assert service_io["intensity"]["write_bytes_per_sec"] == 16 * 1024 * 1024
    assert service_io["intensity"]["fsync_each_block"] is True
    assert "direct" not in service_io["intensity"]
    service_io["intensity"]["ignored_setting"] = True
    bad = tmp_path / "injectors.yaml"
    bad.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(InjectorRegistryError, match="intensity fields mismatch"):
        load_injector_registry(bad, require_frozen=False)


def test_service_io_actor_receives_frozen_write_rate(monkeypatch, tmp_path):
    from proberca.campaign.worker_agent import LinuxWorkerBackend

    actor = tmp_path / "multinode_fault_actor.py"
    actor.write_text("# test actor\n", encoding="utf-8")
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "cgroup.procs").write_text("", encoding="ascii")
    commands = []

    def popen(arguments, **_kwargs):
        commands.append(arguments)
        return SimpleNamespace(pid=12345)

    backend = LinuxWorkerBackend(
        node_id="worker-1", state_root=tmp_path / "state",
        work_root=tmp_path / "work", actor_path=actor, popen=popen,
    )
    monkeypatch.setattr(backend, "_cgroup_path", lambda _target: cgroup)
    profile = next(
        item for item in load_injector_registry(
            REPOSITORY / "configs/final_multinode_injector_candidates.yaml",
            require_frozen=False,
        )["profiles"] if item["mechanism"] == "service_io"
    )
    journal = {}
    backend._spawn_actor("service_io", {
        "session_id": "1" * 64,
        "target": _target().as_dict(),
        "intensity": profile["intensity"],
    }, journal)
    assert "--bytes-per-second" in commands[0]
    assert commands[0][commands[0].index("--bytes-per-second") + 1] == \
        str(16 * 1024 * 1024)


def test_ssh_transport_pins_host_key_and_never_uses_remote_shell(tmp_path):
    identity = tmp_path / "id"
    known_hosts = tmp_path / "known_hosts"
    identity.write_text("key", encoding="utf-8")
    known_hosts.write_text("host key", encoding="utf-8")
    captured = {}

    def runner(command, **kwargs):
        request = json.loads(kwargs["input"])
        captured["command"] = command
        captured["request"] = request
        core = {
            "request_id": request["request_id"], "ok": True,
            "result": {"state_fingerprint": "a" * 64},
        }
        response = {**core, "response_fingerprint": fingerprint(core)}
        return SimpleNamespace(
            returncode=0, stdout=json.dumps(response).encode(), stderr=b"",
        )

    client = SSHAgentClient([
        RemoteNode("worker-1", "10.0.0.2", "ubuntu", 22, identity),
    ], known_hosts_file=known_hosts, runner=runner)
    result = client.invoke("worker-1", "snapshot", {"target": "opaque"})
    assert result["state_fingerprint"] == "a" * 64
    command = captured["command"]
    assert "StrictHostKeyChecking=yes" in command
    assert f"UserKnownHostsFile={known_hosts.resolve()}" in command
    assert command[-8:] == [
        "sudo", "-n", "env", "PYTHONPATH=/opt/proberca/current",
        "PROBERCA_NODE_ID=worker-1", "python3",
        "/opt/proberca/current/scripts/multinode_fault_agent.py", "--request-stdin",
    ]
    assert all(item not in command for item in ("sh", "bash", "-c"))
    with pytest.raises(RemoteAgentError, match="allow-listed"):
        client.invoke("worker-1", "arbitrary", {})


def test_object_store_upload_independent_readback_and_offline_restore(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "normal").mkdir()
    (source / "normal/windows.jsonl").write_text("one\ntwo\n", encoding="utf-8")
    digest = hashlib.sha256((source / "normal/windows.jsonl").read_bytes()).hexdigest()
    (source / "SHA256SUMS").write_text(
        f"{digest}  normal/windows.jsonl\n", encoding="utf-8",
    )
    store = FilesystemArchiveStore(tmp_path / "object-store")
    upload = upload_and_verify_dataset(source, store, object_prefix="campaign/D1")
    assert upload["independent_readback_passed"] is True
    assert upload["verified_file_count"] == 1
    restored = restore_dataset(
        store, object_prefix="campaign/D1", output_root=tmp_path / "restored",
    )
    assert restored["offline_restore_passed"] is True
    assert (tmp_path / "restored/normal/windows.jsonl").read_bytes() == \
        (source / "normal/windows.jsonl").read_bytes()
    with pytest.raises(ArchiveStoreError, match="already exists"):
        upload_and_verify_dataset(source, store, object_prefix="campaign/D1")


def test_campaign_executor_retries_same_case_and_requires_executable_manifest(tmp_path):
    public = {
        "executable": True, "manifest_fingerprint": "e" * 64,
        "cases": [{"case_id": "P001"}, {"case_id": "P002"}],
    }
    state = CampaignState(tmp_path / "state.json", "e" * 64, ["P001", "P002"])
    executor = CampaignExecutor(public, state)

    def fail(_case, _attempt):
        raise RuntimeError("injected interruption")

    with pytest.raises(RuntimeError, match="interruption"):
        executor.run_next(fail)
    attempts = []

    def pass_case(case, attempt):
        attempts.append((case["case_id"], attempt))
        return SealedCaseResult("dataset-" + case["case_id"], "f" * 64)

    assert executor.run_next(pass_case) == "P001"
    assert executor.run_next(pass_case) == "P002"
    assert attempts == [("P001", 2), ("P002", 1)]
    assert executor.run_next(pass_case) is None
    with pytest.raises(CampaignExecutionError, match="not executable"):
        CampaignExecutor({**public, "executable": False}, state)


def test_preflight_separates_pre_rent_code_readiness_from_physical_go():
    code_only = CampaignPreflightObservation(
        repository_tests_passed=True,
        deterministic_manifests_passed=True,
        cms_roundtrip_passed=True,
        interruption_resume_rehearsal_passed=True,
        fake_agent_rehearsal_passed=True,
        filesystem_restore_rehearsal_passed=True,
        object_store_adapter_tested=True,
    )
    report = evaluate_campaign_preflight(code_only)
    assert report["pre_rent_code_ready"] is True
    assert report["formal_campaign_go"] is False
    assert report["status"] == "INFRASTRUCTURE_AND_REAL_PILOTS_PENDING"
    nodes = tuple({
        "node_id": f"node-{index}", "cgroup_v2": True, "btf": True,
        "clock_synchronized": True, "required_tools": True,
        "free_bytes": 500 * 1024 ** 3, "required_free_bytes": 100 * 1024 ** 3,
        "clock_offset_within_10ms": True, "swap_disabled": True,
        "logical_cpu_count": 8, "required_logical_cpu_count": 8,
        "memory_bytes": 16 * 1024 ** 3,
        "required_memory_bytes": 16 * 1024 ** 3,
        "role": "controller" if index == 0 else "worker",
        "fault_interface_ready": True, "host_fault_cgroup_ready": True,
        "node_exporter_ready": True, "beyla_ready": True,
    } for index in range(4))
    full = CampaignPreflightObservation(
        **{
            **code_only.__dict__, "node_reports": nodes,
            "object_store_readback_passed": True,
            "frozen_image_digests": True, "placement_verified": True,
            "injector_registry_frozen": True, "load_profile_frozen": True,
            "real_pilot_count": 12,
            "central_free_bytes": 500 * 1024 ** 3,
        }
    )
    report = evaluate_campaign_preflight(full)
    assert report["formal_campaign_go"] is True
    assert report["status"] == "FORMAL_CAMPAIGN_GO"


def test_worker_agent_rejects_unfingerprinted_or_non_allowlisted_operations():
    class Backend:
        node_id = "worker-1"

        @staticmethod
        def preflight(payload):
            return {"payload": payload}

    agent = WorkerAgent(Backend())
    core = {
        "schema_version": "probeRCA-worker-agent-request-v1",
        "request_id": "r1", "node_id": "worker-1", "action": "preflight",
        "payload": {},
    }
    request = {**core, "request_fingerprint": fingerprint(core)}
    assert agent.dispatch(request) == {"payload": {}}
    with pytest.raises(WorkerAgentError, match="fingerprint"):
        agent.dispatch({**request, "request_fingerprint": "0" * 64})
    arbitrary_core = {**core, "action": "shell"}
    with pytest.raises(WorkerAgentError, match="allow-listed"):
        agent.dispatch({
            **arbitrary_core,
            "request_fingerprint": fingerprint(arbitrary_core),
        })


def test_worker_resolves_container_to_immutable_cgroup_and_netns(
    monkeypatch, tmp_path,
):
    cgroup = tmp_path / ("cri-containerd-" + "1" * 64 + ".scope")
    cgroup.mkdir()
    (cgroup / "cgroup.procs").write_text(f"{os.getpid()}\n", encoding="ascii")
    from proberca.campaign.worker_agent import LinuxWorkerBackend
    backend = LinuxWorkerBackend(node_id="worker-1")
    monkeypatch.setattr(backend, "_container_cgroup", lambda _value: cgroup)
    resolved = backend.resolve({
        "entity_kind": "tcp_edge",
        "entity_id": "frontend->cartservice",
        "container_id": "containerd://" + "1" * 64,
        "destination_ip": "10.96.0.10",
        "destination_port": 7070,
        "interface": "eth0",
    })
    assert resolved["attributes"]["cgroup_path"] == str(cgroup.resolve())
    assert resolved["attributes"]["network_pid"] == os.getpid()
    assert resolved["attributes"]["destination_ip"] == "10.96.0.10"
    assert len(resolved["runtime_identity_fingerprint"]) == 64


def test_tcp_injector_uses_selective_root_netem_not_invalid_tc_action(
    monkeypatch, tmp_path,
):
    from proberca.campaign.worker_agent import LinuxWorkerBackend

    backend = LinuxWorkerBackend(
        node_id="worker-1", state_root=tmp_path / "state",
        work_root=tmp_path / "work",
    )
    commands = []
    monkeypatch.setattr(
        backend, "_run",
        lambda arguments, **_kwargs: commands.append(tuple(arguments)) or "",
    )
    target = {
        "attributes": {
            "interface": "eth0", "destination_ip": "10.96.0.20",
            "destination_port": 3550,
        },
    }
    journal = {
        "baseline_state": {
            "traffic_control": {
                "qdisc": [{"kind": "noqueue"}],
                "filters": {"ingress": [], "egress": []},
            },
        },
    }
    backend._apply_tc("tcp_latency", {
        "session_id": "2" * 64, "target": target,
        "intensity": {"delay_ms": 100},
    }, journal)
    rendered = [" ".join(item) for item in commands]
    assert any("root handle 1: prio" in item for item in rendered)
    assert any("parent 1:3 handle 30: netem delay 100ms" in item for item in rendered)
    assert any(
        "flower dst_ip 10.96.0.20 ip_proto tcp dst_port 3550 flowid 1:3"
        in item for item in rendered
    )
    assert all("action netem" not in item for item in rendered)
    assert journal["tc_mode"] == "root_prio_netem"


def test_label_side_target_resolver_binds_formal_edge_without_hardcoding():
    config = load_campaign_config(CONFIG)
    inventory = {
        "nodes": [
            {
                "node_id": worker, "role": "worker",
                "kubernetes_node_name": worker, "fault_interface": "ens5",
            }
            for worker in config["workers"]
        ]
    }
    namespace = config["formal_scope"]["namespace"]
    pods = [{
        "metadata": {
            "namespace": namespace,
            "labels": {"app": "recommendationservice"},
        },
        "spec": {
            "nodeName": "worker-2",
            "containers": [{"name": "server"}],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "server", "ready": True, "restartCount": 0,
                "containerID": "containerd://" + "2" * 64,
            }],
        },
    }]
    services = [{
        "metadata": {
            "namespace": namespace, "name": "productcatalogservice",
        },
        "spec": {
            "clusterIP": "10.96.0.20",
            "ports": [{"port": 3550, "protocol": "TCP"}],
        },
    }]

    class Client:
        def invoke(self, node_id, action, payload):
            assert node_id == "worker-2"
            assert action == "resolve"
            assert payload["container_id"] == "containerd://" + "2" * 64
            assert payload["destination_ip"] == "10.96.0.20"
            return {
                "node_id": node_id,
                "entity_kind": payload["entity_kind"],
                "entity_id": payload["entity_id"],
                "runtime_identity_fingerprint": "3" * 64,
                "attributes": {
                    "cgroup_path": "/sys/fs/cgroup/container",
                    "runtime_identity": {},
                },
            }

    target = resolve_case_target(
        coordinate={
            "entity_kind": "tcp_edge",
            "entity_id": "recommendationservice->productcatalogservice",
        },
        campaign_config=config,
        node_inventory=inventory,
        pods=pods,
        services=services,
        client=Client(),
    )
    assert target.node_id == "worker-2"
    assert target.entity_id == \
        "recommendationservice->productcatalogservice"


def test_multinode_dataplane_render_is_worker_local_and_formally_complete(tmp_path):
    renderer = _script_module(
        "generate_multinode_dataplane_configs_test",
        "generate_multinode_dataplane_configs.py",
    )
    nodes = yaml.safe_load((
        REPOSITORY / "configs/final_multinode_nodes.example.yaml"
    ).read_text(encoding="utf-8"))
    dataplane_hosts = {
        "worker-1": "192.0.2.11",
        "worker-2": "192.0.2.12",
        "worker-3": "192.0.2.13",
    }
    for node in nodes["nodes"]:
        if node["node_id"] in dataplane_hosts:
            node["dataplane_host"] = dataplane_hosts[node["node_id"]]
    # Make generated paths self-contained; no connection is attempted.
    nodes_path = tmp_path / "nodes.yaml"
    nodes_path.write_text(yaml.safe_dump(nodes), encoding="utf-8")
    output = tmp_path / "rendered"
    report = renderer.render(REPOSITORY, CONFIG, nodes_path, output)
    assert report["worker_count"] == 3
    assert report["formal_service_count"] == 11
    assert report["formal_tcp_edge_count"] == 15
    assert report["expected_records_per_window"] == 156
    union = set()
    projected_edges = set()
    for worker, expected_count, expected_edge_count in (
        ("worker-1", 4, 8), ("worker-2", 4, 7), ("worker-3", 3, 0),
    ):
        exporter_payload = yaml.safe_load((
            output / worker / "primitive-exporter.yaml"
        ).read_text(encoding="utf-8"))
        config = FinalPrimitiveExporterConfig.from_dict(exporter_payload)
        assert config.schema_version == MULTINODE_PRIMITIVE_EXPORTER_SCHEMA_VERSION
        assert config.runtime_mode == "host"
        assert config.monitored_node_name == worker
        assert len(config.local_services) == expected_count
        assert len(config.include_services) == 11
        assert len(config.formal_tcp_edge_entity_ids) == expected_edge_count
        union.update(config.local_services)
        source = yaml.safe_load((
            output / worker / "live-collector.yaml"
        ).read_text(encoding="utf-8"))
        source_config = FinalLiveCollectorConfig.from_dict(source)
        assert source_config.schema_version == \
            MULTINODE_COLLECTOR_CONFIG_SCHEMA_VERSION
        assert source_config.projection_mode == "worker-local"
        assert source_config.projection_owner == worker
        assert len(source_config.projection_service_entity_ids) == expected_count
        assert len(source_config.formal_service_entity_ids) == 11
        assert len(source_config.formal_tcp_edge_entity_ids) == expected_edge_count
        assert len(source_config.topology_tcp_edge_entity_ids) == 15
        assert all(
            item.split("::")[2].split("->", 1)[0]
            in {value.split("::")[2] for value in source_config.projection_service_entity_ids}
            for item in source_config.formal_tcp_edge_entity_ids
        )
        projected_edges.update(source_config.formal_tcp_edge_entity_ids)
        assert all(
            f'worker="{worker}"' in item["promql"]
            and "worker" in item["optional_labels"]
            for item in source["prometheus"]["queries"]
        )
        burst = FinalLiveBurstConfig.from_dict(yaml.safe_load((
            output / worker / "live-burst.yaml"
        ).read_text(encoding="utf-8")))
        assert burst.schema_version == "probeRCA-final-live-burst-v3"
        assert burst.runtime_mode == "host"
        assert burst.monitored_node_name == worker
    assert len(union) == 11
    assert len(projected_edges) == 15
    scrape = yaml.safe_load((
        output / "prometheus-scrape-job.yaml"
    ).read_text(encoding="utf-8"))
    assert scrape["honor_timestamps"] is True
    assert scrape["scrape_interval"] == "250ms"
    assert len(scrape["static_configs"]) == 3
    assert {
        item["targets"][0] for item in scrape["static_configs"]
    } == {f"{value}:9477" for value in dataplane_hosts.values()}
    full_prometheus = yaml.safe_load((output / "prometheus.yaml").read_text(
        encoding="utf-8"
    ))
    assert [item["job_name"] for item in full_prometheus["scrape_configs"]] == [
        "proberca-final-primitives-multinode",
        "proberca-worker-node-exporter",
    ]
    assert len(full_prometheus["scrape_configs"][1]["static_configs"]) == 3
    assert {
        item["targets"][0]
        for item in full_prometheus["scrape_configs"][1]["static_configs"]
    } == {f"{value}:9100" for value in dataplane_hosts.values()}


def _formal_metric_records(worker, services, edges, contract, timestamp_ns=0):
    roles = contract["normal_metric_roles"]
    nodes = []
    output_edges = []
    family = {
        "request_rate": "request", "request_failure_rate": "request",
        "request_latency_p95": "request", "cpu_usage_rate": "cpu",
        "cpu_throttle_ratio": "cpu", "memory_working_set_ratio": "memory",
        "io_psi": "io", "futex_wait_time_rate": "lock",
        "local_socket_failure_rate": "net_local", "cpu_psi": "cpu",
        "memory_psi": "memory", "nic_drop_error_rate": "net_local",
    }
    common = {
        "schema_version": METRIC_RECORD_SCHEMA_VERSION,
        "timestamp_ns": timestamp_ns, "window_sec": 1,
        "cluster_id": "proberca-multinode-formal", "value": 1.0,
        "valid": True, "invalid_reason": None, "sample_count": 25,
        "coverage": 1.0, "event_loss_rate": 0.0,
        "mapping_quality": 1.0, "source": "final_window_aggregation",
        "histogram_upper_bound": None, "histogram_is_inf_bucket": False,
        "histogram_is_cumulative": None,
    }
    for service in services:
        for role in roles:
            if role["entity_type"] != "service":
                continue
            nodes.append(NodeMetricRecord(
                **common, node_name=worker, namespace="online-boutique",
                service_name=service, pod_uid=None, container_id=None,
                metric_family=family[role["metric_name"]],
                metric_name=role["metric_name"], unit=role["unit"],
                metric_kind=role["metric_kind"], scope="service",
                quantile=role["quantile"],
            ))
    for role in roles:
        if role["entity_type"] != "host":
            continue
        nodes.append(NodeMetricRecord(
            **common, node_name=worker, namespace="online-boutique",
            service_name=f"host-{worker}", pod_uid=None, container_id=None,
            metric_family=family[role["metric_name"]],
            metric_name=role["metric_name"], unit=role["unit"],
            metric_kind=role["metric_kind"], scope="node",
            quantile=role["quantile"],
        ))
    for edge in edges:
        for role in roles:
            if role["entity_type"] != "edge":
                continue
            output_edges.append(EdgeMetricRecord(
                **common, namespace="online-boutique",
                src_service=edge["src"], dst_service=edge["dst"],
                src_pod_uid=None, dst_pod_uid=None, src_node=worker,
                dst_node=None, protocol="tcp",
                metric_name=role["metric_name"], unit=role["unit"],
                metric_kind=role["metric_kind"], scope="service_pair",
                quantile=role["quantile"],
            ))
    return tuple(nodes), tuple(output_edges)


def _formal_topology(campaign, timestamp_ns=0):
    services = sorted(
        f"online-boutique::{service}"
        for worker in campaign["workers"]
        for service in campaign["placement"][worker]
    )
    calls = [TopologyEdge(
        item["src"], item["dst"], "call", "online-boutique",
        "online-boutique", "tcp", directed=True,
    ) for item in campaign["formal_tcp_edges"]]
    placements = [ServiceNodePlacement(
        namespace="online-boutique", service_name=service,
        node_name=worker, pod_uid=f"pod-{service}",
    ) for worker in campaign["workers"] for service in campaign["placement"][worker]]
    structure = {
        "cluster": campaign["formal_scope"]["cluster_id"],
        "services": services, "calls": [item.to_dict() for item in calls],
        "hosts": [], "bindings": [],
    }
    return TopologySnapshot(
        schema_version="1.0", snapshot_id=fingerprint({
            "structure_fingerprint": fingerprint(structure),
            "runtime_identity_fingerprints": [],
            "window_start_ns": timestamp_ns,
            "window_end_ns": timestamp_ns + 1_000_000_000,
        }),
        valid_from_ns=timestamp_ns, valid_to_ns=timestamp_ns + 1_000_000_000,
        cluster_id=campaign["formal_scope"]["cluster_id"], services=services,
        call_edges=calls, host_edges=[], resource_edges=[],
        service_nodes=placements, service_resources=[],
        structure_fingerprint=fingerprint(structure),
        inventory_revision_id=fingerprint({"inventory": "same"}),
        resource_version_vector={"Pod": fingerprint({"rv": "1"})},
        runtime_identity_fingerprints=[],
        service_runtime_identity_fingerprints={},
        call_edge_provider_fingerprint=fingerprint({"worker": "partial"}),
        topology_build_issues=[],
    )


def test_three_worker_projection_archives_merge_to_one_formal_window(tmp_path):
    campaign = load_campaign_config(CONFIG)
    contract = yaml.safe_load((
        REPOSITORY / "configs/final_collection_contract.yaml"
    ).read_text(encoding="utf-8"))
    dataset_id = fingerprint({"dataset": "three-worker-projection"})
    topology = _formal_topology(campaign)
    formal_services = sorted(
        f"proberca-multinode-formal::online-boutique::{service}"
        for values in campaign["placement"].values() for service in values
    )
    topology_edges = sorted(
        f"proberca-multinode-formal::online-boutique::"
        f"{item['src']}->{item['dst']}::tcp"
        for item in campaign["formal_tcp_edges"]
    )
    normal_roots = []
    burst_roots = []
    for worker in campaign["workers"]:
        local_services = campaign["placement"][worker]
        local_edges = [
            item for item in campaign["formal_tcp_edges"]
            if item["src"] in local_services
        ]
        local_edge_ids = sorted(
            f"proberca-multinode-formal::online-boutique::"
            f"{item['src']}->{item['dst']}::tcp" for item in local_edges
        )
        projection = {
            "schema_version": "probeRCA-worker-projection-v1",
            "owner": worker, "node_name": worker,
            "service_entity_ids": sorted(
                f"proberca-multinode-formal::online-boutique::{item}"
                for item in local_services
            ),
            "tcp_edge_entity_ids": local_edge_ids,
            "formal_service_entity_ids": formal_services,
            "topology_tcp_edge_entity_ids": topology_edges,
        }
        projection["projection_fingerprint"] = fingerprint(projection)
        metadata = {
            "collector_build_fingerprint": fingerprint({"worker": worker}),
            "aggregation_config_fingerprint": contract[
                "aggregation_config_fingerprint"
            ],
            "burst_config_fingerprint": contract["burst_config_fingerprint"],
        }
        nodes, edges = _formal_metric_records(
            worker, local_services, local_edges, contract,
        )
        window = CollectedWindow.create(
            sequence=1, window_start_ns=0, window_end_ns=1_000_000_000,
            node_metrics=nodes, edge_metrics=edges,
            topology_events=(topology,), burst_evidence=(),
            residual_source_record_ids=(
                "source:" + fingerprint({"raw": worker}),
            ), collection_metadata=metadata,
        )
        normal_root = tmp_path / worker / "normal"
        normal = CollectionArchiveWriter(
            normal_root, dataset_id=dataset_id, collection_contract=contract,
            source_description=contract["source_description"],
            collection_metadata=metadata, projection=projection,
        )
        normal.append(window)
        projected = normal.seal()
        assert projected.schema_version == \
            PROJECTED_COLLECTION_ARCHIVE_SCHEMA_VERSION
        assert tuple(projected.iter_windows()) == (window,)
        normal_roots.append(normal_root)
        burst_root = tmp_path / worker / "burst"
        burst_source = fingerprint({"burst": worker})
        burst = BurstArchiveWriter(
            burst_root, dataset_id=dataset_id,
            cluster_id="proberca-multinode-formal",
            event_source_fingerprint=burst_source,
            burst_config_fingerprint=contract["burst_config_fingerprint"],
        )
        burst.append(RawBurstWindow.create(
            sequence=1, window_start_ns=0, window_end_ns=1_000_000_000,
            cluster_id="proberca-multinode-formal", samples=(),
            event_source_fingerprint=burst_source,
            burst_config_fingerprint=contract["burst_config_fingerprint"],
            event_loss_rate=0.0,
        ))
        burst.seal()
        burst_roots.append(burst_root)
    report = merge_worker_archives(
        normal_roots=normal_roots, burst_roots=burst_roots,
        normal_output=tmp_path / "merged-normal",
        burst_output=tmp_path / "merged-burst",
    )
    merged = CollectionArchive.load(tmp_path / "merged-normal")
    merged_burst = BurstArchive.load(tmp_path / "merged-burst")
    output = tuple(merged.iter_windows())
    assert report["formal_records_per_window"] == 156
    assert merged.projection is None
    assert len(output[0].node_metrics) == 11 * 9 + 3 * 4
    assert len(output[0].edge_metrics) == 15 * 3
    assert merged_burst.window_count == 1


def test_qualification_observation_is_derived_from_archives_without_alerts(tmp_path):
    campaign = copy.deepcopy(load_campaign_config(CONFIG))
    campaign["load_qualification"]["duration_seconds_per_profile"] = 12
    campaign["timing"]["healthy_seconds"] = 24
    profile = campaign["load_qualification"]["profiles"][0]
    control = build_multinode_control_config(
        campaign_config=campaign,
        base_control_config=FinalControlConfig.from_dict(yaml.safe_load((
            REPOSITORY / "configs/final_control.yaml"
        ).read_text(encoding="utf-8"))),
        load_profile_id=profile["profile_id"],
        load_profile_fingerprint=candidate_profile_fingerprint(profile),
    )
    contract = control.collection_contract
    metadata = {
        "collector_build_fingerprint": fingerprint({"collector": "merged"}),
        "aggregation_config_fingerprint": contract[
            "aggregation_config_fingerprint"
        ],
        "burst_config_fingerprint": contract["burst_config_fingerprint"],
    }
    dataset_id = fingerprint({"dataset": "qualification-observation"})
    normal_root = tmp_path / "normal"
    normal = CollectionArchiveWriter(
        normal_root, dataset_id=dataset_id, collection_contract=contract,
        source_description=contract["source_description"],
        collection_metadata=metadata,
    )
    burst_root = tmp_path / "burst"
    event_source = fingerprint({"source": "burst"})
    burst = BurstArchiveWriter(
        burst_root, dataset_id=dataset_id,
        cluster_id=campaign["formal_scope"]["cluster_id"],
        event_source_fingerprint=event_source,
        burst_config_fingerprint=contract["burst_config_fingerprint"],
    )
    for sequence in range(1, 13):
        start = (sequence - 1) * 1_000_000_000
        node_records, edge_records = [], []
        for worker in campaign["workers"]:
            local_services = campaign["placement"][worker]
            local_edges = [
                item for item in campaign["formal_tcp_edges"]
                if item["src"] in local_services
            ]
            nodes, edges = _formal_metric_records(
                worker, local_services, local_edges, contract, start,
            )
            node_records.extend(nodes)
            edge_records.extend(edges)
        normal.append(CollectedWindow.create(
            sequence=sequence, window_start_ns=start,
            window_end_ns=start + 1_000_000_000,
            node_metrics=node_records, edge_metrics=edge_records,
            topology_events=(_formal_topology(campaign, start),),
            burst_evidence=(),
            residual_source_record_ids=(
                "source:" + fingerprint({"sequence": sequence}),
            ),
            collection_metadata=metadata,
        ))
        burst.append(RawBurstWindow.create(
            sequence=sequence, window_start_ns=start,
            window_end_ns=start + 1_000_000_000,
            cluster_id=campaign["formal_scope"]["cluster_id"], samples=(),
            event_source_fingerprint=event_source,
            burst_config_fingerprint=contract["burst_config_fingerprint"],
            event_loss_rate=0.0,
        ))
    normal.seal()
    burst.seal()
    telemetry = {
        "measured_rps": 25.0, "business_error_rate": 0.0,
        "worker_cpu_p95": 0.5, "worker_memory_p95": 0.5,
        "pod_restart_delta": 0,
    }
    observation = build_archive_qualification_observation(
        normal_root=normal_root, burst_root=burst_root,
        campaign_config=campaign, control_config=control,
        profile_id=profile["profile_id"], telemetry=telemetry,
    )
    assert observation["normal_burst_aligned"] is True
    assert observation["source_gap_count"] == 0
    assert observation["topology_change_count"] == 0
    assert observation["runtime_identity_change_count"] == 0
    assert len(observation["observed_tcp_edges"]) == 15
    assert len(observation["projected_av_rows"]) == 108
    assert set(observation["projected_av_rows"]) <= set(
        observation["projected_baseline_rows"]
    )
    with pytest.raises(QualificationObservationError, match="control output"):
        build_archive_qualification_observation(
            normal_root=normal_root, burst_root=burst_root,
            campaign_config=campaign, control_config=control,
            profile_id=profile["profile_id"],
            telemetry={**telemetry, "hard": 0},
        )


def test_multinode_workload_render_pins_images_placement_and_email_probe(tmp_path):
    renderer = _script_module(
        "render_multinode_workloads_test", "render_multinode_workloads.py",
    )
    output = tmp_path / "workloads.yaml"
    report = renderer.render(
        REPOSITORY, CONFIG,
        REPOSITORY / "configs/final_multinode_nodes.example.yaml",
        REPOSITORY / "configs/final_multinode_images.yaml",
        output,
    )
    assert report["formal_service_count"] == 11
    assert report["excluded_loadgenerator"] is True
    assert report["images_pinned"] is True
    assert report["namespace"] == "online-boutique"
    documents = list(yaml.safe_load_all(output.read_text(encoding="utf-8")))
    namespaces = [
        item for item in documents
        if item and item.get("kind") == "Namespace"
    ]
    assert namespaces == [{
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {"name": "online-boutique"},
    }]
    assert all(
        item["metadata"].get("namespace") == "online-boutique"
        for item in documents
        if item and item.get("kind") != "Namespace"
    )
    deployments = {
        item["metadata"]["name"]: item for item in documents
        if item and item.get("kind") == "Deployment"
    }
    assert "loadgenerator" not in deployments
    assert len({
        name for name in deployments if name in {
            service for values in load_campaign_config(CONFIG)["placement"].values()
            for service in values
        }
    }) == 11
    email = deployments["emailservice"]["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert email["startupProbe"] == {
        "grpc": {"port": 8080}, "periodSeconds": 2,
        "timeoutSeconds": 3, "failureThreshold": 30,
    }
    assert email["readinessProbe"]["grpc"]["port"] == 8080
    assert email["livenessProbe"]["grpc"]["port"] == 8080


def test_multinode_workload_source_hash_is_newline_stable(tmp_path):
    renderer = _script_module(
        "render_multinode_workloads_newline_test", "render_multinode_workloads.py",
    )
    linux = tmp_path / "linux.yaml"
    windows = tmp_path / "windows.yaml"
    linux.write_bytes(b"kind: Service\nmetadata:\n  name: frontend\n")
    windows.write_bytes(b"kind: Service\r\nmetadata:\r\n  name: frontend\r\n")
    assert renderer._canonical_text_sha(linux) == renderer._canonical_text_sha(
        windows
    )


def test_block_completion_tracepoint_uses_kernel_event_class_context():
    for relative in (
        "bpf/final_burst/final_burst.bpf.c",
        "bpf/block/block.bpf.c",
    ):
        source = (REPOSITORY / relative).read_text(encoding="utf-8")
        assert "struct trace_event_raw_block_rq_completion *" in source
        assert "struct trace_event_raw_block_rq_complete *" not in source


def test_burst_checkpoint_counters_are_initialized_before_loss_map_lookup():
    source = (
        REPOSITORY / "bpf/user/proberca_final_burst_loader.c"
    ).read_text(encoding="utf-8")
    checkpoint = source.split("static int write_checkpoint(", 1)[1].split(
        "static int expire_dns(", 1,
    )[0]
    assert "uint64_t emitted = 0;" in checkpoint
    assert "uint64_t reserve_failed = 0;" in checkpoint
    assert "if (result != 0)\n        return result;" in checkpoint


def test_multinode_cluster_installer_uses_rendered_workloads_and_worker_beyla(
    monkeypatch, tmp_path,
):
    renderer = _script_module(
        "render_multinode_workloads_install_test", "render_multinode_workloads.py",
    )
    installer = _script_module(
        "install_multinode_cluster_test", "install_multinode_cluster_workloads.py",
    )
    workloads = tmp_path / "workloads.yaml"
    renderer.render(
        REPOSITORY, CONFIG,
        REPOSITORY / "configs/final_multinode_nodes.example.yaml",
        REPOSITORY / "configs/final_multinode_images.yaml", workloads,
    )
    commands = []
    monkeypatch.setattr(
        installer, "_run",
        lambda arguments, **_kwargs: commands.append(tuple(arguments)) or "",
    )
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("test", encoding="utf-8")
    report = installer.install(
        repository=REPOSITORY, campaign_path=CONFIG,
        nodes_path=REPOSITORY / "configs/final_multinode_nodes.example.yaml",
        image_lock_path=REPOSITORY / "configs/final_multinode_images.yaml",
        rendered_workloads=workloads, kubeconfig=kubeconfig, context="campaign",
    )
    flattened = [" ".join(item) for item in commands]
    assert report["formal_service_count"] == 11
    assert any(f"apply -f {workloads}" in item for item in flattened)
    assert any("delete deployment loadgenerator" in item for item in flattened)
    assert any(
        "patch daemonset/proberca-beyla" in item
        and "proberca.io/node-role" in item and "worker" in item
        for item in flattened
    )
    assert all("dns-exposure" not in item for item in flattened)


def test_physical_preflight_checks_exact_image_lock_and_node_name_mapping(
    monkeypatch, tmp_path,
):
    module = _script_module(
        "preflight_multinode_campaign_test", "preflight_multinode_campaign.py",
    )
    campaign = load_campaign_config(CONFIG)
    image_lock = yaml.safe_load((
        REPOSITORY / "configs/final_multinode_images.yaml"
    ).read_text(encoding="utf-8"))
    inventory = yaml.safe_load((
        REPOSITORY / "configs/final_multinode_nodes.example.yaml"
    ).read_text(encoding="utf-8"))
    node_names = {}
    for item in inventory["nodes"]:
        if item["role"] == "worker":
            item["kubernetes_node_name"] = "k8s-" + item["node_id"]
            node_names[item["node_id"]] = item["kubernetes_node_name"]
    pods = []
    for worker, services in campaign["placement"].items():
        for service in services:
            pods.append({
                "metadata": {"labels": {"app": service}},
                "spec": {
                    "nodeName": node_names[worker],
                    "containers": [{"image": image_lock["images"][service]}],
                },
                "status": {"conditions": [{
                    "type": "Ready", "status": "True",
                }]},
            })
    monkeypatch.setattr(module.subprocess, "run", lambda *_args, **_kwargs:
        SimpleNamespace(returncode=0, stdout=json.dumps({"items": pods}), stderr="")
    )
    assert module._pod_layout(
        tmp_path / "kubeconfig", "formal", CONFIG, inventory,
        REPOSITORY / "configs/final_multinode_images.yaml",
    ) == (True, True)
    pods[0]["spec"]["containers"][0]["image"] = "example.invalid:latest"
    assert module._pod_layout(
        tmp_path / "kubeconfig", "formal", CONFIG, inventory,
        REPOSITORY / "configs/final_multinode_images.yaml",
    ) == (True, False)


def test_formal_tcp_fault_edges_are_subset_of_frozen_fifteen_edge_scope():
    config = load_campaign_config(CONFIG)
    formal = {
        (item["src"], item["dst"]) for item in config["formal_tcp_edges"]
    }
    selected = {
        (item["src"], item["dst"])
        for item in config["selected_tcp_fault_edges"]
    }
    assert len(formal) == 15
    assert len(selected) == 4
    assert selected <= formal
    assert {item["placement"] for item in config["selected_tcp_fault_edges"]} == {
        "same-node", "cross-node",
    }

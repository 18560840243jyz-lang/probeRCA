#!/usr/bin/env python3
"""Run the complete pre-rental protocol against non-privileged fake targets."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from proberca.campaign.execution import (
    InjectorExecutionError,
    TargetBinding,
    freeze_injector_registry,
    run_injector_pilot,
)
from proberca.campaign.fake_agent import FakeWorkerAgent
from proberca.campaign.generator import build_campaign_plan, load_campaign_config
from proberca.campaign.injectors import load_injector_registry
from proberca.campaign.orchestrator import CampaignExecutor, SealedCaseResult
from proberca.campaign.preflight import (
    CampaignPreflightObservation,
    evaluate_campaign_preflight,
)
from proberca.campaign.sealing import seal_private_manifest
from proberca.campaign.state import CampaignState
from proberca.campaign.storage import (
    FilesystemArchiveStore,
    restore_dataset,
    upload_and_verify_dataset,
)


def _cms_roundtrip(private_payload: dict, root: Path) -> bool:
    key = root / "test-key.pem"
    certificate = root / "test-cert.pem"
    sealed = root / "private.cms"
    decrypted = root / "decrypted.json"
    command = [
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-subj", "/CN=ProbeRCA Campaign Rehearsal", "-days", "1",
        "-keyout", str(key), "-out", str(certificate),
    ]
    completed = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if completed.returncode != 0:
        return False
    seal_private_manifest(
        private_payload, recipient_certificate=certificate, output_path=sealed,
    )
    completed = subprocess.run([
        "openssl", "cms", "-decrypt", "-binary", "-inform", "DER",
        "-in", str(sealed), "-recip", str(certificate), "-inkey", str(key),
        "-out", str(decrypted),
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    passed = completed.returncode == 0 and json.loads(
        decrypted.read_text(encoding="utf-8")
    ) == private_payload
    key.unlink(missing_ok=True)
    decrypted.unlink(missing_ok=True)
    return passed


def run(repository: Path, output: Path, *, repository_tests_passed: bool) -> dict:
    git_sha = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
    ).stdout.strip()
    tracked_status = subprocess.run(
        [
            "git", "-C", str(repository), "status", "--porcelain",
            "--untracked-files=no",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
    ).stdout.strip()
    if len(git_sha) != 40 or tracked_status:
        raise RuntimeError(
            "pre-rental rehearsal requires one clean, full Git SHA"
        )
    config = load_campaign_config(repository / "configs/final_multinode_campaign.yaml")
    secret = b"pre-rental-rehearsal-secret-not-used-in-formal-campaign"
    supported = build_campaign_plan(config, commitment_secret=secret)
    unsupported_config = copy.deepcopy(config)
    unsupported_config["host_nic"]["supported"] = False
    unsupported = build_campaign_plan(unsupported_config, commitment_secret=secret)
    registry = load_injector_registry(
        repository / "configs/final_multinode_injector_candidates.yaml",
        require_frozen=False,
    )
    reports = []
    for index, profile in enumerate(registry["profiles"], 1):
        mechanism = profile["mechanism"]
        kind = "tcp_edge" if mechanism.startswith("tcp_") else \
            "host" if mechanism.startswith("host_") else "service"
        entity = "frontend->cartservice" if kind == "tcp_edge" else \
            "worker-1" if kind == "host" else "frontend"
        reports.append(run_injector_pilot(
            profile=profile,
            target=TargetBinding(
                node_id="worker-1", entity_kind=kind, entity_id=entity,
                runtime_identity_fingerprint=f"{index:064x}", attributes={},
            ),
            client=FakeWorkerAgent(), dataset_id=f"SIM-P{index:03d}",
            dataset_sha256=hashlib.sha256(profile["profile_id"].encode()).hexdigest(),
            simulated=True,
        ))
    simulated_freeze_rejected = False
    try:
        freeze_injector_registry(registry, reports)
    except InjectorExecutionError:
        simulated_freeze_rejected = True
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="proberca-rehearsal-") as temporary_value:
        temporary = Path(temporary_value)
        cms = _cms_roundtrip(supported.private_manifest, temporary)
        executable = copy.deepcopy(supported.public_manifest)
        executable["executable"] = True
        case_ids = [item["case_id"] for item in executable["cases"][:2]]
        executable["cases"] = executable["cases"][:2]
        state = CampaignState(
            temporary / "campaign-state.json",
            executable["manifest_fingerprint"], case_ids,
        )
        executor = CampaignExecutor(executable, state)
        interrupted = False
        try:
            executor.run_next(lambda _case, _attempt: (_ for _ in ()).throw(
                RuntimeError("deterministic rehearsal interruption")
            ))
        except RuntimeError:
            interrupted = True
        attempts = []

        def handler(case, attempt):
            attempts.append((case["case_id"], attempt))
            return SealedCaseResult("dataset-" + case["case_id"], "a" * 64)

        executor.run_next(handler)
        executor.run_next(handler)
        source = temporary / "source"
        source.mkdir()
        (source / "normal").mkdir()
        payload = b"rehearsal-window\n"
        (source / "normal/windows.jsonl").write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        (source / "SHA256SUMS").write_text(
            f"{digest}  normal/windows.jsonl\n", encoding="utf-8",
        )
        store = FilesystemArchiveStore(temporary / "object-store")
        upload = upload_and_verify_dataset(
            source, store, object_prefix="campaign/REHEARSAL",
        )
        restore = restore_dataset(
            store, object_prefix="campaign/REHEARSAL",
            output_root=temporary / "restore",
        )
    code_observation = CampaignPreflightObservation(
        repository_tests_passed=repository_tests_passed,
        deterministic_manifests_passed=(
            supported.summary["core_dataset_count"] == 135
            and unsupported.summary["core_dataset_count"] == 131
        ),
        cms_roundtrip_passed=cms,
        interruption_resume_rehearsal_passed=(
            interrupted and attempts[0][1] == 2 and attempts[1][1] == 1
        ),
        fake_agent_rehearsal_passed=(
            all(item["accepted"] for item in reports)
            and simulated_freeze_rejected
        ),
        filesystem_restore_rehearsal_passed=
            restore["offline_restore_passed"] is True,
        object_store_adapter_tested=
            upload["independent_readback_passed"] is True,
    )
    preflight = evaluate_campaign_preflight(code_observation)
    report = {
        "schema_version": "probeRCA-pre-rental-rehearsal-v1",
        "git_sha": git_sha,
        "supported_host_nic_core_datasets": supported.summary["core_dataset_count"],
        "unsupported_host_nic_core_datasets": unsupported.summary["core_dataset_count"],
        "simulated_pilot_count": len(reports),
        "simulated_pilots_accepted": sum(item["accepted"] for item in reports),
        "simulated_evidence_rejected_for_formal_freeze": simulated_freeze_rejected,
        "cms_roundtrip_passed": cms,
        "resume_attempts": attempts,
        "upload": upload,
        "restore": restore,
        **preflight,
    }
    (output / "pre-rental-rehearsal-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    (output / "code-evidence.json").write_text(json.dumps({
        "repository_tests_passed": repository_tests_passed,
        "deterministic_manifests_passed":
            code_observation.deterministic_manifests_passed,
        "cms_roundtrip_passed": code_observation.cms_roundtrip_passed,
        "interruption_resume_rehearsal_passed":
            code_observation.interruption_resume_rehearsal_passed,
        "fake_agent_rehearsal_passed": code_observation.fake_agent_rehearsal_passed,
        "filesystem_restore_rehearsal_passed":
            code_observation.filesystem_restore_rehearsal_passed,
        "object_store_adapter_tested": code_observation.object_store_adapter_tested,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repository-tests-passed", action="store_true")
    arguments = parser.parse_args()
    report = run(
        arguments.repository.resolve(), arguments.output.resolve(),
        repository_tests_passed=arguments.repository_tests_passed,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["pre_rent_code_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

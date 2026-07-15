from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

import scripts.validate_p11_image_reference as policy


SUPPLY_ANNOTATION = "proberca.io/image-supply-mode"
FINGERPRINT_ANNOTATION = "proberca.io/runtime-source-fingerprint"
IMAGE_ID_ANNOTATION = "proberca.io/expected-image-id"
IDENTITY_FINGERPRINT_ANNOTATION = "proberca.io/image-identity-fingerprint"


def _canonical(path: Path, value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _identity(tmp_path: Path, *, seed: str = "fixture", release_channel: str = "p11-s3"):
    fingerprint = hashlib.sha256((seed + "-source").encode()).hexdigest()
    image_id = "sha256:" + hashlib.sha256((seed + "-image").encode()).hexdigest()
    revision = hashlib.sha1((seed + "-revision").encode()).hexdigest()
    image_tag = f"registry.invalid/proberca:{release_channel}-{fingerprint[:16]}"
    value = {
        "schema_version": "p11-image-identity-v1",
        "HEAD": revision,
        "runtime_source_fingerprint": fingerprint,
        "image_tag": image_tag,
        "image_id": image_id,
        "architecture": "amd64",
        "OS": "linux",
        "OCI labels": {
            "org.opencontainers.image.revision": revision,
            "io.proberca.source-fingerprint": fingerprint,
        },
        "ready_for_s4": True,
    }
    path = tmp_path / f"{seed}-identity.json"
    identity_fingerprint = _canonical(path, value)
    return value, path, identity_fingerprint


def _release_binding(
    tmp_path: Path,
    identity: dict,
    identity_fingerprint: str,
    *,
    seed: str = "fixture",
):
    value = {
        "schema_version": "p11-release-binding-v1",
        "original_image_identity_fingerprint": identity_fingerprint,
        "runtime_source_fingerprint": identity["runtime_source_fingerprint"],
        "image_tag": identity["image_tag"],
        "image_id": identity["image_id"],
        "OCI_labels": identity["OCI labels"],
        "runtime_source_unchanged": True,
        "image_unchanged": True,
        "supported_supply_modes": [
            "registry_digest",
            "verified_local_import",
        ],
        "ready_for_s4": True,
    }
    path = tmp_path / f"{seed}-release-binding.json"
    _canonical(path, value)
    return value, path


def _evidence(tmp_path: Path, identity: dict, *, seed: str = "fixture"):
    value = {
        "schema_version": "p11-node-image-evidence-v1",
        "cluster_type": "kind",
        "nodes": [
            {
                "node_name": f"{seed}-node",
                "architecture": "amd64",
                "os": "linux",
                "runtime": "containerd",
                "image_tag": identity["image_tag"],
                "runtime_image_id": identity["image_id"],
                "runtime_manifest_digest": "sha256:"
                + hashlib.sha256((seed + "-manifest").encode()).hexdigest(),
                "source_fingerprint": identity["runtime_source_fingerprint"],
                "revision": identity["HEAD"],
                "identity_verified": True,
                "verification_method": "containerd_runtime_query",
                "runtime_query_succeeded": True,
            }
        ],
    }
    path = tmp_path / f"{seed}-nodes.json"
    _canonical(path, value)
    return value, path


def _pod_template(identity: dict, identity_fingerprint: str, *, image=None):
    annotations = {
        SUPPLY_ANNOTATION: "verified-local-import",
        FINGERPRINT_ANNOTATION: identity["runtime_source_fingerprint"],
        IMAGE_ID_ANNOTATION: identity["image_id"],
        IDENTITY_FINGERPRINT_ANNOTATION: identity_fingerprint,
    }
    return {
        "metadata": {"annotations": annotations},
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "proberca",
                    "image": image or identity["image_tag"],
                    "imagePullPolicy": "Never",
                },
                {
                    "name": "sidecar",
                    "image": "registry.invalid/sidecar@sha256:" + "a" * 64,
                },
            ],
        },
    }


def _workload(kind: str, template: dict):
    metadata = {"name": "dynamic-workload"}
    if kind == "Pod":
        return {
            "apiVersion": "v1",
            "kind": kind,
            "metadata": {"name": "dynamic-workload", **template["metadata"]},
            "spec": template["spec"],
        }
    if kind == "CronJob":
        spec = {"jobTemplate": {"spec": {"template": template}}}
    else:
        spec = {"template": template}
    return {"apiVersion": "apps/v1", "kind": kind, "metadata": metadata, "spec": spec}


def _write_yaml(path: Path, documents: list[dict]) -> Path:
    path.write_text(
        "---\n".join(yaml.safe_dump(item, sort_keys=True) for item in documents),
        encoding="utf-8",
    )
    return path


def _validate(
    tmp_path: Path,
    *,
    release_channel: str = "p11-s3",
    mutate_identity=None,
    mutate_binding=None,
    mutate_evidence=None,
    mutate_manifest=None,
):
    identity, identity_path, identity_fingerprint = _identity(
        tmp_path, release_channel=release_channel
    )
    binding, binding_path = _release_binding(
        tmp_path, identity, identity_fingerprint
    )
    evidence, evidence_path = _evidence(tmp_path, identity)
    manifest = _workload("Deployment", _pod_template(identity, identity_fingerprint))
    if mutate_identity:
        mutate_identity(identity)
        _canonical(identity_path, identity)
    if mutate_binding:
        mutate_binding(binding)
        _canonical(binding_path, binding)
    if mutate_evidence:
        mutate_evidence(evidence)
        _canonical(evidence_path, evidence)
    if mutate_manifest:
        mutate_manifest(manifest)
    manifest_path = _write_yaml(tmp_path / "rendered.yaml", [manifest])
    return policy.validate_manifest_images(
        (manifest_path,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    )


@pytest.mark.parametrize(
    "image",
    [
        "registry.invalid/proberca:ordinary",
        "registry.invalid/proberca:p11-s3-" + "1" * 16,
        "registry.invalid/proberca:latest",
        policy.SENTINEL,
        policy.PLACEHOLDER,
        "registry.invalid/proberca@sha256:short",
    ],
)
def test_registry_digest_mode_rejects_non_digest_final_images(tmp_path, image):
    manifest = _write_yaml(
        tmp_path / "registry.yaml",
        [_workload("Deployment", {"metadata": {}, "spec": {"containers": [{"name": "app", "image": image}]}})],
    )
    assert policy.validate_manifest_images((manifest,), mode="registry_digest")


def test_registry_digest_mode_accepts_digest_and_keeps_third_party_strict(tmp_path):
    digest = "registry.invalid/proberca@sha256:" + "b" * 64
    manifest = _write_yaml(
        tmp_path / "registry.yaml",
        [_workload("Deployment", {"metadata": {}, "spec": {"containers": [{"name": "app", "image": digest}]}})],
    )
    assert policy.validate_manifest_images((manifest,), mode="registry_digest") == ()


@pytest.mark.parametrize(
    ("identity", "binding", "evidence", "reason"),
    [
        (False, True, True, "local_identity_record_required"),
        (True, False, True, "local_release_binding_required"),
        (True, True, False, "local_node_evidence_required"),
    ],
)
def test_local_mode_requires_complete_identity_chain(
    tmp_path, identity, binding, evidence, reason
):
    value, identity_path, identity_fingerprint = _identity(tmp_path)
    _, binding_path = _release_binding(
        tmp_path, value, identity_fingerprint
    )
    _, evidence_path = _evidence(tmp_path, value)
    manifest = _write_yaml(
        tmp_path / "local.yaml",
        [_workload("Job", _pod_template(value, identity_fingerprint))],
    )
    issues = policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path if identity else None,
        release_binding=binding_path if binding else None,
        node_image_evidence=evidence_path if evidence else None,
    )
    assert reason in {item.reason_code for item in issues}


@pytest.mark.parametrize(
    "release_channel",
    ("p11-s3", "p11-s4fix", "release-candidate"),
)
def test_verified_local_import_accepts_identity_tag_without_stage_trust(
    tmp_path, release_channel
):
    assert _validate(tmp_path, release_channel=release_channel) == ()


def test_local_mode_requires_release_binding(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    _, evidence_path = _evidence(tmp_path, identity)
    manifest = _write_yaml(
        tmp_path / "missing-binding.yaml",
        [_workload("Job", _pod_template(identity, identity_fingerprint))],
    )
    issues = policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        node_image_evidence=evidence_path,
    )
    assert "local_release_binding_required" in {
        item.reason_code for item in issues
    }


def test_release_binding_must_point_to_exact_identity(tmp_path):
    issues = _validate(
        tmp_path,
        mutate_binding=lambda value: value.__setitem__(
            "image_id", "sha256:" + "c" * 64
        ),
    )
    assert "local_release_binding_mismatch" in {
        item.reason_code for item in issues
    }


@pytest.mark.parametrize(
    ("image", "reason"),
    [
        (policy.SENTINEL, "proberca_sentinel_forbidden"),
        (policy.PLACEHOLDER, "proberca_placeholder_forbidden"),
        ("registry.invalid/proberca:latest", "image_latest_forbidden"),
    ],
)
def test_local_manifest_rejects_unbound_special_references(
    tmp_path, image, reason
):
    def mutate(document):
        document["spec"]["template"]["spec"]["containers"][0]["image"] = image

    issues = _validate(tmp_path, mutate_manifest=mutate)
    assert reason in {item.reason_code for item in issues}


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("image", "registry.invalid/proberca:p11-s3-" + "2" * 16, "local_image_tag_mismatch"),
        ("pull", "IfNotPresent", "local_pull_policy_not_never"),
        ("mode", "registry-digest", "local_annotation_mismatch"),
        ("fingerprint", "3" * 64, "local_source_fingerprint_mismatch"),
        ("image_id", "sha256:" + "4" * 64, "local_image_id_mismatch"),
        ("identity_fp", "5" * 64, "local_identity_fingerprint_mismatch"),
    ],
)
def test_local_manifest_identity_mismatches_are_rejected(tmp_path, field, value, reason):
    def mutate(document):
        template = document["spec"]["template"]
        container = template["spec"]["containers"][0]
        annotations = template["metadata"]["annotations"]
        if field == "image":
            container["image"] = value
        elif field == "pull":
            container["imagePullPolicy"] = value
        else:
            key = {
                "mode": SUPPLY_ANNOTATION,
                "fingerprint": FINGERPRINT_ANNOTATION,
                "image_id": IMAGE_ID_ANNOTATION,
                "identity_fp": IDENTITY_FINGERPRINT_ANNOTATION,
            }[field]
            annotations[key] = value

    issues = _validate(tmp_path, mutate_manifest=mutate)
    assert reason in {item.reason_code for item in issues}


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("ready_for_s4", False, "local_identity_record_invalid"),
        ("runtime_source_fingerprint", "6" * 64, "local_source_fingerprint_mismatch"),
        ("image_id", "sha256:" + "7" * 64, "local_release_binding_mismatch"),
        ("HEAD", "8" * 40, "local_revision_mismatch"),
    ],
)
def test_invalid_identity_record_is_rejected(tmp_path, field, value, reason):
    issues = _validate(tmp_path, mutate_identity=lambda identity: identity.__setitem__(field, value))
    assert reason in {item.reason_code for item in issues}


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda value: value.__setitem__("nodes", []), "local_node_image_missing"),
        (lambda value: value["nodes"][0].__setitem__("image_tag", "registry.invalid/wrong:tag"), "local_node_identity_mismatch"),
        (lambda value: value["nodes"][0].__setitem__("runtime_image_id", "sha256:" + "9" * 64), "local_image_id_mismatch"),
        (lambda value: value["nodes"][0].__setitem__("architecture", "arm64"), "local_platform_mismatch"),
        (lambda value: value["nodes"][0].__setitem__("identity_verified", False), "local_node_identity_mismatch"),
        (lambda value: value["nodes"][0].__setitem__("runtime_query_succeeded", False), "local_node_identity_mismatch"),
    ],
)
def test_node_runtime_evidence_is_required_and_verified(tmp_path, mutation, reason):
    issues = _validate(tmp_path, mutate_evidence=mutation)
    assert reason in {item.reason_code for item in issues}


@pytest.mark.parametrize("kind", ["Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Pod"])
def test_local_policy_covers_pod_template_workloads(tmp_path, kind):
    identity, identity_path, identity_fingerprint = _identity(tmp_path, seed=kind)
    _, binding_path = _release_binding(tmp_path, identity, identity_fingerprint, seed=kind)
    _, evidence_path = _evidence(tmp_path, identity, seed=kind)
    manifest = _write_yaml(tmp_path / f"{kind}.yaml", [_workload(kind, _pod_template(identity, identity_fingerprint))])
    assert policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    ) == ()


def test_local_policy_covers_init_ephemeral_and_multidoc(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    _, binding_path = _release_binding(tmp_path, identity, identity_fingerprint)
    _, evidence_path = _evidence(tmp_path, identity)
    template = _pod_template(identity, identity_fingerprint)
    local = dict(template["spec"]["containers"][0])
    template["spec"]["initContainers"] = [{**local, "name": "init"}]
    template["spec"]["ephemeralContainers"] = [{**local, "name": "debug"}]
    manifest = _write_yaml(tmp_path / "multi.yaml", [_workload("Deployment", template), _workload("Job", template)])
    assert policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    ) == ()


def test_local_policy_does_not_relax_kustomize_or_third_party(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    _, binding_path = _release_binding(tmp_path, identity, identity_fingerprint)
    _, evidence_path = _evidence(tmp_path, identity)
    template = _pod_template(identity, identity_fingerprint)
    template["spec"]["containers"][1]["image"] = "registry.invalid/sidecar:1"
    manifest = _write_yaml(tmp_path / "manifest.yaml", [_workload("Deployment", template)])
    kustomization = _write_yaml(
        tmp_path / "kustomization.yaml",
        [{"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "images": [{"name": "x", "newTag": "1"}]}],
    )
    reasons = {
        item.reason_code
        for item in policy.validate_manifest_images(
            (manifest, kustomization),
            mode="verified_local_import",
            identity_record=identity_path,
            release_binding=binding_path,
            node_image_evidence=evidence_path,
        )
    }
    assert {"third_party_digest_missing", "kustomize_mutable_override"} <= reasons


def test_validator_has_no_current_identity_or_cluster_hardcoding():
    source = Path(policy.__file__).read_text(encoding="utf-8")
    forbidden = (
        "p11-s3-",
        "p11-s4fix-",
        "ca0907cc",
        "ed05480b",
        "proberca-ob",
    )
    assert not any(value in source for value in forbidden)

def test_identity_loader_accepts_s3_oci_label_field_name(tmp_path):
    identity, identity_path, _ = _identity(tmp_path)
    identity["OCI_labels"] = identity.pop("OCI labels")
    _canonical(identity_path, identity)
    loaded = policy.load_local_image_identity(identity_path)
    assert loaded.image_tag == identity["image_tag"]


def test_local_binding_injects_exact_identity_without_changing_third_party(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    third_party = "registry.invalid/sidecar@sha256:" + "b" * 64
    source = _write_yaml(
        tmp_path / "source.yaml",
        [
            _workload(
                "Deployment",
                {
                    "metadata": {},
                    "spec": {
                        "containers": [
                            {"name": "proberca", "image": policy.PLACEHOLDER},
                            {"name": "sidecar", "image": third_party},
                        ]
                    },
                },
            ),
            _workload(
                "Job",
                {
                    "metadata": {},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [{"name": "bounded", "image": policy.PLACEHOLDER}],
                    },
                },
            ),
        ],
    )
    output = tmp_path / "bound.yaml"
    policy.bind_verified_local_manifest(source, output, identity_path)
    documents = list(yaml.safe_load_all(output.read_text(encoding="utf-8")))
    records = policy.collect_manifest_images((output,))
    local = [item for item in records if item.image == identity["image_tag"]]
    assert len(local) == 2
    assert all(item.pull_policy == "Never" for item in local)
    assert all(item.annotations[SUPPLY_ANNOTATION] == "verified-local-import" for item in local)
    assert all(item.annotations[FINGERPRINT_ANNOTATION] == identity["runtime_source_fingerprint"] for item in local)
    assert all(item.annotations[IMAGE_ID_ANNOTATION] == identity["image_id"] for item in local)
    assert all(item.annotations[IDENTITY_FINGERPRINT_ANNOTATION] == identity_fingerprint for item in local)
    assert any(item.image == third_party for item in records)
    assert policy.PLACEHOLDER not in output.read_text(encoding="utf-8")
    assert len(documents) == 2


def test_smoke_runner_requires_explicit_supply_mode_and_validates_before_apply():
    source = (Path(__file__).resolve().parents[1] / "scripts/p11_smoke_run.sh").read_text(
        encoding="utf-8"
    )
    assert "PROBERCA_P11_IMAGE_SUPPLY_MODE" in source
    assert "registry_digest" in source
    assert "verified_local_import" in source
    assert "PROBERCA_P11_IMAGE_IDENTITY_RECORD" in source
    assert "PROBERCA_P11_RELEASE_BINDING" in source
    assert "PROBERCA_P11_NODE_IMAGE_EVIDENCE" in source
    assert "--mode final-local" in source
    assert "--mode final-registry" in source
    assert source.index("validate_p11_image_reference.py") < source.index("kubectl apply")

@pytest.mark.parametrize(
    "annotation",
    [
        SUPPLY_ANNOTATION,
        FINGERPRINT_ANNOTATION,
        IMAGE_ID_ANNOTATION,
        IDENTITY_FINGERPRINT_ANNOTATION,
    ],
)
def test_local_manifest_missing_required_annotation_is_rejected(tmp_path, annotation):
    def mutate(document):
        document["spec"]["template"]["metadata"]["annotations"].pop(annotation)

    issues = _validate(tmp_path, mutate_manifest=mutate)
    assert "local_annotation_missing" in {item.reason_code for item in issues}


def test_noncanonical_identity_record_is_rejected(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    _, binding_path = _release_binding(tmp_path, identity, identity_fingerprint)
    _, evidence_path = _evidence(tmp_path, identity)
    identity_path.write_text(json.dumps(identity, indent=2), encoding="utf-8")
    manifest = _write_yaml(
        tmp_path / "rendered.yaml",
        [_workload("Deployment", _pod_template(identity, identity_fingerprint))],
    )
    issues = policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    )
    assert "local_identity_fingerprint_mismatch" in {
        item.reason_code for item in issues
    }


def test_all_declared_target_nodes_must_verify(tmp_path):
    identity, identity_path, identity_fingerprint = _identity(tmp_path)
    _, binding_path = _release_binding(tmp_path, identity, identity_fingerprint)
    evidence, evidence_path = _evidence(tmp_path, identity)
    second = dict(evidence["nodes"][0])
    second["node_name"] = "second-runtime-node"
    evidence["nodes"].append(second)
    _canonical(evidence_path, evidence)
    manifest = _write_yaml(
        tmp_path / "rendered.yaml",
        [_workload("Deployment", _pod_template(identity, identity_fingerprint))],
    )
    assert policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    ) == ()



def test_verified_local_import_accepts_durable_v2_identity_chain(tmp_path):
    seed = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    source_fingerprint = hashlib.sha256((seed + "-source").encode()).hexdigest()
    image_id = "sha256:" + hashlib.sha256((seed + "-image").encode()).hexdigest()
    revision = hashlib.sha1((seed + "-revision").encode()).hexdigest()
    image_tag = f"proberca:src-{source_fingerprint[:16]}"
    labels = {
        "org.opencontainers.image.revision": revision,
        "io.proberca.source-fingerprint": source_fingerprint,
    }
    identity = {
        "schema_version": "p11-image-identity-v2",
        "HEAD": revision,
        "runtime_source_fingerprint": source_fingerprint,
        "image_tag": image_tag,
        "image_id": image_id,
        "architecture": "amd64",
        "OS": "linux",
        "OCI_labels": labels,
        "ready_for_node_import": True,
    }
    identity_path = tmp_path / "v2-identity.json"
    identity_fingerprint = _canonical(identity_path, identity)
    binding = {
        "schema_version": "p11-release-binding-v2",
        "ready_for_s4": True,
        "image_identity_fingerprint": identity_fingerprint,
        "runtime_source_fingerprint": source_fingerprint,
        "image_tag": image_tag,
        "image_id": image_id,
        "OCI_labels": labels,
        "runtime_source_unchanged": True,
        "image_unchanged": True,
        "image_identity_contract": "stage-independent-v1",
        "artifact_persistence_contract": "p11-durable-artifacts-v1",
        "supported_supply_modes": [
            "registry_digest",
            "verified_local_import",
        ],
    }
    binding_path = tmp_path / "v2-binding.json"
    _canonical(binding_path, binding)
    manifest_digest = "sha256:" + hashlib.sha256((seed + "-manifest").encode()).hexdigest()
    config_digest = "sha256:" + hashlib.sha256((seed + "-config").encode()).hexdigest()
    evidence = {
        "schema_version": "p11-node-image-evidence-v2",
        "image_identity_fingerprint": identity_fingerprint,
        "cluster_type": "kind",
        "nodes": [{
            "node_name": f"node-{seed[:12]}",
            "architecture": "amd64",
            "os": "linux",
            "runtime": "containerd",
            "image_tag": image_tag,
            "runtime_image_id": manifest_digest,
            "runtime_manifest_digest": manifest_digest,
            "runtime_config_id": config_digest,
            "source_fingerprint": source_fingerprint,
            "revision": revision,
            "identity_verified": True,
            "runtime_query_succeeded": True,
            "verification_method": "containerd_runtime_query",
        }],
    }
    evidence_path = tmp_path / "v2-nodes.json"
    _canonical(evidence_path, evidence)
    manifest = _write_yaml(
        tmp_path / "v2-rendered.yaml",
        [_workload("Job", _pod_template(identity, identity_fingerprint))],
    )
    assert policy.validate_manifest_images(
        (manifest,),
        mode="verified_local_import",
        identity_record=identity_path,
        release_binding=binding_path,
        node_image_evidence=evidence_path,
    ) == ()

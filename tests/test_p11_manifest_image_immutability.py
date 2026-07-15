from pathlib import Path
import re
import subprocess
import sys

import pytest
import yaml

from scripts.validate_p11_image_reference import (
    DIGEST,
    ImagePolicyError,
    SENTINEL,
    collect_manifest_images,
    validate_manifest_images,
    validate_rendered_reference,
    validate_third_party_reference,
)


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOTS = (
    ROOT / "deploy/kubernetes/base",
    ROOT / "deploy/kubernetes/test",
)
VALID = "registry.example/component@sha256:" + "a" * 64
VALID_TAGGED = "registry.example/component:1.2.3@sha256:" + "b" * 64
PLACEHOLDER = "PROBERCA_P11_IMAGE"


def _write(path: Path, documents) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "---\n".join(yaml.safe_dump(document) for document in documents),
        encoding="utf-8",
    )
    return path


def _workload(kind: str, pod_spec: dict, *, annotations=None):
    metadata = {"name": "generic-workload"}
    if annotations:
        metadata["annotations"] = annotations
    if kind == "Pod":
        spec = pod_spec
    elif kind == "Job":
        spec = {"template": {"spec": pod_spec}}
    elif kind == "CronJob":
        spec = {"jobTemplate": {"spec": {"template": {"spec": pod_spec}}}}
    else:
        spec = {"template": {"spec": pod_spec}}
    return {"apiVersion": "v1", "kind": kind, "metadata": metadata, "spec": spec}


def test_all_third_party_source_images_are_digest_pinned():
    issues = validate_manifest_images(DEPLOY_ROOTS, mode="source")
    assert issues == (), "\n".join(map(str, issues))


@pytest.mark.parametrize("value", ["registry.example/component:1.2.3", "component:7"])
def test_arbitrary_third_party_tag_is_rejected(value):
    with pytest.raises(ImagePolicyError) as caught:
        validate_third_party_reference(value)
    assert caught.value.reason_code == "image_digest_missing"


def test_latest_is_rejected():
    with pytest.raises(ImagePolicyError) as caught:
        validate_third_party_reference("registry.example/component:latest")
    assert caught.value.reason_code == "image_latest_forbidden"


@pytest.mark.parametrize("value", [VALID, VALID_TAGGED])
def test_valid_digest_forms_pass(value):
    assert validate_third_party_reference(value) == value


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("repo@sha256:abc", "image_digest_invalid"),
        ("repo@sha512:" + "a" * 64, "image_digest_invalid"),
        ("", "image_empty"),
    ],
)
def test_invalid_digest_or_empty_is_rejected(value, reason):
    with pytest.raises(ImagePolicyError) as caught:
        validate_third_party_reference(value)
    assert caught.value.reason_code == reason


def test_scans_containers_init_and_ephemeral_containers(tmp_path):
    path = _write(
        tmp_path / "all-containers.yaml",
        [
            _workload(
                "Deployment",
                {
                    "containers": [{"name": "main", "image": VALID}],
                    "initContainers": [{"name": "init", "image": VALID_TAGGED}],
                    "ephemeralContainers": [{"name": "debug", "image": VALID}],
                },
            )
        ],
    )
    records = collect_manifest_images((path,))
    assert [record.container for record in records] == ["debug", "init", "main"]


@pytest.mark.parametrize("kind", ["Deployment", "Job", "CronJob"])
def test_scans_nested_pod_templates(kind, tmp_path):
    path = _write(
        tmp_path / f"{kind}.yaml",
        [_workload(kind, {"containers": [{"name": "app", "image": VALID}]})],
    )
    records = collect_manifest_images((path,))
    assert [(record.kind, record.container) for record in records] == [(kind, "app")]


def test_scans_multiple_yaml_documents(tmp_path):
    path = _write(
        tmp_path / "multi.yaml",
        [
            _workload("Pod", {"containers": [{"name": "one", "image": VALID}]}),
            _workload("Job", {"containers": [{"name": "two", "image": VALID}]}),
        ],
    )
    assert len(collect_manifest_images((path,))) == 2


def test_kustomize_new_tag_without_digest_is_rejected(tmp_path):
    path = _write(
        tmp_path / "kustomization.yaml",
        [
            {
                "apiVersion": "kustomize.config.k8s.io/v1beta1",
                "kind": "Kustomization",
                "images": [{"name": "component", "newTag": "1.2.3"}],
            }
        ],
    )
    issues = validate_manifest_images((path,), mode="source")
    assert [issue.reason_code for issue in issues] == ["kustomize_mutable_override"]


def test_annotated_sentinel_is_allowed_in_source(tmp_path):
    path = _write(
        tmp_path / "base.yaml",
        [
            _workload(
                "Deployment",
                {"containers": [{"name": "app", "image": SENTINEL}]},
                annotations={"proberca.io/immutable-image-required": "true"},
            )
        ],
    )
    assert validate_manifest_images((path,), mode="source") == ()


def test_unannotated_sentinel_is_rejected(tmp_path):
    path = _write(
        tmp_path / "base.yaml",
        [_workload("Deployment", {"containers": [{"name": "app", "image": SENTINEL}]})],
    )
    issues = validate_manifest_images((path,), mode="source")
    assert [issue.reason_code for issue in issues] == [
        "proberca_sentinel_unannotated"
    ]


def test_smoke_placeholder_is_source_only(tmp_path):
    source = _write(
        tmp_path / "deploy/kubernetes/test/p11-smoke/workload.yaml",
        [_workload("Job", {"containers": [{"name": "app", "image": PLACEHOLDER}]})],
    )
    assert validate_manifest_images((source,), mode="source") == ()
    issues = validate_manifest_images((source,), mode="final")
    assert [issue.reason_code for issue in issues] == [
        "proberca_placeholder_forbidden_in_render"
    ]


def test_final_render_accepts_only_digests(tmp_path):
    path = _write(
        tmp_path / "rendered.yaml",
        [
            _workload(
                "Deployment",
                {
                    "containers": [{"name": "app", "image": VALID}],
                    "initContainers": [{"name": "init", "image": VALID_TAGGED}],
                },
            )
        ],
    )
    assert validate_manifest_images((path,), mode="final") == ()


def test_error_contains_workload_container_image_and_reason(tmp_path):
    image = "registry.example/component:2"
    path = _write(
        tmp_path / "invalid.yaml",
        [_workload("DaemonSet", {"containers": [{"name": "worker", "image": image}]})],
    )
    issue = validate_manifest_images((path,), mode="source")[0]
    rendered = str(issue)
    for value in ("DaemonSet", "generic-workload", "worker", image, "image_digest_missing"):
        assert value in rendered


def test_input_order_does_not_change_records_or_issues(tmp_path):
    first = _write(
        tmp_path / "z.yaml",
        [_workload("Pod", {"containers": [{"name": "z", "image": "component:1"}]})],
    )
    second = _write(
        tmp_path / "a.yaml",
        [_workload("Pod", {"containers": [{"name": "a", "image": VALID}]})],
    )
    assert collect_manifest_images((first, second)) == collect_manifest_images(
        (second, first)
    )
    assert validate_manifest_images((first, second), mode="source") == (
        validate_manifest_images((second, first), mode="source")
    )


def test_cli_reports_context_for_manifest_failure(tmp_path):
    path = _write(
        tmp_path / "invalid.yaml",
        [_workload("StatefulSet", {"containers": [{"name": "app", "image": "repo:1"}]})],
    )
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/validate_p11_image_reference.py"),
            "--manifest",
            str(path),
            "--mode",
            "source",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert all(value in result.stdout for value in ("StatefulSet", "app", "repo:1"))


def test_validator_has_no_workload_or_manifest_filename_special_cases():
    source = (ROOT / "scripts/validate_p11_image_reference.py").read_text(
        encoding="utf-8"
    )
    forbidden = (
        "metrics-source-deployment.yaml",
        "metrics-target-deployment.yaml",
        "prometheus-deployment.yaml",
        "generic-workload",
    )
    assert not any(value in source for value in forbidden)

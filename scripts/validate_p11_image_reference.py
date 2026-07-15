#!/usr/bin/env python3
"""Validate immutable image references in P11 deployment inputs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from pathlib import Path
import re
from typing import Iterable, Mapping, Sequence

import yaml


SENTINEL = "proberca:0.0.0-image-must-be-overridden"
PLACEHOLDER = "PROBERCA_P11_IMAGE"
REGISTRY_DIGEST = "registry_digest"
VERIFIED_LOCAL_IMPORT = "verified_local_import"
DIGEST = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
DIGEST_VALUE = re.compile(r"^sha256:[0-9a-f]{64}$")
FINGERPRINT_VALUE = re.compile(r"^[0-9a-f]{64}$")
_CONTAINER_KEYS = ("containers", "initContainers", "ephemeralContainers")
_LOCAL_ANNOTATIONS = {
    "proberca.io/image-supply-mode": "verified-local-import",
    "proberca.io/runtime-source-fingerprint": None,
    "proberca.io/expected-image-id": None,
    "proberca.io/image-identity-fingerprint": None,
}
_RUNTIME_VERIFICATION_METHODS = {
    "containerd_runtime_query",
    "docker_runtime_query",
    "cri_runtime_query",
}


class ImageSupplyMode(str, Enum):
    REGISTRY_DIGEST = REGISTRY_DIGEST
    VERIFIED_LOCAL_IMPORT = VERIFIED_LOCAL_IMPORT


class ImagePolicyError(ValueError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class ManifestImage:
    source: str
    kind: str
    workload: str
    container: str
    image: str
    annotations: Mapping[str, str]
    pull_policy: str
    container_type: str

    @property
    def classification(self) -> str:
        if self.image == SENTINEL:
            return "proberca_base_sentinel"
        if self.image == PLACEHOLDER:
            return "proberca_smoke_placeholder"
        if "$" in self.image or "{" in self.image or "}" in self.image:
            return "unknown_image_reference"
        return "third_party"


@dataclass(frozen=True)
class ImagePolicyIssue:
    source: str
    kind: str
    workload: str
    container: str
    image: str
    reason_code: str
    supply_mode: str = "source_template"

    def __str__(self) -> str:
        return (
            f"{self.source}: kind={self.kind} workload={self.workload} "
            f"container={self.container} image={self.image!r} "
            f"supply_mode={self.supply_mode} reason={self.reason_code}"
        )


@dataclass(frozen=True)
class LocalImageIdentity:
    path: Path
    fingerprint: str
    image_tag: str
    image_id: str
    runtime_source_fingerprint: str
    revision: str
    architecture: str
    os: str
    labels: Mapping[str, str]


def _is_latest(value: str) -> bool:
    name = value.split("@", 1)[0].rsplit("/", 1)[-1]
    return name.lower().endswith(":latest")


def _repository(value: str) -> str:
    name = value.split("@", 1)[0]
    slash = name.rfind("/")
    colon = name.rfind(":")
    return name[:colon] if colon > slash else name


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def parse_image_reference(value: str) -> str:
    value = value.strip()
    if not value:
        raise ImagePolicyError("image_empty", "image reference is empty")
    if _is_latest(value):
        raise ImagePolicyError("image_latest_forbidden", "latest image tag is mutable")
    if "@" not in value:
        raise ImagePolicyError("image_digest_missing", "image digest is required")
    if not DIGEST.fullmatch(value):
        raise ImagePolicyError("image_digest_invalid", "invalid sha256 image digest")
    return value


def validate_third_party_reference(value: str) -> str:
    if "$" in value or "{" in value or "}" in value:
        raise ImagePolicyError(
            "unknown_image_reference",
            "unrecognized image placeholder",
        )
    return parse_image_reference(value)


def validate_proberca_source_reference(
    value: str,
    *,
    annotations: Mapping[str, str] | None = None,
    source: str = "",
) -> str:
    value = value.strip()
    annotations = annotations or {}
    if value == SENTINEL:
        if annotations.get("proberca.io/immutable-image-required") != "true":
            raise ImagePolicyError(
                "proberca_sentinel_unannotated",
                "ProbeRCA sentinel requires immutable image annotation",
            )
        return value
    if value == PLACEHOLDER:
        normalized = source.replace("\\", "/")
        if "deploy/kubernetes/test/p11-smoke/" not in normalized:
            raise ImagePolicyError(
                "unknown_image_reference",
                "ProbeRCA placeholder is outside the P11 smoke source scope",
            )
        return value
    return parse_image_reference(value)


def validate_rendered_reference(value: str) -> str:
    """Backward-compatible digest-only final-render reference validator."""
    value = value.strip()
    if value == PLACEHOLDER:
        raise ImagePolicyError(
            "proberca_placeholder_forbidden_in_render",
            "ProbeRCA placeholder remains in rendered manifest",
        )
    if value == SENTINEL:
        raise ImagePolicyError(
            "proberca_sentinel_forbidden_in_render",
            "ProbeRCA sentinel remains in rendered manifest",
        )
    return validate_third_party_reference(value)


def validate_image_reference(value: str, *, allow_sentinel: bool = False) -> str:
    """Backward-compatible validator for a single explicitly bound image."""
    value = value.strip()
    if not value:
        raise ValueError("image reference is empty")
    if value == SENTINEL:
        if allow_sentinel:
            return value
        raise ValueError("immutable image sentinel was not overridden")
    if _is_latest(value):
        raise ValueError("latest image tag is mutable")
    if DIGEST.fullmatch(value):
        return value
    raise ValueError(
        "image must use a sha256 manifest digest unless validated through "
        "the complete verified_local_import identity chain"
    )


def _yaml_paths(inputs: Sequence[Path]) -> tuple[Path, ...]:
    paths: set[Path] = set()
    for value in inputs:
        if value.is_dir():
            paths.update(value.rglob("*.yaml"))
            paths.update(value.rglob("*.yml"))
        elif value.is_file():
            paths.add(value)
        else:
            raise FileNotFoundError(value)
    return tuple(sorted(paths, key=lambda path: path.as_posix()))


def _pod_specs(
    value: object,
    inherited_annotations: Mapping[str, str] | None = None,
) -> Iterable[tuple[Mapping[str, object], Mapping[str, str]]]:
    if isinstance(value, dict):
        metadata = value.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        annotations = metadata.get("annotations")
        annotations = annotations if isinstance(annotations, dict) else {}
        stable_annotations = {
            str(key): str(item)
            for key, item in sorted(annotations.items())
        }
        if not stable_annotations:
            stable_annotations = dict(inherited_annotations or {})
        spec = value.get("spec")
        if isinstance(spec, dict) and any(key in spec for key in _CONTAINER_KEYS):
            yield spec, stable_annotations
            return
        for child in value.values():
            yield from _pod_specs(child, stable_annotations)
    elif isinstance(value, list):
        for child in value:
            yield from _pod_specs(child, inherited_annotations)


def collect_manifest_images(inputs: Sequence[Path]) -> tuple[ManifestImage, ...]:
    records: list[ManifestImage] = []
    for path in _yaml_paths(inputs):
        with path.open("r", encoding="utf-8") as handle:
            documents = tuple(yaml.safe_load_all(handle))
        for document in documents:
            if not isinstance(document, dict):
                continue
            kind = str(document.get("kind", "Unknown"))
            metadata = document.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            workload = str(metadata.get("name", "<unnamed>"))
            for pod_spec, annotations in _pod_specs(document):
                for container_type in _CONTAINER_KEYS:
                    containers = pod_spec.get(container_type)
                    if not isinstance(containers, list):
                        continue
                    for container in containers:
                        if not isinstance(container, dict):
                            continue
                        records.append(
                            ManifestImage(
                                source=path.as_posix(),
                                kind=kind,
                                workload=workload,
                                container=str(container.get("name", "<unnamed>")),
                                image=str(container.get("image", "")),
                                annotations=annotations,
                                pull_policy=str(container.get("imagePullPolicy", "")),
                                container_type=container_type,
                            )
                        )
    return tuple(
        sorted(
            records,
            key=lambda item: (
                item.source,
                item.kind,
                item.workload,
                item.container,
                item.container_type,
                item.image,
            ),
        )
    )


def _issue(
    reason_code: str,
    *,
    mode: str,
    record: ManifestImage | None = None,
    source: str = "<policy>",
) -> ImagePolicyIssue:
    if record is None:
        return ImagePolicyIssue(
            source,
            "Policy",
            "<all>",
            "<all>",
            "",
            reason_code,
            mode,
        )
    return ImagePolicyIssue(
        record.source,
        record.kind,
        record.workload,
        record.container,
        record.image,
        reason_code,
        mode,
    )


def _kustomize_issues(
    inputs: Sequence[Path],
    *,
    mode: str = "source_template",
) -> tuple[ImagePolicyIssue, ...]:
    issues: list[ImagePolicyIssue] = []
    for path in _yaml_paths(inputs):
        if path.name not in {"kustomization.yaml", "kustomization.yml"}:
            continue
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            continue
        images = document.get("images") or ()
        for index, image in enumerate(images):
            workload = str((document.get("metadata") or {}).get("name", "<unnamed>"))
            if not isinstance(image, dict):
                issues.append(
                    ImagePolicyIssue(
                        path.as_posix(),
                        "Kustomization",
                        workload,
                        f"images[{index}]",
                        str(image),
                        "unknown_image_reference",
                        mode,
                    )
                )
                continue
            digest = str(image.get("digest", ""))
            new_tag = str(image.get("newTag", ""))
            if new_tag and not digest:
                reason = "kustomize_mutable_override"
            elif digest and not DIGEST_VALUE.fullmatch(digest):
                reason = "image_digest_invalid"
            else:
                continue
            issues.append(
                ImagePolicyIssue(
                    path.as_posix(),
                    "Kustomization",
                    workload,
                    f"images[{index}]",
                    str(image.get("newName") or image.get("name") or ""),
                    reason,
                    mode,
                )
            )
    return tuple(issues)


def _load_canonical_object(path: Path, invalid_reason: str) -> tuple[dict, str]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise ImagePolicyError(invalid_reason, f"cannot read canonical JSON: {error}") from error
    if not isinstance(value, dict) or raw != _canonical_json(value):
        raise ImagePolicyError(invalid_reason, "JSON record is not canonical")
    return value, hashlib.sha256(raw).hexdigest()


def _identity_labels(value: Mapping[str, object]) -> Mapping[str, str]:
    labels = value.get(
        "OCI_labels",
        value.get("OCI labels", value.get("oci_labels", {})),
    )
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(item) for key, item in labels.items()}


def load_local_image_identity(path: Path) -> LocalImageIdentity:
    value, identity_fingerprint = _load_canonical_object(
        path,
        "local_identity_fingerprint_mismatch",
    )
    schema = value.get("schema_version")
    ready = (
        schema == "p11-image-identity-v1"
        and value.get("ready_for_s4") is True
    ) or (
        schema == "p11-image-identity-v2"
        and value.get("ready_for_node_import") is True
    )
    if not ready:
        raise ImagePolicyError(
            "local_identity_record_invalid",
            "identity is not ready for verified local import",
        )
    image_tag = str(value.get("image_tag", ""))
    image_id = str(value.get("image_id", ""))
    source_fingerprint = str(value.get("runtime_source_fingerprint", ""))
    revision = str(value.get("HEAD", value.get("head", "")))
    architecture = str(value.get("architecture", ""))
    os_name = str(value.get("OS", value.get("os", ""))).lower()
    labels = _identity_labels(value)
    if not FINGERPRINT_VALUE.fullmatch(source_fingerprint):
        raise ImagePolicyError(
            "local_source_fingerprint_mismatch",
            "runtime source fingerprint is invalid",
        )
    if not DIGEST_VALUE.fullmatch(image_id):
        raise ImagePolicyError("local_image_id_mismatch", "image ID is invalid")
    if (
        not image_tag
        or image_tag in {SENTINEL, PLACEHOLDER}
        or _is_latest(image_tag)
        or "@" in image_tag
    ):
        raise ImagePolicyError(
            "local_image_tag_mismatch",
            "local image tag is not an eligible exact identity reference",
        )
    if not revision:
        raise ImagePolicyError("local_revision_mismatch", "revision is missing")
    if labels.get("io.proberca.source-fingerprint") != source_fingerprint:
        raise ImagePolicyError(
            "local_source_fingerprint_mismatch",
            "OCI source label does not match identity",
        )
    if labels.get("org.opencontainers.image.revision") != revision:
        raise ImagePolicyError(
            "local_revision_mismatch",
            "OCI revision label does not match identity",
        )
    if not architecture or not os_name:
        raise ImagePolicyError("local_platform_mismatch", "identity platform is missing")
    return LocalImageIdentity(
        path=path,
        fingerprint=identity_fingerprint,
        image_tag=image_tag,
        image_id=image_id,
        runtime_source_fingerprint=source_fingerprint,
        revision=revision,
        architecture=architecture,
        os=os_name,
        labels=labels,
    )


def validate_release_binding(
    path: Path,
    identity: LocalImageIdentity,
) -> tuple[ImagePolicyIssue, ...]:
    mode = VERIFIED_LOCAL_IMPORT
    try:
        value, _fingerprint = _load_canonical_object(
            path,
            "local_release_binding_mismatch",
        )
    except ImagePolicyError as error:
        return (
            _issue(error.reason_code, mode=mode, source=path.as_posix()),
        )
    labels = _identity_labels(value)
    supported_modes = value.get("supported_supply_modes")
    common_matches = (
        value.get("ready_for_s4") is True
        and value.get("runtime_source_unchanged") is True
        and value.get("image_unchanged") is True
        and str(value.get("runtime_source_fingerprint", ""))
        == identity.runtime_source_fingerprint
        and str(value.get("image_tag", "")) == identity.image_tag
        and str(value.get("image_id", "")) == identity.image_id
        and labels.get("io.proberca.source-fingerprint")
        == identity.runtime_source_fingerprint
        and labels.get("org.opencontainers.image.revision")
        == identity.revision
        and isinstance(supported_modes, list)
        and VERIFIED_LOCAL_IMPORT in supported_modes
    )
    schema = value.get("schema_version")
    identity_binding_matches = (
        schema == "p11-release-binding-v1"
        and str(value.get("original_image_identity_fingerprint", ""))
        == identity.fingerprint
    ) or (
        schema == "p11-release-binding-v2"
        and str(value.get("image_identity_fingerprint", ""))
        == identity.fingerprint
        and value.get("image_identity_contract") == "stage-independent-v1"
        and value.get("artifact_persistence_contract")
        == "p11-durable-artifacts-v1"
    )
    matches = common_matches and identity_binding_matches
    if not matches:
        return (
            _issue(
                "local_release_binding_mismatch",
                mode=mode,
                source=path.as_posix(),
            ),
        )
    return ()


def validate_node_image_evidence(
    path: Path,
    identity: LocalImageIdentity,
) -> tuple[ImagePolicyIssue, ...]:
    mode = VERIFIED_LOCAL_IMPORT
    try:
        value, _fingerprint = _load_canonical_object(
            path,
            "local_node_identity_mismatch",
        )
    except ImagePolicyError as error:
        return (_issue(error.reason_code, mode=mode, source=path.as_posix()),)
    schema = value.get("schema_version")
    if schema not in {
        "p11-node-image-evidence-v1",
        "p11-node-image-evidence-v2",
    }:
        return (_issue("local_node_identity_mismatch", mode=mode, source=path.as_posix()),)
    if (
        schema == "p11-node-image-evidence-v2"
        and str(value.get("image_identity_fingerprint", ""))
        != identity.fingerprint
    ):
        return (_issue("local_node_identity_mismatch", mode=mode, source=path.as_posix()),)
    nodes = value.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return (_issue("local_node_image_missing", mode=mode, source=path.as_posix()),)
    issues: list[ImagePolicyIssue] = []
    names: set[str] = set()
    for index, node in enumerate(nodes):
        source = f"{path.as_posix()}#nodes[{index}]"
        if not isinstance(node, dict):
            issues.append(_issue("local_node_identity_mismatch", mode=mode, source=source))
            continue
        name = str(node.get("node_name", ""))
        if not name or name in names:
            issues.append(_issue("local_node_image_missing", mode=mode, source=source))
            continue
        names.add(name)
        if (
            str(node.get("architecture", "")) != identity.architecture
            or str(node.get("os", "")).lower() != identity.os
        ):
            issues.append(_issue("local_platform_mismatch", mode=mode, source=source))
            continue
        if (
            node.get("identity_verified") is not True
            or node.get("runtime_query_succeeded") is not True
            or node.get("verification_method") not in _RUNTIME_VERIFICATION_METHODS
        ):
            issues.append(_issue("local_node_identity_mismatch", mode=mode, source=source))
            continue
        if str(node.get("image_tag", "")) != identity.image_tag:
            issues.append(_issue("local_node_identity_mismatch", mode=mode, source=source))
            continue
        if (
            schema == "p11-node-image-evidence-v1"
            and str(node.get("runtime_image_id", "")) != identity.image_id
        ):
            issues.append(_issue("local_image_id_mismatch", mode=mode, source=source))
            continue
        if (
            str(node.get("source_fingerprint", ""))
            != identity.runtime_source_fingerprint
            or str(node.get("revision", "")) != identity.revision
        ):
            issues.append(_issue("local_node_identity_mismatch", mode=mode, source=source))
            continue
        digest_fields = ["runtime_manifest_digest"]
        if schema == "p11-node-image-evidence-v2":
            digest_fields.extend(("runtime_image_id", "runtime_config_id"))
        if any(
            not DIGEST_VALUE.fullmatch(str(node.get(field, "")))
            for field in digest_fields
        ):
            issues.append(_issue("local_node_identity_mismatch", mode=mode, source=source))
    return tuple(issues)


def _normalize_mode(mode: str) -> str:
    aliases = {
        "source": "source_template",
        "source-template": "source_template",
        "source_template": "source_template",
        "final": REGISTRY_DIGEST,
        "final-registry": REGISTRY_DIGEST,
        REGISTRY_DIGEST: REGISTRY_DIGEST,
        "final-local": VERIFIED_LOCAL_IMPORT,
        VERIFIED_LOCAL_IMPORT: VERIFIED_LOCAL_IMPORT,
    }
    try:
        return aliases[mode]
    except KeyError as error:
        raise ValueError(f"unsupported image policy mode: {mode}") from error


def _registry_reason(record: ManifestImage, *, compatibility: bool) -> str:
    if record.image == PLACEHOLDER:
        return (
            "proberca_placeholder_forbidden_in_render"
            if compatibility
            else "proberca_placeholder_forbidden"
        )
    if record.image == SENTINEL:
        return (
            "proberca_sentinel_forbidden_in_render"
            if compatibility
            else "proberca_sentinel_forbidden"
        )
    try:
        validate_third_party_reference(record.image)
    except ImagePolicyError as error:
        return error.reason_code
    return ""


def _validate_local_record(
    record: ManifestImage,
    identity: LocalImageIdentity,
) -> str:
    if record.image == SENTINEL:
        return "proberca_sentinel_forbidden"
    if record.image == PLACEHOLDER:
        return "proberca_placeholder_forbidden"
    if not record.image.strip():
        return "image_empty"
    if _is_latest(record.image):
        return "image_latest_forbidden"
    if "$" in record.image or "{" in record.image or "}" in record.image:
        return "unknown_image_reference"
    if DIGEST.fullmatch(record.image):
        if _repository(record.image) != _repository(identity.image_tag):
            return ""
        return "local_image_tag_mismatch"
    if record.image != identity.image_tag:
        if _repository(record.image) == _repository(identity.image_tag):
            return "local_image_tag_mismatch"
        return "third_party_digest_missing"
    if record.pull_policy != "Never":
        return "local_pull_policy_not_never"
    expected = {
        "proberca.io/image-supply-mode": "verified-local-import",
        "proberca.io/runtime-source-fingerprint": identity.runtime_source_fingerprint,
        "proberca.io/expected-image-id": identity.image_id,
        "proberca.io/image-identity-fingerprint": identity.fingerprint,
    }
    for key, value in expected.items():
        if key not in record.annotations:
            return "local_annotation_missing"
        if record.annotations[key] != value:
            if key == "proberca.io/runtime-source-fingerprint":
                return "local_source_fingerprint_mismatch"
            if key == "proberca.io/expected-image-id":
                return "local_image_id_mismatch"
            if key == "proberca.io/image-identity-fingerprint":
                return "local_identity_fingerprint_mismatch"
            return "local_annotation_mismatch"
    return ""


def validate_manifest_images(
    inputs: Sequence[Path],
    *,
    mode: str,
    identity_record: Path | None = None,
    release_binding: Path | None = None,
    node_image_evidence: Path | None = None,
) -> tuple[ImagePolicyIssue, ...]:
    normalized = _normalize_mode(mode)
    issues: list[ImagePolicyIssue] = list(_kustomize_issues(inputs, mode=normalized))
    identity: LocalImageIdentity | None = None
    if normalized == VERIFIED_LOCAL_IMPORT:
        if identity_record is None:
            issues.append(_issue("local_identity_record_required", mode=normalized))
            return tuple(sorted(issues, key=str))
        if release_binding is None:
            issues.append(
                _issue("local_release_binding_required", mode=normalized)
            )
            return tuple(sorted(issues, key=str))
        if node_image_evidence is None:
            issues.append(_issue("local_node_evidence_required", mode=normalized))
            return tuple(sorted(issues, key=str))
        try:
            identity = load_local_image_identity(identity_record)
        except ImagePolicyError as error:
            issues.append(
                _issue(error.reason_code, mode=normalized, source=identity_record.as_posix())
            )
            return tuple(sorted(issues, key=str))
        binding_issues = validate_release_binding(release_binding, identity)
        if binding_issues:
            issues.extend(binding_issues)
            return tuple(sorted(issues, key=str))
        node_issues = validate_node_image_evidence(node_image_evidence, identity)
        if node_issues:
            issues.extend(node_issues)
            return tuple(sorted(issues, key=str))
    for record in collect_manifest_images(inputs):
        reason = ""
        if normalized == "source_template":
            try:
                if record.image in {SENTINEL, PLACEHOLDER}:
                    validate_proberca_source_reference(
                        record.image,
                        annotations=record.annotations,
                        source=record.source,
                    )
                else:
                    validate_third_party_reference(record.image)
            except ImagePolicyError as error:
                reason = error.reason_code
        elif normalized == REGISTRY_DIGEST:
            reason = _registry_reason(record, compatibility=mode == "final")
        else:
            assert identity is not None
            reason = _validate_local_record(record, identity)
        if reason:
            issues.append(_issue(reason, mode=normalized, record=record))
    return tuple(sorted(issues, key=str))


def _mutable_pod_templates(value: object) -> Iterable[dict]:
    if isinstance(value, dict):
        spec = value.get("spec")
        if isinstance(spec, dict) and any(key in spec for key in _CONTAINER_KEYS):
            yield value
            return
        for child in value.values():
            yield from _mutable_pod_templates(child)
    elif isinstance(value, list):
        for child in value:
            yield from _mutable_pod_templates(child)


def bind_verified_local_manifest(
    source: Path,
    output: Path,
    identity_record: Path,
) -> None:
    identity = load_local_image_identity(identity_record)
    with source.open("r", encoding="utf-8") as handle:
        documents = list(yaml.safe_load_all(handle))
    replacements = 0
    expected_annotations = {
        "proberca.io/image-supply-mode": "verified-local-import",
        "proberca.io/runtime-source-fingerprint": identity.runtime_source_fingerprint,
        "proberca.io/expected-image-id": identity.image_id,
        "proberca.io/image-identity-fingerprint": identity.fingerprint,
    }
    for document in documents:
        for template in _mutable_pod_templates(document):
            spec = template["spec"]
            local_containers: list[dict] = []
            for container_type in _CONTAINER_KEYS:
                containers = spec.get(container_type)
                if not isinstance(containers, list):
                    continue
                for container in containers:
                    if isinstance(container, dict) and container.get("image") == PLACEHOLDER:
                        local_containers.append(container)
            if not local_containers:
                continue
            metadata = template.setdefault("metadata", {})
            annotations = metadata.setdefault("annotations", {})
            annotations.update(expected_annotations)
            for container in local_containers:
                container["image"] = identity.image_tag
                container["imagePullPolicy"] = "Never"
                replacements += 1
    if replacements == 0:
        raise ImagePolicyError(
            "proberca_placeholder_forbidden",
            "local binding found no ProbeRCA placeholder",
        )
    rendered = "\n---\n".join(
        yaml.safe_dump(document, sort_keys=True).rstrip()
        for document in documents
        if document is not None
    )
    output.write_text(rendered + "\n", encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("image", nargs="?")
    parser.add_argument("--allow-sentinel", action="store_true")
    parser.add_argument("--manifest", action="append", type=Path, default=[])
    parser.add_argument(
        "--mode",
        choices=(
            "source",
            "final",
            "source-template",
            "final-registry",
            "final-local",
            REGISTRY_DIGEST,
            VERIFIED_LOCAL_IMPORT,
        ),
        default="source",
    )
    parser.add_argument("--identity-record", type=Path)
    parser.add_argument("--release-binding", type=Path)
    parser.add_argument("--node-image-evidence", type=Path)
    parser.add_argument("--bind-local", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.bind_local:
        if args.image is not None or args.manifest:
            parser.error("--bind-local cannot be combined with image or --manifest")
        if args.identity_record is None or args.output is None:
            parser.error("--bind-local requires --identity-record and --output")
        try:
            bind_verified_local_manifest(
                args.bind_local,
                args.output,
                args.identity_record,
            )
        except ImagePolicyError as error:
            parser.exit(2, f"{error.reason_code}: {error}\n")
        return 0
    if args.manifest:
        if args.image is not None:
            parser.error("image and --manifest are mutually exclusive")
        issues = validate_manifest_images(
            args.manifest,
            mode=args.mode,
            identity_record=args.identity_record,
            release_binding=args.release_binding,
            node_image_evidence=args.node_image_evidence,
        )
        if issues:
            for issue in issues:
                print(issue)
            return 2
        return 0
    if args.image is None:
        parser.error("image or --manifest is required")
    try:
        validate_image_reference(args.image, allow_sentinel=args.allow_sentinel)
    except ValueError as error:
        parser.exit(2, f"invalid image reference: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

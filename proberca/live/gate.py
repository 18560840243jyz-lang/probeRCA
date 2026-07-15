"""Same-image evidence gate for the final live production smoke."""
from __future__ import annotations

from dataclasses import dataclass, field


class FinalDigestGateError(RuntimeError):
    """Final live evidence is incomplete or belongs to another image."""


@dataclass
class FinalDigestGate:
    source_fingerprint: str
    image_digest: str
    required_scenarios: tuple[str, ...]
    evidence: dict = field(default_factory=dict)

    def __init__(self, source_fingerprint, image_digest, required_scenarios):
        if len(str(source_fingerprint)) != 64 or not str(image_digest).startswith("sha256:"):
            raise ValueError("canonical source fingerprint and image digest are required")
        required = tuple(str(item) for item in required_scenarios)
        if not required or len(required) != len(set(required)):
            raise ValueError("required scenarios must be unique and non-empty")
        self.source_fingerprint = str(source_fingerprint)
        self.image_digest = str(image_digest)
        self.required_scenarios = required
        self.evidence = {}

    def record(self, scenario, source_fingerprint, image_digest, payload):
        scenario = str(scenario)
        if scenario not in self.required_scenarios:
            raise FinalDigestGateError(f"unexpected final scenario: {scenario}")
        self.evidence[scenario] = {
            "source_fingerprint": str(source_fingerprint),
            "image_digest": str(image_digest), "payload": dict(payload),
        }

    def assert_current_source(self, source_fingerprint):
        if str(source_fingerprint) != self.source_fingerprint:
            raise FinalDigestGateError("production source fingerprint changed after build")

    def validate(self):
        mismatched = sorted(
            name for name, item in self.evidence.items()
            if item["source_fingerprint"] != self.source_fingerprint
            or item["image_digest"] != self.image_digest)
        if mismatched:
            raise FinalDigestGateError(
                f"final scenario identity mismatch: {mismatched}")
        failed = sorted(
            name for name, item in self.evidence.items()
            if item["payload"].get("passed") is not True)
        if failed:
            raise FinalDigestGateError(f"final scenarios failed: {failed}")
        missing = sorted(set(self.required_scenarios) - set(self.evidence))
        if missing:
            raise FinalDigestGateError(f"missing final scenarios: {missing}")
        return {
            "passed": True, "source_fingerprint": self.source_fingerprint,
            "image_digest": self.image_digest,
            "scenario_count": len(self.evidence),
        }

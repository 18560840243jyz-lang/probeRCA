"""Small strict Prometheus text parser/renderer for the final data plane.

The exporter deliberately avoids a second metrics dependency.  Only the
classic text format emitted by cAdvisor, node_exporter, CoreDNS, and Beyla is
accepted.  Native histograms and exemplars are outside the frozen raw
primitive contract.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Iterable

from .raw import RawCollectionError


_METRIC_NAME = re.compile(r"^[A-Za-z_:][A-Za-z0-9_:]*$")
_LABEL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAMPLE_LINE = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)"
    r"(?:\{(.*)\})?[ \t]+"
    r"([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)"
    r"(?:[eE][+-]?[0-9]+)?|[+-]?[Ii]nf|[Nn]a[Nn])"
    r"(?:[ \t]+[0-9]+)?$"
)


def _unescape_label(value: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(value):
        character = value[index]
        if character != "\\":
            output.append(character)
            index += 1
            continue
        index += 1
        if index >= len(value):
            raise RawCollectionError("Prometheus label ends with an escape")
        escaped = value[index]
        if escaped == "n":
            output.append("\n")
        elif escaped in {'"', "\\"}:
            output.append(escaped)
        else:
            raise RawCollectionError(
                f"unsupported Prometheus label escape \\{escaped}"
            )
        index += 1
    return "".join(output)


def _parse_labels(text: str | None) -> tuple[tuple[str, str], ...]:
    if text is None or text == "":
        return ()
    labels: dict[str, str] = {}
    index = 0
    while index < len(text):
        name_start = index
        while index < len(text) and text[index] != "=":
            index += 1
        name = text[name_start:index].strip()
        if index >= len(text) or not _LABEL_NAME.fullmatch(name):
            raise RawCollectionError("invalid Prometheus label name")
        index += 1
        if index >= len(text) or text[index] != '"':
            raise RawCollectionError("Prometheus label value must be quoted")
        index += 1
        value_start = index
        escaped = False
        pieces: list[str] = []
        while index < len(text):
            character = text[index]
            if character == '"' and not escaped:
                break
            if character == "\\" and not escaped:
                pieces.append(text[value_start:index])
                value_start = index
                escaped = True
                index += 1
                continue
            if escaped:
                escaped = False
            index += 1
        if index >= len(text) or text[index] != '"':
            raise RawCollectionError("unterminated Prometheus label value")
        pieces.append(text[value_start:index])
        value = _unescape_label("".join(pieces))
        if name in labels:
            raise RawCollectionError("duplicate Prometheus label")
        labels[name] = value
        index += 1
        if index == len(text):
            break
        if text[index] != ",":
            raise RawCollectionError("Prometheus labels require commas")
        index += 1
    return tuple(sorted(labels.items()))


@dataclass(frozen=True)
class PrometheusSample:
    name: str
    labels: tuple[tuple[str, str], ...]
    value: float

    @classmethod
    def create(
        cls, name: str, labels: dict[str, str], value: float,
    ) -> "PrometheusSample":
        result = cls(name, tuple(sorted(labels.items())), float(value))
        result.validate(output=True)
        return result

    def validate(self, *, output: bool = False) -> None:
        if not _METRIC_NAME.fullmatch(self.name):
            raise RawCollectionError("invalid Prometheus metric name")
        if len(self.labels) != len(set(key for key, _ in self.labels)):
            raise RawCollectionError("duplicate Prometheus output label")
        if any(
            not _LABEL_NAME.fullmatch(key)
            or not isinstance(value, str)
            or "\x00" in value
            for key, value in self.labels
        ):
            raise RawCollectionError("invalid Prometheus output labels")
        if not math.isfinite(self.value) or (output and self.value < 0):
            raise RawCollectionError(
                "final raw primitive values must be finite and non-negative"
            )

    @property
    def label_dict(self) -> dict[str, str]:
        return dict(self.labels)

    @property
    def identity(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        return self.name, self.labels


def parse_prometheus_text(text: str) -> tuple[PrometheusSample, ...]:
    if not isinstance(text, str):
        raise RawCollectionError("Prometheus exposition must be text")
    output = []
    identities = set()
    for number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE_LINE.fullmatch(line)
        if match is None:
            raise RawCollectionError(
                f"unsupported Prometheus sample at line {number}"
            )
        try:
            value = float(match.group(3))
        except ValueError as error:
            raise RawCollectionError("invalid Prometheus sample value") from error
        if not math.isfinite(value):
            continue
        sample = PrometheusSample(
            match.group(1), _parse_labels(match.group(2)), value
        )
        sample.validate()
        if sample.identity in identities:
            raise RawCollectionError(
                "Prometheus exposition contains duplicate series"
            )
        identities.add(sample.identity)
        output.append(sample)
    return tuple(output)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_prometheus_text(
    samples: Iterable[PrometheusSample], *, timestamp_ms: int,
) -> str:
    if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int) \
            or timestamp_ms <= 0:
        raise RawCollectionError("Prometheus output timestamp must be positive")
    ordered = sorted(samples, key=lambda item: item.identity)
    identities = [item.identity for item in ordered]
    if len(identities) != len(set(identities)):
        raise RawCollectionError("final exporter produced duplicate series")
    lines = []
    for sample in ordered:
        sample.validate(output=True)
        label_text = ",".join(
            f'{key}="{_escape_label(value)}"'
            for key, value in sample.labels
        )
        labels = f"{{{label_text}}}" if label_text else ""
        lines.append(
            f"{sample.name}{labels} {sample.value:.17g} {timestamp_ms}"
        )
    return "\n".join(lines) + "\n"

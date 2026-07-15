"""Strict Prometheus-compatible HTTP API client."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from pathlib import Path

import requests


class PrometheusResponseError(RuntimeError):
    """Prometheus failure with stable retry classification."""

    def __init__(self, message, *, reason_code="response_error", retryable=True):
        self.reason_code = str(reason_code)
        self.retryable = bool(retryable)
        super().__init__(message)


@dataclass(frozen=True)
class PrometheusSample:
    labels: dict[str, str]
    timestamp_ns: int
    value: float

    def __post_init__(self) -> None:
        if self.timestamp_ns < 0:
            raise ValueError("Prometheus sample timestamp must be non-negative")
        value = float(self.value)
        if not math.isfinite(value):
            raise ValueError("Prometheus sample value must be finite")
        object.__setattr__(self, "value", value)
        if not isinstance(self.labels, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in self.labels.items()):
            raise TypeError("Prometheus sample labels must be strings")


@dataclass(frozen=True)
class QueryMetadata:
    query_duration_ms: float
    sample_count: int
    retry_count: int


class PrometheusClient:
    def __init__(self, base_url, *, token_file=None, ca_file=None, client_cert_file=None,
                 client_key_file=None, session=None, timeout_sec=10.0, max_retries=3,
                 retry_initial_sec=0.5, retry_max_sec=5.0, reject_partial_response=True,
                 sleep=time.sleep):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.retry_initial_sec = retry_initial_sec
        self.retry_max_sec = retry_max_sec
        self.reject_partial_response = reject_partial_response
        self.session = session or requests.Session()
        self.sleep = sleep
        self.verify = ca_file if ca_file else True
        self.cert = ((client_cert_file, client_key_file) if client_cert_file else None)
        self.headers = {}
        if token_file:
            token = Path(token_file).read_text(encoding="utf-8").strip()
            if not token:
                raise ValueError("Prometheus token file is empty")
            self.headers["Authorization"] = f"Bearer {token}"

    def _request(self, endpoint, params, timeout):
        retries = 0
        started = time.monotonic_ns()
        while True:
            try:
                response = self.session.get(
                    self.base_url + endpoint, params=params, timeout=timeout,
                    verify=self.verify, cert=self.cert, headers=self.headers)
            except requests.RequestException as error:
                retryable = True
                last_error = error
            else:
                if response.status_code in {401, 403}:
                    raise PrometheusResponseError(
                        f"Prometheus authorization failed status={response.status_code}", reason_code="authorization", retryable=False)
                retryable = response.status_code == 429 or response.status_code >= 500
                if not retryable and response.status_code >= 400:
                    raise PrometheusResponseError(
                        f"Prometheus HTTP status={response.status_code}",
                        reason_code="http_client_error", retryable=False,
                    )
                if not retryable:
                    try:
                        payload = response.json()
                    except Exception as error:
                        raise PrometheusResponseError(
                            "Prometheus response is not JSON",
                            reason_code="invalid_json", retryable=False,
                        ) from error
                    if payload.get("status") != "success":
                        raise PrometheusResponseError(
                            "Prometheus API status is not success",
                            reason_code="api_status", retryable=True,
                        )
                    if self.reject_partial_response and payload.get("warnings"):
                        raise PrometheusResponseError(
                            "Prometheus partial response was rejected",
                            reason_code="partial_response", retryable=True,
                        )
                    duration = (time.monotonic_ns() - started) / 1_000_000
                    return payload, retries, duration
                last_error = PrometheusResponseError(
                    f"retryable Prometheus HTTP status={response.status_code}")
            if retries >= self.max_retries:
                raise PrometheusResponseError(
                    f"Prometheus request failed after {retries} retries") from last_error
            delay = min(self.retry_initial_sec * (2 ** retries), self.retry_max_sec)
            retries += 1
            self.sleep(delay)

    @staticmethod
    def normalize_samples(samples, window_start_ns, window_end_ns):
        unique = {}
        for sample in samples:
            if not window_start_ns <= sample.timestamp_ns < window_end_ns:
                continue
            key = (tuple(sorted(sample.labels.items())), sample.timestamp_ns)
            previous = unique.get(key)
            if previous is not None and previous.value != sample.value:
                raise PrometheusResponseError(
                    "conflicting duplicate Prometheus sample",
                    reason_code="conflicting_sample", retryable=False,
                )
            unique[key] = sample
        return tuple(sorted(unique.values(), key=lambda item: (
            item.timestamp_ns, tuple(sorted(item.labels.items())))))

    def query_window(self, spec, window_start_ns, window_end_ns):
        timeout = min(float(spec.query_timeout_sec), float(self.timeout_sec))
        if spec.query_mode == "range":
            endpoint = "/api/v1/query_range"
            params = {"query": spec.promql, "start": window_start_ns / 1e9,
                      "end": window_end_ns / 1e9, "step": 1}
        else:
            endpoint = "/api/v1/query"
            params = {"query": spec.promql, "time": window_end_ns / 1e9}
        payload, retries, duration = self._request(endpoint, params, timeout)
        samples = []
        for series in (payload.get("data") or {}).get("result") or []:
            labels = {str(key): str(value) for key, value in (series.get("metric") or {}).items()}
            values = series.get("values")
            if values is None and series.get("value") is not None:
                values = [series["value"]]
            for timestamp, value in values or []:
                try:
                    samples.append(PrometheusSample(
                        labels, int(float(timestamp) * 1_000_000_000), float(value)))
                except (TypeError, ValueError) as error:
                    raise PrometheusResponseError(
                        "invalid Prometheus sample",
                        reason_code="invalid_sample", retryable=False,
                    ) from error
        normalized = self.normalize_samples(samples, window_start_ns, window_end_ns)
        if not normalized and not spec.allow_empty:
            raise PrometheusResponseError(
                f"query spec {spec.spec_id} returned no samples",
                reason_code="no_samples", retryable=True,
            )
        return normalized, QueryMetadata(duration, len(normalized), retries)

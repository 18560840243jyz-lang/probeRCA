#!/usr/bin/env python3
"""One bounded open-loop Online Boutique load source for the multi-node run."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable

import requests


PRODUCTS = (
    "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM",
    "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO",
    "6E92ZMYYFZ",
)
EXPECTED_BEHAVIORS = (
    "browse_search_list", "detail_recommendation_ad_currency", "cart", "checkout",
)
_THREAD_LOCAL = threading.local()


class LoadBackpressureError(RuntimeError):
    pass


@dataclass(frozen=True)
class LoadConfig:
    base_url: str
    target_arrival_rate_rps: float
    workers: int
    maximum_pending: int
    request_timeout_sec: float
    duration_sec: float
    seed: int
    behavior_weights: dict[str, int]
    load_profile_id: str = "development-unfrozen"
    load_profile_fingerprint: str = "development-unfrozen"

    def __post_init__(self) -> None:
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("base URL must use HTTP or HTTPS")
        if self.target_arrival_rate_rps <= 0 or self.workers <= 0:
            raise ValueError("arrival rate and worker count must be positive")
        if self.maximum_pending < self.workers:
            raise ValueError("maximum pending must be at least the worker count")
        if self.request_timeout_sec <= 0 or self.duration_sec < 0:
            raise ValueError("timeouts and duration are invalid")
        if set(self.behavior_weights) != set(EXPECTED_BEHAVIORS):
            raise ValueError("behavior mix is incomplete")
        if sum(self.behavior_weights.values()) != 100:
            raise ValueError("behavior weights must sum to 100")
        if not self.load_profile_id or not self.load_profile_fingerprint:
            raise ValueError("load profile identity is incomplete")


class IntentLedgerWriter:
    """Append and durably flush immutable, interval-level demand intentions."""

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def __call__(self, record: dict[str, object]) -> None:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        with open(self.path, "a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        _THREAD_LOCAL.session = session
    return session


def _request(config: LoadConfig, method: str, path: str, **kwargs) -> None:
    response = _session().request(
        method, config.base_url.rstrip("/") + path,
        timeout=config.request_timeout_sec, allow_redirects=True, **kwargs,
    )
    response.raise_for_status()


def _browse(config: LoadConfig, _rng: random.Random) -> None:
    _request(config, "GET", "/")


def _detail(config: LoadConfig, rng: random.Random) -> None:
    _request(config, "GET", f"/product/{rng.choice(PRODUCTS)}")


def _add_cart(config: LoadConfig, rng: random.Random) -> None:
    _request(config, "POST", "/cart", data={
        "product_id": rng.choice(PRODUCTS), "quantity": "1",
    })
    _request(config, "GET", "/cart")


def _checkout(config: LoadConfig, rng: random.Random) -> None:
    _request(config, "POST", "/cart", data={
        "product_id": rng.choice(PRODUCTS), "quantity": "1",
    })
    _request(config, "POST", "/cart/checkout", data={
        "email": "someone@example.com",
        "street_address": "1600 Amphitheatre Parkway",
        "zip_code": "94043",
        "city": "Mountain View",
        "state": "CA",
        "country": "United States",
        "credit_card_number": "4432801561520454",
        "credit_card_expiration_month": "1",
        "credit_card_expiration_year": "2039",
        "credit_card_cvv": "672",
    })


BEHAVIORS: dict[str, Callable[[LoadConfig, random.Random], None]] = {
    "browse_search_list": _browse,
    "detail_recommendation_ad_currency": _detail,
    "cart": _add_cart,
    "checkout": _checkout,
}


def _choose_behavior(config: LoadConfig, rng: random.Random) -> str:
    return rng.choices(
        list(EXPECTED_BEHAVIORS),
        weights=[config.behavior_weights[name] for name in EXPECTED_BEHAVIORS],
        k=1,
    )[0]


def run_open_loop(
    config: LoadConfig,
    *,
    clock=time.monotonic,
    sleeper=time.sleep,
    executor_factory=concurrent.futures.ThreadPoolExecutor,
    event_sink=print,
    arrival_hook=None,
    intent_sink=None,
    intent_interval_sec: int = 5,
    wall_clock_ns=time.time_ns,
) -> dict[str, int | float]:
    """Schedule Poisson arrivals independently of request completion."""
    scheduler_rng = random.Random(config.seed)
    start = clock()
    start_wall_ns = int(wall_clock_ns())
    deadline = start + config.duration_sec if config.duration_sec else None
    next_arrival = start
    submitted = 0
    completed = 0
    failed = 0
    pending: set[concurrent.futures.Future] = set()
    lock = threading.Lock()
    if intent_interval_sec <= 0:
        raise ValueError("intent interval must be positive")
    intent_interval_ns = int(intent_interval_sec) * 1_000_000_000
    intent_bucket_start_ns = (
        start_wall_ns // intent_interval_ns
    ) * intent_interval_ns
    intent_counts = {name: 0 for name in EXPECTED_BEHAVIORS}

    def emit_completed_intent_buckets(target_epoch_ns: int) -> None:
        nonlocal intent_bucket_start_ns, intent_counts
        while intent_bucket_start_ns + intent_interval_ns <= target_epoch_ns:
            if intent_sink is not None:
                intent_sink({
                    "schema_version": "probeRCA-load-behavior-intent-v1",
                    "interval_start_ns": intent_bucket_start_ns,
                    "interval_end_ns": intent_bucket_start_ns + intent_interval_ns,
                    "load_profile_id": config.load_profile_id,
                    "load_profile_fingerprint": config.load_profile_fingerprint,
                    "scheduled_intents": sum(intent_counts.values()),
                    "behavior_intents": dict(intent_counts),
                })
            intent_bucket_start_ns += intent_interval_ns
            intent_counts = {name: 0 for name in EXPECTED_BEHAVIORS}

    def invoke(index: int, behavior_name: str) -> None:
        nonlocal completed, failed
        rng = random.Random(config.seed + index * 104729)
        try:
            BEHAVIORS[behavior_name](config, rng)
        except Exception as error:
            with lock:
                failed += 1
            event_sink(json.dumps({
                "event": "request_failed", "arrival_index": index,
                "behavior": behavior_name, "error_type": type(error).__name__,
            }, sort_keys=True))
        finally:
            with lock:
                completed += 1

    with executor_factory(max_workers=config.workers) as executor:
        while deadline is None or next_arrival < deadline:
            now = clock()
            if now < next_arrival:
                sleeper(next_arrival - now)
            done = {future for future in pending if future.done()}
            pending.difference_update(done)
            if len(pending) >= config.maximum_pending:
                raise LoadBackpressureError(
                    "open_loop_pending_limit_exceeded"
                )
            behavior = _choose_behavior(config, scheduler_rng)
            target_epoch_ns = start_wall_ns + int(
                round((next_arrival - start) * 1_000_000_000)
            )
            emit_completed_intent_buckets(target_epoch_ns)
            intent_counts[behavior] += 1
            if arrival_hook is not None:
                arrival_hook(submitted, next_arrival, behavior)
            pending.add(executor.submit(invoke, submitted, behavior))
            submitted += 1
            next_arrival += scheduler_rng.expovariate(
                config.target_arrival_rate_rps
            )
        for future in concurrent.futures.as_completed(pending):
            future.result()
    if deadline is not None:
        emit_completed_intent_buckets(
            start_wall_ns + int(round(config.duration_sec * 1_000_000_000))
        )
    elapsed = max(0.0, clock() - start)
    scheduling_elapsed = config.duration_sec if config.duration_sec else elapsed
    summary = {
        "event": "load_finished",
        "submitted": submitted,
        "completed": completed,
        "failed": failed,
        "duration_sec": elapsed,
        "achieved_arrival_rate_rps": (
            submitted / scheduling_elapsed if scheduling_elapsed else 0.0
        ),
    }
    event_sink(json.dumps(summary, sort_keys=True))
    return summary


def _weights(value: str) -> dict[str, int]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("behavior weights must be a JSON object")
    return {str(key): int(weight) for key, weight in parsed.items()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get(
        "TARGET_BASE_URL", "http://frontend.online-boutique.svc.cluster.local:80",
    ))
    parser.add_argument("--rate", type=float, default=float(os.environ.get(
        "TARGET_ARRIVAL_RATE_RPS", "25",
    )))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("WORKERS", "8")))
    parser.add_argument("--maximum-pending", type=int, default=int(os.environ.get(
        "MAXIMUM_PENDING", "64",
    )))
    parser.add_argument("--request-timeout", type=float, default=float(os.environ.get(
        "REQUEST_TIMEOUT_SEC", "10",
    )))
    parser.add_argument("--duration", type=float, default=float(os.environ.get("DURATION_SEC", "0")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("LOAD_SEED", "20260824")))
    parser.add_argument("--behavior-weights", type=_weights, default=_weights(os.environ.get(
        "BEHAVIOR_WEIGHTS_JSON",
        '{"browse_search_list":40,"detail_recommendation_ad_currency":25,"cart":20,"checkout":15}',
    )))
    parser.add_argument("--load-profile-id", default=os.environ.get(
        "LOAD_PROFILE_ID", "development-unfrozen",
    ))
    parser.add_argument("--load-profile-fingerprint", default=os.environ.get(
        "LOAD_PROFILE_FINGERPRINT", "development-unfrozen",
    ))
    parser.add_argument("--intent-ledger", default=os.environ.get(
        "LOAD_INTENT_LEDGER", "",
    ))
    parser.add_argument("--intent-interval", type=int, default=int(os.environ.get(
        "LOAD_INTENT_INTERVAL_SEC", "5",
    )))
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    config = LoadConfig(
        base_url=args.base_url,
        target_arrival_rate_rps=args.rate,
        workers=args.workers,
        maximum_pending=args.maximum_pending,
        request_timeout_sec=args.request_timeout,
        duration_sec=args.duration,
        seed=args.seed,
        behavior_weights=args.behavior_weights,
        load_profile_id=args.load_profile_id,
        load_profile_fingerprint=args.load_profile_fingerprint,
    )
    intent_sink = IntentLedgerWriter(args.intent_ledger) if args.intent_ledger else None
    run_open_loop(
        config, intent_sink=intent_sink, intent_interval_sec=args.intent_interval,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

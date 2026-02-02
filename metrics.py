from __future__ import annotations
from dataclasses import dataclass
from threading import Lock


@dataclass
class Counters:
    requests_total: int = 0
    errors_total: int = 0
    rate_limited_total: int = 0
    auth_failed_total: int = 0


_lock = Lock()
_counters = Counters()


def inc_requests() -> None:
    with _lock:
        _counters.requests_total += 1


def inc_errors() -> None:
    with _lock:
        _counters.errors_total += 1


def inc_rate_limited() -> None:
    with _lock:
        _counters.rate_limited_total += 1


def inc_auth_failed() -> None:
    with _lock:
        _counters.auth_failed_total += 1


def snapshot() -> dict:
    with _lock:
        return {
            "requests_total": _counters.requests_total,
            "errors_total": _counters.errors_total,
            "rate_limited_total": _counters.rate_limited_total,
            "auth_failed_total": _counters.auth_failed_total,
        }

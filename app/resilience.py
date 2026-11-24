"""Fault-tolerance primitives (P9): timeout + retry with exponential backoff + circuit breaker.

Circuit breaker states
    CLOSED    -> calls go through; consecutive failures are counted
    OPEN      -> after `failure_threshold` failures, calls fail fast (no network) for `reset_timeout_s`
    HALF_OPEN -> after the cooldown, ONE trial call is allowed: success -> CLOSED, failure -> OPEN
"""
import threading
import time


class CircuitOpenError(Exception):
    """Raised immediately when the breaker is OPEN (fail fast)."""


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, reset_timeout_s: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout_s = reset_timeout_s
        self._state = "CLOSED"
        self._failures = 0
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == "OPEN" and time.monotonic() - self._opened_at >= self.reset_timeout_s:
                self._state = "HALF_OPEN"
            return self._state

    def call(self, fn, *args, **kwargs):
        if self.state == "OPEN":
            raise CircuitOpenError(f"circuit '{self.name}' is OPEN")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._record_success()
        return result

    def _record_failure(self):
        with self._lock:
            self._failures += 1
            if self._state == "HALF_OPEN" or self._failures >= self.failure_threshold:
                self._state = "OPEN"
                self._opened_at = time.monotonic()

    def _record_success(self):
        with self._lock:
            self._failures = 0
            self._state = "CLOSED"

    def snapshot(self) -> dict:
        return {"name": self.name, "state": self.state, "consecutive_failures": self._failures}


def retry(fn, attempts: int = 3, base_delay_s: float = 0.5, retry_on=(Exception,)):
    """Call fn(); on failure wait base_delay, 2*base_delay, ... and try again (exponential backoff)."""
    last_exc = None
    for i in range(attempts):
        try:
            return fn()
        except CircuitOpenError:
            raise  # never retry against an open circuit
        except retry_on as exc:
            last_exc = exc
            if i < attempts - 1:
                time.sleep(base_delay_s * (2 ** i))
    raise last_exc

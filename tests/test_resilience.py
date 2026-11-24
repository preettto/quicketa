import time

import pytest

from app.resilience import CircuitBreaker, CircuitOpenError, retry


def boom():
    raise ConnectionError("down")


def test_breaker_opens_after_threshold_and_fails_fast():
    cb = CircuitBreaker("t", failure_threshold=3, reset_timeout_s=60)
    for _ in range(3):
        with pytest.raises(ConnectionError):
            cb.call(boom)
    assert cb.state == "OPEN"
    with pytest.raises(CircuitOpenError):      # no call made: fail fast
        cb.call(lambda: "never")


def test_breaker_half_open_then_closes_on_success():
    cb = CircuitBreaker("t", failure_threshold=1, reset_timeout_s=0.05)
    with pytest.raises(ConnectionError):
        cb.call(boom)
    assert cb.state == "OPEN"
    time.sleep(0.06)
    assert cb.state == "HALF_OPEN"
    assert cb.call(lambda: 42) == 42
    assert cb.state == "CLOSED"


def test_breaker_half_open_failure_reopens():
    cb = CircuitBreaker("t", failure_threshold=1, reset_timeout_s=0.05)
    with pytest.raises(ConnectionError):
        cb.call(boom)
    time.sleep(0.06)
    with pytest.raises(ConnectionError):
        cb.call(boom)
    assert cb.state == "OPEN"


def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError
        return "ok"

    assert retry(flaky, attempts=3, base_delay_s=0.001) == "ok"
    assert calls["n"] == 3


def test_retry_gives_up_and_does_not_retry_open_circuit():
    with pytest.raises(ConnectionError):
        retry(boom, attempts=2, base_delay_s=0.001)
    calls = {"n": 0}

    def open_circuit():
        calls["n"] += 1
        raise CircuitOpenError("open")

    with pytest.raises(CircuitOpenError):
        retry(open_circuit, attempts=3, base_delay_s=0.001)
    assert calls["n"] == 1

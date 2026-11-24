from app.services import weather


def test_wmo_mapping():
    assert weather.wmo_to_condition(0, 5) == "Sunny"
    assert weather.wmo_to_condition(3, 5) == "Cloudy"
    assert weather.wmo_to_condition(45, 5) == "Fog"
    assert weather.wmo_to_condition(63, 5) == "Stormy"
    assert weather.wmo_to_condition(1, 40) == "Windy"


def test_chaos_switch_degrades_and_opens_breaker(fake_cache, monkeypatch):
    monkeypatch.setattr(weather, "breaker", weather.CircuitBreaker("test", failure_threshold=3, reset_timeout_s=60))
    monkeypatch.setattr("app.resilience.time.sleep", lambda s: None)
    fake_cache[weather.CHAOS_KEY] = "1"
    w, status = weather.get_weather(22.7, 75.9)
    assert status == "fallback" and w["source"] == "fallback" and w["temp_c"] is None
    weather.get_weather(22.7, 75.9)                    # 2 calls x 2 attempts = 4 failures
    assert weather.breaker.state == "OPEN"


def test_cache_hit_skips_api(fake_cache, monkeypatch):
    def must_not_call(*a, **k):
        raise AssertionError("API must not be called on a cache hit")
    monkeypatch.setattr(weather, "_fetch", must_not_call)
    from datetime import datetime
    fake_cache[f"weather:22.70:75.90:{datetime.now():%Y%m%d%H}"] = {"temp_c": 25, "source": "api"}
    w, status = weather.get_weather(22.7, 75.9)
    assert status == "hit" and w["temp_c"] == 25

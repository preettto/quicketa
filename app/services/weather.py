"""Real-time weather lookup (Open-Meteo forecast API).

Layers, outermost first:
  1. Redis cache   key weather:{lat:.2f}:{lon:.2f}:{YYYYmmddHH}, TTL 10 min    (P7)
  2. retry         2 attempts, exponential backoff                              (P9)
  3. circuit breaker  opens after 3 consecutive failures for 30 s               (P9)
  4. timeout       5 s per HTTP call                                            (P9)
If everything fails -> FALLBACK: numeric weather = None, the model's imputer substitutes the
training median, so the customer still gets an ETA (graceful degradation).

Chaos switch: when Redis key 'chaos:weather_down' == 1 every call fails on purpose (demo of P9).
"""
from datetime import datetime

import requests

from app import cache, config
from app.resilience import CircuitBreaker, CircuitOpenError, retry

breaker = CircuitBreaker("open-meteo-live", failure_threshold=3, reset_timeout_s=30)
CHAOS_KEY = "chaos:weather_down"
FALLBACK = {"temp_c": None, "precip_mm": None, "wind_kmh": None, "condition": "Unknown", "source": "fallback"}


def wmo_to_condition(code, wind_kmh) -> str:
    """Map WMO weather codes to the dataset's categories."""
    if code is None:
        return "Unknown"
    if code >= 51:                 # drizzle, rain, snow, thunderstorm
        return "Stormy"
    if code in (45, 48):           # fog
        return "Fog"
    if wind_kmh is not None and wind_kmh >= 30:
        return "Windy"
    if code in (2, 3):
        return "Cloudy"
    return "Sunny"


def _fetch(lat: float, lon: float, chaos: bool = False) -> dict:
    if chaos:
        raise ConnectionError("simulated weather API outage (chaos switch ON)")
    resp = requests.get(config.WEATHER_FORECAST_URL, timeout=config.WEATHER_TIMEOUT_S, params={
        "latitude": round(lat, 3), "longitude": round(lon, 3),
        "current": "temperature_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "Asia/Kolkata",
    })
    resp.raise_for_status()
    cur = resp.json()["current"]
    return {"temp_c": cur["temperature_2m"], "precip_mm": cur["precipitation"],
            "wind_kmh": cur["wind_speed_10m"],
            "condition": wmo_to_condition(cur.get("weather_code"), cur["wind_speed_10m"]),
            "source": "api"}


def get_weather(lat: float, lon: float):
    """Returns (weather_dict, cache_status) where cache_status in {'hit', 'miss', 'fallback'}."""
    chaos = cache.get_flag(CHAOS_KEY)
    key = f"weather:{lat:.2f}:{lon:.2f}:{datetime.now():%Y%m%d%H}"
    if not chaos:
        hit = cache.get_json(key)
        if hit:
            return hit, "hit"
    try:
        w = retry(lambda: breaker.call(_fetch, lat, lon, chaos), attempts=2, base_delay_s=0.5)
    except (CircuitOpenError, Exception):
        return dict(FALLBACK), "fallback"
    cache.set_json(key, w, ttl_s=600)
    return w, "miss"

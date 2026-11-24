"""Real-time weather lookup (Open-Meteo forecast API) protected by timeout + retry + circuit breaker.

If the API is slow/down, or the breaker is OPEN, we DEGRADE GRACEFULLY: numeric weather is set to
None and the model's imputer substitutes the training median -> the customer still gets an ETA.
"""
import requests

from app import config
from app.resilience import CircuitBreaker, CircuitOpenError, retry

breaker = CircuitBreaker("open-meteo-live", failure_threshold=3, reset_timeout_s=30)


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


def _fetch(lat: float, lon: float) -> dict:
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


FALLBACK = {"temp_c": None, "precip_mm": None, "wind_kmh": None, "condition": "Unknown", "source": "fallback"}


def get_weather(lat: float, lon: float) -> dict:
    try:
        return retry(lambda: breaker.call(_fetch, lat, lon), attempts=2, base_delay_s=0.5)
    except (CircuitOpenError, Exception):
        return dict(FALLBACK)

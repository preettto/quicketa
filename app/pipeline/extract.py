"""EXTRACT stage - pull raw data from three heterogeneous sources.

  1. CSV file           : historical orders (Kaggle food-delivery dataset)
  2. Relational DB      : partner & restaurant master data (PostgreSQL 'partner service')
  3. REST API           : hourly historical weather per city (Open-Meteo archive API)
"""
import json
import os

import pandas as pd
import requests

from app import config
from app.pipeline.seed_postgres import read_raw
from app.resilience import CircuitBreaker, CircuitOpenError, retry

weather_breaker = CircuitBreaker("open-meteo-archive", failure_threshold=3, reset_timeout_s=30)


def extract_csv() -> pd.DataFrame:
    return read_raw(config.RAW_CSV)


def extract_postgres():
    import psycopg2
    def query(cur, sql):
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

    with psycopg2.connect(config.PG_DSN) as conn, conn.cursor() as cur:
        partners = query(cur, "SELECT partner_id, age, rating FROM delivery_partners")
        restaurants = query(cur, "SELECT restaurant_id, city_code, lat, lon FROM restaurants")
    for c in ["age", "rating"]:
        partners[c] = pd.to_numeric(partners[c], errors="coerce")   # NUMERIC -> Decimal -> float
    for c in ["lat", "lon"]:
        restaurants[c] = restaurants[c].astype(float)
    return partners, restaurants


def _fetch_city_weather(lat: float, lon: float, start: str, end: str) -> dict:
    resp = requests.get(config.WEATHER_ARCHIVE_URL, timeout=config.WEATHER_TIMEOUT_S, params={
        "latitude": round(lat, 4), "longitude": round(lon, 4),
        "start_date": start, "end_date": end,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "Asia/Kolkata",
    })
    resp.raise_for_status()
    return resp.json()


def extract_weather(city_centroids: pd.DataFrame, start: str, end: str):
    """One API call per city (not per order): 22 calls instead of ~42,000.

    Responses are cached on disk so re-runs are instant and the API is not hammered.
    Every call is protected by timeout + retry(backoff) + circuit breaker.
    Returns (hourly_weather_df, status_per_city).
    """
    os.makedirs(config.CACHE_DIR, exist_ok=True)
    frames, status = [], {}
    for row in city_centroids.itertuples():
        cache_file = os.path.join(config.CACHE_DIR, f"weather_{row.city_code}_{start}_{end}.json")
        try:
            if os.path.exists(cache_file):
                with open(cache_file) as f:
                    data = json.load(f)
                status[row.city_code] = "cache"
            else:
                data = retry(lambda: weather_breaker.call(_fetch_city_weather, row.lat, row.lon, start, end),
                             attempts=3, base_delay_s=0.5)
                with open(cache_file, "w") as f:
                    json.dump(data, f)
                status[row.city_code] = "api"
        except (CircuitOpenError, Exception) as exc:  # degrade gracefully: continue without this city
            status[row.city_code] = f"failed: {type(exc).__name__}"
            continue
        h = data["hourly"]
        frames.append(pd.DataFrame({
            "city_code": row.city_code,
            "ts_hour": pd.to_datetime(h["time"]),
            "temp_c": h["temperature_2m"],
            "precip_mm": h["precipitation"],
            "wind_kmh": h["wind_speed_10m"],
        }))
    weather = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["city_code", "ts_hour", "temp_c", "precip_mm", "wind_kmh"])
    return weather, status

"""Single source of truth for feature engineering (heavy T).

The SAME function is used by the training pipeline and by the online API, so the model
always sees features computed identically (avoids training/serving skew).

Only information available AT ORDER TIME is used. `Time_Order_picked` is deliberately
excluded: it is only known after the order is placed, so using it would leak the future.
"""
import numpy as np
import pandas as pd

NUMERIC_FEATURES = [
    "distance_km", "order_hour", "day_of_week", "is_weekend", "is_peak",
    "partner_age", "partner_rating", "vehicle_condition", "multiple_deliveries",
    "temp_c", "precip_mm", "wind_kmh",
]
CATEGORICAL_FEATURES = [
    "traffic", "weather_condition", "vehicle_type", "order_type", "city_type", "festival",
]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
PEAK_HOURS = {12, 13, 14, 19, 20, 21, 22}


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points on Earth, in km (vectorised)."""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """df must contain the canonical clean columns produced by the pipeline / API input."""
    out = pd.DataFrame(index=df.index)
    out["distance_km"] = haversine_km(df["rest_lat"], df["rest_lon"], df["del_lat"], df["del_lon"])
    ts = pd.to_datetime(df["order_ts"])
    out["order_hour"] = ts.dt.hour
    out["day_of_week"] = ts.dt.dayofweek
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    out["is_peak"] = out["order_hour"].isin(PEAK_HOURS).astype(int)
    for c in ["partner_age", "partner_rating", "vehicle_condition", "multiple_deliveries",
              "temp_c", "precip_mm", "wind_kmh"]:
        out[c] = pd.to_numeric(df.get(c), errors="coerce")
    for c in CATEGORICAL_FEATURES:
        out[c] = df.get(c).fillna("Unknown").astype(str)
    return out[ALL_FEATURES]

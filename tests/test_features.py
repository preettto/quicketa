import pandas as pd
import pytest

from app.ml.features import ALL_FEATURES, build_features, haversine_km


def test_haversine_delhi_mumbai():
    assert haversine_km(28.6139, 77.2090, 19.0760, 72.8777) == pytest.approx(1150, abs=15)


def order(ts="2022-03-05 20:15"):   # 5 March 2022 = Saturday
    return {"rest_lat": 22.745, "rest_lon": 75.892, "del_lat": 22.765, "del_lon": 75.912,
            "order_ts": ts, "traffic": "Jam", "weather_condition": None, "vehicle_type": "scooter",
            "vehicle_condition": 1, "order_type": "Meal", "multiple_deliveries": 0, "festival": "No",
            "city_type": "Urban", "partner_age": 30, "partner_rating": 4.5,
            "temp_c": None, "precip_mm": None, "wind_kmh": None}


def test_feature_columns_and_derivations():
    X = build_features(pd.DataFrame([order()]))
    assert list(X.columns) == ALL_FEATURES
    row = X.iloc[0]
    assert row.order_hour == 20 and row.is_peak == 1
    assert row.day_of_week == 5 and row.is_weekend == 1
    assert 2.5 < row.distance_km < 3.5
    assert row.weather_condition == "Unknown"          # missing category -> explicit 'Unknown'


def test_off_peak_weekday():
    row = build_features(pd.DataFrame([order("2022-03-07 10:00")])).iloc[0]   # Monday morning
    assert row.is_peak == 0 and row.is_weekend == 0


def test_no_future_leakage():
    assert not any("picked" in f for f in ALL_FEATURES)

"""Loads the trained pipeline once per process and turns an order dict into an ETA."""
import json
import os
from functools import lru_cache

import joblib
import pandas as pd

from app import config
from app.ml.features import build_features


@lru_cache(maxsize=1)
def load_model():
    model = joblib.load(os.path.join(config.MODEL_DIR, "eta_model.joblib"))
    with open(os.path.join(config.MODEL_DIR, "metadata.json")) as f:
        meta = json.load(f)
    return model, meta


def predict_minutes(order: dict) -> float:
    """order uses the canonical column names of the pipeline (rest_lat, order_ts, traffic, ...)."""
    model, _ = load_model()
    X = build_features(pd.DataFrame([order]))
    minutes = float(model.predict(X)[0])
    return round(min(max(minutes, 5.0), 90.0), 1)   # guard-rail: never show absurd ETAs

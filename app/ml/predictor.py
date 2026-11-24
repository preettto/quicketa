"""Loads the trained pipeline once per process and turns an order into an ETA."""
import hashlib
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


def features_for(order: dict) -> pd.DataFrame:
    return build_features(pd.DataFrame([order]))


def feature_key(X: pd.DataFrame) -> str:
    """Stable hash of the feature row -> used as the ETA cache key."""
    row = {k: (None if pd.isna(v) else (round(v, 4) if isinstance(v, float) else v))
           for k, v in X.iloc[0].to_dict().items()}
    return hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()[:20]


def predict_features(X: pd.DataFrame) -> float:
    model, _ = load_model()
    minutes = float(model.predict(X)[0])
    return round(min(max(minutes, 5.0), 90.0), 1)   # guard-rail: never show absurd ETAs


def predict_minutes(order: dict) -> float:
    return predict_features(features_for(order))

"""heavy 'T' + model training.

Reads the loaded orders back FROM MongoDB (the 'warehouse'), builds features with the shared
feature module, trains a baseline (Linear Regression) and the main model (LightGBM), compares
them and saves the winner with its metadata.

Run alone:  docker compose run --rm pipeline python -m app.pipeline.train
"""
import json
import os
import warnings
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from app import config
from app.ml.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, build_features

MODEL_VERSION = "v1"
# harmless: LightGBM sees the encoded matrix (no column names) after the ColumnTransformer
warnings.filterwarnings("ignore", message="X does not have valid feature names")


def read_from_mongo() -> pd.DataFrame:
    from pymongo import MongoClient
    client = MongoClient(config.MONGO_URI)
    rows = list(client[config.MONGO_DB]["orders"].find({"source": "historical"}, {"_id": 0}))
    client.close()
    return documents_to_frame(rows)


def documents_to_frame(rows: list) -> pd.DataFrame:
    return pd.DataFrame([{
        "rest_lat": r["restaurant"]["lat"], "rest_lon": r["restaurant"]["lon"],
        "del_lat": r["delivery"]["lat"], "del_lon": r["delivery"]["lon"],
        "order_ts": r["order_ts"], "traffic": r["traffic"],
        "weather_condition": r["weather"]["condition"], "temp_c": r["weather"]["temp_c"],
        "precip_mm": r["weather"]["precip_mm"], "wind_kmh": r["weather"]["wind_kmh"],
        "vehicle_type": r["vehicle_type"], "vehicle_condition": r["vehicle_condition"],
        "order_type": r["order_type"], "multiple_deliveries": r["multiple_deliveries"],
        "festival": r["festival"], "city_type": r["city_type"],
        "partner_age": r["partner"]["age"], "partner_rating": r["partner"]["rating"],
        "actual_minutes": r["actual_minutes"],
    } for r in rows])


def _preprocessor(scale: bool) -> ColumnTransformer:
    num_steps = [("impute", SimpleImputer(strategy="median"))]
    if scale:
        num_steps.append(("scale", StandardScaler()))
    return ColumnTransformer([
        ("num", Pipeline(num_steps), NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])


def _metrics(y_true, y_pred) -> dict:
    err = np.abs(y_true - y_pred)
    return {"MAE_min": round(float(mean_absolute_error(y_true, y_pred)), 3),
            "RMSE_min": round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 3),
            "R2": round(float(r2_score(y_true, y_pred)), 4),
            "within_5_min_pct": round(float((err <= 5).mean() * 100), 2)}


def train(df: pd.DataFrame, model_dir: str = None) -> dict:
    model_dir = model_dir or config.MODEL_DIR
    X, y = build_features(df), df["actual_minutes"].astype(float).values
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    candidates = {
        "linear_regression": Pipeline([("prep", _preprocessor(scale=True)), ("model", LinearRegression())]),
        "lightgbm": Pipeline([("prep", _preprocessor(scale=False)),
                              ("model", LGBMRegressor(n_estimators=600, learning_rate=0.05, num_leaves=63,
                                                      min_child_samples=30, subsample=0.9, subsample_freq=1,
                                                      colsample_bytree=0.9, random_state=42, verbose=-1))]),
    }
    results = {}
    for name, pipe in candidates.items():
        pipe.fit(X_tr, y_tr)
        results[name] = _metrics(y_te, pipe.predict(X_te))
        print(f"  {name:18s} {results[name]}")

    best_name = min(results, key=lambda n: results[n]["MAE_min"])
    best = candidates[best_name]

    importances = {}
    if best_name == "lightgbm":
        names = best.named_steps["prep"].get_feature_names_out()
        imp = best.named_steps["model"].feature_importances_
        importances = dict(sorted(((n.split("__", 1)[1], int(v)) for n, v in zip(names, imp)),
                                  key=lambda kv: -kv[1])[:10])

    os.makedirs(model_dir, exist_ok=True)
    joblib.dump(best, os.path.join(model_dir, "eta_model.joblib"))
    meta = {"model_version": MODEL_VERSION, "model": best_name,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "train_rows": int(len(X_tr)), "test_rows": int(len(X_te)),
            "features": {"numeric": NUMERIC_FEATURES, "categorical": CATEGORICAL_FEATURES},
            "metrics": results, "top_feature_importance": importances}
    with open(os.path.join(model_dir, "metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)
    return meta


if __name__ == "__main__":
    print(json.dumps(train(read_from_mongo()), indent=2))

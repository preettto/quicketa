"""ML experiment: does REAL weather (from the Open-Meteo API) improve the ETA model?

Trains the same LightGBM pipeline 3 times on the same split:
  A) all features                     B) without real weather (temp/precip/wind)
  C) without the dataset's own weather label
and compares error. Also contrasts split-count vs gain feature importance.
Run:  docker compose run --rm -v ./scripts:/srv/scripts pipeline python scripts/ablation.py
"""
import json
import warnings

import numpy as np
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from app.ml.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, build_features
from app.pipeline.train import read_from_mongo

warnings.filterwarnings("ignore")
REAL_WEATHER = ["temp_c", "precip_mm", "wind_kmh"]


def fit(X_tr, X_te, y_tr, y_te, num, cat):
    pipe = Pipeline([("prep", ColumnTransformer([("num", SimpleImputer(strategy="median"), num),
                                                 ("cat", OneHotEncoder(handle_unknown="ignore"), cat)])),
                     ("model", LGBMRegressor(n_estimators=600, learning_rate=0.05, num_leaves=63, min_child_samples=30,
                                             subsample=0.9, subsample_freq=1, colsample_bytree=0.9,
                                             random_state=42, verbose=-1))])
    pipe.fit(X_tr[num + cat], y_tr)
    p = pipe.predict(X_te[num + cat])
    within5 = round(float((np.abs(y_te - p) <= 5).mean() * 100), 2)
    return pipe, round(mean_absolute_error(y_te, p), 3), round(r2_score(y_te, p), 4), within5


df = read_from_mongo()
X, y = build_features(df), df["actual_minutes"].astype(float).values
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

variants = {
    "A  all features": (NUMERIC_FEATURES, CATEGORICAL_FEATURES),
    "B  without real API weather": ([f for f in NUMERIC_FEATURES if f not in REAL_WEATHER], CATEGORICAL_FEATURES),
    "C  without dataset weather label": (NUMERIC_FEATURES, [c for c in CATEGORICAL_FEATURES if c != "weather_condition"]),
}
rows, models = [], {}
for name, (num, cat) in variants.items():
    pipe, mae, r2, w5 = fit(X_tr, X_te, y_tr, y_te, num, cat)
    models[name] = pipe
    rows.append((name, mae, r2, w5))

print(f"\n{'variant':36s} {'MAE min':>8s} {'R2':>8s} {'within 5 min %':>15s}")
for r in rows:
    print(f"{r[0]:36s} {r[1]:8.3f} {r[2]:8.4f} {r[3]:15.2f}")

booster = models["A  all features"].named_steps["model"].booster_
names = models["A  all features"].named_steps["prep"].get_feature_names_out()
split = dict(zip(names, booster.feature_importance("split")))
gain = dict(zip(names, booster.feature_importance("gain")))
tot = sum(gain.values())
top = sorted(gain, key=gain.get, reverse=True)[:8]
print(f"\n{'feature':34s} {'split count':>12s} {'gain share %':>13s}")
for n in top:
    print(f"{n.split('__', 1)[1]:34s} {split[n]:12d} {100 * gain[n] / tot:13.2f}")
for n in [n for n in names if n.split("__", 1)[1] in REAL_WEATHER]:
    print(f"{n.split('__', 1)[1]:34s} {split[n]:12d} {100 * gain[n] / tot:13.2f}   <- real weather")

with open("/srv/data/output/bench_ablation.json", "w") as f:
    json.dump({"rows": rows}, f, indent=2)
print("\n(saved /srv/data/output/bench_ablation.json)")

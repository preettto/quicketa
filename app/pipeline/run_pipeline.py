"""Orchestrates the full EtLT pipeline:  E (3 sources) -> t (light) -> L (sharded Mongo) -> T (features) -> train

Run:  docker compose run --rm pipeline
"""
import argparse
import json
import os
import time

from app import config
from app.pipeline import extract, load, train, transform


def step(name):
    print(f"\n=== {name} " + "=" * (60 - len(name)))
    return time.perf_counter()


def main(skip_train: bool = False):
    t0 = step("EXTRACT  1/3  CSV")
    raw = extract.extract_csv()
    print(f"  rows={len(raw)}  ({time.perf_counter()-t0:.1f}s)")

    t0 = step("EXTRACT  2/3  PostgreSQL partner service")
    partners, restaurants = extract.extract_postgres()
    print(f"  partners={len(partners)}  restaurants={len(restaurants)}  ({time.perf_counter()-t0:.1f}s)")

    t0 = step("light t  validate / clean / repair / join")
    clean, rejected, stats = transform.light_transform(raw, partners, restaurants)
    print(f"  clean={stats['clean_rows']}  rejected={stats['rejected_by_reason']}  "
          f"repaired={stats['repaired_negative_coords']}  imputed_time={stats['imputed_order_time']}")
    os.makedirs("/srv/data/output", exist_ok=True)
    rejected.to_csv("/srv/data/output/rejected_rows.csv", index=False)

    t0 = step("EXTRACT  3/3  Weather REST API (Open-Meteo)")
    cents = transform.city_centroids(clean)
    start = clean["order_ts"].min().strftime("%Y-%m-%d")
    end = clean["order_ts"].max().strftime("%Y-%m-%d")
    weather, wstatus = extract.extract_weather(cents, start, end)
    clean, wjoin = transform.join_weather(clean, weather)
    stats["weather_join"] = wjoin
    print(f"  cities={len(cents)}  status={wstatus}")
    print(f"  weather joined: {wjoin}  breaker={extract.weather_breaker.snapshot()}  ({time.perf_counter()-t0:.1f}s)")

    t0 = step("LOAD  -> MongoDB sharded cluster (via mongos)")
    audit = load.load_orders(clean, stats, wstatus)
    print(f"  loaded={audit['loaded_rows']}  per_shard={audit['per_shard_counts']}  ({time.perf_counter()-t0:.1f}s)")
    print(f"  RECONCILIATION: input {audit['input_rows']} = loaded {audit['loaded_rows']} "
          f"+ rejected {audit['rejected_rows']}  ->  {'PASS' if audit['reconciled'] else 'FAIL'}")

    if not skip_train:
        t0 = step("heavy T + TRAIN  (features from MongoDB -> models)")
        meta = train.train(train.read_from_mongo())
        print(f"  best={meta['model']}  saved -> {config.MODEL_DIR}/eta_model.joblib  ({time.perf_counter()-t0:.1f}s)")
        print("  top features:", json.dumps(meta["top_feature_importance"]))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-train", action="store_true")
    main(**vars(ap.parse_args()))

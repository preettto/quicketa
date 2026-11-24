"""LOAD stage - bulk insert clean orders into the sharded MongoDB cluster (through mongos)
and write a data-quality audit record that must reconcile exactly:

        input_rows == loaded_rows + rejected_rows
"""
import math
from datetime import datetime, timezone

import pandas as pd
from pymongo import ASCENDING, MongoClient

from app import config

BATCH = 5000


def _clean(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def to_document(r: dict) -> dict:
    r = {k: _clean(v) for k, v in r.items()}
    return {
        "order_id": r["order_id"], "source": "historical",
        "partner_id": r["partner_id"], "city_code": r["city_code"], "restaurant_id": r["restaurant_id"],
        "restaurant": {"lat": r["rest_lat"], "lon": r["rest_lon"]},
        "delivery": {"lat": r["del_lat"], "lon": r["del_lon"]},
        "order_ts": r["order_ts"],
        "traffic": r["traffic"], "vehicle_type": r["vehicle_type"],
        "vehicle_condition": r["vehicle_condition"], "order_type": r["order_type"],
        "multiple_deliveries": r["multiple_deliveries"], "festival": r["festival"],
        "city_type": r["city_type"],
        "partner": {"age": r["partner_age"], "rating": r["partner_rating"]},
        "weather": {"condition": r["weather_condition"], "temp_c": r.get("temp_c"),
                    "precip_mm": r.get("precip_mm"), "wind_kmh": r.get("wind_kmh"),
                    "source": r.get("weather_source", "missing")},
        "actual_minutes": r["actual_minutes"],
    }


def load_orders(clean: pd.DataFrame, stats: dict, weather_status: dict) -> dict:
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    orders = db["orders"]

    removed = orders.delete_many({"source": "historical"}).deleted_count   # idempotent re-runs
    docs = [to_document(r) for r in clean.to_dict("records")]
    for i in range(0, len(docs), BATCH):
        orders.insert_many(docs[i:i + BATCH], ordered=False)

    orders.create_index([("city_code", ASCENDING), ("source", ASCENDING)])
    orders.create_index([("order_ts", ASCENDING)])

    loaded = orders.count_documents({"source": "historical"})
    per_shard = {d["shard"]: d["count"] for d in orders.aggregate([{"$collStats": {"count": {}}}])}

    audit = {
        "run_at": datetime.now(timezone.utc),
        "pattern": "EtLT",
        "sources": {"csv": config.RAW_CSV, "postgres": "partners.delivery_partners/restaurants",
                    "api": config.WEATHER_ARCHIVE_URL},
        "input_rows": stats["input_rows"],
        "loaded_rows": loaded,
        "rejected_rows": stats["rejected_rows"],
        "rejected_by_reason": stats["rejected_by_reason"],
        "repaired_negative_coords": stats["repaired_negative_coords"],
        "imputed_order_time": stats["imputed_order_time"],
        "weather_join": stats.get("weather_join", {}),
        "weather_api_status": weather_status,
        "per_shard_counts": per_shard,
        "previous_rows_replaced": removed,
    }
    audit["reconciled"] = audit["input_rows"] == audit["loaded_rows"] + audit["rejected_rows"]
    db["pipeline_runs"].insert_one(dict(audit))
    client.close()
    return audit

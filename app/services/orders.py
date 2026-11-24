"""Business logic for placing an order and getting its ETA.
Shared by the REST endpoint and the GraphQL mutation, so both behave identically.
"""
import os
import time
import uuid
from datetime import datetime, timedelta

from app.ml.predictor import load_model, predict_minutes
from app.services import partners, weather
from app.services.storage import mongo_db

INSTANCE = os.getenv("INSTANCE_NAME", os.getenv("HOSTNAME", "local"))

ALLOWED = {
    "traffic": {"Low", "Medium", "High", "Jam"},
    "vehicle_type": {"motorcycle", "scooter", "electric_scooter", "bicycle"},
    "order_type": {"Snack", "Meal", "Drinks", "Buffet"},
    "festival": {"Yes", "No"},
    "city_type": {"Urban", "Metropolitian", "Semi-Urban"},
}
REQUIRED = ["partner_id", "restaurant", "delivery", "traffic", "vehicle_type", "order_type"]


class ValidationError(ValueError):
    pass


def validate(body: dict) -> dict:
    missing = [k for k in REQUIRED if k not in body]
    if missing:
        raise ValidationError(f"missing fields: {missing}")
    for side in ("restaurant", "delivery"):
        loc = body[side]
        if not (-90 <= float(loc.get("lat", 999)) <= 90 and -180 <= float(loc.get("lon", 999)) <= 180):
            raise ValidationError(f"{side}.lat/lon out of range")
        if float(loc["lat"]) == 0 and float(loc["lon"]) == 0:
            raise ValidationError(f"{side} location cannot be (0,0)")
    for field, allowed in ALLOWED.items():
        if field in body and body[field] not in allowed:
            raise ValidationError(f"{field} must be one of {sorted(allowed)}")
    if "RES" not in body["partner_id"]:
        raise ValidationError("partner_id must look like 'INDORES13DEL02'")
    return body


def build_order(body: dict) -> dict:
    """Turn the API request into the canonical (pipeline-compatible) order record."""
    ts = datetime.fromisoformat(body["order_ts"]) if body.get("order_ts") else datetime.now()
    p = partners.get_partner(body["partner_id"]) or {"age": None, "rating": None}
    w = weather.get_weather(body["restaurant"]["lat"], body["restaurant"]["lon"])
    return {
        "order_id": uuid.uuid4().hex[:12],
        "partner_id": body["partner_id"],
        "city_code": body["partner_id"].split("RES")[0],
        "rest_lat": float(body["restaurant"]["lat"]), "rest_lon": float(body["restaurant"]["lon"]),
        "del_lat": float(body["delivery"]["lat"]), "del_lon": float(body["delivery"]["lon"]),
        "order_ts": ts,
        "traffic": body["traffic"], "vehicle_type": body["vehicle_type"],
        "vehicle_condition": body.get("vehicle_condition", 1), "order_type": body["order_type"],
        "multiple_deliveries": body.get("multiple_deliveries", 0),
        "festival": body.get("festival", "No"), "city_type": body.get("city_type", "Urban"),
        "partner_age": p["age"], "partner_rating": p["rating"],
        "weather_condition": body.get("weather_condition") or w["condition"],
        "temp_c": w["temp_c"], "precip_mm": w["precip_mm"], "wind_kmh": w["wind_kmh"],
        "weather_source": w["source"],
    }


def to_document(o: dict, eta: float, model_version: str) -> dict:
    return {
        "order_id": o["order_id"], "source": "live",
        "partner_id": o["partner_id"], "city_code": o["city_code"],
        "restaurant": {"lat": o["rest_lat"], "lon": o["rest_lon"]},
        "delivery": {"lat": o["del_lat"], "lon": o["del_lon"]},
        "order_ts": o["order_ts"], "traffic": o["traffic"], "vehicle_type": o["vehicle_type"],
        "vehicle_condition": o["vehicle_condition"], "order_type": o["order_type"],
        "multiple_deliveries": o["multiple_deliveries"], "festival": o["festival"],
        "city_type": o["city_type"],
        "partner": {"age": o["partner_age"], "rating": o["partner_rating"]},
        "weather": {"condition": o["weather_condition"], "temp_c": o["temp_c"],
                    "precip_mm": o["precip_mm"], "wind_kmh": o["wind_kmh"], "source": o["weather_source"]},
        "predicted_minutes": eta, "model_version": model_version, "served_by": INSTANCE,
    }


def store(doc: dict):
    """Phase 5: synchronous write.  (Phase 6 replaces this with a Kafka publish.)"""
    mongo_db()["orders"].insert_one(dict(doc))


def place_order(body: dict) -> dict:
    t0 = time.perf_counter()
    order = build_order(validate(body))
    eta = predict_minutes(order)
    _, meta = load_model()
    doc = to_document(order, eta, meta["model_version"])
    store(doc)
    return {
        "order_id": order["order_id"],
        "city_code": order["city_code"],
        "eta_minutes": eta,
        "eta_time": (order["order_ts"] + timedelta(minutes=eta)).strftime("%Y-%m-%d %H:%M"),
        "weather": doc["weather"],
        "model_version": meta["model_version"],
        "served_by": INSTANCE,
        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
    }


def get_order(order_id: str):
    # order_id is the shard key -> mongos routes this to exactly ONE shard (targeted query)
    doc = mongo_db()["orders"].find_one({"order_id": order_id}, {"_id": 0})
    if doc and isinstance(doc.get("order_ts"), datetime):
        doc["order_ts"] = doc["order_ts"].isoformat()   # local (IST) time, no misleading 'GMT' label
    return doc

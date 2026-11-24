"""Shared fixtures. Tests need NO running infrastructure: databases, Kafka, Redis and the
weather API are replaced by in-memory fakes, and a tiny model is trained on synthetic data."""
import numpy as np
import pandas as pd
import pytest

from app import cache, config


def synthetic_orders(n: int = 400, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    traffic = rng.choice(["Low", "Medium", "High", "Jam"], n)
    rest_lat, rest_lon = 22.7 + rng.random(n) * 0.1, 75.8 + rng.random(n) * 0.1
    del_lat, del_lon = rest_lat + rng.random(n) * 0.1, rest_lon + rng.random(n) * 0.1
    dist = np.hypot(del_lat - rest_lat, del_lon - rest_lon) * 111
    minutes = 12 + 1.6 * dist + pd.Series(traffic).map({"Low": 0, "Medium": 4, "High": 6, "Jam": 10}).values
    return pd.DataFrame({
        "rest_lat": rest_lat, "rest_lon": rest_lon, "del_lat": del_lat, "del_lon": del_lon,
        "order_ts": pd.Timestamp("2022-03-01 08:00") + pd.to_timedelta(rng.integers(0, 60 * 24 * 20, n), unit="min"),
        "traffic": traffic, "weather_condition": rng.choice(["Sunny", "Fog", "Stormy"], n),
        "temp_c": rng.normal(28, 4, n), "precip_mm": rng.random(n), "wind_kmh": rng.random(n) * 20,
        "vehicle_type": rng.choice(["motorcycle", "scooter"], n), "vehicle_condition": rng.integers(0, 3, n),
        "order_type": rng.choice(["Snack", "Meal"], n), "multiple_deliveries": rng.integers(0, 2, n),
        "festival": "No", "city_type": "Urban", "partner_age": rng.integers(20, 40, n).astype(float),
        "partner_rating": rng.uniform(4, 5, n).round(1), "actual_minutes": minutes.round().astype(int),
    })


@pytest.fixture(scope="session")
def model_dir(tmp_path_factory):
    from app.pipeline import train
    d = tmp_path_factory.mktemp("model")
    train.train(synthetic_orders(), model_dir=str(d))
    return str(d)


@pytest.fixture
def fake_cache(monkeypatch):
    store = {}
    monkeypatch.setattr(cache, "get_json", lambda k: store.get(k))
    monkeypatch.setattr(cache, "set_json", lambda k, v, ttl_s: store.__setitem__(k, v))
    monkeypatch.setattr(cache, "get_flag", lambda k: store.get(k) == "1")
    monkeypatch.setattr(cache, "set_flag", lambda k, on: store.__setitem__(k, "1" if on else "0"))
    return store


class FakeCollection:
    def __init__(self):
        self.docs = {}

    def replace_one(self, flt, doc, upsert=False):
        self.docs[flt["order_id"]] = dict(doc)

    def insert_one(self, doc):
        self.docs[doc["order_id"]] = dict(doc)

    def find_one(self, flt, projection=None):
        d = self.docs.get(flt["order_id"])
        return dict(d) if d else None


class FakeDB:
    def __init__(self):
        self.orders = FakeCollection()

    def __getitem__(self, name):
        return self.orders

    def command(self, *_):
        return {"ok": 1}


@pytest.fixture
def api_client(monkeypatch, model_dir, fake_cache):
    from app.api import main
    from app.ml import predictor
    from app.services import events, orders, partners, weather

    monkeypatch.setattr(config, "MODEL_DIR", model_dir)
    predictor.load_model.cache_clear()
    db = FakeDB()
    published = []
    monkeypatch.setattr(orders, "mongo_db", lambda: db)
    monkeypatch.setattr(main, "mongo_db", lambda: db)
    monkeypatch.setattr(main, "pg_pool", lambda: type("P", (), {"getconn": lambda s: 1, "putconn": lambda s, c: None})())
    monkeypatch.setattr(partners, "get_partner", lambda pid: ({"age": 29.0, "rating": 4.7}, "miss"))
    monkeypatch.setattr(weather, "get_weather", lambda lat, lon: (dict(weather.FALLBACK), "fallback"))
    monkeypatch.setattr(events, "publish", lambda doc: published.append(doc) or {"partition": 0, "offset": len(published)})
    client = main.create_app().test_client()
    client.db, client.published = db, published
    yield client
    predictor.load_model.cache_clear()


VALID_ORDER = {
    "partner_id": "INDORES13DEL02",
    "restaurant": {"lat": 22.745049, "lon": 75.892471},
    "delivery": {"lat": 22.765049, "lon": 75.912471},
    "traffic": "Jam", "vehicle_type": "motorcycle", "order_type": "Meal",
}

import pytest

from tests.conftest import VALID_ORDER


def test_place_order_returns_eta_and_publishes_event(api_client):
    r = api_client.post("/api/orders", json=VALID_ORDER)
    assert r.status_code == 201
    body = r.get_json()
    assert 5 <= body["eta_minutes"] <= 90
    assert body["city_code"] == "INDO"
    assert body["storage"]["path"] == "kafka"
    assert api_client.published[0]["order_id"] == body["order_id"]
    assert api_client.published[0]["source"] == "live"


def test_identical_request_hits_eta_cache(api_client):
    first = api_client.post("/api/orders", json=VALID_ORDER).get_json()
    second = api_client.post("/api/orders", json=VALID_ORDER).get_json()
    assert first["cache"]["eta"] == "miss" and second["cache"]["eta"] == "hit"
    assert first["eta_minutes"] == second["eta_minutes"]


def test_jam_slower_than_low_traffic(api_client):
    jam = api_client.post("/api/orders", json=VALID_ORDER).get_json()["eta_minutes"]
    low = api_client.post("/api/orders", json={**VALID_ORDER, "traffic": "Low"}).get_json()["eta_minutes"]
    assert jam > low


@pytest.mark.parametrize("patch", [
    {"traffic": "Crazy"},
    {"restaurant": {"lat": 0, "lon": 0}},
    {"restaurant": {"lat": 200, "lon": 75}},
    {"partner_id": "nobody"},
])
def test_validation_errors_return_400(api_client, patch):
    assert api_client.post("/api/orders", json={**VALID_ORDER, **patch}).status_code == 400


def test_missing_field_returns_400(api_client):
    body = {k: v for k, v in VALID_ORDER.items() if k != "traffic"}
    assert api_client.post("/api/orders", json=body).status_code == 400


def test_kafka_down_falls_back_to_direct_write(api_client, monkeypatch):
    from app.services import events

    def down(doc):
        raise events.KafkaPublishError("broker unavailable")
    monkeypatch.setattr(events, "publish", down)
    body = api_client.post("/api/orders", json=VALID_ORDER).get_json()
    assert body["storage"]["path"] == "direct-mongodb-fallback"
    assert body["order_id"] in api_client.db.orders.docs


def test_get_unknown_order_is_404(api_client):
    assert api_client.get("/api/orders/does-not-exist").status_code == 404


def test_health_ok(api_client, monkeypatch):
    from app import cache
    monkeypatch.setattr(cache, "client", lambda: type("R", (), {"ping": lambda s: True})())
    r = api_client.get("/health")
    assert r.status_code == 200 and r.get_json()["status"] == "ok"


def test_graphql_mutation(api_client):
    q = ('mutation { placeOrder(input: {partnerId: "INDORES13DEL02", restaurant: {lat: 22.745, lon: 75.892}, '
         'delivery: {lat: 22.765, lon: 75.912}, traffic: "High", vehicleType: "scooter", orderType: "Snack"}) '
         '{ orderId etaMinutes } }')
    data = api_client.post("/graphql", json={"query": q}).get_json()["data"]["placeOrder"]
    assert data["orderId"] and 5 <= data["etaMinutes"] <= 90


def test_event_serialisation_round_trip():
    import json
    from datetime import datetime

    from app.consumers.store_consumer import to_document
    from app.services.events import serialize
    raw = serialize({"order_id": "x", "city_code": "INDO", "order_ts": datetime(2026, 1, 1, 12, 30)})
    assert json.loads(raw)["order_ts"] == "2026-01-01T12:30:00"
    doc = to_document(raw, partition=2, offset=7)
    assert doc["order_ts"] == datetime(2026, 1, 1, 12, 30) and doc["kafka"]["partition"] == 2

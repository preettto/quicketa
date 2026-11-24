"""Kafka producer (P3).

Each order becomes an 'order_placed' event on topic `order_placed`, keyed by city_code:
  * same key -> same partition -> per-city ordering is preserved
  * acks=all + idempotence -> the broker never stores a duplicate from producer retries
"""
import json
import os
from functools import lru_cache

from confluent_kafka import Producer

from app import config


class KafkaPublishError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": os.getenv("INSTANCE_NAME", "quicketa"),
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "message.timeout.ms": 3000,
    })


def serialize(doc: dict) -> bytes:
    return json.dumps(doc, default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o)).encode()


def publish(doc: dict, timeout_s: float = 3.0) -> dict:
    """Publish and wait for the broker's acknowledgement; returns {'partition', 'offset'}."""
    result = {}

    def on_delivery(err, msg):
        if err:
            result["error"] = str(err)
        else:
            result.update(partition=msg.partition(), offset=msg.offset())

    p = producer()
    p.produce(config.KAFKA_TOPIC, key=doc["city_code"].encode(), value=serialize(doc), on_delivery=on_delivery)
    if p.flush(timeout_s) > 0 or "error" in result:
        raise KafkaPublishError(result.get("error", "broker did not acknowledge in time"))
    return result

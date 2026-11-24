"""Consumer group 'order-store': Kafka -> sharded MongoDB.

Delivery semantics: AT-LEAST-ONCE (offset is committed only AFTER the write succeeds).
Duplicates are made harmless by an IDEMPOTENT write: replace_one(upsert) keyed on order_id.
At-least-once + idempotent sink  ==  effectively-once.
(In Practical 3 a re-sent order id was processed twice; this design fixes that.)
"""
import json
import os
import signal
from datetime import datetime

from confluent_kafka import Consumer
from pymongo import MongoClient

from app import config

HOST = os.getenv("HOSTNAME", "store-consumer")
running = True


def _stop(*_):
    global running
    running = False


def to_document(raw: bytes, partition: int, offset: int) -> dict:
    doc = json.loads(raw)
    doc["order_ts"] = datetime.fromisoformat(doc["order_ts"])
    doc["kafka"] = {"partition": partition, "offset": offset, "stored_by": HOST}
    return doc


def main():
    signal.signal(signal.SIGTERM, _stop)
    orders = MongoClient(config.MONGO_URI)[config.MONGO_DB]["orders"]
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "order-store",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([config.KAFKA_TOPIC],
                on_assign=lambda _c, parts: print(f"[{HOST}] assigned partitions {[p.partition for p in parts]}", flush=True))
    print(f"[{HOST}] order-store consumer started", flush=True)
    try:
        while running:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{HOST}] kafka error: {msg.error()}", flush=True)
                continue
            doc = to_document(msg.value(), msg.partition(), msg.offset())
            orders.replace_one({"order_id": doc["order_id"]}, doc, upsert=True)   # idempotent
            c.commit(message=msg, asynchronous=False)                             # then commit
            print(f"[{HOST}] p{msg.partition()} off={msg.offset()} order={doc['order_id']} "
                  f"city={doc['city_code']} -> mongodb", flush=True)
    finally:
        c.close()


if __name__ == "__main__":
    main()

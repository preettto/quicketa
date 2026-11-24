"""Consumer group 'live-stats': Kafka -> Redis per-minute counters (a mini stream-processing job).

Because it is a DIFFERENT consumer group from 'order-store', it receives its own full copy of
every event (Kafka pub/sub): adding analytics never slows down or affects order storage.
"""
import json
import os
import signal

from confluent_kafka import Consumer

from app import config
from app.services import live_stats

HOST = os.getenv("HOSTNAME", "stats-consumer")
running = True


def _stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGTERM, _stop)
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "live-stats",
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([config.KAFKA_TOPIC],
                on_assign=lambda _c, parts: print(f"[{HOST}] assigned partitions {[p.partition for p in parts]}", flush=True))
    print(f"[{HOST}] live-stats consumer started", flush=True)
    try:
        while running:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            city = json.loads(msg.value())["city_code"]
            live_stats.record(city)
            print(f"[{HOST}] p{msg.partition()} off={msg.offset()} +1 {city}", flush=True)
    finally:
        c.close()


if __name__ == "__main__":
    main()

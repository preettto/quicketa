"""Rolling 'orders per city in the last N minutes' counters (written by stats_consumer, read by the API).

Each minute gets its own Redis key  stats:{city}:{minute_bucket}  that expires after 25 minutes;
a window = sum of the last N buckets.  This is a tumbling-window aggregation, the same idea as
DoorDash's Flink job 'number of orders a store received in the last X minutes'.
"""
import time

from app import cache

WINDOW_MIN = 20
TTL_S = 25 * 60


def minute_bucket(ts: float = None) -> int:
    return int((ts or time.time()) // 60)


def record(city_code: str, ts: float = None):
    r = cache.client()
    b = minute_bucket(ts)
    pipe = r.pipeline()
    pipe.incr(f"stats:{city_code}:{b}")
    pipe.expire(f"stats:{city_code}:{b}", TTL_S)
    pipe.sadd("stats:cities", city_code)
    pipe.incr("stats:total_events")
    pipe.execute()


def window(minutes: int = WINDOW_MIN) -> dict:
    r = cache.client()
    now = minute_bucket()
    out = {}
    for city in sorted(c.decode() for c in r.smembers("stats:cities")):
        vals = r.mget([f"stats:{city}:{b}" for b in range(now - minutes + 1, now + 1)])
        total = sum(int(v) for v in vals if v)
        if total:
            out[city] = total
    return {"window_minutes": minutes, "orders_per_city": out,
            "total_in_window": sum(out.values()),
            "total_events_ever": int(r.get("stats:total_events") or 0)}

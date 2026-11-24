"""Redis cache helpers (P7).

The cache is an OPTIONAL dependency: every call has a short timeout and swallows Redis errors,
so if Redis is down the API simply behaves as if every lookup were a cache MISS (slower, but up).
"""
import json
from functools import lru_cache

import redis

from app import config


@lru_cache(maxsize=1)
def client() -> redis.Redis:
    return redis.from_url(config.REDIS_URL, socket_timeout=0.5, socket_connect_timeout=0.5)


def get_json(key: str):
    try:
        raw = client().get(key)
        return json.loads(raw) if raw else None
    except redis.RedisError:
        return None


def set_json(key: str, value, ttl_s: int):
    try:
        client().setex(key, ttl_s, json.dumps(value, default=str))
    except redis.RedisError:
        pass


def get_flag(key: str) -> bool:
    try:
        return client().get(key) == b"1"
    except redis.RedisError:
        return False


def set_flag(key: str, on: bool):
    if on:
        client().set(key, "1")
    else:
        client().delete(key)


def delete_pattern(pattern: str) -> int:
    n = 0
    for k in client().scan_iter(match=pattern, count=500):
        n += client().delete(k)
    return n

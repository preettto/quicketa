"""Shared connections (one per process; both clients are thread-safe / pooled)."""
from functools import lru_cache

from pymongo import MongoClient
from psycopg2.pool import ThreadedConnectionPool

from app import config


@lru_cache(maxsize=1)
def mongo_db():
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)
    return client[config.MONGO_DB]


@lru_cache(maxsize=1)
def pg_pool() -> ThreadedConnectionPool:
    return ThreadedConnectionPool(1, 5, config.PG_DSN)

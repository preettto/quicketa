"""Central configuration: every setting comes from an environment variable with a sensible default."""
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongos:27017")
MONGO_DB = os.getenv("MONGO_DB", "quicketa")

PG_DSN = os.getenv("PG_DSN", "host=postgres port=5432 dbname=partners user=quicketa password=quicketa")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order_placed")

RAW_CSV = os.getenv("RAW_CSV", "/srv/data/raw/train.csv")
CACHE_DIR = os.getenv("CACHE_DIR", "/srv/data/cache")
MODEL_DIR = os.getenv("MODEL_DIR", "/srv/models")

WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_TIMEOUT_S = float(os.getenv("WEATHER_TIMEOUT_S", "5"))

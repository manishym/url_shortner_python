"""Shared configuration loaded from environment variables."""
import os

ID_SERVICE_URL = os.getenv("ID_SERVICE_URL", "http://id-service:8000")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://shortener:shortener@postgres:5432/shortener",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
PURGE_TOPIC = os.getenv("PURGE_TOPIC", "url-purge-events")
LRU_MAX_SIZE = int(os.getenv("LRU_MAX_SIZE", "10000"))
REDIS_DEFAULT_TTL = 300


def _redis_ttl(expires_in_seconds: int | None) -> int:
    """Compute Redis TTL capped at REDIS_DEFAULT_TTL."""
    if expires_in_seconds is None:
        return REDIS_DEFAULT_TTL
    return min(REDIS_DEFAULT_TTL, max(1, expires_in_seconds))

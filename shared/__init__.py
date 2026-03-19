"""Shared utilities for URL shortener services."""
from shared.base62 import decode_base62, encode_base62
from shared.config import (
    DATABASE_URL,
    ID_SERVICE_URL,
    KAFKA_BOOTSTRAP,
    LRU_MAX_SIZE,
    PURGE_TOPIC,
    REDIS_DEFAULT_TTL,
    REDIS_URL,
    _redis_ttl,
)
from shared.db import get_pg, init_db
from shared.kafka_consumer import run_consumer
from shared.kafka_utils import (
    _short_path_from_message,
    ensure_purge_topic,
    kafka_producer,
    send_purge_event,
)
from shared.lru_cache import LRUCache

__all__ = [
    "DATABASE_URL",
    "ID_SERVICE_URL",
    "KAFKA_BOOTSTRAP",
    "LRU_MAX_SIZE",
    "PURGE_TOPIC",
    "REDIS_DEFAULT_TTL",
    "REDIS_URL",
    "_redis_ttl",
    "decode_base62",
    "encode_base62",
    "get_pg",
    "init_db",
    "kafka_producer",
    "run_consumer",
    "ensure_purge_topic",
    "send_purge_event",
    "_short_path_from_message",
    "LRUCache",
]

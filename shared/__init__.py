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
from shared.lru_cache import LRUCache

# Lazy imports for kafka components (avoid loading confluent_kafka in services that don't need it)
def __getattr__(name):
    if name == "kafka_producer":
        from shared.kafka_utils import kafka_producer
        globals()["kafka_producer"] = kafka_producer
        return kafka_producer
    if name == "run_consumer":
        from shared.kafka_consumer import run_consumer
        globals()["run_consumer"] = run_consumer
        return run_consumer
    if name == "ensure_purge_topic":
        from shared.kafka_utils import ensure_purge_topic
        globals()["ensure_purge_topic"] = ensure_purge_topic
        return ensure_purge_topic
    if name == "send_purge_event":
        from shared.kafka_utils import send_purge_event
        globals()["send_purge_event"] = send_purge_event
        return send_purge_event
    if name == "_short_path_from_message":
        from shared.kafka_utils import _short_path_from_message
        globals()["_short_path_from_message"] = _short_path_from_message
        return _short_path_from_message
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

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

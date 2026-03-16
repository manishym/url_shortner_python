"""
URL Redirection Service - Handles GET /r/{short_path}
"""
import logging
import threading
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from redis import Redis

from shared import config, db, kafka_utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[REDIRECTION-SERVICE]"


# --- Thread-safe LRU Cache ---
class LRUCache:
    def __init__(self, max_size: int):
        self._max_size = max_size
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> str | None:
        with self._lock:
            if key not in self._cache:
                return None
            self._cache.move_to_end(key)
            return self._cache[key]

    def set(self, key: str, value: str) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def delete(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)


# --- Globals ---
redis_client: Redis | None = None
lru_cache: LRUCache | None = None
consumer_thread: threading.Thread | None = None
consumer_stop = threading.Event()


def _run_purge_consumer_lru():
    """Consumer group: on purge message -> delete from LRU cache."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "url-redirection-purge-lru",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([config.PURGE_TOPIC])
    logger.info("%s LRU purge consumer started, topic=%s", LOG_PREFIX, config.PURGE_TOPIC)
    while not consumer_stop.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("%s LRU consumer error: %s", LOG_PREFIX, msg.error())
            continue
        key = kafka_utils._short_path_from_message(msg)
        if not key:
            continue
        if lru_cache:
            lru_cache.delete(key)
            logger.info("%s LRU cache purged: %s", LOG_PREFIX, key)
            try:
                consumer.commit(message=msg)
            except Exception as ce:
                logger.warning("%s LRU commit failed for %s: %s", LOG_PREFIX, key, ce)
    consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, lru_cache, consumer_thread
    db.init_db()
    redis_client = Redis.from_url(config.REDIS_URL)
    lru_cache = LRUCache(config.LRU_MAX_SIZE)
    consumer_thread = threading.Thread(target=_run_purge_consumer_lru, daemon=True)
    consumer_thread.start()
    yield
    consumer_stop.set()
    if consumer_thread:
        consumer_thread.join(timeout=5)
    if redis_client:
        redis_client.close()


app = FastAPI(title="URL Redirection Service", version="1.0.0", lifespan=lifespan)


@app.get("/r/{short_path}")
def redirect(short_path: str):
    """Multi-tier cache-aside: Local LRU -> Redis -> Postgres."""
    # 1. Local LRU
    if lru_cache:
        hit = lru_cache.get(short_path)
        if hit is not None:
            logger.info("%s Redirect cache hit (LRU): %s", LOG_PREFIX, short_path)
            return RedirectResponse(url=hit, status_code=302)

    # 2. Redis
    if redis_client:
        try:
            hit = redis_client.get(short_path)
            if hit is not None:
                long_url = hit.decode() if isinstance(hit, bytes) else hit
                logger.info("%s Redirect cache hit (Redis): %s", LOG_PREFIX, short_path)
                if lru_cache:
                    lru_cache.set(short_path, long_url)
                return RedirectResponse(url=long_url, status_code=302)
        except Exception as e:
            logger.warning("%s Redis get failed: %s", LOG_PREFIX, e)

    # 3. Postgres
    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT long_url, expires_at FROM short_urls WHERE short_path = %s", (short_path,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Short URL not found")
    long_url, expires_at = row[0], row[1]
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        raise HTTPException(status_code=404, detail="Short URL has expired")
    logger.info("%s Redirect DB hit: %s", LOG_PREFIX, short_path)
    if expires_at:
        ttl = min(config.REDIS_DEFAULT_TTL, max(1, int((expires_at - datetime.now(timezone.utc)).total_seconds())))
    else:
        ttl = config.REDIS_DEFAULT_TTL
    if lru_cache and expires_at is None:
        lru_cache.set(short_path, long_url)
    if redis_client:
        try:
            redis_client.setex(short_path, ttl, long_url)
        except Exception as e:
            logger.warning("%s Redis setex failed: %s", LOG_PREFIX, e)
    return RedirectResponse(url=long_url, status_code=302)


@app.get("/health")
def health():
    return {"status": "ok", "service": "redirection"}

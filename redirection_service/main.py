"""
URL Redirection Service - Handles GET /r/{short_path}
"""
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.exceptions
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from redis import Redis

from shared import LRUCache, config, db, kafka_consumer
from shared.kafka_consumer import ConsumerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[REDIRECTION-SERVICE]"


# --- Globals ---
redis_client: Redis | None = None
lru_cache: LRUCache | None = None
consumer_thread: threading.Thread | None = None
consumer_stop = threading.Event()


def _purge_lru_action(key: str) -> bool:
    """Remove *key* from LRU cache.  Returns True on success (commit)."""
    if not lru_cache:
        return False
    lru_cache.delete(key)
    logger.info("%s LRU cache purged: %s", LOG_PREFIX, key)
    return True


def _run_purge_consumer_lru():
    """Consumer group: on purge message -> delete from LRU cache."""
    kafka_consumer.run_consumer(ConsumerConfig(
        group_id="url-redirection-purge-lru",
        consumer_stop=consumer_stop,
        action_callback=_purge_lru_action,
        log_prefix=LOG_PREFIX,
        consumer_label="LRU",
        logger=logger,
    ))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB, Redis, LRU cache, and start purge consumer."""
    global redis_client, lru_cache, consumer_thread
    db.init_db()
    redis_client = Redis.from_url(config.REDIS_URL)
    lru_cache = LRUCache(config.LRU_MAX_SIZE)
    consumer_thread = threading.Thread(
        target=_run_purge_consumer_lru, daemon=True,
    )
    consumer_thread.start()
    yield
    consumer_stop.set()
    if consumer_thread:
        consumer_thread.join(timeout=5)
    if redis_client:
        redis_client.close()


app = FastAPI(
    title="URL Redirection Service", version="1.0.0",
    lifespan=lifespan,
)


@app.get("/r/{short_path}")
def redirect(short_path: str):
    """Multi-tier cache-aside: Local LRU -> Redis -> Postgres."""
    # 1. Local LRU
    if lru_cache:
        hit = lru_cache.get(short_path)
        if hit is not None:
            logger.info(
                "%s Redirect cache hit (LRU): %s",
                LOG_PREFIX, short_path,
            )
            return RedirectResponse(url=hit, status_code=302)

    # 2. Redis
    if redis_client:
        try:
            hit = redis_client.get(short_path)
            if hit is not None:
                long_url = (
                    hit.decode() if isinstance(hit, bytes) else hit
                )
                logger.info(
                    "%s Redirect cache hit (Redis): %s",
                    LOG_PREFIX, short_path,
                )
                if lru_cache:
                    lru_cache.set(short_path, long_url)
                return RedirectResponse(
                    url=long_url, status_code=302,
                )
        except redis.exceptions.RedisError as e:
            logger.warning(
                "%s Redis get failed: %s", LOG_PREFIX, e,
            )

    # 3. Postgres
    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT long_url, expires_at "
                "FROM short_urls WHERE short_path = %s",
                (short_path,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=404, detail="Short URL not found",
        )
    long_url, expires_at = row[0], row[1]
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        raise HTTPException(
            status_code=404, detail="Short URL has expired",
        )
    logger.info(
        "%s Redirect DB hit: %s", LOG_PREFIX, short_path,
    )
    if expires_at:
        remaining = (
            expires_at - datetime.now(timezone.utc)
        ).total_seconds()
        ttl = min(
            config.REDIS_DEFAULT_TTL, max(1, int(remaining)),
        )
    else:
        ttl = config.REDIS_DEFAULT_TTL
    if lru_cache and expires_at is None:
        lru_cache.set(short_path, long_url)
    if redis_client:
        try:
            redis_client.setex(short_path, ttl, long_url)
        except redis.exceptions.RedisError as e:
            logger.warning(
                "%s Redis setex failed: %s", LOG_PREFIX, e,
            )
    return RedirectResponse(url=long_url, status_code=302)


@app.get("/health")
def health():
    """Return health status of the redirection service."""
    return {"status": "ok", "service": "redirection"}

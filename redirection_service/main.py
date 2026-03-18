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


def _purge_lru_action(
    lru_cache: LRUCache | None, key: str
) -> bool:
    """Remove *key* from LRU cache.  Returns True on success (commit)."""
    if not lru_cache:
        return False
    lru_cache.delete(key)
    logger.info("%s LRU cache purged: %s", LOG_PREFIX, key)
    return True


def _run_purge_consumer_lru(
    consumer_stop: threading.Event, lru_cache: LRUCache | None
):
    """Consumer group: on purge message -> delete from LRU cache."""
    def lru_callback(key: str) -> bool:
        return _purge_lru_action(lru_cache, key)

    kafka_consumer.run_consumer(ConsumerConfig(
        group_id="url-redirection-purge-lru",
        consumer_stop=consumer_stop,
        action_callback=lru_callback,
        log_prefix=LOG_PREFIX,
        consumer_label="LRU",
        logger=logger,
    ))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB, Redis, LRU cache, and start purge consumer."""
    db.init_db()
    app.state.redis = Redis.from_url(config.REDIS_URL)
    app.state.lru_cache = LRUCache(config.LRU_MAX_SIZE)
    app.state.consumer_stop = threading.Event()
    app.state.consumer_thread = threading.Thread(
        target=_run_purge_consumer_lru,
        args=(app.state.consumer_stop, app.state.lru_cache),
        daemon=True,
    )
    app.state.consumer_thread.start()
    yield
    app.state.consumer_stop.set()
    if app.state.consumer_thread:
        app.state.consumer_thread.join(timeout=5)
    if app.state.redis:
        app.state.redis.close()


app = FastAPI(
    title="URL Redirection Service", version="1.0.0",
    lifespan=lifespan,
)


def _calculate_ttl(expires_at) -> int:
    """Calculate Redis TTL based on expires_at timestamp."""
    if expires_at:
        remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
        return min(config.REDIS_DEFAULT_TTL, max(1, int(remaining)))
    return config.REDIS_DEFAULT_TTL


def _try_redis_hit(
    redis_client: Redis | None, short_path: str
) -> str | None:
    """Check Redis cache and return URL if found."""
    if redis_client is None:
        return None
    try:
        hit = redis_client.get(short_path)
        if hit is not None:
            return hit.decode() if isinstance(hit, bytes) else hit
    except redis.exceptions.RedisError as e:
        logger.warning("%s Redis get failed: %s", LOG_PREFIX, e)
    return None


def _try_db_lookup(short_path: str) -> tuple[str, object] | None:
    """Look up URL in database. Returns (long_url, expires_at) or None."""
    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT long_url, expires_at "
                "FROM short_urls WHERE short_path = %s",
                (short_path,),
            )
            return cur.fetchone()


@app.get("/r/{short_path}")
def redirect(short_path: str):
    """Multi-tier cache-aside: Local LRU -> Redis -> Postgres."""
    # 1. Local LRU
    lru_cache = app.state.lru_cache
    if lru_cache:
        hit = lru_cache.get(short_path)
        if hit is not None:
            logger.info(
                "%s Redirect cache hit (LRU): %s",
                LOG_PREFIX, short_path,
            )
            return RedirectResponse(url=hit, status_code=302)

    # 2. Redis
    redis_client = app.state.redis
    redis_hit = _try_redis_hit(redis_client, short_path)
    if redis_hit is not None:
        logger.info(
            "%s Redirect cache hit (Redis): %s",
            LOG_PREFIX, short_path,
        )
        if lru_cache:
            lru_cache.set(short_path, redis_hit)
        return RedirectResponse(url=redis_hit, status_code=302)

    # 3. Postgres
    row = _try_db_lookup(short_path)
    if not row:
        raise HTTPException(
            status_code=404, detail="Short URL not found",
        )
    long_url, expires_at = row[0], row[1]
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        raise HTTPException(
            status_code=404, detail="Short URL has expired",
        )
    logger.info("%s Redirect DB hit: %s", LOG_PREFIX, short_path)

    ttl = _calculate_ttl(expires_at)
    if lru_cache and expires_at is None:
        lru_cache.set(short_path, long_url)
    if redis_client:
        try:
            redis_client.setex(short_path, ttl, long_url)
        except redis.exceptions.RedisError as e:
            logger.warning("%s Redis setex failed: %s", LOG_PREFIX, e)
    return RedirectResponse(url=long_url, status_code=302)


@app.get("/health")
def health():
    """Return health status of the redirection service."""
    return {"status": "ok", "service": "redirection"}

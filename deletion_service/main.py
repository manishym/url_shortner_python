"""
URL Deletion Service - Handles DELETE /r/{short_path}
"""
import hmac
import logging
import threading
from contextlib import asynccontextmanager

import psycopg2
import redis.exceptions
from fastapi import FastAPI, HTTPException, Query
from redis import Redis

from shared import config, db, kafka_consumer, kafka_utils
from shared.kafka_consumer import ConsumerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[DELETION-SERVICE]"


def _purge_db_action(key: str) -> bool:
    """Delete *key* from Postgres.  Returns True on success (commit)."""
    try:
        with db.get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM short_urls WHERE short_path = %s",
                    (key,),
                )
                conn.commit()
        logger.info("%s DB deleted: %s", LOG_PREFIX, key)
        return True
    except psycopg2.Error as e:
        logger.warning(
            "%s DB delete failed for %s: %s", LOG_PREFIX, key, e,
        )
        return False


def _purge_redis_action(redis_client: Redis | None, key: str) -> bool:
    """Delete *key* from Redis.  Returns True on success (commit)."""
    if not redis_client:
        return False
    try:
        redis_client.delete(key)
        logger.info("%s Redis deleted: %s", LOG_PREFIX, key)
        return True
    except redis.exceptions.RedisError as e:
        logger.warning(
            "%s Redis delete failed for %s: %s", LOG_PREFIX, key, e,
        )
        return False


def _run_purge_consumer_db(consumer_stop: threading.Event):
    """Consumer group: on purge message -> delete from Postgres."""
    kafka_consumer.run_consumer(ConsumerConfig(
        group_id="url-deletion-purge-db",
        consumer_stop=consumer_stop,
        action_callback=_purge_db_action,
        log_prefix=LOG_PREFIX,
        consumer_label="DB",
        logger=logger,
    ))


def _run_purge_consumer_redis(
    consumer_stop: threading.Event, redis_client: Redis | None
):
    """Consumer group: on purge message -> delete from Redis."""
    def redis_callback(key: str) -> bool:
        return _purge_redis_action(redis_client, key)

    kafka_consumer.run_consumer(ConsumerConfig(
        group_id="url-deletion-purge-redis",
        consumer_stop=consumer_stop,
        action_callback=redis_callback,
        log_prefix=LOG_PREFIX,
        consumer_label="Redis",
        logger=logger,
    ))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB, Redis, Kafka topic, and start consumers."""
    db.init_db()
    app.state.redis = Redis.from_url(config.REDIS_URL)
    app.state.consumer_stop = threading.Event()
    kafka_utils.ensure_purge_topic()
    consumer_threads = [
        threading.Thread(
            target=_run_purge_consumer_db,
            args=(app.state.consumer_stop,),
            daemon=True,
        ),
        threading.Thread(
            target=_run_purge_consumer_redis,
            args=(app.state.consumer_stop, app.state.redis),
            daemon=True,
        ),
    ]
    for thread in consumer_threads:
        thread.start()
    app.state.consumer_threads = consumer_threads
    yield
    app.state.consumer_stop.set()
    for thread in consumer_threads:
        thread.join(timeout=5)
    if app.state.redis:
        app.state.redis.close()


app = FastAPI(
    title="URL Deletion Service", version="1.0.0", lifespan=lifespan,
)


@app.delete("/r/{short_path}")
def delete_short_url(
    short_path: str,
    delete_key: str | None = Query(
        None, description="Delete key required for deletion",
    ),
):
    """Produce purge event to Kafka; DB and Redis consumers do deletes."""
    if delete_key is None:
        raise HTTPException(
            status_code=403, detail="Delete key is required",
        )
    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT delete_key FROM short_urls "
                "WHERE short_path = %s",
                (short_path,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(
                    status_code=404, detail="Short URL not found",
                )
            stored_delete_key = row[0]
            if not hmac.compare_digest(stored_delete_key, delete_key):
                raise HTTPException(
                    status_code=403, detail="Invalid delete key",
                )
    kafka_utils.send_purge_event(short_path)
    logger.info(
        "%s Purge event produced for: %s", LOG_PREFIX, short_path,
    )
    return {
        "ok": True,
        "short_path": short_path,
        "message": (
            "Purge event sent; DB and Redis will be "
            "updated by consumers"
        ),
    }


@app.get("/health")
def health():
    """Return health status of the deletion service."""
    return {"status": "ok", "service": "deletion"}

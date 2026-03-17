"""
URL Deletion Service - Handles DELETE /r/{short_path}
"""
import logging
import threading
from contextlib import asynccontextmanager

from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from redis import Redis

from shared import config, db, kafka_utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[DELETION-SERVICE]"


# --- Globals ---
redis_client: Redis | None = None
consumer_threads: list[threading.Thread] = []
consumer_stop = threading.Event()


def _run_purge_consumer_db():
    """Consumer group: on purge message -> delete from Postgres."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "url-deletion-purge-db",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([config.PURGE_TOPIC])
    logger.info("%s DB purge consumer started, topic=%s", LOG_PREFIX, config.PURGE_TOPIC)
    while not consumer_stop.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("%s DB consumer error: %s", LOG_PREFIX, msg.error())
            continue
        key = kafka_utils._short_path_from_message(msg)
        if not key:
            continue
        try:
            with db.get_pg() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM short_urls WHERE short_path = %s", (key,))
                    conn.commit()
            logger.info("%s DB deleted: %s", LOG_PREFIX, key)
            try:
                consumer.commit(message=msg)
            except Exception as ce:
                logger.warning("%s DB commit failed for %s: %s", LOG_PREFIX, key, ce)
        except Exception as e:
            logger.warning("%s DB delete failed for %s: %s", LOG_PREFIX, key, e)
    consumer.close()


def _run_purge_consumer_redis():
    """Consumer group: on purge message -> delete from Redis."""
    global redis_client
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "url-deletion-purge-redis",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([config.PURGE_TOPIC])
    logger.info("%s Redis purge consumer started, topic=%s", LOG_PREFIX, config.PURGE_TOPIC)
    while not consumer_stop.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("%s Redis consumer error: %s", LOG_PREFIX, msg.error())
            continue
        key = kafka_utils._short_path_from_message(msg)
        if not key:
            continue
        if redis_client:
            try:
                redis_client.delete(key)
                logger.info("%s Redis deleted: %s", LOG_PREFIX, key)
                try:
                    consumer.commit(message=msg)
                except Exception as ce:
                    logger.warning("%s Redis commit failed for %s: %s", LOG_PREFIX, key, ce)
            except Exception as e:
                logger.warning("%s Redis delete failed for %s: %s", LOG_PREFIX, key, e)
    consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, consumer_threads
    db.init_db()
    redis_client = Redis.from_url(config.REDIS_URL)
    kafka_utils.ensure_purge_topic()
    consumer_threads = [
        threading.Thread(target=_run_purge_consumer_db, daemon=True),
        threading.Thread(target=_run_purge_consumer_redis, daemon=True),
    ]
    for t in consumer_threads:
        t.start()
    yield
    consumer_stop.set()
    for t in consumer_threads:
        t.join(timeout=5)
    if redis_client:
        redis_client.close()


app = FastAPI(title="URL Deletion Service", version="1.0.0", lifespan=lifespan)


@app.delete("/r/{short_path}")
def delete_short_url(short_path: str, delete_key: str | None = Query(None, description="Delete key required for deletion")):
    """Produce purge event to Kafka only; DB and Redis consumers perform the deletes."""
    if delete_key is None:
        raise HTTPException(status_code=403, detail="Delete key is required")
    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT delete_key FROM short_urls WHERE short_path = %s", (short_path,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Short URL not found")
            stored_delete_key = row[0]
            if stored_delete_key != delete_key:
                raise HTTPException(status_code=403, detail="Invalid delete key")
    kafka_utils.send_purge_event(short_path)
    logger.info("%s Purge event produced for: %s", LOG_PREFIX, short_path)
    return {"ok": True, "short_path": short_path, "message": "Purge event sent; DB and Redis will be updated by consumers"}


@app.get("/health")
def health():
    return {"status": "ok", "service": "deletion"}

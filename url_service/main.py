"""
URL Shortener Microservice.
Shorten: ID service -> Base62 -> Postgres + Redis.
Redirect: Local LRU -> Redis -> Postgres (cache-aside).
Delete: Postgres + Redpanda purge event; consumer invalidates Local LRU + Redis.
"""
import logging
import os
import threading
from collections import OrderedDict
from contextlib import asynccontextmanager

import httpx
import psycopg2
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl
from redis import Redis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[URL-SERVICE]"

# --- Config from env ---
ID_SERVICE_URL = os.environ.get("ID_SERVICE_URL", "http://id-service:8000")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://shortener:shortener@postgres:5432/shortener",
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
PURGE_TOPIC = os.environ.get("PURGE_TOPIC", "url-purge-events")
LRU_MAX_SIZE = int(os.environ.get("LRU_MAX_SIZE", "10_000"))
BASE62_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


# --- Base62 ---
def encode_base62(num: int, length: int = 7) -> str:
    if num == 0:
        return "0" * length
    result = []
    while num > 0:
        result.append(BASE62_CHARS[num % 62])
        num //= 62
    encoded = "".join(reversed(result))
    if len(encoded) < length:
        encoded = encoded.rjust(length, "0")
    elif len(encoded) > length:
        encoded = encoded[-length:]
    return encoded


def decode_base62(s: str) -> int:
    n = 0
    for c in s:
        n = n * 62 + BASE62_CHARS.index(c)
    return n


# --- Thread-safe LRU (in-memory) ---
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


# --- DB helpers ---
def get_pg():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS short_urls (
                    short_path VARCHAR(32) PRIMARY KEY,
                    long_url TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()
    logger.info("%s Postgres schema initialized", LOG_PREFIX)


# --- Kafka (Redpanda) producer ---
def kafka_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def send_purge_event(short_path: str) -> None:
    try:
        prod = kafka_producer()
        prod.produce(PURGE_TOPIC, key=short_path.encode(), value=short_path.encode())
        prod.flush()
        logger.info("%s Produced purge event for key: %s", LOG_PREFIX, short_path)
    except Exception as e:
        logger.error("%s Failed to produce purge event: %s", LOG_PREFIX, e)


# --- Globals (set in lifespan) ---
redis_client: Redis | None = None
lru_cache: LRUCache | None = None
consumer_thread: threading.Thread | None = None
consumer_stop = threading.Event()


def run_purge_consumer():
    """Background consumer: on purge message -> invalidate LRU + Redis."""
    global lru_cache, redis_client
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "url-shortener-purge-consumer",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([PURGE_TOPIC])
    logger.info("%s Purge consumer started, topic=%s", LOG_PREFIX, PURGE_TOPIC)
    while not consumer_stop.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("%s Consumer error: %s", LOG_PREFIX, msg.error())
            continue
        key = msg.value().decode() if msg.value() else msg.key().decode() if msg.key() else None
        if not key:
            continue
        logger.info("%s Purging Cache for Key: %s", LOG_PREFIX, key)
        if lru_cache:
            lru_cache.delete(key)
        if redis_client:
            try:
                redis_client.delete(key)
            except Exception as e:
                logger.warning("%s Redis delete failed for %s: %s", LOG_PREFIX, key, e)
    consumer.close()


def ensure_purge_topic():
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        fs = admin.create_topics([NewTopic(PURGE_TOPIC, num_partitions=1, replication_factor=1)])
        fs[PURGE_TOPIC].result(timeout=10)
        logger.info("%s Topic created or exists: %s", LOG_PREFIX, PURGE_TOPIC)
    except Exception as e:
        logger.warning("%s Topic creation (may already exist): %s", LOG_PREFIX, e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, lru_cache, consumer_thread
    init_db()
    redis_client = Redis.from_url(REDIS_URL)
    lru_cache = LRUCache(LRU_MAX_SIZE)
    ensure_purge_topic()
    consumer_thread = threading.Thread(target=run_purge_consumer, daemon=True)
    consumer_thread.start()
    yield
    consumer_stop.set()
    if consumer_thread:
        consumer_thread.join(timeout=5)


app = FastAPI(title="URL Shortener", version="1.0.0", lifespan=lifespan)

# Frontend: serve demo UI at /
_STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/")
def index():
    """Serve the demo frontend."""
    index_file = _STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"service": "URL Shortener", "docs": "/docs"}


app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


class ShortenRequest(BaseModel):
    long_url: HttpUrl


@app.post("/shorten")
def shorten(body: ShortenRequest):
    """Call ID service, Base62 encode, save to Postgres, cache in Redis."""
    long_url = str(body.long_url)
    try:
        with httpx.Client(timeout=5.0) as client:
            r = client.get(f"{ID_SERVICE_URL.rstrip('/')}/generate")
        r.raise_for_status()
        raw = r.json()
        if isinstance(raw, dict) and "error" in raw:
            raise HTTPException(status_code=503, detail=raw.get("error", "ID service error"))
        id_val = raw if isinstance(raw, int) else raw.get("id", raw)
        snowflake_id = int(id_val)
    except Exception as e:
        logger.error("%s ID service call failed: %s", LOG_PREFIX, e)
        raise HTTPException(status_code=503, detail="ID service unavailable") from e

    short_path = encode_base62(snowflake_id)
    logger.info("%s Shorten: id=%s -> path=%s -> %s", LOG_PREFIX, snowflake_id, short_path, long_url)

    with get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO short_urls (short_path, long_url) VALUES (%s, %s) ON CONFLICT (short_path) DO NOTHING",
                (short_path, long_url),
            )
            conn.commit()

    if redis_client:
        try:
            redis_client.setex(short_path, 86400 * 7, long_url)  # 7 days TTL
        except Exception as e:
            logger.warning("%s Redis set failed: %s", LOG_PREFIX, e)
    if lru_cache:
        lru_cache.set(short_path, long_url)

    return {"short_path": short_path, "short_url": f"/r/{short_path}", "long_url": long_url}


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
    with get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT long_url FROM short_urls WHERE short_path = %s", (short_path,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Short URL not found")
    long_url = row[0]
    logger.info("%s Redirect DB hit: %s", LOG_PREFIX, short_path)
    if lru_cache:
        lru_cache.set(short_path, long_url)
    if redis_client:
        try:
            redis_client.setex(short_path, 86400 * 7, long_url)
        except Exception as e:
            logger.warning("%s Redis setex failed: %s", LOG_PREFIX, e)
    return RedirectResponse(url=long_url, status_code=302)


@app.delete("/r/{short_path}")
def delete_short_url(short_path: str):
    """Remove from Postgres and produce purge event to Redpanda."""
    with get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM short_urls WHERE short_path = %s RETURNING short_path", (short_path,))
            deleted = cur.rowcount
            conn.commit()
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Short URL not found")
    logger.info("%s Deleted from DB: %s", LOG_PREFIX, short_path)
    # Invalidate local + Redis via consumer (we could also invalidate locally here for consistency)
    if lru_cache:
        lru_cache.delete(short_path)
    if redis_client:
        try:
            redis_client.delete(short_path)
        except Exception as e:
            logger.warning("%s Redis delete failed: %s", LOG_PREFIX, e)
    send_purge_event(short_path)
    return {"ok": True, "short_path": short_path}

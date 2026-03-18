"""
URL Shortener Service - Handles POST /shorten
"""
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from redis import Redis

from shared import base62, config, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[SHORTENER-SERVICE]"

# --- Globals ---
redis_client: Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    db.init_db()
    redis_client = Redis.from_url(config.REDIS_URL)
    yield
    if redis_client:
        redis_client.close()


app = FastAPI(title="URL Shortener Service", version="1.0.0", lifespan=lifespan)


class ShortenRequest(BaseModel):
    long_url: HttpUrl
    expires_in_seconds: int | None = None


def _get_next_id() -> int:
    """Call ID service to generate a unique snowflake ID.
    
    Returns:
        int: The generated snowflake ID
        
    Raises:
        HTTPException: 503 if ID service is unavailable or returns an error
    """
    try:
        with httpx.Client(timeout=5.0) as client:
            r = client.get(f"{config.ID_SERVICE_URL.rstrip('/')}/generate")
        r.raise_for_status()
        raw = r.json()
        if isinstance(raw, dict) and "error" in raw:
            raise HTTPException(status_code=503, detail=raw.get("error", "ID service error"))
        id_val = raw if isinstance(raw, int) else raw.get("id", raw)
        return int(id_val)
    except Exception as e:
        logger.error("%s ID service call failed: %s", LOG_PREFIX, e)
        raise HTTPException(status_code=503, detail="ID service unavailable") from e


@app.post("/shorten")
def shorten(body: ShortenRequest):
    """Call ID service, Base62 encode, save to Postgres, cache in Redis."""
    long_url = str(body.long_url)
    expires_in = body.expires_in_seconds
    snowflake_id = _get_next_id()

    # Generate delete key (call ID service again for unique key)
    delete_key_id = _get_next_id()

    short_path = base62.encode_base62(snowflake_id)
    delete_key = base62.encode_base62(delete_key_id, length=10)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)) if expires_in else None
    logger.info("%s Shorten: id=%s -> path=%s -> %s", LOG_PREFIX, snowflake_id, short_path, long_url)
    logger.info("%s Generated delete_key=%s for path=%s", LOG_PREFIX, delete_key, short_path)

    with db.get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO short_urls (short_path, long_url, expires_at, delete_key) VALUES (%s, %s, %s, %s) ON CONFLICT (short_path) DO NOTHING",
                (short_path, long_url, expires_at, delete_key),
            )
            conn.commit()

    ttl = config._redis_ttl(expires_in)
    if redis_client:
        try:
            redis_client.setex(short_path, ttl, long_url)
        except Exception as e:
            logger.warning("%s Redis set failed: %s", LOG_PREFIX, e)

    return {"short_path": short_path, "short_url": f"/r/{short_path}", "long_url": long_url, "delete_key": delete_key, "expires_in_seconds": expires_in}


@app.get("/health")
def health():
    return {"status": "ok", "service": "shortener"}

"""
ID Generation Microservice — Snowflake IDs only.
WORKER_ID from environment. Clock drift protection.
Endpoint: GET /generate returns 64-bit integer.
"""
import logging
import os
import time
from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)
LOG_PREFIX = "[ID-SERVICE]"

# --- Configuration ---
# 41 bits time, 10 bits worker_id, 12 bits sequence
EPOCH_MS = int(os.environ.get("EPOCH_MS", "1739980800000"))  # Feb 19, 2026
WORKER_ID = int(os.environ.get("WORKER_ID", "0"))
MAX_WORKER_ID = (1 << 10) - 1  # 1023
MAX_SEQUENCE = (1 << 12) - 1   # 4095
CLOCK_DRIFT_MAX_WAIT_MS = 5000  # Max wait for clock to catch up

if not (0 <= WORKER_ID <= MAX_WORKER_ID):
    raise ValueError(f"WORKER_ID must be 0..{MAX_WORKER_ID}, got {WORKER_ID}")

_app_state = {"sequence": 0, "last_timestamp_ms": -1}

app = FastAPI(title="ID Generation Service", version="1.0.0")


def generate_snowflake_id() -> int | None:
    """
    Generate a 64-bit Snowflake ID with clock drift protection.
    If system clock moves backward, waits up to CLOCK_DRIFT_MAX_WAIT_MS for it to catch up.
    """
    state = _app_state
    now_ms = int(time.time() * 1000)
    last_ms = state["last_timestamp_ms"]
    seq = state["sequence"]

    # --- Clock drift protection ---
    if now_ms < last_ms:
        logger.warning("%s Clock drift detected: now=%s < last=%s", LOG_PREFIX, now_ms, last_ms)
        wait_until_ms = last_ms - now_ms
        if wait_until_ms > CLOCK_DRIFT_MAX_WAIT_MS:
            logger.error("%s Clock drift too large (%s ms), refusing to generate", LOG_PREFIX, wait_until_ms)
            return None
        deadline_ms = now_ms + CLOCK_DRIFT_MAX_WAIT_MS
        while True:
            time.sleep(0.001)  # 1 ms
            now_ms = int(time.time() * 1000)
            if now_ms >= last_ms:
                break
            if now_ms >= deadline_ms:
                logger.error("%s Clock drift wait timeout", LOG_PREFIX)
                return None
        logger.info("%s Clock caught up after drift", LOG_PREFIX)

    if now_ms == last_ms:
        seq = (seq + 1) & MAX_SEQUENCE
        if seq == 0:
            # Sequence overflow: wait for next millisecond
            while (now_ms := int(time.time() * 1000)) <= last_ms:
                time.sleep(0.001)
    else:
        seq = 0

    state["last_timestamp_ms"] = now_ms
    state["sequence"] = seq

    # 41 bits (timestamp - epoch) | 10 bits worker | 12 bits sequence
    time_bits = (now_ms - EPOCH_MS) & ((1 << 41) - 1)
    snowflake = (time_bits << 22) | (WORKER_ID << 12) | seq
    return snowflake


@app.get("/generate")
def get_generate():
    """Return a single 64-bit Snowflake ID as integer."""
    sid = generate_snowflake_id()
    if sid is None:
        logger.error("%s Failed to generate ID (clock drift)", LOG_PREFIX)
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"error": "Clock drift protection: could not generate ID"},
        )
    logger.info("%s Generated ID: %s", LOG_PREFIX, sid)
    return sid

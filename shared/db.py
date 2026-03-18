"""Database connection and schema initialization for Postgres."""
import logging

import psycopg2

from shared.config import DATABASE_URL

LOG_PREFIX = "[DB]"
logger = logging.getLogger(__name__)


def get_pg():
    """Open and return a new Postgres connection."""
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create short_urls table and add delete_key column if missing."""
    with get_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS short_urls (
                    short_path VARCHAR(32) PRIMARY KEY,
                    long_url TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    expires_at TIMESTAMPTZ
                )
            """)
            cur.execute("""
                ALTER TABLE short_urls
                ADD COLUMN IF NOT EXISTS delete_key VARCHAR(32)
            """)
            conn.commit()
    logger.info("%s Postgres schema initialized", LOG_PREFIX)

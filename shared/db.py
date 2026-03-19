import logging
import psycopg2
from shared.config import DATABASE_URL

LOG_PREFIX = "[DB]"
logger = logging.getLogger(__name__)

def get_pg():
    return psycopg2.connect(DATABASE_URL)

def init_db():
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
                ALTER TABLE short_urls ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
            """)
            conn.commit()
    logger.info("%s Postgres schema initialized", LOG_PREFIX)

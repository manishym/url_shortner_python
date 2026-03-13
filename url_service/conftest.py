"""
Pytest fixtures: mocks for Kafka, Redis, Postgres, and ID service (httpx).
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_pg_conn():
    """Mock psycopg2 connection and cursor for get_pg() context manager."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.commit = MagicMock()
    cursor.execute = MagicMock()
    cursor.fetchone = MagicMock(return_value=None)
    cursor.fetchall = MagicMock(return_value=[])
    cursor.rowcount = 0
    return conn, cursor


@pytest.fixture
def mock_get_pg(mock_pg_conn):
    """Patch get_pg to return mock connection."""

    conn, _ = mock_pg_conn

    @contextmanager
    def _get_pg():
        yield conn

    with patch("main.get_pg", _get_pg):
        yield mock_pg_conn


@pytest.fixture
def mock_redis():
    """Mock Redis client: get returns None by default, setex/delete no-op."""
    m = MagicMock()
    m.get.return_value = None
    m.setex.return_value = None
    m.delete.return_value = 0
    return m


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer: produce and flush no-op."""
    m = MagicMock()
    m.produce.return_value = None
    m.flush.return_value = None
    return m


@pytest.fixture
def noop_consumer_threads():
    """Patch purge consumer targets to no-op so threads exit immediately."""

    def _noop():
        pass

    with patch("main._run_purge_consumer_db", _noop), patch(
        "main._run_purge_consumer_redis", _noop
    ), patch("main._run_purge_consumer_lru", _noop):
        yield


@pytest.fixture
def client(mock_get_pg, mock_redis, mock_kafka_producer, noop_consumer_threads):
    """FastAPI TestClient with DB, Redis, Kafka, and consumer threads mocked."""
    with patch("main.Redis.from_url", return_value=mock_redis), patch(
        "main.init_db"
    ), patch("main.ensure_purge_topic"), patch(
        "main.kafka_producer", return_value=mock_kafka_producer
    ):
        from fastapi.testclient import TestClient
        from main import app
        with TestClient(app) as c:
            yield c

"""
Pytest fixtures for redirection service tests.
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_pg_conn():
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
    conn, _ = mock_pg_conn

    @contextmanager
    def _get_pg():
        yield conn

    with patch("shared.db.get_pg", _get_pg):
        yield mock_pg_conn


@pytest.fixture
def mock_redis():
    m = MagicMock()
    m.get.return_value = None
    m.setex.return_value = None
    m.close.return_value = None
    return m


@pytest.fixture
def mock_lru_cache():
    m = MagicMock()
    # IMPORTANT: lru_cache.get must return None by default to avoid infinite redirects
    m.get.return_value = None
    m.set.return_value = None
    m.delete.return_value = None
    return m


@pytest.fixture
def noop_consumer():
    def _noop():
        pass

    with patch("main._run_purge_consumer_lru", _noop):
        yield


@pytest.fixture
def client(mock_get_pg, mock_redis, mock_lru_cache, noop_consumer):
    """Create TestClient with proper Redis mock."""
    with patch("main.db.init_db"), patch("main.LRUCache", return_value=mock_lru_cache):
        import main
        main.lru_cache = mock_lru_cache
        # Patch Redis at import level
        with patch("redis.Redis.from_url", return_value=mock_redis):
            from fastapi.testclient import TestClient
            from main import app
            # Set the global redis_client to our mock
            main.redis_client = mock_redis
            with TestClient(app) as c:
                yield c

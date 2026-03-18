"""Pytest fixtures for deletion service tests."""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def mock_pg_conn():
    """Create a mock postgres connection with cursor."""
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
def mock_redis():
    """Create a mock Redis client."""
    mock = MagicMock()
    mock.get.return_value = None
    mock.setex.return_value = None
    mock.delete.return_value = 0
    mock.close.return_value = None
    return mock


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer."""
    mock = MagicMock()
    mock.produce.return_value = None
    mock.flush.return_value = None
    return mock


@pytest.fixture
def mock_consumer_stop():
    """Create a mock threading.Event for consumer stop signal."""
    mock = MagicMock()
    mock.set = MagicMock()
    mock.clear = MagicMock()
    return mock


@pytest.fixture
def mock_get_pg(mock_pg_conn):
    """Patch shared.db.get_pg to yield a mock connection."""
    conn, _ = mock_pg_conn

    @contextmanager
    def _get_pg():
        yield conn

    with patch("shared.db.get_pg", _get_pg):
        yield mock_pg_conn


@pytest.fixture
def client(mock_get_pg, mock_redis, mock_kafka_producer, mock_consumer_stop):
    """Create TestClient with mocked dependencies."""
    with patch("main.Redis.from_url", return_value=mock_redis):
        with patch("main.db.init_db"):
            with patch("main.kafka_utils.ensure_purge_topic"):
                with patch("main.kafka_utils.kafka_producer", return_value=mock_kafka_producer):
                    with patch("main.kafka_consumer.run_consumer"):
                        with TestClient(app) as test_client:
                            yield test_client

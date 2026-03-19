"""Pytest fixtures for shortener service tests."""
from contextlib import contextmanager, ExitStack
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
    mock.close.return_value = None
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
def mock_id_service():
    """Mock that returns a fixed ID for ID service calls."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = 1234567890123456789

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response

    return patch("httpx.Client", return_value=mock_client)


@pytest.fixture
def client(mock_get_pg, mock_redis, mock_id_service):
    """Create TestClient without patching httpx."""
    with patch("main.Redis.from_url", return_value=mock_redis):
        with patch("main.db.init_db"):
            with mock_id_service:
                with TestClient(app) as test_client:
                    yield test_client

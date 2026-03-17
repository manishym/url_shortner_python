"""
Pytest fixtures for shortener service tests.
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
def client(mock_get_pg, mock_redis):
    """Create TestClient without patching httpx."""
    with patch("main.Redis.from_url", return_value=mock_redis), patch(
        "main.db.init_db"
    ):
        from fastapi.testclient import TestClient
        from main import app
        with TestClient(app) as c:
            yield c


@pytest.fixture
def mock_id_service():
    """Mock that returns a fixed ID for ID service calls.
    
    This fixture should be used alongside `client` fixture in tests that 
    need to mock the ID service. It patches httpx.Client.
    """
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = 1234567890123456789
    
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response
    
    # Patch httpx.Client globally - this will affect all imports of httpx
    with patch("httpx.Client", return_value=mock_client):
        yield mock_client

"""
API / integration tests: shorten, redirect, delete.
Mocks: Postgres (get_pg), Redis, Kafka (producer), ID service (httpx).
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture
def mock_id_service():
    """Mock httpx.Client.get for ID service /generate returning a snowflake id."""
    with patch("main.httpx.Client") as mock_client_cls:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = 1234567890123456789
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client
        yield mock_client_cls


class TestShorten:
    def test_shorten_creates_url(
        self, client, mock_get_pg, mock_redis, mock_id_service
    ):
        conn, cursor = mock_get_pg
        r = client.post(
            "/shorten",
            json={"long_url": "https://example.com/page"},
        )
        assert r.status_code == 200
        data = r.json()
        assert "short_path" in data
        assert data["long_url"] == "https://example.com/page"
        assert data["short_url"] == f"/r/{data['short_path']}"
        assert data.get("expires_in_seconds") is None
        cursor.execute.assert_called()
        mock_redis.setex.assert_called_once()

    def test_shorten_with_expiry(
        self, client, mock_get_pg, mock_redis, mock_id_service
    ):
        conn, cursor = mock_get_pg
        r = client.post(
            "/shorten",
            json={
                "long_url": "https://example.com/exp",
                "expires_in_seconds": 120,
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["expires_in_seconds"] == 120
        # Redis TTL should be min(300, 120) = 120
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args[0]
        assert call_args[1] == 120

    def test_shorten_id_service_error(self, client, mock_get_pg, mock_id_service):
        with patch("main.httpx.Client") as mock_client_cls:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = Exception("timeout")
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client
            r = client.post(
                "/shorten",
                json={"long_url": "https://example.com"},
            )
        assert r.status_code == 503

    def test_shorten_id_service_returns_error_json(
        self, client, mock_get_pg, mock_id_service
    ):
        with patch("main.httpx.Client") as mock_client_cls:
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json.return_value = {"error": "ID service overloaded"}
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client
            r = client.post(
                "/shorten",
                json={"long_url": "https://example.com"},
            )
        assert r.status_code == 503

    def test_shorten_id_service_returns_dict_with_id_key(
        self, client, mock_get_pg, mock_redis, mock_id_service
    ):
        with patch("main.httpx.Client") as mock_client_cls:
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.json.return_value = {"id": 999888777}
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client
            r = client.post(
                "/shorten",
                json={"long_url": "https://example.com/dict"},
            )
        assert r.status_code == 200
        assert "short_path" in r.json()

    def test_shorten_redis_setex_failure_does_not_fail_request(
        self, client, mock_get_pg, mock_redis, mock_id_service
    ):
        mock_redis.setex.side_effect = Exception("redis setex failed")
        r = client.post(
            "/shorten",
            json={"long_url": "https://example.com/redis-fail"},
        )
        assert r.status_code == 200


class TestRedirect:
    def test_redirect_lru_hit(self, client, mock_get_pg, mock_redis):
        from main import lru_cache
        assert lru_cache is not None
        lru_cache.set("abc1234", "https://example.com/target")
        r = client.get("/r/abc1234", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://example.com/target"

    def test_redirect_redis_hit(self, client, mock_get_pg, mock_redis):
        mock_redis.get.return_value = b"https://redis-hit.com"
        r = client.get("/r/xyz9999", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://redis-hit.com"

    def test_redirect_db_hit(self, client, mock_get_pg, mock_redis):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = (
            "https://db-hit.com",
            None,  # expires_at
        )
        r = client.get("/r/dbpath1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://db-hit.com"
        mock_redis.setex.assert_called_once()

    def test_redirect_db_expired_returns_404(self, client, mock_get_pg, mock_redis):
        conn, cursor = mock_get_pg
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        cursor.fetchone.return_value = ("https://expired.com", past)
        r = client.get("/r/expired1")
        assert r.status_code == 404
        assert "expired" in r.json()["detail"].lower()

    def test_redirect_not_found(self, client, mock_get_pg, mock_redis):
        mock_redis.get.return_value = None
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = None
        r = client.get("/r/nonexistent")
        assert r.status_code == 404

    def test_redirect_redis_returns_str_not_bytes(self, client, mock_get_pg, mock_redis):
        mock_redis.get.return_value = "https://str-url.com"  # str
        r = client.get("/r/strpath", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://str-url.com"

    def test_redirect_redis_exception_falls_through_to_db(self, client, mock_get_pg, mock_redis):
        mock_redis.get.side_effect = Exception("redis down")
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://db-fallback.com", None)
        r = client.get("/r/fallback1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://db-fallback.com"

    def test_redirect_db_hit_with_future_expires_at_uses_ttl(self, client, mock_get_pg, mock_redis):
        """Covers redirect branch when expires_at is set (ttl from remaining time)."""
        conn, cursor = mock_get_pg
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        cursor.fetchone.return_value = ("https://future-expiry.com", future)
        r = client.get("/r/future1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://future-expiry.com"
        mock_redis.setex.assert_called_once()
        ttl = mock_redis.setex.call_args[0][1]
        assert 1 <= ttl <= 300

    def test_redirect_db_hit_redis_setex_failure_returns_302(self, client, mock_get_pg, mock_redis):
        """Redirect still succeeds when Redis setex fails (covers warning path)."""
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://ok.com", None)
        mock_redis.setex.side_effect = Exception("redis setex failed")
        r = client.get("/r/okpath", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://ok.com"


class TestDelete:
    def test_delete_produces_purge_event(
        self, client, mock_get_pg, mock_kafka_producer
    ):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = (1,)  # exists
        r = client.delete("/r/abc1234")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["short_path"] == "abc1234"
        mock_kafka_producer.produce.assert_called_once()
        from main import PURGE_TOPIC
        assert mock_kafka_producer.produce.call_args[0][0] == PURGE_TOPIC
        mock_kafka_producer.flush.assert_called_once()

    def test_delete_not_found(self, client, mock_get_pg, mock_kafka_producer):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = None
        r = client.delete("/r/nonexistent")
        assert r.status_code == 404
        mock_kafka_producer.produce.assert_not_called()


class TestIndexAndStatic:
    def test_index_returns_200_or_json(self, client):
        r = client.get("/")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            if "text/html" in r.headers.get("content-type", ""):
                assert b"URL Shortener" in r.content or b"Shorten" in r.content
            else:
                assert "service" in r.json() or "docs" in r.json()

    def test_index_when_file_missing_returns_json(self, client):
        """When index.html does not exist, / returns JSON fallback."""
        from unittest.mock import patch
        with patch("main._STATIC_DIR") as mock_static:
            mock_index_file = MagicMock()
            mock_index_file.exists.return_value = False
            mock_static.__truediv__.return_value = mock_index_file
            from main import index
            r = index()
        assert r == {"service": "URL Shortener", "docs": "/docs"}

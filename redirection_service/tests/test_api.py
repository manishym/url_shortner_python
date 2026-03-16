"""
API tests for redirection service.
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest


class TestRedirect:
    def test_redirect_lru_hit(self, client, mock_get_pg, mock_redis, mock_lru_cache):
        mock_lru_cache.get.return_value = "https://example.com/target"
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
        mock_redis.get.return_value = "https://str-url.com"
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
        conn, cursor = mock_get_pg
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        cursor.fetchone.return_value = ("https://future-expiry.com", future)
        r = client.get("/r/future1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://future-expiry.com"
        mock_redis.setex.assert_called_once()
        ttl = mock_redis.setex.call_args[0][1]
        assert 1 <= ttl <= 300


class TestHealth:
    def test_health_returns_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "redirection"}

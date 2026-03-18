"""
API tests for shortener service.
"""
from unittest.mock import patch, MagicMock
import redis.exceptions


class TestShorten:
    def test_shorten_creates_url(self, client, mock_get_pg, mock_redis, mock_id_service):
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
        assert "delete_key" in data
        assert len(data["delete_key"]) == 10
        cursor.execute.assert_called()
        mock_redis.setex.assert_called_once()

    def test_shorten_with_expiry(self, client, mock_get_pg, mock_redis, mock_id_service):
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
        assert "delete_key" in data
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

    def test_shorten_id_service_returns_error_json(self, client, mock_get_pg, mock_id_service):
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

    def test_shorten_redis_setex_failure_does_not_fail_request(
        self, client, mock_get_pg, mock_redis, mock_id_service
    ):
        mock_redis.setex.side_effect = redis.exceptions.RedisError("redis setex failed")
        r = client.post(
            "/shorten",
            json={"long_url": "https://example.com/redis-fail"},
        )
        assert r.status_code == 200


class TestHealth:
    def test_health_returns_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "shortener"}

"""
API tests for redirection service.
"""
import threading
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
import redis.exceptions
from confluent_kafka import KafkaException


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
        mock_redis.get.side_effect = redis.exceptions.RedisError("redis down")
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

    def test_redirect_redis_setex_exception(self, client, mock_get_pg, mock_redis):
        mock_redis.setex.side_effect = redis.exceptions.RedisError("redis write failed")
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = (
            "https://example.com",
            None,
        )
        r = client.get("/r/newpath", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://example.com"

    def test_redirect_lru_cache_populates_on_db_hit(self, client, mock_get_pg, mock_redis, mock_lru_cache):
        conn, cursor = mock_get_pg
        cursor.fetchone.return_value = (
            "https://db-with-lru.com",
            None,
        )
        r = client.get("/r/lrupath", follow_redirects=False)
        assert r.status_code == 302
        mock_lru_cache.set.assert_called_once()

    def test_redirect_skips_lru_when_none(self, client, mock_get_pg, mock_redis):
        import main
        original = main.lru_cache
        main.lru_cache = None
        try:
            mock_redis.get.return_value = b"https://no-lru.com"
            r = client.get("/r/nolru1", follow_redirects=False)
            assert r.status_code == 302
            assert r.headers["location"] == "https://no-lru.com"
        finally:
            main.lru_cache = original

    def test_redirect_skips_redis_when_none(self, client, mock_get_pg, mock_redis, mock_lru_cache):
        import main
        original = main.redis_client
        main.redis_client = None
        try:
            conn, cursor = mock_get_pg
            cursor.fetchone.return_value = ("https://no-redis.com", None)
            r = client.get("/r/noredis1", follow_redirects=False)
            assert r.status_code == 302
            assert r.headers["location"] == "https://no-redis.com"
        finally:
            main.redis_client = original


class TestHealth:
    def test_health_returns_ok(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "redirection"}


class TestLRUCache:
    def test_cache_set_and_get(self):
        from main import LRUCache
        cache = LRUCache(max_size=3)
        cache.set("a", "value_a")
        assert cache.get("a") == "value_a"

    def test_cache_miss(self):
        from main import LRUCache
        cache = LRUCache(max_size=3)
        assert cache.get("nonexistent") is None

    def test_cache_eviction(self):
        from main import LRUCache
        cache = LRUCache(max_size=2)
        cache.set("a", "value_a")
        cache.set("b", "value_b")
        cache.set("c", "value_c")
        assert cache.get("a") is None

    def test_cache_delete(self):
        from main import LRUCache
        cache = LRUCache(max_size=3)
        cache.set("a", "value_a")
        cache.delete("a")
        assert cache.get("a") is None

    def test_cache_set_updates_existing_key(self):
        from main import LRUCache
        cache = LRUCache(max_size=3)
        cache.set("a", "old_value")
        cache.set("b", "value_b")
        cache.set("a", "new_value")
        assert cache.get("a") == "new_value"
        cache.set("c", "value_c")
        cache.set("d", "value_d")
        assert cache.get("b") is None
        assert cache.get("a") == "new_value"


class TestPurgeConsumer:
    def _make_msg(self, value=None, key=None, error=None):
        msg = MagicMock()
        msg.value.return_value = value
        msg.key.return_value = key
        msg.error.return_value = error
        return msg

    def test_purge_consumer_deletes_from_lru(self):
        from main import LRUCache
        import main

        cache = LRUCache(max_size=10)
        cache.set("abc123", "https://example.com")

        msg_valid = self._make_msg(value=b"abc123")
        mock_consumer = MagicMock()
        poll_results = [None, msg_valid, None]
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            if call_count < len(poll_results):
                result = poll_results[call_count]
                call_count += 1
                return result
            main.consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        original_cache = main.lru_cache
        main.consumer_stop.clear()
        main.lru_cache = cache

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            main._run_purge_consumer_lru()

        assert cache.get("abc123") is None
        mock_consumer.commit.assert_called_once_with(message=msg_valid)
        mock_consumer.close.assert_called_once()
        main.lru_cache = original_cache
        main.consumer_stop.clear()

    def test_purge_consumer_skips_error_messages(self):
        import main

        error_msg = self._make_msg(error="broker error")
        mock_consumer = MagicMock()
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return error_msg
            main.consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        main.consumer_stop.clear()
        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            main._run_purge_consumer_lru()

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()
        main.consumer_stop.clear()

    def test_purge_consumer_skips_empty_key(self):
        import main

        empty_msg = self._make_msg(value=None, key=None)
        empty_msg.error.return_value = None
        mock_consumer = MagicMock()
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return empty_msg
            main.consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        main.consumer_stop.clear()
        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            main._run_purge_consumer_lru()

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()
        main.consumer_stop.clear()

    def test_purge_consumer_handles_commit_failure(self):
        from main import LRUCache
        import main

        cache = LRUCache(max_size=10)
        cache.set("failkey", "https://example.com")

        msg = self._make_msg(value=b"failkey")
        mock_consumer = MagicMock()
        mock_consumer.commit.side_effect = KafkaException("commit error")
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return msg
            main.consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        original_cache = main.lru_cache
        main.consumer_stop.clear()
        main.lru_cache = cache

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            main._run_purge_consumer_lru()

        assert cache.get("failkey") is None
        main.lru_cache = original_cache
        main.consumer_stop.clear()

    def test_purge_consumer_skips_delete_when_lru_none(self):
        import main

        msg = self._make_msg(value=b"somekey")
        mock_consumer = MagicMock()
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return msg
            main.consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        original_cache = main.lru_cache
        main.consumer_stop.clear()
        main.lru_cache = None

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            main._run_purge_consumer_lru()

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()
        main.lru_cache = original_cache
        main.consumer_stop.clear()

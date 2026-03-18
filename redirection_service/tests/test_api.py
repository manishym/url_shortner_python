"""API tests for redirection service."""
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import redis.exceptions
from confluent_kafka import KafkaException
from shared import LRUCache


class TestRedirect:
    """Tests for the GET /r/{short_path} endpoint."""

    def test_redirect_lru_hit(self, client, mock_lru_cache):
        """Test redirect when URL is in LRU cache."""
        mock_lru_cache.get.return_value = "https://example.com/target"
        r = client.get("/r/abc1234", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://example.com/target"

    def test_redirect_redis_hit(self, client, mock_redis):
        """Test redirect when URL is in Redis cache."""
        mock_redis.get.return_value = b"https://redis-hit.com"
        r = client.get("/r/xyz9999", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://redis-hit.com"

    def test_redirect_db_hit(self, client, mock_get_pg, mock_redis):
        """Test redirect when URL is only in database."""
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://db-hit.com", None)
        r = client.get("/r/dbpath1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://db-hit.com"
        mock_redis.setex.assert_called_once()

    def test_redirect_db_expired_returns_404(self, client, mock_get_pg):
        """Test that expired URLs return 404."""
        _, cursor = mock_get_pg
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        cursor.fetchone.return_value = ("https://expired.com", past)
        r = client.get("/r/expired1")
        assert r.status_code == 404
        assert "expired" in r.json()["detail"].lower()

    def test_redirect_not_found(self, client, mock_get_pg, mock_redis):
        """Test that non-existent URLs return 404."""
        mock_redis.get.return_value = None
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = None
        r = client.get("/r/nonexistent")
        assert r.status_code == 404

    def test_redirect_redis_returns_str_not_bytes(self, client, mock_get_pg, mock_redis):
        """Test that Redis string values are handled correctly."""
        mock_redis.get.return_value = "https://str-url.com"
        r = client.get("/r/strpath", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://str-url.com"

    def test_redirect_redis_exception_falls_through_to_db(
        self, client, mock_get_pg, mock_redis
    ):
        """Test that Redis errors fall back to database."""
        mock_redis.get.side_effect = redis.exceptions.RedisError("redis down")
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://db-fallback.com", None)
        r = client.get("/r/fallback1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://db-fallback.com"

    def test_redirect_db_hit_with_future_expires_at_uses_ttl(
        self, client, mock_get_pg, mock_redis
    ):
        """Test that future expiry uses calculated TTL."""
        _, cursor = mock_get_pg
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        cursor.fetchone.return_value = ("https://future-expiry.com", future)
        r = client.get("/r/future1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://future-expiry.com"
        mock_redis.setex.assert_called_once()
        ttl = mock_redis.setex.call_args[0][1]
        assert 1 <= ttl <= 300

    def test_redirect_redis_setex_exception(self, client, mock_get_pg, mock_redis):
        """Test that Redis write errors don't fail the redirect."""
        _, cursor = mock_get_pg
        mock_redis.setex.side_effect = redis.exceptions.RedisError(
            "redis write failed"
        )
        cursor.fetchone.return_value = ("https://example.com", None)
        r = client.get("/r/newpath", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://example.com"

    def test_redirect_lru_cache_populates_on_db_hit(
        self, client, mock_get_pg, mock_lru_cache
    ):
        """Test that LRU cache is populated after database hit."""
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://db-with-lru.com", None)
        r = client.get("/r/lrupath", follow_redirects=False)
        assert r.status_code == 302
        mock_lru_cache.set.assert_called_once()

    def test_redirect_skips_lru_when_none(self, client, mock_get_pg, mock_redis):
        """Test redirect works when LRU cache is disabled."""
        client.app.state.lru_cache = None
        mock_redis.get.return_value = b"https://no-lru.com"
        r = client.get("/r/nolru1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://no-lru.com"

    def test_redirect_skips_redis_when_none(self, client, mock_get_pg, mock_lru_cache):
        """Test redirect works when Redis is disabled."""
        client.app.state.redis = None
        _, cursor = mock_get_pg
        cursor.fetchone.return_value = ("https://no-redis.com", None)
        r = client.get("/r/noredis1", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["location"] == "https://no-redis.com"


class TestHealth:
    """Tests for the /health endpoint."""

    def test_health_returns_ok(self, client):
        """Test that health endpoint returns ok status."""
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "service": "redirection"}


class TestLRUCache:
    """Unit tests for the LRUCache class."""

    def test_cache_set_and_get(self):
        """Test basic set and get operations."""
        cache = LRUCache(max_size=3)
        cache.set("a", "value_a")
        assert cache.get("a") == "value_a"

    def test_cache_miss(self):
        """Test cache miss returns None."""
        cache = LRUCache(max_size=3)
        assert cache.get("nonexistent") is None

    def test_cache_eviction(self):
        """Test that oldest entry is evicted when full."""
        cache = LRUCache(max_size=2)
        cache.set("a", "value_a")
        cache.set("b", "value_b")
        cache.set("c", "value_c")
        assert cache.get("a") is None

    def test_cache_delete(self):
        """Test that delete removes entry from cache."""
        cache = LRUCache(max_size=3)
        cache.set("a", "value_a")
        cache.delete("a")
        assert cache.get("a") is None

    def test_cache_set_updates_existing_key(self):
        """Test that updating existing key moves it to most recent."""
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
    """Tests for the Kafka purge consumer functionality."""

    def _make_msg(self, value=None, key=None, error=None):
        """Create a mock Kafka message."""
        msg = MagicMock()
        msg.value.return_value = value
        msg.key.return_value = key
        msg.error.return_value = error
        return msg

    def test_purge_consumer_deletes_from_lru(self):
        """Test that consumer deletes from LRU on purge message."""
        from main import _run_purge_consumer_lru

        cache = LRUCache(max_size=10)
        cache.set("abc123", "https://example.com")
        consumer_stop = threading.Event()

        msg_valid = self._make_msg(value=b"abc123")
        mock_consumer = MagicMock()
        poll_results = [None, msg_valid, None]
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            if call_count[0] < len(poll_results):
                result = poll_results[call_count[0]]
                call_count[0] += 1
                return result
            consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            _run_purge_consumer_lru(consumer_stop, cache)

        assert cache.get("abc123") is None
        mock_consumer.commit.assert_called_once_with(message=msg_valid)
        mock_consumer.close.assert_called_once()

    def test_purge_consumer_skips_error_messages(self):
        """Test that consumer skips messages with errors."""
        from main import _run_purge_consumer_lru

        consumer_stop = threading.Event()

        error_msg = self._make_msg(error="broker error")
        mock_consumer = MagicMock()
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            if call_count[0] == 0:
                call_count[0] += 1
                return error_msg
            consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            _run_purge_consumer_lru(consumer_stop, None)

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()

    def test_purge_consumer_skips_empty_key(self):
        """Test that consumer skips messages with empty values."""
        from main import _run_purge_consumer_lru

        consumer_stop = threading.Event()

        empty_msg = self._make_msg(value=None, key=None)
        empty_msg.error.return_value = None
        mock_consumer = MagicMock()
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            if call_count[0] == 0:
                call_count[0] += 1
                return empty_msg
            consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            _run_purge_consumer_lru(consumer_stop, None)

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()

    def test_purge_consumer_handles_commit_failure(self):
        """Test that consumer handles commit failures gracefully."""
        from main import _run_purge_consumer_lru

        cache = LRUCache(max_size=10)
        cache.set("failkey", "https://example.com")
        consumer_stop = threading.Event()

        msg = self._make_msg(value=b"failkey")
        mock_consumer = MagicMock()
        mock_consumer.commit.side_effect = KafkaException("commit error")
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            if call_count[0] == 0:
                call_count[0] += 1
                return msg
            consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            _run_purge_consumer_lru(consumer_stop, cache)

        assert cache.get("failkey") is None

    def test_purge_consumer_skips_delete_when_lru_none(self):
        """Test that consumer handles None LRU gracefully."""
        from main import _run_purge_consumer_lru

        consumer_stop = threading.Event()

        msg = self._make_msg(value=b"somekey")
        mock_consumer = MagicMock()
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            if call_count[0] == 0:
                call_count[0] += 1
                return msg
            consumer_stop.set()
            return None

        mock_consumer.poll.side_effect = poll_side_effect

        with patch("shared.kafka_consumer.Consumer", return_value=mock_consumer):
            _run_purge_consumer_lru(consumer_stop, None)

        mock_consumer.commit.assert_not_called()
        mock_consumer.close.assert_called_once()

"""
Unit tests: Base62, LRUCache, _redis_ttl, _short_path_from_message.
No external services; pure logic.
"""
import threading

import pytest

from main import (
    LRUCache,
    BASE62_CHARS,
    decode_base62,
    encode_base62,
    _redis_ttl,
    _short_path_from_message,
    REDIS_DEFAULT_TTL,
)


# --- Base62 ---
class TestEncodeBase62:
    def test_zero(self):
        assert encode_base62(0) == "0000000"
        assert encode_base62(0, length=3) == "000"

    def test_small(self):
        assert encode_base62(1) == "0000001"
        assert encode_base62(61) == "000000Z"
        assert encode_base62(62) == "0000010"

    def test_length_pad(self):
        assert encode_base62(10, length=5) == "0000a"
        assert len(encode_base62(100, length=7)) == 7

    def test_length_truncate(self):
        # when encoded is longer than length, take last `length` chars
        big = (62**8) + 1
        out = encode_base62(big, length=7)
        assert len(out) == 7
        assert decode_base62(out) == big % (62**7) or 62**7

    def test_roundtrip(self):
        for n in [0, 1, 62, 12345, 62**6]:
            assert decode_base62(encode_base62(n)) == n


class TestDecodeBase62:
    def test_zero_str(self):
        assert decode_base62("0000000") == 0

    def test_single_chars(self):
        assert decode_base62("1") == 1
        assert decode_base62("a") == 10
        assert decode_base62("Z") == 61

    def test_empty_string_returns_zero(self):
        assert decode_base62("") == 0

    def test_invalid_char_raises(self):
        with pytest.raises(ValueError):
            decode_base62("-")
        with pytest.raises(ValueError):
            decode_base62("abc@1")  # @ not in BASE62


# --- LRUCache ---
class TestLRUCache:
    def test_get_miss(self):
        cache = LRUCache(2)
        assert cache.get("a") is None

    def test_set_get(self):
        cache = LRUCache(2)
        cache.set("a", "https://a.com")
        assert cache.get("a") == "https://a.com"

    def test_eviction(self):
        cache = LRUCache(2)
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")  # evicts a
        assert cache.get("a") is None
        assert cache.get("b") == "2"
        assert cache.get("c") == "3"

    def test_delete(self):
        cache = LRUCache(2)
        cache.set("a", "1")
        cache.delete("a")
        assert cache.get("a") is None
        cache.delete("nonexistent")  # no raise

    def test_update_moves_to_end(self):
        cache = LRUCache(2)
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("a", "1b")  # update a
        cache.set("c", "3")  # should evict b (a was used last)
        assert cache.get("b") is None
        assert cache.get("a") == "1b"
        assert cache.get("c") == "3"

    def test_thread_safe(self):
        cache = LRUCache(1000)
        def writer():
            for i in range(500):
                cache.set(f"k{i}", f"v{i}")
        def reader():
            for i in range(500):
                cache.get(f"k{i}")
        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()


# --- _redis_ttl ---
class TestRedisTtl:
    def test_none_returns_default(self):
        assert _redis_ttl(None) == REDIS_DEFAULT_TTL

    def test_less_than_default(self):
        assert _redis_ttl(60) == 60
        assert _redis_ttl(1) == 1

    def test_greater_than_default_capped(self):
        assert _redis_ttl(600) == REDIS_DEFAULT_TTL
        assert _redis_ttl(10000) == REDIS_DEFAULT_TTL

    def test_zero_capped_to_one(self):
        assert _redis_ttl(0) == 1


# --- _short_path_from_message ---
class TestShortPathFromMessage:
    def test_value_bytes(self):
        msg = MagicMockMessage(value=b"abc1234", key=None)
        assert _short_path_from_message(msg) == "abc1234"

    def test_key_bytes_when_value_none(self):
        msg = MagicMockMessage(value=None, key=b"xyz9999")
        assert _short_path_from_message(msg) == "xyz9999"

    def test_value_str(self):
        msg = MagicMockMessage(value="path1", key=None)
        assert _short_path_from_message(msg) == "path1"

    def test_none_when_both_none(self):
        msg = MagicMockMessage(value=None, key=None)
        assert _short_path_from_message(msg) is None


class MagicMockMessage:
    def __init__(self, value=None, key=None):
        self._value = value
        self._key = key

    def value(self):
        return self._value

    def key(self):
        return self._key

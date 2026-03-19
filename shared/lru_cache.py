"""Thread-safe LRU cache implementation using OrderedDict."""
import threading
from collections import OrderedDict


class LRUCache:
    """Thread-safe Least Recently Used cache backed by OrderedDict."""

    def __init__(self, max_size: int):
        """Initialize cache with given maximum size."""
        self._max_size = max_size
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> str | None:
        """Retrieve value by key, moving it to most-recent position."""
        with self._lock:
            if key not in self._cache:
                return None
            self._cache.move_to_end(key)
            return self._cache[key]

    def set(self, key: str, value: str) -> None:
        """Insert or update key, evicting oldest if over capacity."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def delete(self, key: str) -> None:
        """Remove key from cache if present."""
        with self._lock:
            self._cache.pop(key, None)

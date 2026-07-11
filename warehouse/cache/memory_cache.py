"""
In-process cache with TTL (time-to-live).

This replaces Redis for the MVP warehouse service. It is intentionally simple:
a module-level dict keyed by the cache key, storing ``(timestamp, value)`` pairs.
Entries expire after *ttl_seconds*.

Known limitation: this cache is **not shared across multiple service instances**
(unlike Redis). If the service is scaled horizontally, each instance maintains
its own cache. This is documented in the README as a known trade-off.
"""

import time
from functools import wraps
from typing import Any, Callable, Dict, Tuple

# Module-level cache store: {cache_key: (cached_at_timestamp, value)}
_cache: Dict[str, Tuple[float, Any]] = {}


def cached(ttl_seconds: int = 300):
    """
    Decorator that caches the return value of a function for *ttl_seconds*.

    The decorated function's **first positional argument** is used as the
    cache key. For the DuckDB engine this is the SQL string itself.

    Args:
        ttl_seconds: How long (in seconds) a cached entry is considered fresh.

    Example::

        @cached(ttl_seconds=300)
        def query(self, sql: str):
            ...
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(cache_key: str, *args, **kwargs):
            now = time.time()
            if cache_key in _cache:
                cached_at, value = _cache[cache_key]
                if now - cached_at < ttl_seconds:
                    # Mark the result as a cache hit so callers can report it.
                    if isinstance(value, dict):
                        value = {**value, "cache_hit": True}
                    return value
            result = fn(cache_key, *args, **kwargs)
            # Store a shallow copy so cache_hit doesn't leak into stored value.
            stored = dict(result) if isinstance(result, dict) else result
            _cache[cache_key] = (now, stored)
            if isinstance(result, dict):
                result = {**result, "cache_hit": False}
            return result

        return wrapper

    return decorator


def clear_cache() -> None:
    """Remove all entries from the in-process cache."""
    _cache.clear()


def cache_size() -> int:
    """Return the number of entries currently in the cache."""
    return len(_cache)

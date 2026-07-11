"""
Tests for the in-process cache (memory_cache.py).

Verifies that:
  - First query misses cache (cache_hit=False)
  - Second identical query hits cache (cache_hit=True)
  - Cache expires after TTL
  - clear_cache() removes all entries
"""

import sys
import time
from pathlib import Path

# Add warehouse package to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from warehouse.cache.memory_cache import _cache, cached, clear_cache, cache_size


class TestCacheHitMiss:
    """Test basic cache hit/miss behavior."""

    def setup_method(self):
        """Clear cache before each test."""
        clear_cache()

    def test_first_call_misses_cache(self):
        @cached(ttl_seconds=300)
        def fake_query(sql):
            return {"columns": ["a"], "rows": [[1]], "cache_hit": False}

        result = fake_query("SELECT 1")
        assert result["cache_hit"] is False

    def test_second_call_hits_cache(self):
        call_count = 0

        @cached(ttl_seconds=300)
        def fake_query(sql):
            nonlocal call_count
            call_count += 1
            return {"columns": ["a"], "rows": [[1]], "cache_hit": False}

        # First call — miss
        r1 = fake_query("SELECT 1")
        assert r1["cache_hit"] is False
        assert call_count == 1

        # Second call — hit
        r2 = fake_query("SELECT 1")
        assert r2["cache_hit"] is True
        assert call_count == 1  # Function body not executed again

    def test_different_queries_both_miss(self):
        @cached(ttl_seconds=300)
        def fake_query(sql):
            return {"result": sql, "cache_hit": False}

        r1 = fake_query("SELECT 1")
        r2 = fake_query("SELECT 2")
        assert r1["cache_hit"] is False
        assert r2["cache_hit"] is False

    def test_cache_stores_correct_data(self):
        @cached(ttl_seconds=300)
        def fake_query(sql):
            return {"columns": ["col1", "col2"], "rows": [[1, 2], [3, 4]], "cache_hit": False}

        fake_query("SELECT * FROM t")
        # Second call should return same data from cache
        r2 = fake_query("SELECT * FROM t")
        assert r2["columns"] == ["col1", "col2"]
        assert r2["rows"] == [[1, 2], [3, 4]]
        assert r2["cache_hit"] is True


class TestCacheTTL:
    """Test TTL-based cache expiry."""

    def setup_method(self):
        clear_cache()

    def test_cache_expires_after_ttl(self):
        call_count = 0

        @cached(ttl_seconds=1)  # Very short TTL for testing
        def fake_query(sql):
            nonlocal call_count
            call_count += 1
            return {"result": call_count, "cache_hit": False}

        # First call — miss
        r1 = fake_query("SELECT 1")
        assert r1["cache_hit"] is False
        assert call_count == 1

        # Wait for TTL to expire
        time.sleep(1.1)

        # Second call — should miss again (cache expired)
        r2 = fake_query("SELECT 1")
        assert r2["cache_hit"] is False
        assert call_count == 2  # Function body executed again

    def test_cache_valid_within_ttl(self):
        call_count = 0

        @cached(ttl_seconds=10)  # Long TTL
        def fake_query(sql):
            nonlocal call_count
            call_count += 1
            return {"result": call_count, "cache_hit": False}

        # First call — miss
        fake_query("SELECT 1")
        assert call_count == 1

        # Immediate second call — should hit
        r2 = fake_query("SELECT 1")
        assert r2["cache_hit"] is True
        assert call_count == 1  # Not re-executed


class TestCacheManagement:
    """Test cache management functions."""

    def setup_method(self):
        clear_cache()

    def test_clear_cache_removes_entries(self):
        @cached(ttl_seconds=300)
        def fake_query(sql):
            return {"result": 1, "cache_hit": False}

        fake_query("SELECT 1")
        assert cache_size() == 1

        clear_cache()
        assert cache_size() == 0

    def test_clear_cache_causes_miss_on_next_call(self):
        @cached(ttl_seconds=300)
        def fake_query(sql):
            return {"result": 1, "cache_hit": False}

        fake_query("SELECT 1")
        clear_cache()

        r = fake_query("SELECT 1")
        assert r["cache_hit"] is False  # Miss because cache was cleared

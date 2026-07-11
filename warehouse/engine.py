"""
DuckDB query engine for Iceberg Gold layer.

MVP scope: ``query()`` (execute) + ``close()`` only.
The ``explain()``/``tables()``/``schema()`` helper methods were cut per the
blueprint cutting-back table — they are not needed for the core /query endpoint.
"""

import duckdb
import pandas as pd
import hashlib
import time
from typing import Dict, Any
import os

from .cache.memory_cache import _cache, clear_cache


class DuckDBEngine:
    """DuckDB engine for querying Iceberg Gold tables (read-only)."""

    def __init__(self, iceberg_path: str = None, cache_ttl: int = 300):
        """
        Initialize DuckDB connection with Iceberg catalog.

        Args:
            iceberg_path: S3 path to Iceberg warehouse (e.g. s3://bucket/gold/).
            cache_ttl: Cache time-to-live in seconds (default 300 = 5 min).
        """
        self.iceberg_path = iceberg_path or os.getenv(
            "S3_GOLD_PATH", "s3://instacart-lakehouse/gold"
        )
        self.cache_ttl = cache_ttl
        self.conn = duckdb.connect(database=":memory:", read_only=False)

        # Install and load Iceberg extension
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")

        # Configure AWS credentials if available
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")

        if aws_key and aws_secret:
            self.conn.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{aws_key}',
                    SECRET '{aws_secret}',
                    REGION '{aws_region}'
                )
            """)

    def query(self, sql: str) -> Dict[str, Any]:
        """
        Execute a read-only SQL query and return results.

        Results are cached in-process (TTL-based). Repeated identical queries
        within the TTL window return the cached result with ``cache_hit: True``.

        Args:
            sql: SQL query string (already validated as SELECT/WITH by caller).

        Returns:
            Dict with columns, rows, row_count, execution_time_ms, cache_hit.
        """
        cache_key = hashlib.md5(sql.strip().encode()).hexdigest()
        now = time.time()

        # --- Cache lookup ---
        if cache_key in _cache:
            cached_at, value = _cache[cache_key]
            if now - cached_at < self.cache_ttl:
                return {**value, "cache_hit": True}

        # --- Execute query ---
        start_time = time.time()
        try:
            result = self.conn.execute(sql).fetchdf()
        except Exception as e:
            raise ValueError(f"Query execution failed: {e}")

        execution_time = (time.time() - start_time) * 1000

        output = {
            "columns": result.columns.tolist(),
            "rows": result.values.tolist(),
            "row_count": len(result),
            "execution_time_ms": round(execution_time, 2),
            "cache_hit": False,
        }

        # Store in cache (without the cache_hit flag so it stays clean)
        stored = {k: v for k, v in output.items() if k != "cache_hit"}
        _cache[cache_key] = (now, stored)

        return output

    def close(self):
        """Close DuckDB connection and clear the in-process cache."""
        clear_cache()
        self.conn.close()

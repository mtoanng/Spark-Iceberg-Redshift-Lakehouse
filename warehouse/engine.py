"""
DuckDB query engine for Iceberg Gold layer.

Reads Iceberg tables from S3 WITHOUT a catalog (no Hive/REST/Nessie).
To avoid the per-query metadata scan bottleneck, all Gold tables are
registered as DuckDB views at startup — metadata is resolved ONCE.

DuckDB iceberg extension read path (per table):
  S3 metadata.json → manifest-list.avro → manifest.avro → data.parquet

By creating views at init, this 4+ round-trip chain happens once,
not on every query. Subsequent queries hit the cached view metadata.
"""

import duckdb
import hashlib
import time
import logging
from typing import Dict, Any, List, Optional
import os

from .cache.memory_cache import _cache, clear_cache

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """DuckDB engine for querying Iceberg Gold tables (read-only).

    On init:
      1. Configure DuckDB tuning (memory, threads)
      2. Install iceberg extension + S3 credentials
      3. Register all Gold tables as views (iceberg_scan → view)

    Queries hit the views, not raw S3 paths — no per-query metadata scan.
    """

    def __init__(
        self,
        iceberg_path: str = None,
        cache_ttl: int = 300,
        gold_tables: Dict[str, str] = None,
        row_limit: int = 10_000,
    ):
        """
        Initialize DuckDB connection with Iceberg views.

        Args:
            iceberg_path: S3 path to Gold warehouse root (unused if gold_tables provided).
            cache_ttl: Cache time-to-live in seconds (default 300 = 5 min).
            gold_tables: Dict of {view_name: s3_iceberg_path}. If None, uses config.
            row_limit: Max rows returned per query (prevents OOM).
        """
        self.cache_ttl = cache_ttl
        self.row_limit = row_limit
        self.gold_tables = gold_tables or self._load_gold_tables()
        self.registered_views: List[str] = []

        # --- DuckDB connection with tuning ---
        self.conn = duckdb.connect(database=":memory:", read_only=False)
        self._apply_tuning()

        # --- Iceberg extension + S3 auth ---
        self._setup_iceberg()

        # --- Register views (resolve metadata ONCE) ---
        self._register_views()

    @staticmethod
    def _load_gold_tables() -> Dict[str, str]:
        """Load Gold table paths from config."""
        try:
            from config.instacart_config import GOLD_ICEBERG_TABLES, DUCKDB_DEFAULT_ROW_LIMIT
            return GOLD_ICEBERG_TABLES
        except ImportError:
            logger.warning("config.instacart_config not found — no Gold tables registered")
            return {}

    def _apply_tuning(self):
        """Apply DuckDB performance tuning from config."""
        try:
            from config.instacart_config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS
        except ImportError:
            DUCKDB_MEMORY_LIMIT = "2GB"
            DUCKDB_THREADS = 4

        self.conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        self.conn.execute(f"SET threads={DUCKDB_THREADS}")
        # Enable parallel execution for large scans
        self.conn.execute("SET preserve_insertion_order=false")
        logger.info(f"DuckDB tuning applied: memory={DUCKDB_MEMORY_LIMIT}, threads={DUCKDB_THREADS}")

    def _setup_iceberg(self):
        """Install iceberg extension and configure S3 credentials."""
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")

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
            logger.info("S3 credentials configured for DuckDB iceberg")
        else:
            logger.warning("No AWS credentials found — S3 reads will fail unless using local files")

    def _register_views(self):
        """
        Register each Gold Iceberg table as a DuckDB view.

        This resolves the Iceberg metadata (metadata.json → manifest-list → manifest)
        ONCE at startup. Subsequent queries against these views skip the S3 metadata
        walk and go straight to reading Parquet data files.

        Without this, EVERY query would do:
          GET metadata.json → GET snap.avro → GET manifest.avro → GET data.parquet
        With views, only the data files are fetched per query.
        """
        for table_name, s3_path in self.gold_tables.items():
            view_name = f"gold_{table_name}"
            try:
                self.conn.execute(f"""
                    CREATE VIEW {view_name} AS
                    SELECT * FROM iceberg_scan('{s3_path}')
                """)
                self.registered_views.append(view_name)
                logger.info(f"Registered Iceberg view: {view_name} → {s3_path}")
            except Exception as e:
                # Log but don't crash — table might not exist yet (first run)
                logger.warning(f"Failed to register view {view_name}: {e}")

        if self.registered_views:
            logger.info(f"Registered {len(self.registered_views)} Iceberg views")
        else:
            logger.warning("No Iceberg views registered — Gold tables may not exist on S3 yet")

    def refresh_views(self):
        """
        Re-register views after Iceberg table updates (e.g., after dbt run).
        Call this when new snapshots are written to S3.
        """
        # Drop existing views
        for view_name in self.registered_views:
            try:
                self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            except Exception:
                pass
        self.registered_views.clear()

        # Re-register
        self._register_views()
        logger.info("Iceberg views refreshed")

    def query(self, sql: str) -> Dict[str, Any]:
        """
        Execute a read-only SQL query and return results.

        Results are cached in-process (TTL-based). Repeated identical queries
        within the TTL window return the cached result with ``cache_hit: True``.

        A row limit is enforced (default 10,000) to prevent OOM from unbounded
        SELECT * on large tables.

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

        # --- Execute query with row limit ---
        # Wrap the query to enforce row limit if not already present
        limited_sql = self._apply_row_limit(sql)

        start_time = time.time()
        try:
            result = self.conn.execute(limited_sql).fetchdf()
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

    def _apply_row_limit(self, sql: str) -> str:
        """
        Wrap SQL with LIMIT if not already present.
        Prevents OOM from unbounded SELECT * on large tables.
        """
        stripped = sql.strip().rstrip(";").upper()
        if "LIMIT" not in stripped:
            return f"{sql.strip().rstrip(';')} LIMIT {self.row_limit}"
        return sql

    def close(self):
        """Close DuckDB connection and clear the in-process cache."""
        clear_cache()
        self.conn.close()

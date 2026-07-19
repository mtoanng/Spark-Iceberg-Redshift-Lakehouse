"""Read-only DuckDB consumer for the published Gold contract."""

from .duckdb_consumer import DuckDBGoldConsumer, QueryName, QueryResult

__all__ = ["DuckDBGoldConsumer", "QueryName", "QueryResult"]

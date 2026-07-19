"""Fixed-query DuckDB consumer for the six-table Gold contract.

The class deliberately has no method that accepts SQL from a caller. Production
table registration creates session-scoped views over fixed Gold Iceberg tables;
it never copies or writes canonical lakehouse data.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from typing import Any, Mapping, Sequence

import duckdb


class QueryName(str, Enum):
    HOURLY_PICKUPS_BY_ZONE = "hourly_pickups_by_zone"
    OPERATOR_TRIP_COUNT_AVERAGE_FARE = "operator_trip_count_average_fare"
    TOP_PICKUP_ZONES = "top_pickup_zones"
    FARE_DRIVER_PAY_RECONCILIATION = "fare_driver_pay_reconciliation"
    EXPLAIN_FILTERED_TRIPS = "explain_filtered_trips"


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


GOLD_TABLES = frozenset(
    {
        "dim_date",
        "dim_operator",
        "dim_zone",
        "fct_trips",
        "mart_hourly_zone_demand",
        "mart_operator_metrics",
    }
)


class DuckDBGoldConsumer:
    """Run only the named analytical queries shipped with this package."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, *, owns_connection: bool = False):
        self._connection = connection
        self._owns_connection = owns_connection

    @classmethod
    def from_read_only_database(cls, database_path: str) -> "DuckDBGoldConsumer":
        """Open an existing DuckDB catalog/cache without write permission."""

        connection = duckdb.connect(database=database_path, read_only=True)
        return cls(connection, owns_connection=True)

    @classmethod
    def from_iceberg_locations(
        cls, table_locations: Mapping[str, str]
    ) -> "DuckDBGoldConsumer":
        """Create temporary views over the six published S3 Iceberg locations.

        The Iceberg extension must already be installed. AWS authentication is
        delegated to DuckDB's configured credential chain. This path is intended
        for cloud smoke tests; local tests inject deterministic Gold fixtures.
        """

        supplied = set(table_locations)
        if supplied != GOLD_TABLES:
            missing = sorted(GOLD_TABLES - supplied)
            extra = sorted(supplied - GOLD_TABLES)
            raise ValueError(f"Gold table contract mismatch; missing={missing}, extra={extra}")

        for location in table_locations.values():
            _validate_s3_location(location)

        connection = duckdb.connect(database=":memory:")
        try:
            connection.execute("LOAD iceberg")
            connection.execute("CREATE SCHEMA gold")
            for table_name in sorted(GOLD_TABLES):
                location = table_locations[table_name]
                connection.execute(
                    f"CREATE VIEW gold.{table_name} AS "
                    f"SELECT * FROM iceberg_scan('{location}')"
                )
        except Exception:
            connection.close()
            raise
        return cls(connection, owns_connection=True)

    def run(
        self,
        query_name: QueryName,
        parameters: Mapping[str, Any] | None = None,
    ) -> QueryResult:
        """Execute one bundled query selected by its enum value."""

        if not isinstance(query_name, QueryName):
            raise TypeError("query_name must be a QueryName; arbitrary SQL is not accepted")

        query_path = files("consumer.queries").joinpath(f"{query_name.value}.sql")
        sql = query_path.read_text(encoding="utf-8")
        cursor = self._connection.execute(sql, dict(parameters or {}))
        columns = tuple(item[0] for item in cursor.description)
        return QueryResult(columns=columns, rows=tuple(cursor.fetchall()))

    def close(self) -> None:
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> "DuckDBGoldConsumer":
        return self

    def __exit__(self, *_: Sequence[object]) -> None:
        self.close()


def _validate_s3_location(location: str) -> None:
    if not location.startswith("s3://") or any(char in location for char in ("'", ";", "\n", "\r")):
        raise ValueError("Iceberg table locations must be safe s3:// URIs")

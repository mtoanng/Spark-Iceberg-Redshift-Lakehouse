"""Small deterministic Gold contract used by DuckDB tests."""

from __future__ import annotations

import duckdb


def create_gold_fixture(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA gold")
    connection.execute(
        """
        CREATE TABLE gold.dim_zone (
            zone_id INTEGER,
            borough VARCHAR,
            zone_name VARCHAR,
            service_zone VARCHAR
        );
        INSERT INTO gold.dim_zone VALUES
            (1, 'Queens', 'Astoria', 'Boro Zone'),
            (2, 'Manhattan', 'Midtown', 'Yellow Zone'),
            (3, 'Brooklyn', 'Downtown Brooklyn', 'Boro Zone');

        CREATE TABLE gold.mart_hourly_zone_demand (
            pickup_date_key INTEGER,
            pickup_hour INTEGER,
            pickup_zone_id INTEGER,
            trip_count BIGINT
        );
        INSERT INTO gold.mart_hourly_zone_demand VALUES
            (20240115, 8, 1, 2),
            (20240115, 8, 2, 1),
            (20240115, 9, 3, 1);

        CREATE TABLE gold.mart_operator_metrics (
            source_year INTEGER,
            source_month INTEGER,
            operator_code VARCHAR,
            trip_count BIGINT,
            average_passenger_fare DECIMAL(18, 2)
        );
        INSERT INTO gold.mart_operator_metrics VALUES
            (2024, 1, 'HV0003', 3, 20.00),
            (2024, 1, 'HV0005', 1, 12.00),
            (2024, 2, 'HV0003', 1, 18.00);

        CREATE TABLE gold.fct_trips (
            trip_id VARCHAR,
            operator_code VARCHAR,
            pickup_datetime TIMESTAMP,
            pickup_zone_id INTEGER,
            passenger_fare DECIMAL(18, 2),
            driver_pay DECIMAL(18, 2),
            source_year INTEGER,
            source_month INTEGER
        );
        INSERT INTO gold.fct_trips VALUES
            ('trip-1', 'HV0003', TIMESTAMP '2024-01-15 08:00:00', 1, 20.00, 14.00, 2024, 1),
            ('trip-2', 'HV0003', TIMESTAMP '2024-01-15 08:10:00', 1, 25.00, 17.00, 2024, 1),
            ('trip-3', 'HV0003', TIMESTAMP '2024-01-15 08:20:00', 2, 15.00, 10.00, 2024, 1),
            ('trip-4', 'HV0005', TIMESTAMP '2024-01-15 09:00:00', 3, 12.00, 8.00, 2024, 1),
            ('trip-5', 'HV0003', TIMESTAMP '2024-02-01 08:00:00', 2, 18.00, 12.00, 2024, 2);
        """
    )

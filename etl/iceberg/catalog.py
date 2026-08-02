"""Credential-free Iceberg DDL definitions executed by EMR Spark."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    namespace: str
    name: str
    columns: tuple[tuple[str, str], ...]
    partitioned_by: tuple[str, ...] = ()

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}"


BRONZE_SOURCE_COLUMNS = (
    ("hvfhs_license_num", "STRING"),
    ("dispatching_base_num", "STRING"),
    ("originating_base_num", "STRING"),
    ("request_datetime", "TIMESTAMP"),
    ("on_scene_datetime", "TIMESTAMP"),
    ("pickup_datetime", "TIMESTAMP"),
    ("dropoff_datetime", "TIMESTAMP"),
    ("PULocationID", "INT"),
    ("DOLocationID", "INT"),
    ("trip_miles", "DOUBLE"),
    ("trip_time", "BIGINT"),
    ("base_passenger_fare", "DOUBLE"),
    ("tolls", "DOUBLE"),
    ("bcf", "DOUBLE"),
    ("sales_tax", "DOUBLE"),
    ("congestion_surcharge", "DOUBLE"),
    ("airport_fee", "DOUBLE"),
    ("tips", "DOUBLE"),
    ("driver_pay", "DOUBLE"),
    ("shared_request_flag", "STRING"),
    ("shared_match_flag", "STRING"),
    ("access_a_ride_flag", "STRING"),
    ("wav_request_flag", "STRING"),
    ("wav_match_flag", "STRING"),
)

INGESTION_COLUMNS = (
    ("_source_uri", "STRING"),
    ("_source_file", "STRING"),
    ("_source_year", "INT"),
    ("_source_month", "INT"),
    ("_source_checksum", "STRING"),
    ("_ingestion_run_id", "STRING"),
    ("_ingested_at", "TIMESTAMP"),
)

IDENTITY_COLUMNS = (
    ("row_id", "STRING"),
    ("business_trip_key", "STRING"),
    ("identity_policy_version", "STRING"),
)

SILVER_COLUMNS = (
    ("row_id", "STRING"),
    ("business_trip_key", "STRING"),
    ("identity_policy_version", "STRING"),
    ("operator_code", "STRING"),
    ("request_datetime", "TIMESTAMP"),
    ("pickup_datetime", "TIMESTAMP"),
    ("dropoff_datetime", "TIMESTAMP"),
    ("pickup_zone_id", "INT"),
    ("dropoff_zone_id", "INT"),
    ("trip_miles", "DOUBLE"),
    ("trip_time_seconds", "BIGINT"),
    ("passenger_fare", "DOUBLE"),
    ("tolls", "DOUBLE"),
    ("sales_tax", "DOUBLE"),
    ("tips", "DOUBLE"),
    ("driver_pay", "DOUBLE"),
    ("shared_request_flag", "STRING"),
    ("shared_match_flag", "STRING"),
    ("source_year", "INT"),
    ("source_month", "INT"),
    ("ingestion_run_id", "STRING"),
    ("trip_duration_minutes", "DOUBLE"),
    ("pickup_date", "DATE"),
    ("pickup_hour", "INT"),
)

TABLE_SPECS = (
    TableSpec(
        "bronze",
        "bronze_hvfhs_trips",
        BRONZE_SOURCE_COLUMNS + INGESTION_COLUMNS + IDENTITY_COLUMNS,
        ("_source_year", "_source_month"),
    ),
    TableSpec(
        "bronze",
        "bronze_taxi_zones",
        (
            ("LocationID", "INT"),
            ("Borough", "STRING"),
            ("Zone", "STRING"),
            ("service_zone", "STRING"),
            ("_source_uri", "STRING"),
            ("_source_checksum", "STRING"),
            ("_ingested_at", "TIMESTAMP"),
        ),
    ),
    TableSpec(
        "silver", "silver_trips", SILVER_COLUMNS, ("source_year", "source_month")
    ),
    TableSpec(
        "silver",
        "quarantine_trips",
        BRONZE_SOURCE_COLUMNS
        + INGESTION_COLUMNS
        + IDENTITY_COLUMNS
        + (
            ("pickup_zone_id", "INT"),
            ("dropoff_zone_id", "INT"),
            ("reason_code", "STRING"),
        ),
        ("_source_year", "_source_month"),
    ),
    TableSpec(
        "ops",
        "source_run_manifest",
        (
            ("source_uri", "STRING"),
            ("source_checksum", "STRING"),
            ("source_size_bytes", "BIGINT"),
            ("source_year", "INT"),
            ("source_month", "INT"),
            ("ingestion_run_id", "STRING"),
            ("identity_policy_version", "STRING"),
            ("run_status", "STRING"),
            ("first_seen_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
            ("bronze_row_count", "BIGINT"),
            ("silver_row_count", "BIGINT"),
            ("quarantine_row_count", "BIGINT"),
            ("bronze_snapshot_id", "STRING"),
            ("silver_snapshot_id", "STRING"),
            ("quarantine_snapshot_id", "STRING"),
            ("failure_stage", "STRING"),
            ("failure_message", "STRING"),
            ("completed_at", "TIMESTAMP"),
        ),
        ("source_year", "source_month"),
    ),
)


def schema_evolution_ddl(
    *,
    catalog: str = "glue_catalog",
    bronze_database: str = "bronze",
    silver_database: str = "silver",
) -> tuple[str, ...]:
    """Return only the approved 2025 nullable-column evolution."""

    return (
        f"ALTER TABLE {catalog}.{bronze_database}.bronze_hvfhs_trips "
        "ADD COLUMN cbd_congestion_fee DOUBLE",
        f"ALTER TABLE {catalog}.{silver_database}.silver_trips "
        "ADD COLUMN cbd_congestion_fee DOUBLE",
        f"ALTER TABLE {catalog}.{silver_database}.quarantine_trips "
        "ADD COLUMN cbd_congestion_fee DOUBLE",
    )


def namespace_ddl(namespace: str, *, catalog: str = "glue_catalog") -> str:
    return f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}"


def table_ddl(
    spec: TableSpec, warehouse_uri: str, *, catalog: str = "glue_catalog"
) -> str:
    location = f"{warehouse_uri.rstrip('/')}/{spec.namespace}/{spec.name}"
    column_sql = ",\n  ".join(f"{name} {data_type}" for name, data_type in spec.columns)
    partition_sql = ""
    if spec.partitioned_by:
        partition_sql = f"\nPARTITIONED BY ({', '.join(spec.partitioned_by)})"
    return (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{spec.identifier} (\n"
        f"  {column_sql}\n"
        f") USING iceberg"
        f"{partition_sql}\n"
        f"LOCATION '{location}'\n"
        "TBLPROPERTIES ('format-version'='2', "
        "'write.parquet.compression-codec'='snappy')"
    )

"""Credential-independent checks for Phase 3 Iceberg DDL contracts."""

from etl.iceberg.catalog import TABLE_SPECS, namespace_ddl, table_ddl


def test_locked_bronze_and_silver_tables_are_defined() -> None:
    assert {spec.identifier for spec in TABLE_SPECS} == {
        "bronze.bronze_hvfhs_trips",
        "bronze.bronze_taxi_zones",
        "silver.silver_trips",
        "silver.quarantine_trips",
    }


def test_bronze_trip_table_is_source_faithful_and_partitioned_by_month() -> None:
    spec = next(spec for spec in TABLE_SPECS if spec.name == "bronze_hvfhs_trips")
    columns = dict(spec.columns)
    assert columns["hvfhs_license_num"] == "STRING"
    assert columns["pickup_datetime"] == "TIMESTAMP"
    assert columns["_source_checksum"] == "STRING"
    assert columns["_ingestion_run_id"] == "STRING"
    assert spec.partitioned_by == ("_source_year", "_source_month")


def test_silver_table_matches_trip_contract() -> None:
    spec = next(spec for spec in TABLE_SPECS if spec.name == "silver_trips")
    columns = dict(spec.columns)
    assert columns["trip_id"] == "STRING"
    assert columns["pickup_date"] == "DATE"
    assert columns["trip_duration_minutes"] == "DOUBLE"
    assert "reason_code" not in columns


def test_ddl_targets_glue_iceberg_v2_under_bounded_location() -> None:
    ddl = table_ddl(TABLE_SPECS[0], "s3://example-bucket/warehouse/")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS glue_catalog.bronze.bronze_hvfhs_trips")
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (_source_year, _source_month)" in ddl
    assert "LOCATION 's3://example-bucket/warehouse/bronze/bronze_hvfhs_trips'" in ddl
    assert "'format-version'='2'" in ddl
    assert namespace_ddl("silver") == "CREATE NAMESPACE IF NOT EXISTS glue_catalog.silver"

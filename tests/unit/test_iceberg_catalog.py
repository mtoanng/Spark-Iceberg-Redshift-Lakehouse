"""Credential-independent checks for Phase 3 Iceberg DDL contracts."""

from etl.iceberg.catalog import (
    TABLE_SPECS,
    namespace_ddl,
    schema_evolution_ddl,
    table_ddl,
)


def test_locked_bronze_and_silver_tables_are_defined() -> None:
    assert {spec.identifier for spec in TABLE_SPECS} == {
        "bronze.bronze_hvfhs_trips",
        "bronze.bronze_taxi_zones",
        "silver.silver_trips",
        "silver.quarantine_trips",
        "ops.source_run_manifest",
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
    assert columns["row_id"] == "STRING"
    assert columns["business_trip_key"] == "STRING"
    assert columns["identity_policy_version"] == "STRING"
    assert columns["pickup_date"] == "DATE"
    assert columns["trip_duration_minutes"] == "DOUBLE"
    assert "reason_code" not in columns


def test_ddl_targets_glue_iceberg_v2_under_bounded_location() -> None:
    ddl = table_ddl(TABLE_SPECS[0], "s3://example-bucket/warehouse/")
    assert ddl.startswith(
        "CREATE TABLE IF NOT EXISTS glue_catalog.bronze.bronze_hvfhs_trips"
    )
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (_source_year, _source_month)" in ddl
    assert "LOCATION 's3://example-bucket/warehouse/bronze/bronze_hvfhs_trips'" in ddl
    assert "'format-version'='2'" in ddl
    assert (
        namespace_ddl("silver") == "CREATE NAMESPACE IF NOT EXISTS glue_catalog.silver"
    )


def test_manifest_table_persists_source_identity_status_and_reconciliation_counts() -> (
    None
):
    spec = next(
        spec for spec in TABLE_SPECS if spec.identifier == "ops.source_run_manifest"
    )
    columns = dict(spec.columns)
    for column in (
        "source_uri",
        "source_checksum",
        "source_size_bytes",
        "ingestion_run_id",
        "identity_policy_version",
        "run_status",
        "bronze_row_count",
        "silver_row_count",
        "quarantine_row_count",
        "failure_message",
        "bronze_snapshot_id",
        "silver_snapshot_id",
        "quarantine_snapshot_id",
    ):
        assert column in columns
    assert "publication_manifest_uri" not in columns
    assert "publication_status" not in columns
    assert spec.partitioned_by == ("source_year", "source_month")


def test_only_approved_2025_nullable_column_evolution_is_declared() -> None:
    statements = schema_evolution_ddl()
    assert len(statements) == 3
    assert all("ADD COLUMN cbd_congestion_fee DOUBLE" in ddl for ddl in statements)
    assert not any(
        token in "\n".join(statements).lower()
        for token in ("partition", "expire", "orphan", "compact")
    )

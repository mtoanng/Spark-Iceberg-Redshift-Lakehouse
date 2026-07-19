"""Fixture tests for Phase 2 Bronze metadata and Silver quarantine behavior."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from etl.sources.nyc_hvfhs import SourceFile
from etl.transforms.nyc_hvfhs import bronze_records, load_zone_ids, reconcile, transform_silver


FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "nyc_hvfhs"
SOURCE = SourceFile(
    source_year=2024,
    source_month=1,
    source_uri="s3://landing/fhvhv_tripdata_2024-01.parquet",
    source_checksum="fixture-checksum",
    source_size_bytes=2689,
)
RUN_ID = "fhvhv-2024-01-fixture"
INGESTED_AT = datetime(2026, 7, 19, 0, 0, tzinfo=timezone.utc)


def _records() -> list[dict]:
    return json.loads((FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json").read_text(encoding="utf-8"))["records"]


def _bronze():
    return bronze_records(_records(), SOURCE, RUN_ID, ingested_at=INGESTED_AT)


def test_bronze_preserves_source_columns_and_adds_only_metadata() -> None:
    source_record = _records()[0]
    bronze = _bronze()
    bronze_record = bronze.rows[0]
    for column, value in source_record.items():
        assert bronze_record[column] == value
    assert bronze_record["_source_file"] == "fhvhv_tripdata_2024-01.parquet"
    assert bronze_record["_source_year"] == 2024
    assert bronze_record["_source_month"] == 1
    assert bronze_record["_source_checksum"] == "fixture-checksum"
    assert bronze_record["_ingestion_run_id"] == RUN_ID
    assert bronze_record["_ingested_at"] == INGESTED_AT


def test_silver_validates_deduplicates_and_quarantines_fixture_rows() -> None:
    bronze = _bronze()
    silver = transform_silver(bronze.rows, load_zone_ids(FIXTURE_DIR / "taxi_zone_lookup.fixture.csv"))
    assert len(silver.silver_rows) == 1
    assert silver.silver_rows[0]["trip_duration_minutes"] == 20
    assert silver.silver_rows[0]["pickup_date"] == "2024-01-15"
    assert silver.silver_rows[0]["pickup_hour"] == 8
    assert {row["reason_code"] for row in silver.quarantine_rows} == {
        "DUPLICATE_TRIP_ID",
        "DROPOFF_BEFORE_PICKUP",
        "UNKNOWN_PICKUP_ZONE",
        "NEGATIVE_DRIVER_PAY",
    }


def test_reconciliation_explains_every_bronze_row() -> None:
    bronze = _bronze()
    silver = transform_silver(bronze.rows, load_zone_ids(FIXTURE_DIR / "taxi_zone_lookup.fixture.csv"))
    counts = reconcile(bronze, silver)
    assert (counts.bronze_count, counts.silver_count, counts.quarantine_count) == (5, 1, 4)
    assert counts.explained


def test_rerun_does_not_add_existing_canonical_trip() -> None:
    bronze = _bronze()
    zone_ids = load_zone_ids(FIXTURE_DIR / "taxi_zone_lookup.fixture.csv")
    first_run = transform_silver(bronze.rows, zone_ids)
    second_run = transform_silver(
        bronze.rows,
        zone_ids,
        existing_trip_ids={row["trip_id"] for row in first_run.silver_rows},
    )
    assert len(first_run.silver_rows) == 1
    assert len(second_run.silver_rows) == 0
    assert second_run.quarantine_rows[0]["reason_code"] == "DUPLICATE_TRIP_ID"

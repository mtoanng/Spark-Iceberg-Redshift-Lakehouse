"""Unit tests for the Phase 1 NYC HVFHV source and fixture contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl.sources.nyc_hvfhs import (
    CBD_CONGESTION_FEE_COLUMN,
    SourceContractError,
    SourceFile,
    canonical_row_id,
    inspect_local_source,
    monthly_trip_uri,
    required_trip_columns,
    stable_run_id,
    validate_trip_schema,
    validate_landed_source,
)


FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "nyc_hvfhs"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_official_monthly_uri_is_deterministic() -> None:
    assert monthly_trip_uri(2024, 1) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet"
    )


def test_landed_source_requires_exact_s3_month_checksum_and_size() -> None:
    valid = SourceFile(
        2024,
        1,
        "s3://bucket/landing/fhvhv_tripdata_2024-01.parquet",
        "a" * 64,
        123,
    )
    validate_landed_source(valid)
    with pytest.raises(SourceContractError, match="s3://"):
        validate_landed_source(
            SourceFile(2024, 1, monthly_trip_uri(2024, 1), "a" * 64, 123)
        )
    with pytest.raises(SourceContractError, match="SHA-256"):
        validate_landed_source(
            SourceFile(2024, 1, valid.source_uri, "not-a-checksum", 123)
        )
    with pytest.raises(SourceContractError, match="lowercase"):
        validate_landed_source(SourceFile(2024, 1, valid.source_uri, "A" * 64, 123))


def test_2024_fixture_is_source_shaped_and_contains_required_scenarios() -> None:
    fixture = _fixture("fhvhv_tripdata_2024-01.fixture.json")
    records = fixture["records"]
    assert len(records) == 5
    validate_trip_schema(records[0].keys(), fixture["source_year"])
    assert canonical_row_id(records[0], 2024) == canonical_row_id(records[1], 2024)
    assert records[2]["dropoff_datetime"] < records[2]["pickup_datetime"]
    assert records[3]["PULocationID"] == 999
    assert records[4]["driver_pay"] < 0


def test_2025_fixture_requires_and_includes_congestion_fee() -> None:
    fixture = _fixture("fhvhv_tripdata_2025-01.fixture.json")
    record = fixture["records"][0]
    assert CBD_CONGESTION_FEE_COLUMN in required_trip_columns(2025)
    validate_trip_schema(record.keys(), fixture["source_year"])


def test_schema_rejects_missing_required_column() -> None:
    columns = required_trip_columns(2024) - {"driver_pay"}
    with pytest.raises(SourceContractError, match="driver_pay"):
        validate_trip_schema(columns, 2024)


def test_local_source_identity_and_run_id_are_deterministic() -> None:
    source = inspect_local_source(
        FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json", 2024, 1
    )
    same_source = inspect_local_source(
        FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json", 2024, 1
    )
    assert source.source_checksum == same_source.source_checksum
    assert source.source_size_bytes > 0
    assert stable_run_id(source) == stable_run_id(same_source)
    assert stable_run_id(source) != stable_run_id(
        SourceFile(
            source.source_year,
            source.source_month,
            "s3://another-bucket/landing/fhvhv_tripdata_2024-01.parquet",
            source.source_checksum,
            source.source_size_bytes,
        )
    )


@pytest.mark.parametrize(
    ("filename", "year", "month", "expected_size", "expected_checksum"),
    [
        (
            "fhvhv_tripdata_2024-01.fixture.json",
            2024,
            1,
            3894,
            "3e905cacbb8bf06438b4a252b4eb3c5ab730f834776b1710669e4f5c9c241a90",
        ),
        (
            "fhvhv_tripdata_2025-01.fixture.json",
            2025,
            1,
            866,
            "c4b1c80369beb12db5025664dd67a3317c4bacf25035bc561a3b1f23412b0d5b",
        ),
        (
            "taxi_zone_lookup.fixture.csv",
            2024,
            1,
            135,
            "276b2b2febced718bad06d370397eec5c8a13840c6b3d2daccf28bd08bbd1023",
        ),
    ],
)
def test_fixture_identity_is_pinned(
    filename: str, year: int, month: int, expected_size: int, expected_checksum: str
) -> None:
    source = inspect_local_source(FIXTURE_DIR / filename, year, month)
    assert source.source_size_bytes == expected_size
    assert source.source_checksum == expected_checksum

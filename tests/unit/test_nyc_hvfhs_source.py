"""Unit tests for the Phase 1 NYC HVFHV source and fixture contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from etl.sources.nyc_hvfhs import (
    CBD_CONGESTION_FEE_COLUMN,
    ManifestAction,
    SourceContractError,
    SourceFile,
    SourceManifestEntry,
    inspect_local_source,
    canonical_trip_id,
    manifest_decision,
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


def test_2024_fixture_is_source_shaped_and_contains_required_scenarios() -> None:
    fixture = _fixture("fhvhv_tripdata_2024-01.fixture.json")
    records = fixture["records"]
    assert len(records) == 5
    validate_trip_schema(records[0].keys(), fixture["source_year"])
    assert canonical_trip_id(records[0]) == canonical_trip_id(records[1])  # duplicate
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
    source = inspect_local_source(FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json", 2024, 1)
    same_source = inspect_local_source(FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json", 2024, 1)
    assert source.source_checksum == same_source.source_checksum
    assert source.source_size_bytes > 0
    assert stable_run_id(source) == stable_run_id(same_source)


@pytest.mark.parametrize(
    ("filename", "year", "month", "expected_size", "expected_checksum"),
    [
        (
            "fhvhv_tripdata_2024-01.fixture.json",
            2024,
            1,
            2689,
            "77be47c88c1ae823be38bebf3b3c30d3b564b87f05023d270270caabced64b83",
        ),
        (
            "fhvhv_tripdata_2025-01.fixture.json",
            2025,
            1,
            625,
            "652ad753e710e14b18f2020f098b93f224892f564537dc1868df6f2295cc9f84",
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


def test_processed_identical_source_is_skipped_without_force() -> None:
    source = inspect_local_source(FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json", 2024, 1)
    entry = SourceManifestEntry.discovered(source, datetime(2026, 1, 1, tzinfo=timezone.utc)).processed(
        stable_run_id(source), datetime(2026, 1, 2, tzinfo=timezone.utc)
    )
    assert manifest_decision(entry, source) is ManifestAction.SKIP_IDENTICAL_PROCESSED_SOURCE
    assert manifest_decision(entry, source, force=True) is ManifestAction.PROCESS_FORCED_RETRY


def test_changed_checksum_is_blocked_even_when_forced(tmp_path: Path) -> None:
    fixture_path = FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json"
    original = inspect_local_source(fixture_path, 2024, 1, source_uri="fixture://2024-01")
    entry = SourceManifestEntry.discovered(original).processed(stable_run_id(original))
    changed_path = tmp_path / fixture_path.name
    changed_path.write_bytes(fixture_path.read_bytes() + b"\nchanged")
    changed = inspect_local_source(changed_path, 2024, 1, source_uri="fixture://2024-01")
    assert manifest_decision(entry, changed, force=True) is ManifestAction.BLOCK_CHANGED_CHECKSUM

"""Credential-independent closure Phase A manifest and GE contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from etl.manifests.nyc_hvfhs import RunStatus, SourceRunManifest, retry_is_safe
from etl.quality.nyc_hvfhs_ge import evaluate_fixture_ge_checkpoint, expectation_suite
from etl.sources.nyc_hvfhs import SourceContractError, SourceFile
from etl.transforms.nyc_hvfhs import bronze_records, load_zone_ids, transform_silver


FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "nyc_hvfhs"
SOURCE = SourceFile(
    2024, 1, "fixture:///fhvhv_tripdata_2024-01.parquet", "fixture-checksum", 2689
)
NOW = datetime(2026, 7, 21, tzinfo=timezone.utc)


def _bronze():
    records = json.loads(
        (FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json").read_text(
            encoding="utf-8"
        )
    )["records"]
    return bronze_records(records, SOURCE, "fhvhv-2024-01-fixture", ingested_at=NOW)


def test_durable_manifest_requires_ge_before_canonical_silver_and_persists_counts() -> (
    None
):
    manifest = SourceRunManifest.discovered(SOURCE, NOW).bronze_published(5, at=NOW)
    with pytest.raises(SourceContractError, match="Great Expectations"):
        manifest.silver_published(1, 4, at=NOW)
    ge_passed = manifest.ge_result(
        blocking_success=True,
        result_uri="s3://results/ge.json",
        result_summary="{}",
        at=NOW,
    )
    complete = ge_passed.silver_published(1, 4, at=NOW)
    assert complete.run_status is RunStatus.SILVER_PUBLISHED
    assert (
        complete.bronze_row_count,
        complete.silver_row_count,
        complete.quarantine_row_count,
    ) == (5, 1, 4)
    assert complete.validation_status == "passed"


def test_blocking_ge_failure_persists_failure_and_prevents_silver() -> None:
    blocked = (
        SourceRunManifest.discovered(SOURCE, NOW)
        .bronze_published(1, at=NOW)
        .ge_result(blocking_success=False, result_uri=None, at=NOW)
    )
    assert blocked.run_status is RunStatus.GE_BLOCKED
    assert blocked.failure_stage == "great_expectations"
    with pytest.raises(SourceContractError, match="requires a passed"):
        blocked.silver_published(1, 0, at=NOW)


def test_ge_is_structural_while_silver_preserves_quarantine_evidence() -> None:
    bronze = _bronze()
    zones = load_zone_ids(FIXTURE_DIR / "taxi_zone_lookup.fixture.csv")
    ge_result = evaluate_fixture_ge_checkpoint(bronze.rows, zones)
    silver = transform_silver(bronze.rows, zones)
    assert ge_result.blocking_success
    assert len(silver.quarantine_rows) == 4
    assert all(row["reason_code"] for row in silver.quarantine_rows)


def test_ge_suite_uses_installed_great_expectations_configuration() -> None:
    suite = expectation_suite()
    assert suite.name == "nyc_hvfhs_bronze_pre_silver"
    assert len(suite.expectations) >= 10
    assert suite.meta["scope"] == "required_columns_non_empty_month_identity_inputs"
    assert (
        suite.meta["row_validation_owner"]
        == "etl.contracts.nyc_hvfhs_quality.reason_code"
    )


def test_retry_safety_allows_only_identical_forced_completed_source() -> None:
    complete = (
        SourceRunManifest.discovered(SOURCE, NOW)
        .bronze_published(0, at=NOW)
        .ge_result(blocking_success=True, result_uri=None, at=NOW)
        .silver_published(0, 0, at=NOW)
    )
    changed = SourceFile(
        2024, 1, SOURCE.source_uri, "changed", SOURCE.source_size_bytes
    )
    assert not retry_is_safe(complete, SOURCE, force=False)
    assert retry_is_safe(complete, SOURCE, force=True)
    assert not retry_is_safe(complete, changed, force=True)

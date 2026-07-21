"""Local contracts for Phase 5 planning, quality, and rerun behavior."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from etl.orchestration.nyc_hvfhs_runs import (
    MonthlyRunRequest,
    audit_for_source,
    sequential_backfill_requests,
)
from etl.quality.nyc_hvfhs_checkpoint import (
    QualityCheckpointError,
    evaluate_fixture_checkpoint,
)
from etl.sources.nyc_hvfhs import (
    SourceContractError,
    SourceFile,
    SourceManifestEntry,
    manifest_decision,
    stable_run_id,
)
from etl.transforms.nyc_hvfhs import (
    BronzeBatch,
    SilverBatch,
    bronze_records,
    load_zone_ids,
    transform_silver,
)


FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "nyc_hvfhs"
SOURCE = SourceFile(
    2024, 1, "fixture:///fhvhv_tripdata_2024-01.parquet", "fixture-checksum", 2689
)


def _fixture_batch() -> tuple[BronzeBatch, SilverBatch]:
    records = json.loads(
        (FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json").read_text(
            encoding="utf-8"
        )
    )["records"]
    bronze = bronze_records(records, SOURCE, stable_run_id(SOURCE))
    silver = transform_silver(
        bronze.rows, load_zone_ids(FIXTURE_DIR / "taxi_zone_lookup.fixture.csv")
    )
    return bronze, silver


def test_quality_checkpoint_accepts_reconciled_fixture() -> None:
    bronze, silver = _fixture_batch()

    result = evaluate_fixture_checkpoint(bronze, silver)

    assert (
        result.bronze_count,
        result.silver_count,
        result.quarantine_count,
        result.distinct_trip_count,
    ) == (5, 1, 4, 1)


def test_quality_checkpoint_rejects_duplicate_canonical_trip() -> None:
    bronze, silver = _fixture_batch()
    duplicated_bronze = replace(bronze, rows=(bronze.rows[0], bronze.rows[0]))
    duplicated_silver = replace(
        silver,
        silver_rows=(silver.silver_rows[0], silver.silver_rows[0]),
        quarantine_rows=(),
    )

    with pytest.raises(QualityCheckpointError, match="unique"):
        evaluate_fixture_checkpoint(duplicated_bronze, duplicated_silver)


def test_quality_checkpoint_rejects_quarantine_without_reason() -> None:
    bronze, silver = _fixture_batch()
    missing_reason = dict(silver.quarantine_rows[0])
    missing_reason["reason_code"] = ""
    broken_silver = replace(
        silver, quarantine_rows=(missing_reason, *silver.quarantine_rows[1:])
    )

    with pytest.raises(QualityCheckpointError, match="reason_code"):
        evaluate_fixture_checkpoint(bronze, broken_silver)


def test_four_month_backfill_is_sequential_and_bounded() -> None:
    requests = sequential_backfill_requests(2024, 1)

    assert [(request.year, request.month, request.force) for request in requests] == [
        (2024, 1, False),
        (2024, 2, False),
        (2024, 3, False),
        (2024, 4, False),
    ]
    with pytest.raises(SourceContractError, match="September"):
        sequential_backfill_requests(2024, 10)


def test_run_audit_binds_the_request_to_immutable_source_identity() -> None:
    audit = audit_for_source(
        MonthlyRunRequest(2024, 1, force=True),
        SOURCE,
        requested_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )

    assert audit.run_id == stable_run_id(SOURCE)
    assert audit.force is True
    assert audit.source_checksum == "fixture-checksum"


def test_identical_rerun_skips_then_force_requests_same_source_retry() -> None:
    processed = SourceManifestEntry.discovered(SOURCE).processed(stable_run_id(SOURCE))

    assert (
        manifest_decision(processed, SOURCE, force=False).value
        == "skip_identical_processed_source"
    )
    assert (
        manifest_decision(processed, SOURCE, force=True).value == "process_forced_retry"
    )

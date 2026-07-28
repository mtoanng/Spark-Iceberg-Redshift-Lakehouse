"""Local contracts for Phase 5 planning, quality, and rerun behavior."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from etl.orchestration.nyc_hvfhs_runs import (
    MonthlyRunRequest,
    audit_for_source,
    sequential_backfill_requests,
)
from etl.sources.nyc_hvfhs import SourceContractError, SourceFile, stable_run_id

SOURCE = SourceFile(
    2024, 1, "fixture:///fhvhv_tripdata_2024-01.parquet", "fixture-checksum", 2689
)


def test_four_month_backfill_is_sequential_and_bounded() -> None:
    requests = sequential_backfill_requests(2024, 1)

    assert [(request.year, request.month) for request in requests] == [
        (2024, 1),
        (2024, 2),
        (2024, 3),
        (2024, 4),
    ]
    with pytest.raises(SourceContractError, match="September"):
        sequential_backfill_requests(2024, 10)


def test_run_audit_binds_the_request_to_immutable_source_identity() -> None:
    audit = audit_for_source(
        MonthlyRunRequest(2024, 1),
        SOURCE,
        requested_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )

    assert audit.run_id == stable_run_id(SOURCE)
    assert audit.source_checksum == "fixture-checksum"

"""Great Expectations suite definition and fixture-scale checkpoint contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import great_expectations as gx

from etl.sources.nyc_hvfhs import BASE_REQUIRED_TRIP_COLUMNS
from etl.transforms.nyc_hvfhs import _reason_code


BLOCKING_EXPECTATION_NAMES = frozenset({"required_columns", "non_empty_batch"})
ROW_LEVEL_EXPECTATION_NAMES = frozenset(
    {"timestamps", "timestamp_order", "non_negative_metrics", "zone_resolution"}
)


@dataclass(frozen=True)
class GECheckpointResult:
    blocking_success: bool
    observed_invalid_row_count: int
    expectation_suite_name: str


def expectation_suite() -> gx.ExpectationSuite:
    """Return the versioned suite used by the pre-Silver checkpoint.

    Schema and non-empty checks are promotion-blocking.  Row-level checks are
    deliberately observed here and deterministically quarantined by Silver;
    they are not silently discarded by Great Expectations.
    """
    expectations = [
        gx.expectations.ExpectColumnToExist(column=name)
        for name in sorted(BASE_REQUIRED_TRIP_COLUMNS)
    ]
    return gx.ExpectationSuite(
        name="nyc_hvfhs_bronze_pre_silver",
        expectations=expectations,
        meta={
            "blocking_expectations": sorted(BLOCKING_EXPECTATION_NAMES),
            "row_level_expectations": sorted(ROW_LEVEL_EXPECTATION_NAMES),
            "quarantine_contract": "etl.transforms.nyc_hvfhs._reason_code",
        },
    )


def evaluate_fixture_ge_checkpoint(
    rows: Iterable[Mapping[str, object]], zone_ids: set[int]
) -> GECheckpointResult:
    """Evaluate the gate without starting Spark.

    This validates the installed Great Expectations suite configuration and
    applies the row-level observations using the shared Silver reason-code
    contract.  Production uses the same suite over the month-scoped Spark
    frame, then Silver writes every observed invalid row to quarantine.
    """
    materialized = [dict(row) for row in rows]
    present = set(materialized[0]) if materialized else set()
    blocking_success = bool(materialized) and BASE_REQUIRED_TRIP_COLUMNS.issubset(
        present
    )
    invalid_count = (
        sum(_reason_code(row, zone_ids) is not None for row in materialized)
        if blocking_success
        else 0
    )
    suite = expectation_suite()
    return GECheckpointResult(
        blocking_success, invalid_count, suite.name or "nyc_hvfhs_bronze_pre_silver"
    )

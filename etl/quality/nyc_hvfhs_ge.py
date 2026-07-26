"""Great Expectations suite definition and fixture-scale checkpoint contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import great_expectations as gx

from etl.contracts.nyc_hvfhs_identity import required_identity_columns
from etl.sources.nyc_hvfhs import required_trip_columns


BLOCKING_EXPECTATION_NAMES = frozenset({"required_columns", "non_empty_batch"})


@dataclass(frozen=True)
class GECheckpointResult:
    blocking_success: bool
    expectation_suite_name: str


def expectation_suite(year: int = 2024) -> gx.ExpectationSuite:
    """Return the versioned suite used by the pre-Silver checkpoint.

    Schema and non-empty checks are promotion-blocking. Row-level business
    validation belongs exclusively to Silver's deterministic quarantine.
    """
    expectations = [
        gx.expectations.ExpectColumnToExist(column=name)
        for name in sorted(
            required_trip_columns(year) | required_identity_columns(year)
        )
    ]
    return gx.ExpectationSuite(
        name="nyc_hvfhs_bronze_pre_silver",
        expectations=expectations,
        meta={
            "blocking_expectations": sorted(BLOCKING_EXPECTATION_NAMES),
            "scope": "required_columns_non_empty_month_identity_inputs",
            "row_validation_owner": "etl.contracts.nyc_hvfhs_quality.reason_code",
            "source_year": year,
        },
    )


def evaluate_fixture_ge_checkpoint(
    rows: Iterable[Mapping[str, object]], _zone_ids: set[int] | None = None
) -> GECheckpointResult:
    """Evaluate the gate without starting Spark.

    This validates the installed Great Expectations suite configuration and
    checks the same structural conditions as production without starting
    Spark. Silver separately validates every row and preserves failures.
    """
    materialized = [dict(row) for row in rows]
    year = int(materialized[0].get("_source_year", 2024)) if materialized else 2024
    present = set(materialized[0]) if materialized else set()
    structural_columns = required_trip_columns(year) | required_identity_columns(year)
    blocking_success = bool(materialized) and structural_columns.issubset(present)
    suite = expectation_suite(year)
    return GECheckpointResult(
        blocking_success, suite.name or "nyc_hvfhs_bronze_pre_silver"
    )

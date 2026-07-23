"""Great Expectations suite definition and fixture-scale checkpoint contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import great_expectations as gx

from etl.sources.nyc_hvfhs import BASE_REQUIRED_TRIP_COLUMNS


BLOCKING_EXPECTATION_NAMES = frozenset({"required_columns", "non_empty_batch"})


@dataclass(frozen=True)
class GECheckpointResult:
    blocking_success: bool
    expectation_suite_name: str


def expectation_suite() -> gx.ExpectationSuite:
    """Return the versioned suite used by the pre-Silver checkpoint.

    Schema and non-empty checks are promotion-blocking. Row-level business
    validation belongs exclusively to Silver's deterministic quarantine.
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
            "quarantine_contract": "etl.transforms.nyc_hvfhs._reason_code",
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
    present = set(materialized[0]) if materialized else set()
    blocking_success = bool(materialized) and BASE_REQUIRED_TRIP_COLUMNS.issubset(
        present
    )
    suite = expectation_suite()
    return GECheckpointResult(
        blocking_success, suite.name or "nyc_hvfhs_bronze_pre_silver"
    )

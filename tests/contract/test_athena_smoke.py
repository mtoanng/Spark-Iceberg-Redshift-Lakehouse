"""Mocked Gold-smoke verifier contract; no AWS query is made."""

from __future__ import annotations

import pytest

from athena.query_runner import AthenaQueryError, AthenaQueryResult
from athena.verify_gold import verify_gold_smoke


class FakeRunner:
    def __init__(self, result: AthenaQueryResult):
        self.result = result
        self.calls = []

    def run(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        return self.result


def _result(
    row=("4", "4", "2024-01-15 08:00:00", "2024-01-15 09:20:00")
) -> AthenaQueryResult:
    return AthenaQueryResult(
        "q-smoke",
        (
            "row_count",
            "distinct_trip_count",
            "min_pickup_datetime",
            "max_dropoff_datetime",
        ),
        (row,),
        12,
        8,
        "s3://bucket/results/q.csv",
    )


def test_gold_smoke_passes_and_uses_only_bound_parameters() -> None:
    runner = FakeRunner(_result())
    outcome = verify_gold_smoke(
        runner, year=2024, month=1, database="gold", workgroup="gold-wg"
    )
    assert outcome.row_count == 4
    assert runner.calls[0][1]["execution_parameters"] == ("2024", "1")
    assert "gold-smoke-2024-01" == runner.calls[0][1]["client_request_token"]


@pytest.mark.parametrize(
    "row, message",
    [
        (("0", "0", None, None), "empty"),
        (("4", "3", "a", "b"), "not unique"),
        (("4", "4", None, "b"), "timestamp"),
    ],
)
def test_gold_smoke_rejects_empty_duplicate_or_missing_bounds(row, message) -> None:
    with pytest.raises(AthenaQueryError, match=message):
        verify_gold_smoke(
            FakeRunner(_result(row)),
            year=2024,
            month=1,
            database="gold",
            workgroup="gold-wg",
        )

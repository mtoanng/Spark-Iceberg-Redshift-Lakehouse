"""Mocked Gold-smoke verifier contract; no AWS query is made."""

from __future__ import annotations

import pytest

from athena.query_runner import AthenaQueryError, AthenaQueryResult
from athena.verify_gold import EXPECTED_GOLD_COLUMNS, verify_gold_catalog, verify_gold_smoke


class FakeRunner:
    def __init__(self, result: AthenaQueryResult):
        self.result = result
        self.calls = []

    def run(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        return self.result


class FakeGlue:
    def __init__(self, missing_table=None, missing_column=None, extra_table=None):
        self.missing_table = missing_table
        self.missing_column = missing_column
        self.extra_table = extra_table

    def get_tables(self, DatabaseName):
        assert DatabaseName == "gold"
        tables = [
            {"Name": name}
            for name in EXPECTED_GOLD_COLUMNS
            if name != self.missing_table
        ]
        if self.extra_table:
            tables.append({"Name": self.extra_table})
        return {"TableList": tables}

    def get_table(self, DatabaseName, Name):
        columns = set(EXPECTED_GOLD_COLUMNS[Name])
        if self.missing_column and Name == "fct_trips":
            columns.discard(self.missing_column)
        return {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [{"Name": column} for column in sorted(columns)]
                }
            }
        }


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
        runner,
        year=2024,
        month=1,
        database="gold",
        workgroup="gold-wg",
        glue_client=FakeGlue(),
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


def test_gold_catalog_rejects_missing_table_or_column() -> None:
    with pytest.raises(AthenaQueryError, match="missing expected tables"):
        verify_gold_catalog(FakeGlue(missing_table="dim_zone"), database="gold")
    with pytest.raises(AthenaQueryError, match="missing columns"):
        verify_gold_catalog(
            FakeGlue(missing_column="source_month"), database="gold"
        )
    with pytest.raises(AthenaQueryError, match="out-of-scope"):
        verify_gold_catalog(FakeGlue(extra_table="stale_model"), database="gold")


def test_gold_smoke_enforces_scan_bound() -> None:
    with pytest.raises(AthenaQueryError, match="exceeding"):
        verify_gold_smoke(
            FakeRunner(_result()),
            year=2024,
            month=1,
            database="gold",
            workgroup="gold-wg",
            max_scanned_bytes=1,
        )

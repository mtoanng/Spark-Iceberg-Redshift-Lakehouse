"""Mocked contracts for the small Redshift Data API runner."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from etl.orchestration.redshift_data import RedshiftQueryError, run_query


class FakeRedshiftData:
    def __init__(
        self, states: list[str], records: list[list[dict[str, object]]] | None = None
    ) -> None:
        self.states = iter(states)
        self.records = records or [[{"longValue": 7}]]
        self.request: dict[str, object] = {}

    def execute_statement(self, **request: object) -> dict[str, str]:
        self.request = request
        return {"Id": "statement-123"}

    def describe_statement(self, **_: object) -> dict[str, object]:
        return {"Status": next(self.states), "Error": "controlled failure"}

    def get_statement_result(self, **_: object) -> dict[str, object]:
        return {"Records": self.records}


def test_runner_returns_rows_and_uses_named_parameters() -> None:
    client = FakeRedshiftData(["STARTED", "FINISHED"])
    result = run_query(
        client,
        database="lakehouse",
        workgroup_name="serverless",
        sql="SELECT :source_year",
        statement_name="test-query",
        parameters={"source_year": 2024},
        sleep=Mock(),
    )
    assert result.statement_id == "statement-123"
    assert result.rows == ((7,),)
    assert client.request["Parameters"] == [{"name": "source_year", "value": "2024"}]


@pytest.mark.parametrize("state", ["FAILED", "ABORTED"])
def test_runner_raises_on_failed_terminal_state(state: str) -> None:
    client = FakeRedshiftData([state])
    with pytest.raises(RedshiftQueryError, match=state):
        run_query(
            client,
            database="lakehouse",
            workgroup_name="serverless",
            sql="SELECT 1",
            statement_name="failed-query",
        )


def test_runner_times_out(monkeypatch) -> None:
    client = FakeRedshiftData(["STARTED", "STARTED"])
    monotonic = iter([0.0, 2.0])
    monkeypatch.setattr(
        "etl.orchestration.redshift_data.time.monotonic", lambda: next(monotonic)
    )
    with pytest.raises(RedshiftQueryError, match="timed out"):
        run_query(
            client,
            database="lakehouse",
            workgroup_name="serverless",
            sql="SELECT 1",
            statement_name="timeout-query",
            timeout_seconds=1,
            sleep=Mock(),
        )

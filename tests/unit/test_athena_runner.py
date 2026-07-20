"""Mocked contracts for the intentionally small Athena Boto3 runner."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from athena.query_runner import AthenaQueryError, AthenaQueryRunner


def _execution(state: str = "SUCCEEDED") -> dict:
    return {
        "QueryExecution": {
            "Status": {"State": state, "StateChangeReason": "controlled failure"},
            "Statistics": {
                "DataScannedInBytes": 123,
                "EngineExecutionTimeInMillis": 45,
            },
            "ResultConfiguration": {
                "OutputLocation": "s3://bucket/athena-results/query.csv"
            },
        }
    }


def _page(rows, token=None, include_header=True) -> dict:
    response = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "value"}]},
            "Rows": ([{"Data": [{"VarCharValue": "value"}]}] if include_header else [])
            + [{"Data": [{"VarCharValue": value}]} for value in rows],
        }
    }
    if token:
        response["NextToken"] = token
    return response


def test_runner_forwards_token_and_execution_parameters_and_paginates() -> None:
    client = Mock()
    client.start_query_execution.return_value = {"QueryExecutionId": "q-1"}
    client.get_query_execution.return_value = _execution()
    client.get_query_results.side_effect = [
        _page(["first"], "next"),
        _page(["second"], include_header=False),
    ]

    result = AthenaQueryRunner(client, sleep=Mock()).run(
        "SELECT ?",
        database="gold",
        workgroup="wg",
        execution_parameters=("2024",),
        client_request_token="stable-token",
    )

    assert result.rows == (("first",), ("second",))
    assert result.data_scanned_bytes == 123
    assert (
        client.start_query_execution.call_args.kwargs["ClientRequestToken"]
        == "stable-token"
    )
    assert client.start_query_execution.call_args.kwargs["ExecutionParameters"] == [
        "2024"
    ]
    assert client.get_query_results.call_args_list[1].kwargs["NextToken"] == "next"


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_runner_reports_failed_or_cancelled_query_with_query_id(state: str) -> None:
    client = Mock()
    client.start_query_execution.return_value = {"QueryExecutionId": "q-failed"}
    client.get_query_execution.return_value = _execution(state)

    with pytest.raises(AthenaQueryError, match=f"q-failed {state}"):
        AthenaQueryRunner(client).run("SELECT 1", database="gold", workgroup="wg")


def test_runner_stops_query_when_timeout_expires(monkeypatch) -> None:
    client = Mock()
    client.start_query_execution.return_value = {"QueryExecutionId": "q-timeout"}
    client.get_query_execution.return_value = _execution("RUNNING")
    monotonic = iter([0.0, 2.0])
    monkeypatch.setattr("athena.query_runner.time.monotonic", lambda: next(monotonic))

    with pytest.raises(AthenaQueryError, match="q-timeout timed out"):
        AthenaQueryRunner(client, sleep=Mock()).run(
            "SELECT 1", database="gold", workgroup="wg", timeout_seconds=1
        )
    client.stop_query_execution.assert_called_once_with(QueryExecutionId="q-timeout")

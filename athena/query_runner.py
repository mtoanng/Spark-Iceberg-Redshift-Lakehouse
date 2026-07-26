"""Small Boto3 Athena runner for the approved read-only query pack."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Sequence

import boto3


class AthenaQueryError(RuntimeError):
    """Athena did not reach a successful terminal state."""


@dataclass(frozen=True)
class AthenaQueryResult:
    query_execution_id: str
    columns: tuple[str, ...]
    rows: tuple[tuple[str | None, ...], ...]
    data_scanned_bytes: int
    engine_execution_time_ms: int
    result_location: str | None
    database: str = ""
    workgroup: str = ""
    execution_state: str = "SUCCEEDED"


class AthenaQueryRunner:
    """Run one supplied read-only query through Boto3's default credential chain."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        region_name: str | None = None,
        sleep=time.sleep,
    ):
        self._client = client or boto3.client("athena", region_name=region_name)
        self._sleep = sleep

    def run(
        self,
        sql: str,
        *,
        database: str,
        catalog: str = "AwsDataCatalog",
        workgroup: str,
        execution_parameters: Sequence[str] = (),
        client_request_token: str | None = None,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 1.0,
    ) -> AthenaQueryResult:
        if not sql.strip() or not database or not catalog or not workgroup:
            raise ValueError("sql, database, catalog, and workgroup are required.")
        request: dict[str, Any] = {
            "QueryString": sql,
            "QueryExecutionContext": {"Database": database, "Catalog": catalog},
            "WorkGroup": workgroup,
        }
        if execution_parameters:
            request["ExecutionParameters"] = list(execution_parameters)
        if client_request_token:
            request["ClientRequestToken"] = client_request_token
        query_execution_id = self._client.start_query_execution(**request)[
            "QueryExecutionId"
        ]
        deadline = time.monotonic() + timeout_seconds

        while True:
            execution = self._client.get_query_execution(
                QueryExecutionId=query_execution_id
            )["QueryExecution"]
            status = execution["Status"]
            state = status["State"]
            if state == "SUCCEEDED":
                return self._results(
                    query_execution_id,
                    execution,
                    database=database,
                    workgroup=workgroup,
                )
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get(
                    "StateChangeReason", "No Athena state reason supplied."
                )
                raise AthenaQueryError(
                    f"Athena query {query_execution_id} {state}: {reason}"
                )
            if time.monotonic() >= deadline:
                self._client.stop_query_execution(QueryExecutionId=query_execution_id)
                raise AthenaQueryError(
                    f"Athena query {query_execution_id} timed out after {timeout_seconds} seconds."
                )
            self._sleep(poll_interval_seconds)

    def _results(
        self,
        query_execution_id: str,
        execution: dict[str, Any],
        *,
        database: str,
        workgroup: str,
    ) -> AthenaQueryResult:
        columns: tuple[str, ...] = ()
        rows: list[tuple[str | None, ...]] = []
        token: str | None = None
        first_page = True
        while True:
            request = {"QueryExecutionId": query_execution_id}
            if token:
                request["NextToken"] = token
            response = self._client.get_query_results(**request)
            page = response["ResultSet"]
            if not columns:
                columns = tuple(
                    item["Name"] for item in page["ResultSetMetadata"]["ColumnInfo"]
                )
            page_rows = page.get("Rows", [])
            if first_page and page_rows:
                page_rows = page_rows[
                    1:
                ]  # Athena includes the header on the first page.
            rows.extend(
                tuple(cell.get("VarCharValue") for cell in row.get("Data", []))
                for row in page_rows
            )
            token = response.get("NextToken")
            if not token:
                break
            first_page = False
        statistics = execution.get("Statistics", {})
        configuration = execution.get("ResultConfiguration", {})
        return AthenaQueryResult(
            query_execution_id=query_execution_id,
            columns=columns,
            rows=tuple(rows),
            data_scanned_bytes=int(statistics.get("DataScannedInBytes", 0)),
            engine_execution_time_ms=int(
                statistics.get("EngineExecutionTimeInMillis", 0)
            ),
            result_location=configuration.get("OutputLocation"),
            database=database,
            workgroup=workgroup,
            execution_state="SUCCEEDED",
        )

"""Airflow-side monthly reconciliation across Iceberg and Redshift Gold."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import time
from typing import Any

import boto3

from athena.query_runner import AthenaQueryRunner


class ReconciliationError(RuntimeError):
    """Raised when a monthly cross-layer count invariant fails."""


@dataclass(frozen=True)
class ReconciliationResult:
    source_year: int
    source_month: int
    ingestion_run_id: str
    bronze_row_count: int
    silver_row_count: int
    quarantine_row_count: int
    gold_row_count: int
    bronze_equals_classified: bool
    silver_equals_gold: bool
    athena_query_execution_ids: dict[str, str]
    redshift_statement_id: str
    reconciled_at: str


def _count_from_athena(
    runner: AthenaQueryRunner,
    *,
    sql: str,
    database: str,
    workgroup: str,
    year: int,
    month: int,
    token: str,
) -> tuple[int, str]:
    result = runner.run(
        sql,
        database=database,
        workgroup=workgroup,
        execution_parameters=(str(year), str(month)),
        client_request_token=token,
    )
    if len(result.rows) != 1 or len(result.rows[0]) != 1:
        raise ReconciliationError("Athena count query returned an unexpected result.")
    return int(result.rows[0][0] or 0), result.query_execution_id


def _record_value(record: list[dict[str, Any]]) -> int:
    if len(record) != 1:
        raise ReconciliationError("Redshift count query returned an unexpected result.")
    value = record[0]
    for key in ("longValue", "doubleValue", "stringValue"):
        if key in value:
            return int(value[key])
    raise ReconciliationError("Redshift count query returned no scalar value.")


def _redshift_count(
    client: Any,
    *,
    database: str,
    workgroup_name: str,
    schema: str,
    year: int,
    month: int,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 1.0,
    sleep=time.sleep,
) -> tuple[int, str]:
    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=(
            f'SELECT count(*) FROM "{schema}"."fct_trips" '
            f"WHERE source_year = {year} AND source_month = {month}"
        ),
        StatementName=f"nyc-reconcile-{year}-{month:02d}",
    )
    statement_id = response["Id"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        statement = client.describe_statement(Id=statement_id)
        status = statement["Status"]
        if status == "FINISHED":
            records = client.get_statement_result(Id=statement_id).get("Records", [])
            if len(records) != 1:
                raise ReconciliationError(
                    "Redshift count query returned no result row."
                )
            return _record_value(records[0]), statement_id
        if status in {"FAILED", "ABORTED"}:
            raise ReconciliationError(
                f"Redshift reconciliation statement {statement_id} {status}: "
                f"{statement.get('Error', 'no error supplied')}"
            )
        if time.monotonic() >= deadline:
            raise ReconciliationError(
                f"Redshift reconciliation statement {statement_id} timed out."
            )
        sleep(poll_interval_seconds)


def reconcile_month(
    *,
    source_year: int,
    source_month: int,
    ingestion_run_id: str,
    athena_workgroup: str,
    redshift_database: str,
    redshift_workgroup_name: str,
    redshift_schema: str = "gold",
    bronze_database: str = "bronze",
    silver_database: str = "silver",
    athena_runner: AthenaQueryRunner | None = None,
    redshift_client: Any | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return or fail the two required month-scoped reconciliation invariants."""

    year, month = int(source_year), int(source_month)
    if not 2019 <= year <= 2099 or not 1 <= month <= 12 or not ingestion_run_id:
        raise ValueError("source year/month and immutable run ID are required.")
    runner = athena_runner or AthenaQueryRunner()
    bronze, bronze_query_id = _count_from_athena(
        runner,
        sql=(
            'SELECT count(*) FROM "bronze_hvfhs_trips" '
            "WHERE _source_year = ? AND _source_month = ?"
        ),
        database=bronze_database,
        workgroup=athena_workgroup,
        year=year,
        month=month,
        token=f"nyc-reconcile-bronze-{year}-{month:02d}",
    )
    silver, silver_query_id = _count_from_athena(
        runner,
        sql=(
            'SELECT count(*) FROM "silver_trips" '
            "WHERE source_year = ? AND source_month = ?"
        ),
        database=silver_database,
        workgroup=athena_workgroup,
        year=year,
        month=month,
        token=f"nyc-reconcile-silver-{year}-{month:02d}",
    )
    quarantine, quarantine_query_id = _count_from_athena(
        runner,
        sql=(
            'SELECT count(*) FROM "quarantine_trips" '
            "WHERE _source_year = ? AND _source_month = ?"
        ),
        database=silver_database,
        workgroup=athena_workgroup,
        year=year,
        month=month,
        token=f"nyc-reconcile-quarantine-{year}-{month:02d}",
    )
    gold, statement_id = _redshift_count(
        redshift_client or boto3.client("redshift-data"),
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        schema=redshift_schema,
        year=year,
        month=month,
    )
    outcome = ReconciliationResult(
        source_year=year,
        source_month=month,
        ingestion_run_id=ingestion_run_id,
        bronze_row_count=bronze,
        silver_row_count=silver,
        quarantine_row_count=quarantine,
        gold_row_count=gold,
        bronze_equals_classified=bronze == silver + quarantine,
        silver_equals_gold=silver == gold,
        athena_query_execution_ids={
            "bronze": bronze_query_id,
            "silver": silver_query_id,
            "quarantine": quarantine_query_id,
        },
        redshift_statement_id=statement_id,
        reconciled_at=(now or datetime.now(timezone.utc)).isoformat(),
    )
    if not outcome.bronze_equals_classified or not outcome.silver_equals_gold:
        raise ReconciliationError(f"Monthly reconciliation failed: {asdict(outcome)}")
    return asdict(outcome)

"""Final read-only verification of open Iceberg layers and Redshift Gold."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import time
from typing import Any

import boto3

from athena.query_runner import AthenaQueryRunner
from etl.publication.nyc_hvfhs import REQUIRED_GOLD_RELATIONS


class VerificationError(RuntimeError):
    """Raised when post-publication read verification fails."""


@dataclass(frozen=True)
class VerificationResult:
    source_year: int
    source_month: int
    ingestion_run_id: str
    athena_query_execution_ids: dict[str, str]
    redshift_statement_ids: dict[str, str]
    gold_row_count: int


def _athena_partition_count(
    runner: AthenaQueryRunner,
    *,
    database: str,
    table: str,
    year_column: str,
    month_column: str,
    workgroup: str,
    year: int,
    month: int,
) -> tuple[int, str]:
    result = runner.run(
        f'SELECT count(*) FROM "{table}" WHERE {year_column} = ? AND {month_column} = ?',
        database=database,
        workgroup=workgroup,
        execution_parameters=(str(year), str(month)),
        client_request_token=f"nyc-verify-{database}-{table}-{year}-{month:02d}",
    )
    if len(result.rows) != 1 or len(result.rows[0]) != 1:
        raise VerificationError(
            "Athena layer verification returned an unexpected result."
        )
    return int(result.rows[0][0] or 0), result.query_execution_id


def _value(cell: dict[str, Any]) -> str:
    for key in ("stringValue", "longValue", "doubleValue"):
        if key in cell:
            return str(cell[key])
    raise VerificationError("Redshift Data API returned an empty cell.")


def _statement(
    client: Any,
    *,
    database: str,
    workgroup_name: str,
    sql: str,
    name: str,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 1.0,
    sleep=time.sleep,
) -> tuple[list[list[str]], str]:
    statement_id = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=sql,
        StatementName=name,
    )["Id"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        details = client.describe_statement(Id=statement_id)
        if details["Status"] == "FINISHED":
            records = client.get_statement_result(Id=statement_id).get("Records", [])
            return [
                [_value(cell) for cell in record] for record in records
            ], statement_id
        if details["Status"] in {"FAILED", "ABORTED"}:
            raise VerificationError(
                f"Redshift verification statement {statement_id} {details['Status']}: "
                f"{details.get('Error', 'no error supplied')}"
            )
        if time.monotonic() >= deadline:
            raise VerificationError(
                f"Redshift verification statement {statement_id} timed out."
            )
        sleep(poll_interval_seconds)


def verify_month(
    *,
    source_year: int,
    source_month: int,
    ingestion_run_id: str,
    reconciliation: dict[str, object],
    athena_workgroup: str,
    redshift_database: str,
    redshift_workgroup_name: str,
    redshift_schema: str = "gold",
    athena_runner: AthenaQueryRunner | None = None,
    redshift_client: Any | None = None,
) -> dict[str, object]:
    """Verify only Bronze/Silver/quarantine through Athena and Gold through Redshift."""

    year, month = int(source_year), int(source_month)
    runner = athena_runner or AthenaQueryRunner()
    bronze, bronze_id = _athena_partition_count(
        runner,
        database="bronze",
        table="bronze_hvfhs_trips",
        year_column="_source_year",
        month_column="_source_month",
        workgroup=athena_workgroup,
        year=year,
        month=month,
    )
    silver, silver_id = _athena_partition_count(
        runner,
        database="silver",
        table="silver_trips",
        year_column="source_year",
        month_column="source_month",
        workgroup=athena_workgroup,
        year=year,
        month=month,
    )
    quarantine, quarantine_id = _athena_partition_count(
        runner,
        database="silver",
        table="quarantine_trips",
        year_column="_source_year",
        month_column="_source_month",
        workgroup=athena_workgroup,
        year=year,
        month=month,
    )
    expected = {
        "bronze": int(reconciliation["bronze_row_count"]),
        "silver": int(reconciliation["silver_row_count"]),
        "quarantine": int(reconciliation["quarantine_row_count"]),
    }
    if {"bronze": bronze, "silver": silver, "quarantine": quarantine} != expected:
        raise VerificationError("Athena open-layer counts differ from reconciliation.")
    redshift = redshift_client or boto3.client("redshift-data")
    listed, listed_id = _statement(
        redshift,
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        name=f"nyc-verify-relations-{year}-{month:02d}",
        sql=(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{redshift_schema}' AND table_name IN ("
            + ", ".join(f"'{name}'" for name in REQUIRED_GOLD_RELATIONS)
            + ")"
        ),
    )
    if {row[0] for row in listed} != set(REQUIRED_GOLD_RELATIONS):
        raise VerificationError("Redshift Gold relation set is incomplete.")
    fact, fact_id = _statement(
        redshift,
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        name=f"nyc-verify-fact-{year}-{month:02d}",
        sql=(
            f'SELECT count(*) FROM "{redshift_schema}"."fct_trips" '
            f"WHERE source_year = {year} AND source_month = {month}"
        ),
    )
    gold_count = int(fact[0][0]) if len(fact) == 1 and len(fact[0]) == 1 else -1
    if gold_count != int(reconciliation["gold_row_count"]):
        raise VerificationError("Redshift fct_trips count differs from reconciliation.")
    statement_ids = {"relations": listed_id, "fct_trips": fact_id}
    for mart in ("mart_hourly_zone_demand", "mart_operator_metrics"):
        _, statement_ids[mart] = _statement(
            redshift,
            database=redshift_database,
            workgroup_name=redshift_workgroup_name,
            name=f"nyc-verify-{mart}-{year}-{month:02d}",
            sql=(
                f'SELECT count(*) FROM "{redshift_schema}"."{mart}" '
                f"WHERE source_year = {year} AND source_month = {month}"
            ),
        )
    return asdict(
        VerificationResult(
            source_year=year,
            source_month=month,
            ingestion_run_id=ingestion_run_id,
            athena_query_execution_ids={
                "bronze": bronze_id,
                "silver": silver_id,
                "quarantine": quarantine_id,
            },
            redshift_statement_ids=statement_ids,
            gold_row_count=gold_count,
        )
    )

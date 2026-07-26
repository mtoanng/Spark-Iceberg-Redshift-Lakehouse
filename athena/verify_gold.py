"""Minimal verifier for the one approved Athena Gold smoke query."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

import boto3

from athena.query_runner import AthenaQueryError, AthenaQueryRunner


@dataclass(frozen=True)
class GoldSmokeResult:
    query_execution_id: str
    database: str
    workgroup: str
    result_location: str | None
    execution_state: str
    data_scanned_bytes: int
    engine_execution_time_ms: int
    row_count: int
    distinct_row_count: int


EXPECTED_GOLD_COLUMNS = {
    "dim_date": {"date_key", "calendar_date"},
    "dim_operator": {"operator_code"},
    "dim_zone": {"zone_id", "zone_name"},
    "fct_trips": {
        "row_id",
        "business_trip_key",
        "identity_policy_version",
        "source_year",
        "source_month",
        "pickup_datetime",
    },
    "mart_hourly_zone_demand": {"hourly_zone_key", "trip_count"},
    "mart_operator_metrics": {"operator_month_key", "trip_count"},
}


def verify_gold_catalog(glue_client: Any, *, database: str) -> None:
    """Verify exactly the expected Gold contract needed by the query pack."""

    response = glue_client.get_tables(DatabaseName=database)
    actual_names = {table["Name"] for table in response.get("TableList", [])}
    missing_tables = set(EXPECTED_GOLD_COLUMNS) - actual_names
    if missing_tables:
        raise AthenaQueryError(
            "Gold catalog is missing expected tables: "
            + ", ".join(sorted(missing_tables))
        )
    unexpected_tables = actual_names - set(EXPECTED_GOLD_COLUMNS)
    if unexpected_tables:
        raise AthenaQueryError(
            "Gold catalog contains out-of-scope tables: "
            + ", ".join(sorted(unexpected_tables))
        )
    for table, required_columns in EXPECTED_GOLD_COLUMNS.items():
        metadata = glue_client.get_table(DatabaseName=database, Name=table)["Table"]
        columns = {
            column["Name"]
            for column in metadata.get("StorageDescriptor", {}).get("Columns", [])
        }
        missing_columns = required_columns - columns
        if missing_columns:
            raise AthenaQueryError(
                f"Gold table {table} is missing columns: "
                + ", ".join(sorted(missing_columns))
            )


def verify_gold_smoke(
    runner: AthenaQueryRunner,
    *,
    year: int,
    month: int,
    database: str,
    workgroup: str,
    catalog: str = "AwsDataCatalog",
    glue_client: Any | None = None,
    max_scanned_bytes: int = 104_857_600,
    expected_row_count: int | None = None,
) -> GoldSmokeResult:
    if not 2019 <= year <= 2099 or not 1 <= month <= 12:
        raise ValueError("year/month are outside the HVFHV source contract.")
    if max_scanned_bytes <= 0:
        raise ValueError("max_scanned_bytes must be positive.")
    if glue_client is not None:
        verify_gold_catalog(glue_client, database=database)
    sql = files("athena.sql").joinpath("gold_smoke.sql").read_text(encoding="utf-8")
    result = runner.run(
        sql,
        database=database,
        catalog=catalog,
        workgroup=workgroup,
        execution_parameters=(str(year), str(month)),
        client_request_token=f"gold-smoke-{year}-{month:02d}",
    )
    if len(result.rows) != 1 or len(result.columns) != 4:
        raise AthenaQueryError("Gold smoke query returned an unexpected result shape.")
    values = dict(zip(result.columns, result.rows[0], strict=True))
    row_count = int(values["row_count"] or 0)
    distinct_row_count = int(values["distinct_row_count"] or 0)
    if row_count <= 0 or row_count != distinct_row_count:
        raise AthenaQueryError(
            "Gold smoke failed: fct_trips is empty or row_id is not unique."
        )
    if expected_row_count is not None and row_count != expected_row_count:
        raise AthenaQueryError(
            f"Gold smoke count {row_count} does not match publication {expected_row_count}."
        )
    if not values.get("min_pickup_datetime") or not values.get("max_dropoff_datetime"):
        raise AthenaQueryError("Gold smoke failed: required timestamp bounds are null.")
    if result.data_scanned_bytes > max_scanned_bytes:
        raise AthenaQueryError(
            f"Gold smoke scanned {result.data_scanned_bytes} bytes, exceeding "
            f"the {max_scanned_bytes}-byte verification bound."
        )
    return GoldSmokeResult(
        result.query_execution_id,
        result.database or database,
        result.workgroup or workgroup,
        result.result_location,
        result.execution_state,
        result.data_scanned_bytes,
        result.engine_execution_time_ms,
        row_count,
        distinct_row_count,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the bounded Athena Gold smoke verifier."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--catalog", default="AwsDataCatalog")
    parser.add_argument("--region")
    parser.add_argument("--max-scanned-bytes", type=int, default=104_857_600)
    args = parser.parse_args()
    outcome = verify_gold_smoke(
        AthenaQueryRunner(region_name=args.region),
        year=args.year,
        month=args.month,
        database=args.database,
        workgroup=args.workgroup,
        catalog=args.catalog,
        glue_client=boto3.client("glue", region_name=args.region),
        max_scanned_bytes=args.max_scanned_bytes,
    )
    print(
        f"PASS query_id={outcome.query_execution_id} state={outcome.execution_state} "
        f"database={outcome.database} workgroup={outcome.workgroup} "
        f"result_location={outcome.result_location} scanned_bytes={outcome.data_scanned_bytes} "
        f"engine_ms={outcome.engine_execution_time_ms}"
    )


if __name__ == "__main__":
    main()

"""Minimal verifier for the one approved Athena Gold smoke query."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from importlib.resources import files

from athena.query_runner import AthenaQueryError, AthenaQueryRunner


@dataclass(frozen=True)
class GoldSmokeResult:
    query_execution_id: str
    data_scanned_bytes: int
    row_count: int
    distinct_trip_count: int


def verify_gold_smoke(
    runner: AthenaQueryRunner,
    *,
    year: int,
    month: int,
    database: str,
    workgroup: str,
    catalog: str = "AwsDataCatalog",
) -> GoldSmokeResult:
    if not 2019 <= year <= 2099 or not 1 <= month <= 12:
        raise ValueError("year/month are outside the HVFHV source contract.")
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
    distinct_trip_count = int(values["distinct_trip_count"] or 0)
    if row_count <= 0 or row_count != distinct_trip_count:
        raise AthenaQueryError(
            "Gold smoke failed: fct_trips is empty or trip_id is not unique."
        )
    if not values.get("min_pickup_datetime") or not values.get("max_dropoff_datetime"):
        raise AthenaQueryError("Gold smoke failed: required timestamp bounds are null.")
    return GoldSmokeResult(
        result.query_execution_id,
        result.data_scanned_bytes,
        row_count,
        distinct_trip_count,
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
    args = parser.parse_args()
    outcome = verify_gold_smoke(
        AthenaQueryRunner(region_name=args.region),
        year=args.year,
        month=args.month,
        database=args.database,
        workgroup=args.workgroup,
        catalog=args.catalog,
    )
    print(
        f"PASS query_id={outcome.query_execution_id} scanned_bytes={outcome.data_scanned_bytes}"
    )


if __name__ == "__main__":
    main()

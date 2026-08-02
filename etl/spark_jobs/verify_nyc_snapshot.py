"""Read one retained Silver Iceberg snapshot for the bounded evolution proof."""

from __future__ import annotations

import json
import re

from pyspark.sql import SparkSession

from etl.spark_jobs.arguments import parse_arguments


args = parse_arguments(
    ["SNAPSHOT_ID", "SOURCE_YEAR", "SOURCE_MONTH"],
    {
        "CATALOG_NAME": "glue_catalog",
        "SILVER_DATABASE": "silver",
    },
)


def main() -> None:
    snapshot_id = args["SNAPSHOT_ID"]
    year, month = int(args["SOURCE_YEAR"]), int(args["SOURCE_MONTH"])
    if not re.fullmatch(r"[0-9]+", snapshot_id):
        raise ValueError("SNAPSHOT_ID must be an Iceberg numeric snapshot ID.")
    if not 2019 <= year <= 2099 or not 1 <= month <= 12:
        raise ValueError("SOURCE_YEAR and SOURCE_MONTH are invalid.")
    table = (
        f"{args['CATALOG_NAME']}.{args['SILVER_DATABASE']}.silver_trips "
        f"VERSION AS OF {snapshot_id}"
    )
    spark = SparkSession.builder.getOrCreate()
    count = spark.sql(
        f"SELECT count(*) AS row_count FROM {table} "
        f"WHERE source_year={year} AND source_month={month}"
    ).first()
    if count is None:
        raise ValueError("Snapshot verification returned no row count.")
    print(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "source_year": year,
                "source_month": month,
                "silver_row_count": int(count.row_count),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

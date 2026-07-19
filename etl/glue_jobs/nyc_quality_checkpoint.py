"""Remote-only explicit quality gate for one NYC HVFHV source month.

It intentionally reads canonical Iceberg tables and fails the Glue run on any
broken reconciliation or duplicate fact identity. It does not mutate data.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_YEAR", "SOURCE_MONTH"])

BRONZE_TRIPS_TABLE = "glue_catalog.bronze.bronze_hvfhs_trips"
SILVER_TRIPS_TABLE = "glue_catalog.silver.silver_trips"
QUARANTINE_TABLE = "glue_catalog.silver.quarantine_trips"
GOLD_FACT_TABLE = "glue_catalog.gold.fct_trips"


def _count_for_month(spark, table_name: str, year: int, month: int) -> int:
    return (
        spark.table(table_name)
        .filter((F.col("source_year") == year) & (F.col("source_month") == month))
        .count()
    )


def _assert_quality(spark, year: int, month: int) -> None:
    bronze_count = _count_for_month(spark, BRONZE_TRIPS_TABLE, year, month)
    silver_count = _count_for_month(spark, SILVER_TRIPS_TABLE, year, month)
    quarantine_count = _count_for_month(spark, QUARANTINE_TABLE, year, month)
    fact_count = _count_for_month(spark, GOLD_FACT_TABLE, year, month)

    duplicate_silver = (
        spark.table(SILVER_TRIPS_TABLE)
        .filter((F.col("source_year") == year) & (F.col("source_month") == month))
        .groupBy("trip_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    missing_quarantine_reason = (
        spark.table(QUARANTINE_TABLE)
        .filter((F.col("source_year") == year) & (F.col("source_month") == month))
        .filter(F.col("reason_code").isNull() | (F.length(F.trim(F.col("reason_code"))) == 0))
        .limit(1)
        .count()
    )

    failures: list[str] = []
    if bronze_count != silver_count + quarantine_count:
        failures.append("bronze_count must equal silver_count plus quarantine_count")
    if duplicate_silver:
        failures.append("silver trip_id must be unique for the source month")
    if missing_quarantine_reason:
        failures.append("quarantine rows must include reason_code")
    if fact_count != silver_count:
        failures.append("Gold fct_trips count must reconcile to valid Silver rows")
    if failures:
        raise ValueError("Quality checkpoint failed: " + "; ".join(failures))


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    _assert_quality(glue_context.spark_session, int(args["SOURCE_YEAR"]), int(args["SOURCE_MONTH"]))
    job.commit()


if __name__ == "__main__":
    main()

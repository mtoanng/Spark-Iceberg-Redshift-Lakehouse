"""Publish the small monthly Gold completion record used by Athena smoke checks."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"])


def _table(namespace: str, name: str) -> str:
    return f"glue_catalog.{namespace}.{name}"


def main() -> None:
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    spark = context.spark_session
    year = int(args["SOURCE_YEAR"])
    month = int(args["SOURCE_MONTH"])
    run_id = args["INGESTION_RUN_ID"].replace("'", "''")
    tables = (
        "dim_date",
        "dim_operator",
        "dim_zone",
        "fct_trips",
        "mart_hourly_zone_demand",
        "mart_operator_metrics",
    )
    missing = [name for name in tables if not spark.catalog.tableExists(_table("gold", name))]
    if missing:
        raise ValueError(f"Gold publication is missing tables: {', '.join(missing)}")
    spark.sql(
        f"UPDATE {_table('ops', 'source_run_manifest')} "
        f"SET publication_status='published', published_at=current_timestamp(), "
        f"updated_at=current_timestamp() WHERE source_year={year} AND source_month={month} "
        f"AND ingestion_run_id='{run_id}' AND run_status='reconciled'"
    )
    job.commit()


if __name__ == "__main__":
    main()

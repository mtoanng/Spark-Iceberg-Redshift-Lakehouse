"""Month-scoped, retry-safe Glue Bronze ingestion for NYC HVFHV sources."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name, lit


REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_URI",
    "SOURCE_YEAR",
    "SOURCE_MONTH",
    "SOURCE_CHECKSUM",
    "INGESTION_RUN_ID",
    "TAXI_ZONE_URI",
    "TAXI_ZONE_CHECKSUM",
]
args = getResolvedOptions(sys.argv, REQUIRED_ARGS)


def _optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def _table(namespace: str, name: str) -> str:
    return f"{_optional_arg('CATALOG_NAME', 'glue_catalog')}.{_optional_arg(f'{namespace.upper()}_DATABASE', namespace)}.{name}"


TRIPS_TABLE = _table("bronze", "bronze_hvfhs_trips")
ZONES_TABLE = _table("bronze", "bronze_taxi_zones")
MANIFEST_TABLE = _table("ops", "source_run_manifest")


def _merge_manifest(
    spark,
    *,
    status: str,
    bronze_count: int = 0,
    failure_stage: str | None = None,
    failure_message: str | None = None,
) -> None:
    update = spark.createDataFrame(
        [
            (
                args["SOURCE_URI"],
                args["SOURCE_CHECKSUM"],
                int(args["SOURCE_YEAR"]),
                int(args["SOURCE_MONTH"]),
                int(_optional_arg("SOURCE_SIZE_BYTES", "0")),
                args["INGESTION_RUN_ID"],
                status,
                bronze_count,
                failure_stage,
                failure_message,
            )
        ],
        "source_uri string, source_checksum string, source_year int, source_month int, source_size_bytes long, ingestion_run_id string, run_status string, bronze_row_count long, failure_stage string, failure_message string",
    )
    update.createOrReplaceTempView("manifest_update")
    spark.sql(
        f"""
        MERGE INTO {MANIFEST_TABLE} target USING manifest_update source
        ON target.source_year = source.source_year AND target.source_month = source.source_month
        WHEN MATCHED AND target.source_checksum <> source.source_checksum THEN
          UPDATE SET run_status = 'failed', failure_stage = 'source_manifest', failure_message = 'Checksum changed for an existing month.', updated_at = current_timestamp()
        WHEN MATCHED THEN UPDATE SET
          source_uri = source.source_uri, source_size_bytes = source.source_size_bytes, ingestion_run_id = source.ingestion_run_id,
          run_status = source.run_status, bronze_row_count = source.bronze_row_count, failure_stage = source.failure_stage,
          failure_message = source.failure_message, updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_uri, source_checksum, source_size_bytes, source_year, source_month, ingestion_run_id, run_status, first_seen_at, updated_at, bronze_row_count, silver_row_count, quarantine_row_count, failure_stage, failure_message)
          VALUES (source.source_uri, source.source_checksum, source.source_size_bytes, source.source_year, source.source_month, source.ingestion_run_id, source.run_status, current_timestamp(), current_timestamp(), source.bronze_row_count, 0, 0, source.failure_stage, source.failure_message)
    """
    )


def _may_process(spark) -> bool:
    existing = spark.sql(
        f"SELECT source_uri, source_checksum, run_status FROM {MANIFEST_TABLE} WHERE source_year = {int(args['SOURCE_YEAR'])} AND source_month = {int(args['SOURCE_MONTH'])} ORDER BY updated_at DESC LIMIT 1"
    ).collect()
    if not existing:
        return True
    row = existing[0]
    if (
        row.source_uri != args["SOURCE_URI"]
        or row.source_checksum != args["SOURCE_CHECKSUM"]
    ):
        raise ValueError(
            "Changed source URI or checksum is blocked for an existing monthly partition."
        )
    return not (
        row.run_status == "silver_published"
        and _optional_arg("FORCE", "false").lower() != "true"
    )


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session
    if not _may_process(spark):
        job.commit()
        return
    try:
        trips = (
            spark.read.parquet(args["SOURCE_URI"])
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
            .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
            .withColumn("_source_checksum", lit(args["SOURCE_CHECKSUM"]))
            .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
            .withColumn("_ingested_at", current_timestamp())
        )
        bronze_count = trips.count()
        trips.writeTo(TRIPS_TABLE).overwritePartitions()
        zones = (
            spark.read.option("header", True)
            .csv(args["TAXI_ZONE_URI"])
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
            .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
            .withColumn("_source_checksum", lit(args["TAXI_ZONE_CHECKSUM"]))
            .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
            .withColumn("_ingested_at", current_timestamp())
        )
        zones.writeTo(ZONES_TABLE).overwritePartitions()
        _merge_manifest(spark, status="bronze_published", bronze_count=bronze_count)
        job.commit()
    except Exception as error:
        _merge_manifest(
            spark,
            status="failed",
            failure_stage="bronze",
            failure_message=str(error)[:2000],
        )
        raise


if __name__ == "__main__":
    main()

"""Glue entry point for source-faithful NYC HVFHV Bronze ingestion.

Run this only on the approved remote/AWS environment. It assumes the Glue
catalog and Iceberg tables are provisioned by a later phase; no local Spark
execution is supported or attempted.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name, lit


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_URI",
        "SOURCE_YEAR",
        "SOURCE_MONTH",
        "SOURCE_CHECKSUM",
        "INGESTION_RUN_ID",
        "TAXI_ZONE_URI",
        "TAXI_ZONE_CHECKSUM",
    ],
)

TRIPS_TABLE = "glue_catalog.bronze.bronze_hvfhs_trips"
ZONES_TABLE = "glue_catalog.bronze.bronze_taxi_zones"


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session

    trips = (
        spark.read.parquet(args["SOURCE_URI"])
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
        .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
        .withColumn("_source_checksum", lit(args["SOURCE_CHECKSUM"]))
        .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
        .withColumn("_ingested_at", current_timestamp())
    )
    # Table creation/catalog provisioning is Phase 3. Append preserves raw
    # source columns; the manifest gate decides whether this job may run.
    trips.writeTo(TRIPS_TABLE).append()

    zones = (
        spark.read.option("header", True).csv(args["TAXI_ZONE_URI"])
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
        .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
        .withColumn("_source_checksum", lit(args["TAXI_ZONE_CHECKSUM"]))
        .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
        .withColumn("_ingested_at", current_timestamp())
    )
    zones.writeTo(ZONES_TABLE).append()
    job.commit()


if __name__ == "__main__":
    main()

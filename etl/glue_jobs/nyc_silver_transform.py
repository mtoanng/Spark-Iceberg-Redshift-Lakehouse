"""Glue entry point for NYC HVFHV Silver validation and quarantine.

This job is not run locally. It assumes Phase 3 provisions the referenced
Iceberg tables; its validation order and reason codes match the fixture-tested
pure-Python contract in ``etl.transforms.nyc_hvfhs``.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    date_format,
    hour,
    lit,
    row_number,
    sha2,
    to_date,
    when,
)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
BRONZE_TRIPS_TABLE = "glue_catalog.bronze.bronze_hvfhs_trips"
BRONZE_ZONES_TABLE = "glue_catalog.bronze.bronze_taxi_zones"
SILVER_TRIPS_TABLE = "glue_catalog.silver.silver_trips"
QUARANTINE_TABLE = "glue_catalog.silver.quarantine_trips"


def _trip_fingerprint():
    identity_columns = (
        "hvfhs_license_num",
        "dispatching_base_num",
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "driver_pay",
    )
    return sha2(concat_ws("\u001f", *[coalesce(col(name).cast("string"), lit("")) for name in identity_columns]), 256)


def _validated_frames(spark):
    trips = spark.table(BRONZE_TRIPS_TABLE).withColumn("trip_id", _trip_fingerprint())
    zones = spark.table(BRONZE_ZONES_TABLE).select(col("LocationID").cast("int").alias("zone_id")).distinct()
    joined = (
        trips.join(zones.withColumnRenamed("zone_id", "pickup_zone_id"), col("PULocationID") == col("pickup_zone_id"), "left")
        .join(zones.withColumnRenamed("zone_id", "dropoff_zone_id"), col("DOLocationID") == col("dropoff_zone_id"), "left")
    )
    reasoned = joined.withColumn(
        "reason_code",
        when(col("pickup_datetime").isNull() | col("dropoff_datetime").isNull(), "MISSING_OR_INVALID_TIMESTAMP")
        .when(col("dropoff_datetime") < col("pickup_datetime"), "DROPOFF_BEFORE_PICKUP")
        .when(col("pickup_zone_id").isNull(), "UNKNOWN_PICKUP_ZONE")
        .when(col("dropoff_zone_id").isNull(), "UNKNOWN_DROPOFF_ZONE")
        .when(col("trip_miles") < 0, "NEGATIVE_TRIP_MILES")
        .when(col("trip_time") < 0, "NEGATIVE_TRIP_TIME")
        .when(col("base_passenger_fare") < 0, "NEGATIVE_PASSENGER_FARE")
        .when(col("driver_pay") < 0, "NEGATIVE_DRIVER_PAY"),
    )
    duplicate_window = Window.partitionBy("trip_id").orderBy(col("_ingested_at"))
    numbered = reasoned.withColumn("_trip_row_number", row_number().over(duplicate_window))
    classified = numbered.withColumn(
        "reason_code",
        when(col("reason_code").isNull() & (col("_trip_row_number") > 1), "DUPLICATE_TRIP_ID").otherwise(col("reason_code")),
    )
    quarantine = classified.filter(col("reason_code").isNotNull()).drop("_trip_row_number")
    silver = (
        classified.filter(col("reason_code").isNull())
        .select(
            "trip_id",
            col("hvfhs_license_num").alias("operator_code"),
            "request_datetime",
            "pickup_datetime",
            "dropoff_datetime",
            col("PULocationID").cast("int").alias("pickup_zone_id"),
            col("DOLocationID").cast("int").alias("dropoff_zone_id"),
            "trip_miles",
            col("trip_time").cast("int").alias("trip_time_seconds"),
            col("base_passenger_fare").alias("passenger_fare"),
            "tolls",
            "sales_tax",
            "tips",
            "driver_pay",
            "shared_request_flag",
            "shared_match_flag",
            col("_source_year").alias("source_year"),
            col("_source_month").alias("source_month"),
            col("_ingestion_run_id").alias("ingestion_run_id"),
            ((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60).alias("trip_duration_minutes"),
            to_date("pickup_datetime").alias("pickup_date"),
            hour("pickup_datetime").alias("pickup_hour"),
        )
    )
    return silver, quarantine


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    silver, quarantine = _validated_frames(glue_context.spark_session)
    silver.writeTo(SILVER_TRIPS_TABLE).append()
    quarantine.writeTo(QUARANTINE_TABLE).append()
    job.commit()


if __name__ == "__main__":
    main()

"""Month-scoped Silver/quarantine publication guarded by Great Expectations."""

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
    hour,
    lit,
    row_number,
    sha2,
    to_date,
    when,
)


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"]
)


def _optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def _table(namespace: str, name: str) -> str:
    return f"{_optional_arg('CATALOG_NAME', 'glue_catalog')}.{_optional_arg(f'{namespace.upper()}_DATABASE', namespace)}.{name}"


BRONZE_TRIPS_TABLE, BRONZE_ZONES_TABLE = _table("bronze", "bronze_hvfhs_trips"), _table(
    "bronze", "bronze_taxi_zones"
)
SILVER_TRIPS_TABLE, QUARANTINE_TABLE, MANIFEST_TABLE = (
    _table("silver", "silver_trips"),
    _table("silver", "quarantine_trips"),
    _table("ops", "source_run_manifest"),
)


def _fingerprint():
    columns = (
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
    return sha2(
        concat_ws(
            "\u001f", *[coalesce(col(name).cast("string"), lit("")) for name in columns]
        ),
        256,
    )


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session
    year, month, run_id = (
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["INGESTION_RUN_ID"],
    )
    status = spark.sql(
        f"SELECT run_status FROM {MANIFEST_TABLE} WHERE source_year={year} AND source_month={month} AND ingestion_run_id='{run_id}' ORDER BY updated_at DESC LIMIT 1"
    ).collect()
    if not status or status[0].run_status != "ge_passed":
        raise ValueError(
            "Silver publication is blocked until the Great Expectations gate passes for this run."
        )
    try:
        trips = (
            spark.table(BRONZE_TRIPS_TABLE)
            .filter(
                (col("_source_year") == year)
                & (col("_source_month") == month)
                & (col("_ingestion_run_id") == run_id)
            )
            .withColumn("trip_id", _fingerprint())
        )
        zones = (
            spark.table(BRONZE_ZONES_TABLE)
            .filter((col("_source_year") == year) & (col("_source_month") == month))
            .select(col("LocationID").cast("int").alias("zone_id"))
            .distinct()
        )
        other_ids = (
            spark.table(SILVER_TRIPS_TABLE)
            .filter(~((col("source_year") == year) & (col("source_month") == month)))
            .select("trip_id")
            .distinct()
            .withColumn("_already_published", lit(True))
        )
        joined = (
            trips.join(
                zones.withColumnRenamed("zone_id", "pickup_zone_id"),
                col("PULocationID") == col("pickup_zone_id"),
                "left",
            )
            .join(
                zones.withColumnRenamed("zone_id", "dropoff_zone_id"),
                col("DOLocationID") == col("dropoff_zone_id"),
                "left",
            )
            .join(other_ids, "trip_id", "left")
        )
        reasoned = joined.withColumn(
            "reason_code",
            when(
                col("pickup_datetime").isNull() | col("dropoff_datetime").isNull(),
                "MISSING_OR_INVALID_TIMESTAMP",
            )
            .when(
                col("dropoff_datetime") < col("pickup_datetime"),
                "DROPOFF_BEFORE_PICKUP",
            )
            .when(col("pickup_zone_id").isNull(), "UNKNOWN_PICKUP_ZONE")
            .when(col("dropoff_zone_id").isNull(), "UNKNOWN_DROPOFF_ZONE")
            .when(col("trip_miles") < 0, "NEGATIVE_TRIP_MILES")
            .when(col("trip_time") < 0, "NEGATIVE_TRIP_TIME")
            .when(col("base_passenger_fare") < 0, "NEGATIVE_PASSENGER_FARE")
            .when(col("driver_pay") < 0, "NEGATIVE_DRIVER_PAY"),
        )
        numbered = reasoned.withColumn(
            "_trip_row_number",
            row_number().over(
                Window.partitionBy("trip_id").orderBy(col("_ingested_at"))
            ),
        )
        classified = numbered.withColumn(
            "reason_code",
            when(
                col("reason_code").isNull()
                & (
                    (col("_trip_row_number") > 1)
                    | col("_already_published").isNotNull()
                ),
                "DUPLICATE_TRIP_ID",
            ).otherwise(col("reason_code")),
        )
        quarantine = classified.filter(col("reason_code").isNotNull()).drop(
            "_trip_row_number", "_already_published"
        )
        silver = classified.filter(col("reason_code").isNull()).select(
            "trip_id",
            col("hvfhs_license_num").alias("operator_code"),
            "request_datetime",
            "pickup_datetime",
            "dropoff_datetime",
            col("PULocationID").cast("int").alias("pickup_zone_id"),
            col("DOLocationID").cast("int").alias("dropoff_zone_id"),
            "trip_miles",
            col("trip_time").cast("bigint").alias("trip_time_seconds"),
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
            (
                (
                    col("dropoff_datetime").cast("long")
                    - col("pickup_datetime").cast("long")
                )
                / 60
            ).alias("trip_duration_minutes"),
            to_date("pickup_datetime").alias("pickup_date"),
            hour("pickup_datetime").alias("pickup_hour"),
        )
        silver_count, quarantine_count = silver.count(), quarantine.count()
        if trips.count() != silver_count + quarantine_count:
            raise ValueError(
                "Bronze/Silver/quarantine reconciliation failed before canonical publication."
            )
        silver.writeTo(SILVER_TRIPS_TABLE).overwritePartitions()
        quarantine.writeTo(QUARANTINE_TABLE).overwritePartitions()
        spark.sql(
            f"UPDATE {MANIFEST_TABLE} SET run_status='silver_published', silver_row_count={silver_count}, quarantine_row_count={quarantine_count}, completed_at=current_timestamp(), updated_at=current_timestamp(), failure_stage=NULL, failure_message=NULL WHERE source_year={year} AND source_month={month} AND ingestion_run_id='{run_id}'"
        )
        job.commit()
    except Exception as error:
        spark.sql(
            f"UPDATE {MANIFEST_TABLE} SET run_status='failed', failure_stage='silver', failure_message='{str(error).replace("'", "''")[:2000]}', updated_at=current_timestamp() WHERE source_year={year} AND source_month={month} AND ingestion_run_id='{run_id}'"
        )
        raise


if __name__ == "__main__":
    main()

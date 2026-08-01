"""Month-scoped Silver/quarantine publication after Bronze validation."""

from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import (
    col,
    count,
    hour,
    lit,
    row_number,
    sum as spark_sum,
    to_date,
    when,
)

from etl.contracts.nyc_hvfhs_identity import identity_policy_version
from etl.contracts.nyc_hvfhs_quality import spark_reason_expression
from etl.spark_jobs.arguments import parse_arguments

args = parse_arguments(
    ["SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"],
    {
        "CATALOG_NAME": "glue_catalog",
        "BRONZE_DATABASE": "bronze",
        "SILVER_DATABASE": "silver",
        "OPS_DATABASE": "ops",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def _table(namespace: str, name: str) -> str:
    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    database = _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
    return f"{catalog}.{database}.{name}"


BRONZE_TRIPS_TABLE = _table("bronze", "bronze_hvfhs_trips")
BRONZE_ZONES_TABLE = _table("bronze", "bronze_taxi_zones")
SILVER_TRIPS_TABLE = _table("silver", "silver_trips")
QUARANTINE_TABLE = _table("silver", "quarantine_trips")
MANIFEST_TABLE = _table("ops", "source_run_manifest")


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    year, month, run_id = (
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["INGESTION_RUN_ID"],
    )
    status = spark.sql(
        f"""
        SELECT run_status, failure_stage
        FROM {MANIFEST_TABLE}
        WHERE source_year = {year}
          AND source_month = {month}
          AND ingestion_run_id = '{run_id}'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).collect()
    if status and status[0].run_status == "silver_published":
        return
    retrying_silver = bool(status) and (
        status[0].run_status == "failed" and status[0].failure_stage == "silver"
    )
    if not status or (
        status[0].run_status != "bronze_published" and not retrying_silver
    ):
        raise ValueError("Silver publication requires the requested Bronze run.")
    classified = None
    try:
        trips = spark.table(BRONZE_TRIPS_TABLE).filter(
            (col("_source_year") == year)
            & (col("_source_month") == month)
            & (col("_ingestion_run_id") == run_id)
        )
        expected_policy = identity_policy_version(year)
        if (
            trips.filter(col("identity_policy_version") != expected_policy)
            .limit(1)
            .count()
        ):
            raise ValueError(
                "Bronze identity policy does not match the requested source year."
            )
        zones = (
            spark.table(BRONZE_ZONES_TABLE)
            .select(col("LocationID").cast("int").alias("zone_id"))
            .distinct()
        )
        joined = trips.join(
            zones.withColumnRenamed("zone_id", "pickup_zone_id"),
            col("PULocationID") == col("pickup_zone_id"),
            "left",
        ).join(
            zones.withColumnRenamed("zone_id", "dropoff_zone_id"),
            col("DOLocationID") == col("dropoff_zone_id"),
            "left",
        )
        reasoned = joined.withColumn("reason_code", spark_reason_expression())
        numbered = reasoned.withColumn(
            "_trip_row_number",
            row_number().over(
                Window.partitionBy("row_id").orderBy(
                    col("_source_file"), col("_ingested_at")
                )
            ),
        )
        classified = numbered.withColumn(
            "reason_code",
            when(
                col("reason_code").isNull() & (col("_trip_row_number") > 1),
                "DUPLICATE_ROW_ID",
            ).otherwise(col("reason_code")),
        )
        classified.persist(StorageLevel.MEMORY_AND_DISK)
        quarantine = classified.filter(col("reason_code").isNotNull()).drop(
            "_trip_row_number"
        )
        silver_columns = [
            "row_id",
            "business_trip_key",
            "identity_policy_version",
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
        ]
        if "cbd_congestion_fee" in trips.columns:
            silver_columns.append(
                col("cbd_congestion_fee").cast("double").alias("cbd_congestion_fee")
            )
        silver = classified.filter(col("reason_code").isNull()).select(*silver_columns)
        counts = classified.agg(
            count(lit(1)).alias("bronze_count"),
            spark_sum(when(col("reason_code").isNull(), 1).otherwise(0)).alias(
                "silver_count"
            ),
        ).first()
        bronze_count = int(counts.bronze_count)
        silver_count = int(counts.silver_count or 0)
        quarantine_count = bronze_count - silver_count
        if bronze_count != silver_count + quarantine_count:
            raise ValueError(
                "Bronze/Silver/quarantine reconciliation failed before canonical publication."
            )
        silver.writeTo(SILVER_TRIPS_TABLE).overwritePartitions()
        quarantine.writeTo(QUARANTINE_TABLE).overwritePartitions()
        silver_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {SILVER_TRIPS_TABLE}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        quarantine_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {QUARANTINE_TABLE}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        if silver_snapshot is None or quarantine_snapshot is None:
            raise ValueError("Silver publication must expose both Iceberg snapshots.")
        spark.sql(
            f"""
            UPDATE {MANIFEST_TABLE}
            SET run_status = 'silver_published',
                silver_row_count = {silver_count},
                quarantine_row_count = {quarantine_count},
                silver_snapshot_id = '{silver_snapshot.snapshot_id}',
                quarantine_snapshot_id = '{quarantine_snapshot.snapshot_id}',
                completed_at = current_timestamp(),
                updated_at = current_timestamp(),
                failure_stage = NULL,
                failure_message = NULL
            WHERE source_year = {year}
              AND source_month = {month}
              AND ingestion_run_id = '{run_id}'
            """
        )
    except Exception as error:
        failure_message = str(error).replace("'", "''")[:2000]
        spark.sql(
            f"""
            UPDATE {MANIFEST_TABLE}
            SET run_status = 'failed',
                failure_stage = 'silver',
                failure_message = '{failure_message}',
                updated_at = current_timestamp()
            WHERE source_year = {year}
              AND source_month = {month}
              AND ingestion_run_id = '{run_id}'
            """
        )
        raise
    finally:
        if classified is not None:
            classified.unpersist()


if __name__ == "__main__":
    main()

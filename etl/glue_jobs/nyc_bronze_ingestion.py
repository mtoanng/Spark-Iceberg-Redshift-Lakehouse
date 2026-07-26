"""Month-scoped, retry-safe Glue Bronze ingestion for NYC HVFHV sources."""

import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from etl.contracts.nyc_hvfhs_identity import (
    identity_policy_version,
    spark_identity_expressions,
)
from etl.sources.nyc_hvfhs import (
    SourceFile,
    stable_run_id,
    validate_landed_source,
    validate_trip_schema,
)


REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_URI",
    "SOURCE_YEAR",
    "SOURCE_MONTH",
    "SOURCE_CHECKSUM",
    "INGESTION_RUN_ID",
    "TAXI_ZONE_URI",
    "TAXI_ZONE_CHECKSUM",
    "SOURCE_SIZE_BYTES",
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


def _verify_landed_object(
    uri: str, expected_checksum: str, *, expected_size: int | None = None
) -> None:
    """Bind run arguments to the immutable S3 object before Spark reads it."""

    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("Landed objects must use a complete s3://bucket/key URI.")
    response = boto3.client("s3").head_object(Bucket=parsed.netloc, Key=key)
    actual_checksum = response.get("Metadata", {}).get("sha256")
    if actual_checksum != expected_checksum:
        raise ValueError(f"Landed object SHA-256 metadata does not match {uri}.")
    if (
        expected_size is not None
        and int(response.get("ContentLength", -1)) != expected_size
    ):
        raise ValueError(f"Landed object byte size does not match {uri}.")


def _merge_manifest(
    spark,
    *,
    status: str,
    bronze_count: int = 0,
    bronze_snapshot_id: str | None = None,
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
                identity_policy_version(int(args["SOURCE_YEAR"])),
                status,
                bronze_count,
                bronze_snapshot_id,
                failure_stage,
                failure_message,
            )
        ],
        "source_uri string, source_checksum string, source_year int, source_month int, source_size_bytes long, ingestion_run_id string, identity_policy_version string, run_status string, bronze_row_count long, bronze_snapshot_id string, failure_stage string, failure_message string",
    )
    update.createOrReplaceTempView("manifest_update")
    spark.sql(
        f"""
        MERGE INTO {MANIFEST_TABLE} target USING manifest_update source
        ON target.source_year = source.source_year AND target.source_month = source.source_month
        WHEN MATCHED AND (
          target.source_uri <> source.source_uri OR
          target.source_checksum <> source.source_checksum OR
          target.source_size_bytes <> source.source_size_bytes
        ) THEN UPDATE SET
          failure_stage = 'source_manifest',
          failure_message = 'Changed source URI, checksum, or size was rejected.',
          updated_at = current_timestamp()
        WHEN MATCHED AND
          target.source_uri = source.source_uri AND
          target.source_checksum = source.source_checksum AND
          target.source_size_bytes = source.source_size_bytes
        THEN UPDATE SET
          source_uri = source.source_uri, source_size_bytes = source.source_size_bytes, ingestion_run_id = source.ingestion_run_id,
          identity_policy_version = source.identity_policy_version,
          run_status = CASE
            WHEN source.run_status = 'failed' AND target.run_status IN ('silver_published', 'reconciled', 'published')
              THEN target.run_status
            ELSE source.run_status
          END,
          bronze_row_count = CASE
            WHEN source.run_status = 'failed' AND target.run_status IN ('silver_published', 'reconciled', 'published')
              THEN target.bronze_row_count
            ELSE source.bronze_row_count
          END,
          bronze_snapshot_id = CASE WHEN source.run_status = 'bronze_published' THEN source.bronze_snapshot_id ELSE target.bronze_snapshot_id END,
          silver_row_count = CASE WHEN source.run_status = 'bronze_published' THEN 0 ELSE target.silver_row_count END,
          quarantine_row_count = CASE WHEN source.run_status = 'bronze_published' THEN 0 ELSE target.quarantine_row_count END,
          gold_row_count = CASE WHEN source.run_status = 'bronze_published' THEN 0 ELSE target.gold_row_count END,
          validation_status = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.validation_status END,
          validation_result_uri = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.validation_result_uri END,
          validation_result_summary = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.validation_result_summary END,
          completed_at = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.completed_at END,
          publication_status = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.publication_status END,
          publication_manifest_uri = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.publication_manifest_uri END,
          published_at = CASE WHEN source.run_status = 'bronze_published' THEN NULL ELSE target.published_at END,
          failure_stage = source.failure_stage,
          failure_message = source.failure_message,
          updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_uri, source_checksum, source_size_bytes, source_year, source_month, ingestion_run_id, identity_policy_version, run_status, first_seen_at, updated_at, bronze_row_count, bronze_snapshot_id, silver_row_count, quarantine_row_count, failure_stage, failure_message)
          VALUES (source.source_uri, source.source_checksum, source.source_size_bytes, source.source_year, source.source_month, source.ingestion_run_id, source.identity_policy_version, source.run_status, current_timestamp(), current_timestamp(), source.bronze_row_count, source.bronze_snapshot_id, 0, 0, source.failure_stage, source.failure_message)
    """
    )


def _may_process(spark) -> bool:
    existing = spark.sql(
        f"SELECT source_uri, source_checksum, source_size_bytes, run_status FROM {MANIFEST_TABLE} WHERE source_year = {int(args['SOURCE_YEAR'])} AND source_month = {int(args['SOURCE_MONTH'])} ORDER BY updated_at DESC LIMIT 1"
    ).collect()
    if not existing:
        return True
    row = existing[0]
    if (
        row.source_uri != args["SOURCE_URI"]
        or row.source_checksum != args["SOURCE_CHECKSUM"]
        or row.source_size_bytes != int(args["SOURCE_SIZE_BYTES"])
    ):
        spark.sql(
            f"UPDATE {MANIFEST_TABLE} SET failure_stage='source_manifest', failure_message='Changed source URI, checksum, or size was rejected.', updated_at=current_timestamp() WHERE source_year={int(args['SOURCE_YEAR'])} AND source_month={int(args['SOURCE_MONTH'])}"
        )
        raise ValueError(
            "Changed source URI, checksum, or size is blocked for an existing monthly partition."
        )
    return not (
        row.run_status in {"silver_published", "reconciled", "published"}
        and _optional_arg("FORCE", "false").lower() != "true"
    )


def main() -> None:
    source = SourceFile(
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["SOURCE_URI"],
        args["SOURCE_CHECKSUM"],
        int(args["SOURCE_SIZE_BYTES"]),
    )
    validate_landed_source(source)
    if stable_run_id(source) != args["INGESTION_RUN_ID"]:
        raise ValueError(
            "INGESTION_RUN_ID does not match the immutable source identity."
        )
    if not args["TAXI_ZONE_URI"].startswith("s3://"):
        raise ValueError("TAXI_ZONE_URI must reference the S3 reference prefix.")
    if len(args["TAXI_ZONE_CHECKSUM"]) != 64 or any(
        character not in "0123456789abcdefABCDEF"
        for character in args["TAXI_ZONE_CHECKSUM"]
    ):
        raise ValueError("TAXI_ZONE_CHECKSUM must be a SHA-256 hex digest.")
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    trips_cached = False
    try:
        _verify_landed_object(
            args["SOURCE_URI"],
            args["SOURCE_CHECKSUM"],
            expected_size=int(args["SOURCE_SIZE_BYTES"]),
        )
        _verify_landed_object(args["TAXI_ZONE_URI"], args["TAXI_ZONE_CHECKSUM"])
        if not _may_process(spark):
            job.commit()
            return
        source_frame = spark.read.parquet(args["SOURCE_URI"])
        validate_trip_schema(source_frame.columns, int(args["SOURCE_YEAR"]))
        exact_id, probable_key, policy = spark_identity_expressions(
            int(args["SOURCE_YEAR"])
        )
        trips = (
            source_frame.withColumn("_source_uri", lit(args["SOURCE_URI"]))
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
            .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
            .withColumn("_source_checksum", lit(args["SOURCE_CHECKSUM"]))
            .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("row_id", exact_id)
            .withColumn("business_trip_key", probable_key)
            .withColumn("identity_policy_version", policy)
        )
        trips.persist(StorageLevel.MEMORY_AND_DISK)
        trips_cached = True
        bronze_count = trips.count()
        trips.writeTo(TRIPS_TABLE).overwritePartitions()
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {TRIPS_TABLE}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        bronze_snapshot_id = str(snapshot.snapshot_id) if snapshot else None
        trips.unpersist()
        trips_cached = False
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
        _merge_manifest(
            spark,
            status="bronze_published",
            bronze_count=bronze_count,
            bronze_snapshot_id=bronze_snapshot_id,
        )
        job.commit()
    except Exception as error:
        if trips_cached:
            trips.unpersist()
        failure_stage = "source_manifest" if "Landed object" in str(error) else "bronze"
        _merge_manifest(
            spark,
            status="failed",
            failure_stage=failure_stage,
            failure_message=str(error)[:2000],
        )
        raise


if __name__ == "__main__":
    main()

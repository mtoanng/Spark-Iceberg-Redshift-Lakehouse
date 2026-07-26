"""Write a retry-safe snapshot-aware publication artifact, then mark published."""

from datetime import datetime, timezone
import json
import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl.publication.nyc_hvfhs import (
    REQUIRED_GOLD_TABLES,
    TablePublication,
    build_publication_document,
    canonical_json,
    publication_key,
)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_YEAR",
        "SOURCE_MONTH",
        "INGESTION_RUN_ID",
        "DBT_RESULT_URI",
    ],
)


def _optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def _table(namespace: str, name: str) -> str:
    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    database = _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
    return f"{catalog}.{database}.{name}"


def _snapshot_id(spark, table: str) -> str:
    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if not row:
        raise ValueError(f"No Iceberg snapshot metadata for {table}.")
    return str(row.snapshot_id)


def _location(spark, table: str) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    for row in rows:
        if str(row.col_name).strip().lower() == "location":
            return str(row.data_type).strip()
    raise ValueError(f"No Iceberg location metadata for {table}.")


def main() -> None:
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    spark = context.spark_session
    year, month = int(args["SOURCE_YEAR"]), int(args["SOURCE_MONTH"])
    run_id = args["INGESTION_RUN_ID"].replace("'", "''")
    manifest_table = _table("ops", "source_run_manifest")
    row = spark.sql(
        f"SELECT * FROM {manifest_table} WHERE source_year={year} AND source_month={month} "
        f"AND ingestion_run_id='{run_id}' ORDER BY updated_at DESC LIMIT 1"
    ).first()
    if not row or row.run_status != "reconciled":
        raise ValueError("Publication requires a reconciled operational manifest.")
    gold = []
    for name in REQUIRED_GOLD_TABLES:
        table = _table("gold", name)
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Gold publication is missing table: {name}")
        gold.append(
            TablePublication(
                name=name,
                location=_location(spark, table),
                row_count=spark.table(table).count(),
                snapshot_id=_snapshot_id(spark, table),
            )
        )
    dbt_uri = urlparse(args["DBT_RESULT_URI"])
    if dbt_uri.scheme != "s3" or not dbt_uri.netloc:
        raise ValueError("DBT_RESULT_URI must be a durable s3:// object.")
    dbt_payload = json.loads(
        boto3.client("s3")
        .get_object(Bucket=dbt_uri.netloc, Key=dbt_uri.path.lstrip("/"))["Body"]
        .read()
    )
    dbt_results = dbt_payload.get("results", [])
    if not dbt_results or any(
        result.get("status") not in {"success", "pass"} for result in dbt_results
    ):
        raise ValueError("Publication requires successful retained dbt run_results.")
    published_at = datetime.now(timezone.utc).isoformat()
    document = build_publication_document(
        source={
            "source_uri": row.source_uri,
            "source_checksum": row.source_checksum,
            "source_size_bytes": row.source_size_bytes,
            "source_year": year,
            "source_month": month,
        },
        ingestion_run_id=args["INGESTION_RUN_ID"],
        identity_policy_version=row.identity_policy_version,
        published_at=published_at,
        bronze={
            "row_count": row.bronze_row_count,
            "snapshot_id": row.bronze_snapshot_id,
        },
        silver={
            "row_count": row.silver_row_count,
            "snapshot_id": row.silver_snapshot_id,
        },
        quarantine={
            "row_count": row.quarantine_row_count,
            "snapshot_id": row.quarantine_snapshot_id,
        },
        gold_tables=gold,
        dbt_summary={
            "status": "succeeded",
            "run_results_uri": args["DBT_RESULT_URI"],
            "invocation_id": dbt_payload.get("metadata", {}).get("invocation_id"),
            "results": [
                {
                    "unique_id": result.get("unique_id"),
                    "status": result.get("status"),
                    "execution_time": result.get("execution_time"),
                }
                for result in dbt_results
            ],
        },
    )
    prefix = _optional_arg("PUBLICATION_PREFIX_URI", "")
    parsed = urlparse(prefix)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("PUBLICATION_PREFIX_URI must be an s3:// URI.")
    key = "/".join(
        part.strip("/")
        for part in (
            parsed.path,
            publication_key(year, month, args["INGESTION_RUN_ID"]),
        )
        if part.strip("/")
    )
    body = canonical_json(document)
    boto3.client("s3").put_object(
        Bucket=parsed.netloc,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"sha256": __import__("hashlib").sha256(body).hexdigest()},
    )
    uri = f"s3://{parsed.netloc}/{key}"
    spark.sql(
        f"UPDATE {manifest_table} SET run_status='published', publication_status='published', "
        f"publication_manifest_uri='{uri}', published_at=current_timestamp(), "
        f"updated_at=current_timestamp() WHERE source_year={year} AND source_month={month} "
        f"AND ingestion_run_id='{run_id}' AND run_status='reconciled'"
    )
    job.commit()


if __name__ == "__main__":
    main()

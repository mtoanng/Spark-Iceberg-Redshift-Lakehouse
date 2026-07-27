"""Airflow-side durable publication for the Redshift Gold architecture."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Mapping
from urllib.parse import urlparse

import boto3

from athena.query_runner import AthenaQueryRunner
from etl.publication.nyc_hvfhs import (
    build_publication_document,
    canonical_json,
    logical_document,
    publication_key,
)


def _bucket_key(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Expected a non-empty s3:// URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _latest_snapshot_id(
    runner: AthenaQueryRunner, *, database: str, table: str, workgroup: str
) -> str | None:
    result = runner.run(
        f'SELECT snapshot_id FROM "{table}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1",
        database=database,
        workgroup=workgroup,
        client_request_token=f"nyc-publication-snapshot-{database}-{table}",
    )
    return str(result.rows[0][0]) if result.rows else None


def _dbt_artifact(s3_client: Any, uri: str) -> tuple[dict[str, Any], str]:
    bucket, key = _bucket_key(uri)
    head = s3_client.head_object(Bucket=bucket, Key=key)
    checksum = str(head.get("Metadata", {}).get("sha256", ""))
    if len(checksum) != 64:
        raise ValueError("dbt run_results.json is missing SHA-256 metadata.")
    payload = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if hashlib.sha256(payload).hexdigest() != checksum:
        raise ValueError("dbt run_results.json SHA-256 does not match its metadata.")
    document = json.loads(payload)
    results = document.get("results", [])
    if not results or any(
        item.get("status") not in {"success", "pass"} for item in results
    ):
        raise ValueError(
            "Publication requires a successful archived dbt run_results.json."
        )
    return document, checksum


def _existing_document(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return json.loads(s3_client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as error:  # boto3 exposes provider-specific not-found errors.
        code = getattr(error, "response", {}).get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"} or isinstance(error, KeyError):
            return None
        raise


def publish_month(
    *,
    audit: Mapping[str, object],
    reconciliation: Mapping[str, object],
    dbt_result_uri: str,
    publication_prefix_uri: str,
    athena_workgroup: str,
    redshift_database: str,
    redshift_schema: str = "gold",
    athena_runner: AthenaQueryRunner | None = None,
    s3_client: Any | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Upload or safely reuse one deterministic publication object per run."""

    year, month = int(audit["source_year"]), int(audit["source_month"])
    run_id = str(audit["run_id"])
    s3 = s3_client or boto3.client("s3")
    _, dbt_checksum = _dbt_artifact(s3, dbt_result_uri)
    runner = athena_runner or AthenaQueryRunner()
    iceberg_layers = {
        "bronze": {
            "table_identifier": "bronze.bronze_hvfhs_trips",
            "snapshot_id": _latest_snapshot_id(
                runner,
                database="bronze",
                table="bronze_hvfhs_trips",
                workgroup=athena_workgroup,
            ),
            "row_count": reconciliation["bronze_row_count"],
        },
        "silver": {
            "table_identifier": "silver.silver_trips",
            "snapshot_id": _latest_snapshot_id(
                runner,
                database="silver",
                table="silver_trips",
                workgroup=athena_workgroup,
            ),
            "row_count": reconciliation["silver_row_count"],
        },
        "quarantine": {
            "table_identifier": "silver.quarantine_trips",
            "snapshot_id": _latest_snapshot_id(
                runner,
                database="silver",
                table="quarantine_trips",
                workgroup=athena_workgroup,
            ),
            "row_count": reconciliation["quarantine_row_count"],
        },
    }
    document = build_publication_document(
        source={
            key: audit[key]
            for key in (
                "source_uri",
                "source_checksum",
                "source_size_bytes",
                "source_year",
                "source_month",
            )
        },
        ingestion_run_id=run_id,
        identity_policy_version=str(audit["identity_policy_version"]),
        iceberg_layers=iceberg_layers,
        redshift_database=redshift_database,
        redshift_schema=redshift_schema,
        reconciliation=reconciliation,
        dbt_artifact_uri=dbt_result_uri,
        dbt_artifact_sha256=dbt_checksum,
        published_at=(now or datetime.now(timezone.utc)).isoformat(),
    )
    bucket, prefix = _bucket_key(publication_prefix_uri)
    key = "/".join((prefix.rstrip("/"), publication_key(year, month, run_id)))
    existing = _existing_document(s3, bucket, key)
    if existing is not None:
        if logical_document(existing) != logical_document(document):
            raise ValueError(
                "Publication key already holds conflicting immutable content."
            )
        body = canonical_json(existing)
    else:
        body = canonical_json(document)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata={"sha256": hashlib.sha256(body).hexdigest()},
        )
    return {
        "publication_uri": f"s3://{bucket}/{key}",
        "sha256": hashlib.sha256(body).hexdigest(),
        "status": "published",
    }

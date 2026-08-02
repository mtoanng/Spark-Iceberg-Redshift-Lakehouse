"""Durable dbt artifacts for the Cosmos-owned Gold task group."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import boto3


SUCCESS_STATUSES = {"success", "pass"}


def dbt_result_uri(
    publication_prefix_uri: str,
    source_year: int | str,
    source_month: int | str,
    run_id: str,
) -> str:
    return (
        f"{publication_prefix_uri.rstrip('/')}/dbt-results/year={int(source_year):04d}"
        f"/month={int(source_month):02d}/{run_id}.json"
    )


def _bucket_and_key(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Expected a non-empty S3 URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _validated_run_results(payload: bytes) -> None:
    document = json.loads(payload)
    results = document.get("results")
    invocation_id = document.get("metadata", {}).get("invocation_id")
    if not isinstance(results, list) or not results:
        raise ValueError("dbt run_results.json must contain at least one result")
    if not invocation_id:
        raise ValueError("dbt run_results.json must contain metadata.invocation_id")
    statuses = [
        result.get("status") if isinstance(result, dict) else None for result in results
    ]
    if any(status not in SUCCESS_STATUSES for status in statuses):
        raise ValueError(
            "dbt run_results.json contains non-success statuses: "
            f"{sorted(str(status) for status in statuses)}"
        )


def archive_dbt_run_results(
    project_dir: Path,
    *,
    publication_prefix_uri: str,
    source_year: int | str,
    source_month: int | str,
    run_id: str,
    s3_client: Any | None = None,
) -> str:
    artifact = Path(project_dir) / "target" / "run_results.json"
    if not artifact.is_file():
        raise FileNotFoundError(f"Cosmos dbt artifact does not exist: {artifact}")
    payload = artifact.read_bytes()
    _validated_run_results(payload)
    uri = dbt_result_uri(publication_prefix_uri, source_year, source_month, run_id)
    bucket, key = _bucket_and_key(uri)
    s3 = s3_client or boto3.client("s3")
    try:
        existing = s3.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code", "")
        if code not in {"NoSuchKey", "404", "NotFound"} and not isinstance(
            error, KeyError
        ):
            raise
    else:
        existing_payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        existing_checksum = str(existing.get("Metadata", {}).get("sha256", ""))
        if (
            len(existing_checksum) != 64
            or hashlib.sha256(existing_payload).hexdigest() != existing_checksum
        ):
            raise ValueError(f"Existing dbt artifact is corrupt: {uri}")
        _validated_run_results(existing_payload)
        return uri
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
    )
    return uri


def archive_cosmos_dbt_run_results(
    project_dir: Path,
    context: Mapping[str, Any],
    **_: Any,
) -> None:
    """Archive the producer's full run_results before Cosmos removes its copy."""

    from airflow.sdk import Variable

    audit = context["ti"].xcom_pull(task_ids="prepare_month")
    archive_dbt_run_results(
        project_dir,
        publication_prefix_uri=Variable.get("nyc_publication_prefix_uri"),
        source_year=audit["source_year"],
        source_month=audit["source_month"],
        run_id=audit["run_id"],
    )


def require_dbt_result_artifact(
    publication_prefix_uri: str,
    source_year: int | str,
    source_month: int | str,
    run_id: str,
    s3_client: Any | None = None,
) -> str:
    uri = dbt_result_uri(publication_prefix_uri, source_year, source_month, run_id)
    bucket, key = _bucket_and_key(uri)
    head = (s3_client or boto3.client("s3")).head_object(Bucket=bucket, Key=key)
    if int(head.get("ContentLength", 0)) <= 0:
        raise ValueError(f"dbt artifact is empty: {uri}")
    checksum = head.get("Metadata", {}).get("sha256", "")
    if len(checksum) != 64:
        raise ValueError(f"dbt artifact is missing its SHA-256 metadata: {uri}")
    return uri

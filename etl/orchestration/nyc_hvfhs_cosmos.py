"""Durable dbt artifacts for the Cosmos-owned Gold task group.

Cosmos runs dbt from a temporary project copy.  The producer callback copies
the complete ``run_results.json`` to the publication prefix before that copy is
removed.  A separate downstream Airflow task verifies the immutable location
that publication consumes.
"""

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
    """Return the deterministic object URI used by publication."""

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
    statuses = {result.get("status") for result in results if isinstance(result, dict)}
    if len(statuses) != len(results) or not statuses.issubset(SUCCESS_STATUSES):
        raise ValueError(
            f"dbt run_results.json contains non-success statuses: {sorted(statuses)}"
        )


def archive_dbt_run_results_for_run(
    project_dir: Path,
    *,
    publication_prefix_uri: str,
    source_year: int | str,
    source_month: int | str,
    run_id: str,
    s3_client: Any | None = None,
) -> str:
    """Validate and upload the complete dbt artifact for one immutable run."""

    artifact = Path(project_dir) / "target" / "run_results.json"
    if not artifact.is_file():
        raise FileNotFoundError(f"Cosmos dbt artifact does not exist: {artifact}")
    payload = artifact.read_bytes()
    _validated_run_results(payload)
    uri = dbt_result_uri(publication_prefix_uri, source_year, source_month, run_id)
    bucket, key = _bucket_and_key(uri)
    (s3_client or boto3.client("s3")).put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
        Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
    )
    return uri


def archive_dbt_run_results(project_dir: Path, *, context: Mapping[str, Any]) -> None:
    """Cosmos producer callback that archives its full ``dbt build`` result."""

    from airflow.sdk import Variable

    audit = context["ti"].xcom_pull(task_ids="prepare_month")
    archive_dbt_run_results_for_run(
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
    """Require the callback artifact before reconciliation and publication."""

    uri = dbt_result_uri(publication_prefix_uri, source_year, source_month, run_id)
    bucket, key = _bucket_and_key(uri)
    head = (s3_client or boto3.client("s3")).head_object(Bucket=bucket, Key=key)
    if int(head.get("ContentLength", 0)) <= 0:
        raise ValueError(f"dbt artifact is empty: {uri}")
    checksum = head.get("Metadata", {}).get("sha256", "")
    if len(checksum) != 64:
        raise ValueError(f"dbt artifact is missing its SHA-256 metadata: {uri}")
    return uri

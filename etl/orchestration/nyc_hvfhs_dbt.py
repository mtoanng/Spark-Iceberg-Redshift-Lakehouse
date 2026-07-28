"""Run one complete dbt build and retain its result artifact.

The Airflow task treats dbt as one atomic Gold producer. dbt owns its model
graph and tests; Airflow owns stage ordering, retry, and durable evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any
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
    artifact = project_dir / "target" / "run_results.json"
    if not artifact.is_file():
        raise FileNotFoundError(f"dbt artifact does not exist: {artifact}")
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


def run_dbt_build_from_environment() -> None:
    """Entry point invoked by Airflow's BashOperator on one worker."""

    required = (
        "DBT_PROJECT_PATH",
        "DBT_PUBLICATION_PREFIX_URI",
        "DBT_SOURCE_YEAR",
        "DBT_SOURCE_MONTH",
        "DBT_RUN_ID",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"Missing dbt runtime variables: {', '.join(missing)}")

    source_project = Path(os.environ["DBT_PROJECT_PATH"]).resolve()
    if not (source_project / "dbt_project.yml").is_file():
        raise FileNotFoundError(f"Invalid dbt project path: {source_project}")

    with tempfile.TemporaryDirectory(prefix="nyc-hvfhs-dbt-") as temporary:
        project = Path(temporary) / "project"
        shutil.copytree(source_project, project)
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(project),
                "--target",
                "redshift",
                "--no-partial-parse",
                "--vars",
                json.dumps(
                    {
                        "source_year": int(os.environ["DBT_SOURCE_YEAR"]),
                        "source_month": int(os.environ["DBT_SOURCE_MONTH"]),
                    },
                    separators=(",", ":"),
                ),
            ],
            check=True,
        )
        archive_dbt_run_results(
            project,
            publication_prefix_uri=os.environ["DBT_PUBLICATION_PREFIX_URI"],
            source_year=os.environ["DBT_SOURCE_YEAR"],
            source_month=os.environ["DBT_SOURCE_MONTH"],
            run_id=os.environ["DBT_RUN_ID"],
        )


if __name__ == "__main__":
    run_dbt_build_from_environment()

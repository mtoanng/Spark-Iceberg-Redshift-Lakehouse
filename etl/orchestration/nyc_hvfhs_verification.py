"""Minimal read-after-publish verification for open Silver and Gold."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import time
from typing import Any, Mapping
from urllib.parse import urlparse

import boto3

from athena.query_runner import AthenaQueryRunner


class VerificationError(RuntimeError):
    """Raised when published evidence or consumer reads disagree."""


@dataclass(frozen=True)
class VerificationResult:
    source_year: int
    source_month: int
    ingestion_run_id: str
    publication_uri: str
    athena_query_execution_id: str
    redshift_statement_id: str
    silver_row_count: int
    gold_row_count: int


def _publication_document(
    publication: Mapping[str, object], s3_client: Any
) -> tuple[str, dict[str, object]]:
    uri = str(publication.get("publication_uri", ""))
    checksum = str(publication.get("sha256", ""))
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key or len(checksum) != 64:
        raise VerificationError("Publication reference is incomplete.")
    payload = s3_client.get_object(Bucket=parsed.netloc, Key=key)["Body"].read()
    if hashlib.sha256(payload).hexdigest() != checksum:
        raise VerificationError("Publication object SHA-256 does not match.")
    document = json.loads(payload)
    if document.get("status") != "published":
        raise VerificationError("Publication object is not marked published.")
    return uri, document


def _silver_count(
    runner: AthenaQueryRunner,
    *,
    workgroup: str,
    year: int,
    month: int,
) -> tuple[int, str]:
    result = runner.run(
        'SELECT count(*) FROM "silver_trips" '
        "WHERE source_year = ? AND source_month = ?",
        database="silver",
        workgroup=workgroup,
        execution_parameters=(str(year), str(month)),
        client_request_token=f"nyc-verify-silver-{year}-{month:02d}",
    )
    if len(result.rows) != 1 or len(result.rows[0]) != 1:
        raise VerificationError("Athena Silver verification returned no scalar.")
    return int(result.rows[0][0] or 0), result.query_execution_id


def _gold_count(
    client: Any,
    *,
    database: str,
    workgroup_name: str,
    schema: str,
    year: int,
    month: int,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 1.0,
    sleep=time.sleep,
) -> tuple[int, str]:
    statement_id = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=(
            f'SELECT count(*) FROM "{schema}"."fct_trips" '
            f"WHERE source_year = {year} AND source_month = {month}"
        ),
        StatementName=f"nyc-verify-gold-{year}-{month:02d}",
    )["Id"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        details = client.describe_statement(Id=statement_id)
        if details["Status"] == "FINISHED":
            records = client.get_statement_result(Id=statement_id).get("Records", [])
            if len(records) != 1 or len(records[0]) != 1:
                raise VerificationError(
                    "Redshift Gold verification returned no scalar."
                )
            cell = records[0][0]
            for key in ("longValue", "doubleValue", "stringValue"):
                if key in cell:
                    return int(cell[key]), statement_id
            raise VerificationError(
                "Redshift Gold verification returned an empty cell."
            )
        if details["Status"] in {"FAILED", "ABORTED"}:
            raise VerificationError(
                f"Redshift verification {statement_id} {details['Status']}: "
                f"{details.get('Error', 'no error supplied')}"
            )
        if time.monotonic() >= deadline:
            raise VerificationError(f"Redshift verification {statement_id} timed out.")
        sleep(poll_interval_seconds)


def verify_month(
    *,
    source_year: int,
    source_month: int,
    ingestion_run_id: str,
    publication: Mapping[str, object],
    athena_workgroup: str,
    redshift_database: str,
    redshift_workgroup_name: str,
    redshift_schema: str = "gold",
    athena_runner: AthenaQueryRunner | None = None,
    redshift_client: Any | None = None,
    s3_client: Any | None = None,
) -> dict[str, object]:
    year, month = int(source_year), int(source_month)
    uri, document = _publication_document(publication, s3_client or boto3.client("s3"))
    source = document.get("source", {})
    counts = document.get("row_counts", {})
    if (
        not isinstance(source, dict)
        or not isinstance(counts, dict)
        or int(source.get("source_year", -1)) != year
        or int(source.get("source_month", -1)) != month
        or str(document.get("ingestion_run_id", "")) != ingestion_run_id
    ):
        raise VerificationError("Publication identity differs from the requested run.")

    silver, athena_id = _silver_count(
        athena_runner or AthenaQueryRunner(),
        workgroup=athena_workgroup,
        year=year,
        month=month,
    )
    gold, redshift_id = _gold_count(
        redshift_client or boto3.client("redshift-data"),
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        schema=redshift_schema,
        year=year,
        month=month,
    )
    if silver != int(counts.get("silver", -1)):
        raise VerificationError(
            "Published Silver count is not readable through Athena."
        )
    if gold != int(counts.get("gold_fct_trips", -1)) or gold != silver:
        raise VerificationError(
            "Published Gold count is not readable through Redshift."
        )
    return asdict(
        VerificationResult(
            source_year=year,
            source_month=month,
            ingestion_run_id=ingestion_run_id,
            publication_uri=uri,
            athena_query_execution_id=athena_id,
            redshift_statement_id=redshift_id,
            silver_row_count=silver,
            gold_row_count=gold,
        )
    )

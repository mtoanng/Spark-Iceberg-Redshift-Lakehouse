"""Bounded read-after-publish verification through Redshift."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Mapping
from urllib.parse import urlparse

import boto3

from etl.orchestration.redshift_data import run_query


class VerificationError(RuntimeError):
    """Raised when published evidence or consumer reads disagree."""


@dataclass(frozen=True)
class VerificationResult:
    source_year: int
    source_month: int
    ingestion_run_id: str
    publication_uri: str
    redshift_statement_id: str
    silver_row_count: int
    gold_row_count: int


VERIFICATION_SQL = """
SELECT
    (
        SELECT count(*)
        FROM silver_external.silver_trips
        WHERE source_year = :source_year
          AND source_month = :source_month
    ) AS silver_row_count,
    (
        SELECT count(*)
        FROM gold.fct_trips
        WHERE source_year = :source_year
          AND source_month = :source_month
    ) AS gold_row_count
""".strip()


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


def verify_month(
    *,
    source_year: int,
    source_month: int,
    ingestion_run_id: str,
    publication: Mapping[str, object],
    redshift_database: str,
    redshift_workgroup_name: str,
    redshift_client: Any | None = None,
    s3_client: Any | None = None,
) -> dict[str, object]:
    """Verify publication integrity plus one Spectrum and one Gold count."""

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
    result = run_query(
        redshift_client or boto3.client("redshift-data"),
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        sql=VERIFICATION_SQL,
        statement_name=f"nyc-verify-{year}-{month:02d}",
        parameters={"source_year": year, "source_month": month},
    )
    if len(result.rows) != 1 or len(result.rows[0]) != 2:
        raise VerificationError("Redshift verification returned no count pair.")
    silver, gold = (int(value or 0) for value in result.rows[0])
    if silver != int(counts.get("silver", -1)):
        raise VerificationError(
            "Published Silver count is not readable through Redshift Spectrum."
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
            redshift_statement_id=result.statement_id,
            silver_row_count=silver,
            gold_row_count=gold,
        )
    )

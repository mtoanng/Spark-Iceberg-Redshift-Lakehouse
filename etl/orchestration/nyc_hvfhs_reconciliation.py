"""Monthly reconciliation through the single Redshift query plane."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import boto3

from etl.orchestration.redshift_data import run_query


class ReconciliationError(RuntimeError):
    """Raised when monthly open-layer and Gold evidence does not reconcile."""


@dataclass(frozen=True)
class ReconciliationResult:
    source_year: int
    source_month: int
    ingestion_run_id: str
    bronze_row_count: int
    silver_row_count: int
    quarantine_row_count: int
    gold_row_count: int
    bronze_equals_classified: bool
    silver_equals_gold: bool
    iceberg_snapshot_ids: dict[str, str]
    redshift_statement_id: str
    reconciled_at: str


RECONCILIATION_SQL = """
WITH consumer_counts AS (
    SELECT
        (
            SELECT count(*)
            FROM bronze_external.bronze_hvfhs_trips
            WHERE _source_year = :source_year
              AND _source_month = :source_month
        ) AS bronze_row_count,
        (
            SELECT count(*)
            FROM silver_external.silver_trips
            WHERE source_year = :source_year
              AND source_month = :source_month
        ) AS silver_row_count,
        (
            SELECT count(*)
            FROM silver_external.quarantine_trips
            WHERE _source_year = :source_year
              AND _source_month = :source_month
        ) AS quarantine_row_count,
        (
            SELECT count(*)
            FROM gold.fct_trips
            WHERE source_year = :source_year
              AND source_month = :source_month
        ) AS gold_row_count
)
SELECT
    counts.bronze_row_count,
    counts.silver_row_count,
    counts.quarantine_row_count,
    counts.gold_row_count,
    manifest.run_status,
    manifest.bronze_row_count,
    manifest.silver_row_count,
    manifest.quarantine_row_count,
    manifest.bronze_snapshot_id,
    manifest.silver_snapshot_id,
    manifest.quarantine_snapshot_id
FROM consumer_counts AS counts
CROSS JOIN ops_external.source_run_manifest AS manifest
WHERE manifest.source_year = :source_year
  AND manifest.source_month = :source_month
  AND manifest.ingestion_run_id = :ingestion_run_id
""".strip()


def reconcile_month(
    *,
    source_year: int,
    source_month: int,
    ingestion_run_id: str,
    redshift_database: str,
    redshift_workgroup_name: str,
    redshift_client: Any | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Run one Redshift statement and enforce the two release invariants."""

    year, month = int(source_year), int(source_month)
    if not 2019 <= year <= 2099 or not 1 <= month <= 12 or not ingestion_run_id:
        raise ValueError("source year/month and immutable run ID are required.")
    result = run_query(
        redshift_client or boto3.client("redshift-data"),
        database=redshift_database,
        workgroup_name=redshift_workgroup_name,
        sql=RECONCILIATION_SQL,
        statement_name=f"nyc-reconcile-{year}-{month:02d}",
        parameters={
            "source_year": year,
            "source_month": month,
            "ingestion_run_id": ingestion_run_id,
        },
    )
    if len(result.rows) != 1 or len(result.rows[0]) != 11:
        raise ReconciliationError(
            "Redshift reconciliation returned no unique operational manifest row."
        )
    row = result.rows[0]
    bronze, silver, quarantine, gold = (int(value or 0) for value in row[:4])
    if row[4] != "silver_published":
        raise ReconciliationError("Operational manifest is not Silver-published.")
    manifest_counts = tuple(int(value or 0) for value in row[5:8])
    if manifest_counts != (bronze, silver, quarantine):
        raise ReconciliationError(
            "Operational manifest counts differ from Spectrum-visible counts."
        )
    names = ("bronze", "silver", "quarantine")
    snapshots = {
        name: str(value)
        for name, value in zip(names, row[8:], strict=True)
        if value is not None and str(value)
    }
    if set(snapshots) != set(names):
        raise ReconciliationError(
            "Operational manifest snapshot evidence is incomplete."
        )
    outcome = ReconciliationResult(
        source_year=year,
        source_month=month,
        ingestion_run_id=ingestion_run_id,
        bronze_row_count=bronze,
        silver_row_count=silver,
        quarantine_row_count=quarantine,
        gold_row_count=gold,
        bronze_equals_classified=bronze == silver + quarantine,
        silver_equals_gold=silver == gold,
        iceberg_snapshot_ids=snapshots,
        redshift_statement_id=result.statement_id,
        reconciled_at=(now or datetime.now(timezone.utc)).isoformat(),
    )
    if not outcome.bronze_equals_classified or not outcome.silver_equals_gold:
        raise ReconciliationError(f"Monthly reconciliation failed: {asdict(outcome)}")
    return asdict(outcome)
